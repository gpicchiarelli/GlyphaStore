#include "glyphastore/persistence/bootstrap.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class TemporaryDirectory final {
  public:
    TemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-fs-test-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    auto operator=(const TemporaryDirectory&) -> TemporaryDirectory& = delete;

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto test_manifest(std::uint64_t manifest_generation = 1) -> glyphastore::Manifest {
    return {
        .store_id = {std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14},
                     std::byte{0x15}, std::byte{0x16}, std::byte{0x17}, std::byte{0x18}, std::byte{0x19},
                     std::byte{0x1A}, std::byte{0x1B}, std::byte{0x1C}, std::byte{0x1D}, std::byte{0x1E},
                     std::byte{0x1F}},
        .manifest_generation = manifest_generation,
        .worker_count = 1,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{2},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = {{.segment_id = glyphastore::SegmentId{1},
                      .generation = glyphastore::GenerationId{1},
                      .owner_worker = glyphastore::WorkerId{0},
                      .role = glyphastore::ManifestSegmentRole::active}},
    };
}

auto test_compaction_intent() -> glyphastore::DurableCompactionIntent {
    auto old = test_manifest(7);
    old.next_segment_id = glyphastore::SegmentId{4};
    old.segments = {
        {.segment_id = glyphastore::SegmentId{1},
         .generation = glyphastore::GenerationId{1},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        {.segment_id = glyphastore::SegmentId{2},
         .generation = glyphastore::GenerationId{1},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        {.segment_id = glyphastore::SegmentId{3},
         .generation = glyphastore::GenerationId{1},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::active},
    };
    auto next = old;
    ++next.manifest_generation;
    next.segments = {
        {.segment_id = glyphastore::SegmentId{1},
         .generation = glyphastore::GenerationId{2},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        old.segments.back(),
    };
    return {.worker_id = glyphastore::WorkerId{0},
            .old_manifest = std::move(old),
            .next_manifest = std::move(next)};
}

struct InjectedFailure {
    glyphastore::FilesystemOperation operation{glyphastore::FilesystemOperation::write_manifest};
    bool enabled{};
};

struct AvailableSpaceProbe {
    std::uint64_t bytes{};

    static auto read(void* context) -> glyphastore::Result<std::uint64_t> {
        return static_cast<AvailableSpaceProbe*>(context)->bytes;
    }
};

struct FragmentedPositionalIo {
    std::size_t maximum_chunk{2};
    std::size_t read_calls{};
    std::size_t write_calls{};
    std::size_t sync_calls{};
    bool interrupt_next_read{true};
    bool interrupt_next_write{true};
    bool interrupt_next_sync{true};

    static auto read(void* context, const int descriptor, const std::span<std::byte> bytes,
                     const std::uint64_t offset) -> std::ptrdiff_t {
        auto& io = *static_cast<FragmentedPositionalIo*>(context);
        ++io.read_calls;
        if (io.interrupt_next_read) {
            io.interrupt_next_read = false;
            errno = EINTR;
            return -1;
        }
        const auto count = std::min(bytes.size(), io.maximum_chunk);
        return static_cast<std::ptrdiff_t>(
            ::pread(descriptor, bytes.data(), count, static_cast<off_t>(offset)));
    }

    static auto write(void* context, const int descriptor, const std::span<const std::byte> bytes,
                      const std::uint64_t offset) -> std::ptrdiff_t {
        auto& io = *static_cast<FragmentedPositionalIo*>(context);
        ++io.write_calls;
        if (io.interrupt_next_write) {
            io.interrupt_next_write = false;
            errno = EINTR;
            return -1;
        }
        const auto count = std::min(bytes.size(), io.maximum_chunk);
        return static_cast<std::ptrdiff_t>(
            ::pwrite(descriptor, bytes.data(), count, static_cast<off_t>(offset)));
    }

    static auto sync(void* context, const int descriptor, glyphastore::FileSyncMode) -> int {
        auto& io = *static_cast<FragmentedPositionalIo*>(context);
        ++io.sync_calls;
        if (io.interrupt_next_sync) {
            io.interrupt_next_sync = false;
            errno = EINTR;
            return -1;
        }
        return ::fsync(descriptor);
    }
};

struct FailingPositionalIo {
    int error_number{EIO};

    static auto read(void* context, int, std::span<std::byte>, std::uint64_t) -> std::ptrdiff_t {
        errno = static_cast<FailingPositionalIo*>(context)->error_number;
        return -1;
    }

    static auto write(void* context, int, std::span<const std::byte>, std::uint64_t) -> std::ptrdiff_t {
        errno = static_cast<FailingPositionalIo*>(context)->error_number;
        return -1;
    }

    static auto sync(void* context, int, glyphastore::FileSyncMode) -> int {
        errno = static_cast<FailingPositionalIo*>(context)->error_number;
        return -1;
    }
};

auto fail_before(void* context, glyphastore::FilesystemOperation operation) -> glyphastore::Status {
    auto& failure = *static_cast<InjectedFailure*>(context);
    if (failure.enabled && operation == failure.operation) {
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected filesystem failure");
    }
    return {};
}

} // namespace

GLYPHA_TEST("positional file IO completes exact extents and rejects offset overflow") {
    TemporaryDirectory temporary;
    const auto path = temporary.path() / "positional.bin";
    glyphastore::FileDescriptor file{::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600)};
    GLYPHA_REQUIRE(file.valid());

    static constexpr std::array payload{std::byte{0xA1}, std::byte{0xB2}, std::byte{0xC3}};
    GLYPHA_REQUIRE(file.write_all_at(payload, 3).has_value());
    GLYPHA_REQUIRE(file.sync(glyphastore::FileSyncMode::ordered).has_value());
    GLYPHA_REQUIRE(file.sync(glyphastore::FileSyncMode::data).has_value());
    const auto size = file.size();
    GLYPHA_REQUIRE(size.has_value());
    GLYPHA_REQUIRE(*size == 6);

    std::array<std::byte, payload.size()> decoded{};
    GLYPHA_REQUIRE(file.read_exact_at(decoded, 3).has_value());
    GLYPHA_REQUIRE(decoded == payload);
    GLYPHA_REQUIRE(!file.read_exact_at(decoded, 6).has_value());
    GLYPHA_REQUIRE(!file.write_all_at(payload, std::numeric_limits<std::uint64_t>::max()).has_value());
}

GLYPHA_TEST("positional file IO retries EINTR and completes deterministic short transfers") {
    TemporaryDirectory temporary;
    const auto path = temporary.path() / "fragmented.bin";
    FragmentedPositionalIo io{};
    glyphastore::FileDescriptor original{::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600),
                                         {.context = &io,
                                          .read_some_at = &FragmentedPositionalIo::read,
                                          .write_some_at = &FragmentedPositionalIo::write,
                                          .sync_file = &FragmentedPositionalIo::sync}};
    glyphastore::FileDescriptor file{std::move(original)};
    GLYPHA_REQUIRE(file.valid());
    GLYPHA_REQUIRE(!original.valid());

    static constexpr std::array payload{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40},
                                        std::byte{0x50}, std::byte{0x60}, std::byte{0x70}};
    GLYPHA_REQUIRE(file.write_all_at(payload, 5).has_value());
    GLYPHA_REQUIRE(file.sync(glyphastore::FileSyncMode::full).has_value());
    std::array<std::byte, payload.size()> decoded{};
    GLYPHA_REQUIRE(file.read_exact_at(decoded, 5).has_value());
    GLYPHA_REQUIRE(decoded == payload);
    GLYPHA_REQUIRE(io.write_calls == 5);
    GLYPHA_REQUIRE(io.read_calls == 5);
    GLYPHA_REQUIRE(io.sync_calls == 2);
}

GLYPHA_TEST("positional write faults preserve native resource categories") {
    TemporaryDirectory temporary;
    const auto path = temporary.path() / "faulted.bin";
    FailingPositionalIo io{};
    glyphastore::FileDescriptor file{::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600),
                                     {.context = &io,
                                      .read_some_at = &FailingPositionalIo::read,
                                      .write_some_at = &FailingPositionalIo::write,
                                      .sync_file = &FailingPositionalIo::sync}};
    GLYPHA_REQUIRE(file.valid());
    static constexpr std::array payload{std::byte{0x01}};

    io.error_number = ENOSPC;
    auto result = file.write_all_at(payload, 0);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);
#if defined(EDQUOT)
    io.error_number = EDQUOT;
    result = file.write_all_at(payload, 0);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);
#endif
    io.error_number = EROFS;
    result = file.write_all_at(payload, 0);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::read_only_filesystem);
    io.error_number = EIO;
    result = file.write_all_at(payload, 0);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::io_error);
    std::array<std::byte, 1> decoded{};
    result = file.read_exact_at(decoded, 0);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::io_error);
    result = file.sync(glyphastore::FileSyncMode::data);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::io_error);
}

GLYPHA_TEST("data directory holds one process lock and rejects a symlink root") {
    TemporaryDirectory temporary;
    {
        const auto first = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(first.has_value());
        const auto second = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(!second.has_value());
        GLYPHA_REQUIRE(second.error().code == glyphastore::ErrorCode::io_error);
    }
    GLYPHA_REQUIRE(glyphastore::DataDirectory::open_and_lock(temporary.path()).has_value());

    TemporaryDirectory parent;
    const auto target = parent.path() / "target";
    const auto link = parent.path() / "link";
    GLYPHA_REQUIRE(std::filesystem::create_directory(target));
    std::filesystem::create_directory_symlink(target, link);
    const auto through_symlink = glyphastore::DataDirectory::open_and_lock(link);
    GLYPHA_REQUIRE(!through_symlink.has_value());
}

GLYPHA_TEST("data directory creation faults distinguish mkdir from parent synchronization") {
    {
        TemporaryDirectory parent;
        const auto path = parent.path() / "store";
        InjectedFailure failure{.operation = glyphastore::FilesystemOperation::create_data_directory,
                                .enabled = true};
        const auto rejected =
            glyphastore::DataDirectory::open_and_lock(path, glyphastore::DataDirectoryOpenMode::create_new,
                                                      {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(!rejected.has_value());
        GLYPHA_REQUIRE(!std::filesystem::exists(path));
    }
    {
        TemporaryDirectory parent;
        const auto path = parent.path() / "store";
        InjectedFailure failure{.operation = glyphastore::FilesystemOperation::sync_parent_directory,
                                .enabled = true};
        const auto indeterminate =
            glyphastore::DataDirectory::open_and_lock(path, glyphastore::DataDirectoryOpenMode::create_new,
                                                      {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(!indeterminate.has_value());
        GLYPHA_REQUIRE(std::filesystem::is_directory(path));
        auto reopened = glyphastore::DataDirectory::open_and_lock(path);
        GLYPHA_REQUIRE(reopened.has_value());
        const auto pristine = reopened->pristine_for_bootstrap();
        GLYPHA_REQUIRE(pristine.has_value());
        GLYPHA_REQUIRE(*pristine);
    }
}

GLYPHA_TEST("data directory available-space preflight preserves the configured reserve") {
    TemporaryDirectory temporary;
    AvailableSpaceProbe probe{.bytes = 1023};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &probe, .available_space_bytes = &AvailableSpaceProbe::read});
    GLYPHA_REQUIRE(directory.has_value());
    const auto sampled = directory->available_space_bytes();
    GLYPHA_REQUIRE(sampled.has_value());
    GLYPHA_REQUIRE(*sampled == probe.bytes);

    auto limits = glyphastore::DurableResourceLimits{};
    limits.reserved_free_bytes = 512;
    const auto insufficient = glyphastore::require_durable_available_space(*directory, 512, limits);
    GLYPHA_REQUIRE(!insufficient.has_value());
    GLYPHA_REQUIRE(insufficient.error().code == glyphastore::ErrorCode::storage_exhausted);

    probe.bytes = 1024;
    GLYPHA_REQUIRE(glyphastore::require_durable_available_space(*directory, 512, limits).has_value());
}

GLYPHA_TEST("durable bootstrap rejects insufficient space before publishing intent") {
    TemporaryDirectory temporary;
    AvailableSpaceProbe probe{.bytes = glyphastore::kSegmentSizeBytes};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &probe, .available_space_bytes = &AvailableSpaceProbe::read});
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.reserved_free_bytes = 0;
    const auto prepared = glyphastore::prepare_durable_store(
        *directory, glyphastore::DurableOpenMode::open_or_create, 1, 1, limits);
    GLYPHA_REQUIRE(!prepared.has_value());
    GLYPHA_REQUIRE(prepared.error().code == glyphastore::ErrorCode::storage_exhausted);
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kBootstrapIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kManifestFilename));
}

GLYPHA_TEST("manifest publication atomically replaces and reads a complete generation") {
    TemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());

    const auto first = test_manifest(1);
    const auto first_publication = directory->publish_manifest(first);
    GLYPHA_REQUIRE(first_publication.durable());
    GLYPHA_REQUIRE(!first_publication.error.has_value());
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kManifestFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kManifestTemporaryFilename));
    const auto first_read = directory->read_manifest();
    GLYPHA_REQUIRE(first_read.has_value());
    GLYPHA_REQUIRE(*first_read == first);
    const auto encoded_size = glyphastore::encoded_manifest_size(first);
    GLYPHA_REQUIRE(encoded_size.has_value());
    const auto limited_read = directory->read_manifest(*encoded_size - 1U);
    GLYPHA_REQUIRE(!limited_read.has_value());
    GLYPHA_REQUIRE(limited_read.error().code == glyphastore::ErrorCode::storage_exhausted);
    const auto second = test_manifest(2);
    const auto limited_publication = directory->publish_manifest(second, *encoded_size - 1U);
    GLYPHA_REQUIRE(limited_publication.outcome == glyphastore::ManifestPublicationOutcome::not_published);
    GLYPHA_REQUIRE(limited_publication.error.has_value());
    GLYPHA_REQUIRE(limited_publication.error->code == glyphastore::ErrorCode::storage_exhausted);

    GLYPHA_REQUIRE(directory->publish_manifest(second).durable());
    const auto second_read = directory->read_manifest();
    GLYPHA_REQUIRE(second_read.has_value());
    GLYPHA_REQUIRE(*second_read == second);

    const auto rollback = directory->publish_manifest(first);
    GLYPHA_REQUIRE(rollback.outcome == glyphastore::ManifestPublicationOutcome::not_published);
    GLYPHA_REQUIRE(rollback.error.has_value());
    GLYPHA_REQUIRE(rollback.error->code == glyphastore::ErrorCode::sequence_conflict);
    const auto after_rollback = directory->read_manifest();
    GLYPHA_REQUIRE(after_rollback.has_value());
    GLYPHA_REQUIRE(*after_rollback == second);
}

GLYPHA_TEST("manifest publication and reopen tolerate EINTR and fragmented positional IO") {
    TemporaryDirectory temporary;
    FragmentedPositionalIo io{.maximum_chunk = 7};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.file_io = {.context = &io,
                                       .read_some_at = &FragmentedPositionalIo::read,
                                       .write_some_at = &FragmentedPositionalIo::write}});
    GLYPHA_REQUIRE(directory.has_value());
    const auto expected = test_manifest();
    GLYPHA_REQUIRE(directory->publish_manifest(expected).durable());
    const auto recovered = directory->read_manifest();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(*recovered == expected);
    GLYPHA_REQUIRE(io.write_calls > 1);
    GLYPHA_REQUIRE(io.read_calls > 1);
}

GLYPHA_TEST("pre-rename failure preserves the old manifest and keeps the directory usable") {
    TemporaryDirectory temporary;
    InjectedFailure failure{};
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(),
                                                               {.context = &failure, .before = &fail_before});
    GLYPHA_REQUIRE(directory.has_value());
    const auto first = test_manifest(1);
    GLYPHA_REQUIRE(directory->publish_manifest(first).durable());

    failure.operation = glyphastore::FilesystemOperation::sync_manifest;
    failure.enabled = true;
    const auto failed = directory->publish_manifest(test_manifest(2));
    GLYPHA_REQUIRE(failed.outcome == glyphastore::ManifestPublicationOutcome::not_published);
    GLYPHA_REQUIRE(failed.error.has_value());
    GLYPHA_REQUIRE(directory->healthy());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kManifestTemporaryFilename));
    const auto recovered = directory->read_manifest();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(*recovered == first);
}

GLYPHA_TEST("manifest prepublication fault matrix preserves the durable generation") {
    static constexpr std::array boundaries{
        glyphastore::FilesystemOperation::write_manifest,
        glyphastore::FilesystemOperation::sync_manifest,
        glyphastore::FilesystemOperation::rename_manifest,
    };
    for (const auto boundary : boundaries) {
        TemporaryDirectory temporary;
        InjectedFailure failure{};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        const auto first = test_manifest(1);
        GLYPHA_REQUIRE(directory->publish_manifest(first).durable());

        failure.operation = boundary;
        failure.enabled = true;
        const auto rejected = directory->publish_manifest(test_manifest(2));
        GLYPHA_REQUIRE(rejected.outcome == glyphastore::ManifestPublicationOutcome::not_published);
        GLYPHA_REQUIRE(rejected.error.has_value());
        GLYPHA_REQUIRE(directory->healthy());
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kManifestTemporaryFilename));
        failure.enabled = false;
        const auto recovered = directory->read_manifest();
        GLYPHA_REQUIRE(recovered.has_value());
        GLYPHA_REQUIRE(*recovered == first);
    }
}

GLYPHA_TEST("post-rename sync failure is indeterminate and poisons the directory instance") {
    TemporaryDirectory temporary;
    InjectedFailure failure{};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        GLYPHA_REQUIRE(directory->publish_manifest(test_manifest(1)).durable());

        failure.operation = glyphastore::FilesystemOperation::sync_directory;
        failure.enabled = true;
        const auto failed = directory->publish_manifest(test_manifest(2));
        GLYPHA_REQUIRE(failed.outcome == glyphastore::ManifestPublicationOutcome::indeterminate);
        GLYPHA_REQUIRE(failed.error.has_value());
        GLYPHA_REQUIRE(!directory->healthy());
        GLYPHA_REQUIRE(!directory->read_manifest().has_value());
        GLYPHA_REQUIRE(directory->publish_manifest(test_manifest(3)).outcome ==
                       glyphastore::ManifestPublicationOutcome::indeterminate);
    }

    const auto reopened = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto recovered = reopened->read_manifest();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(recovered->manifest_generation == 2);
}

GLYPHA_TEST("invalid manifest fails before creating a publication") {
    TemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto invalid = test_manifest();
    invalid.worker_count = 0;
    const auto result = directory->publish_manifest(invalid);
    GLYPHA_REQUIRE(result.outcome == glyphastore::ManifestPublicationOutcome::not_published);
    GLYPHA_REQUIRE(result.error.has_value());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kManifestFilename));
}

GLYPHA_TEST("bootstrap intent publication cleans pre-rename failures") {
    TemporaryDirectory temporary;
    InjectedFailure failure{.operation = glyphastore::FilesystemOperation::sync_bootstrap, .enabled = true};
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(),
                                                               {.context = &failure, .before = &fail_before});
    GLYPHA_REQUIRE(directory.has_value());
    const auto published = directory->publish_bootstrap_intent(test_manifest());
    GLYPHA_REQUIRE(!published.has_value());
    GLYPHA_REQUIRE(directory->healthy());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kBootstrapTemporaryFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kBootstrapIntentFilename));
}

GLYPHA_TEST("bootstrap intent prepublication fault matrix leaves a pristine namespace") {
    static constexpr std::array boundaries{
        glyphastore::FilesystemOperation::write_bootstrap,
        glyphastore::FilesystemOperation::sync_bootstrap,
        glyphastore::FilesystemOperation::rename_bootstrap,
    };
    for (const auto boundary : boundaries) {
        TemporaryDirectory temporary;
        InjectedFailure failure{.operation = boundary, .enabled = true};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        const auto rejected = directory->publish_bootstrap_intent(test_manifest());
        GLYPHA_REQUIRE(!rejected.has_value());
        GLYPHA_REQUIRE(directory->healthy());
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kBootstrapTemporaryFilename));
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kBootstrapIntentFilename));
        const auto pristine = directory->pristine_for_bootstrap();
        GLYPHA_REQUIRE(pristine.has_value());
        GLYPHA_REQUIRE(*pristine);
    }
}

GLYPHA_TEST("bootstrap intent post-rename sync failure reopens as a complete intent") {
    TemporaryDirectory temporary;
    InjectedFailure failure{.operation = glyphastore::FilesystemOperation::sync_directory, .enabled = true};
    const auto expected = test_manifest();
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        const auto published = directory->publish_bootstrap_intent(expected);
        GLYPHA_REQUIRE(!published.has_value());
        GLYPHA_REQUIRE(!directory->healthy());
    }
    auto reopened = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto intent = reopened->read_bootstrap_intent();
    GLYPHA_REQUIRE(intent.has_value());
    GLYPHA_REQUIRE(*intent == expected);
}

GLYPHA_TEST("compaction intent publication reads and removes one exact transaction") {
    TemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto expected = test_compaction_intent();
    const auto published = directory->publish_compaction_intent(expected);
    GLYPHA_REQUIRE(published.durable());
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionTemporaryFilename));
    const auto recovered = directory->read_compaction_intent();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(*recovered == expected);
    const auto over_budget = directory->read_compaction_intent(glyphastore::kManifestHeaderBytes);
    GLYPHA_REQUIRE(!over_budget.has_value());
    GLYPHA_REQUIRE(over_budget.error().code == glyphastore::ErrorCode::storage_exhausted);

    const auto duplicate = directory->publish_compaction_intent(expected);
    GLYPHA_REQUIRE(duplicate.outcome == glyphastore::CompactionIntentPublicationOutcome::not_published);
    GLYPHA_REQUIRE(duplicate.error.has_value());
    GLYPHA_REQUIRE(duplicate.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE(directory->healthy());

    const auto removed = directory->remove_compaction_intent();
    GLYPHA_REQUIRE(removed.durable());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    const auto absent = directory->read_compaction_intent();
    GLYPHA_REQUIRE(!absent.has_value());
    GLYPHA_REQUIRE(absent.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("compaction intent prepublication fault matrix leaves no authority") {
    static constexpr std::array boundaries{
        glyphastore::FilesystemOperation::write_compaction_intent,
        glyphastore::FilesystemOperation::sync_compaction_intent,
        glyphastore::FilesystemOperation::rename_compaction_intent,
    };
    for (const auto boundary : boundaries) {
        TemporaryDirectory temporary;
        InjectedFailure failure{.operation = boundary, .enabled = true};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        const auto rejected = directory->publish_compaction_intent(test_compaction_intent());
        GLYPHA_REQUIRE(rejected.outcome == glyphastore::CompactionIntentPublicationOutcome::not_published);
        GLYPHA_REQUIRE(rejected.error.has_value());
        GLYPHA_REQUIRE(directory->healthy());
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
        GLYPHA_REQUIRE(
            !std::filesystem::exists(temporary.path() / glyphastore::kCompactionTemporaryFilename));
    }
}

GLYPHA_TEST("compaction intent post-rename sync failure is reopenable and indeterminate") {
    TemporaryDirectory temporary;
    InjectedFailure failure{.operation = glyphastore::FilesystemOperation::sync_directory, .enabled = true};
    const auto expected = test_compaction_intent();
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        const auto published = directory->publish_compaction_intent(expected);
        GLYPHA_REQUIRE(published.outcome == glyphastore::CompactionIntentPublicationOutcome::indeterminate);
        GLYPHA_REQUIRE(published.error.has_value());
        GLYPHA_REQUIRE(!directory->healthy());
    }
    auto reopened = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto recovered = reopened->read_compaction_intent();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(*recovered == expected);
}

GLYPHA_TEST("compaction intent removal sync failure is indeterminate after unlink") {
    TemporaryDirectory temporary;
    InjectedFailure failure{};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_before});
        GLYPHA_REQUIRE(directory.has_value());
        GLYPHA_REQUIRE(directory->publish_compaction_intent(test_compaction_intent()).durable());
        failure.operation = glyphastore::FilesystemOperation::sync_directory;
        failure.enabled = true;
        const auto removed = directory->remove_compaction_intent();
        GLYPHA_REQUIRE(removed.outcome == glyphastore::CompactionIntentRemovalOutcome::indeterminate);
        GLYPHA_REQUIRE(removed.error.has_value());
        GLYPHA_REQUIRE(!directory->healthy());
    }
    auto reopened = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto recovered = reopened->read_compaction_intent();
    GLYPHA_REQUIRE(!recovered.has_value());
    GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("compaction intent removal fault before unlink preserves the authority") {
    TemporaryDirectory temporary;
    InjectedFailure failure{.operation = glyphastore::FilesystemOperation::remove_compaction_intent,
                            .enabled = true};
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(),
                                                               {.context = &failure, .before = &fail_before});
    GLYPHA_REQUIRE(directory.has_value());
    const auto expected = test_compaction_intent();
    failure.enabled = false;
    GLYPHA_REQUIRE(directory->publish_compaction_intent(expected).durable());
    failure.enabled = true;
    const auto rejected = directory->remove_compaction_intent();
    GLYPHA_REQUIRE(rejected.outcome == glyphastore::CompactionIntentRemovalOutcome::not_removed);
    GLYPHA_REQUIRE(rejected.error.has_value());
    GLYPHA_REQUIRE(directory->healthy());
    failure.enabled = false;
    const auto recovered = directory->read_compaction_intent();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(*recovered == expected);
}
