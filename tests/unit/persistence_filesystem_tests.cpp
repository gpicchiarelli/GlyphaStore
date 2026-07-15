#include "glyphastore/persistence/filesystem.hpp"
#include "test.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>
#include <unistd.h>
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

struct InjectedFailure {
    glyphastore::FilesystemOperation operation{glyphastore::FilesystemOperation::write_manifest};
    bool enabled{};
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

    const auto second = test_manifest(2);
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
