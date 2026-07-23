#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/recovery.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class RecoveryTemporaryDirectory final {
  public:
    RecoveryTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-recovery-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~RecoveryTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

class BlockingRecordRead final {
  public:
    void arm() {
        const std::lock_guard lock{mutex_};
        armed_ = true;
    }

    [[nodiscard]] auto wait_until_blocked() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [&] { return blocked_; });
    }

    void release() {
        {
            const std::lock_guard lock{mutex_};
            released_ = true;
        }
        condition_.notify_all();
    }

    void force_next_record_write_full() {
        const std::lock_guard lock{mutex_};
        force_record_full_ = true;
    }

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& state = *static_cast<BlockingRecordRead*>(opaque);
        const std::lock_guard lock{state.mutex_};
        if (operation == glyphastore::FilesystemOperation::write_record && state.force_record_full_) {
            state.force_record_full_ = false;
            return glyphastore::fail(glyphastore::ErrorCode::segment_full,
                                     "injected full Segment during blocked compaction");
        }
        return {};
    }

    static auto read_some_at(void* opaque, const int descriptor, const std::span<std::byte> bytes,
                             const std::uint64_t offset) -> std::ptrdiff_t {
        auto& state = *static_cast<BlockingRecordRead*>(opaque);
        if (offset >= glyphastore::kSegmentHeaderReservedBytes) {
            std::unique_lock lock{state.mutex_};
            if (state.armed_ && !state.claimed_) {
                state.claimed_ = true;
                state.blocked_ = true;
                state.condition_.notify_all();
                state.condition_.wait(lock, [&] { return state.released_; });
            }
        }
        return ::pread(descriptor, bytes.data(), bytes.size(), static_cast<off_t>(offset));
    }

  private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool armed_{};
    bool claimed_{};
    bool blocked_{};
    bool released_{};
    bool force_record_full_{};
};

class BlockingFilesystemOperation final {
  public:
    explicit BlockingFilesystemOperation(const glyphastore::FilesystemOperation target) : target_(target) {}

    [[nodiscard]] auto wait_until_blocked() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [&] { return blocked_; });
    }

    void release() {
        {
            const std::lock_guard lock{mutex_};
            released_ = true;
        }
        condition_.notify_all();
    }

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& state = *static_cast<BlockingFilesystemOperation*>(opaque);
        if (operation != state.target_) {
            return {};
        }
        std::unique_lock lock{state.mutex_};
        if (state.claimed_) {
            return {};
        }
        state.claimed_ = true;
        state.blocked_ = true;
        state.condition_.notify_all();
        state.condition_.wait(lock, [&] { return state.released_; });
        return {};
    }

  private:
    glyphastore::FilesystemOperation target_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool claimed_{};
    bool blocked_{};
    bool released_{};
};

auto recovery_store_id(std::byte first = std::byte{0x20}) -> glyphastore::StoreId {
    return {first,           std::byte{0x21}, std::byte{0x22}, std::byte{0x23},
            std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27},
            std::byte{0x28}, std::byte{0x29}, std::byte{0x2A}, std::byte{0x2B},
            std::byte{0x2C}, std::byte{0x2D}, std::byte{0x2E}, std::byte{0x2F}};
}

auto key_for_worker(std::size_t worker, std::size_t worker_count, std::string_view prefix) -> std::string {
    for (std::size_t suffix = 0; suffix < 10'000; ++suffix) {
        auto candidate = std::string{prefix} + std::to_string(suffix);
        if (glyphastore::route_worker(candidate, worker_count) == worker) {
            return candidate;
        }
    }
    throw std::runtime_error("failed to construct a routed test key");
}

auto segment_identity(const glyphastore::StoreId& store_id, const glyphastore::ManifestSegmentEntry& entry)
    -> glyphastore::SegmentHeaderIdentity {
    return {
        .store_id = store_id,
        .segment_id = entry.segment_id,
        .generation = entry.generation,
        .owner_worker = entry.owner_worker,
    };
}

auto create_segment(glyphastore::DataDirectory& directory, const glyphastore::StoreId& store_id,
                    const glyphastore::ManifestSegmentEntry& entry) -> glyphastore::DurableSegmentFile {
    auto created = glyphastore::DurableSegmentFile::create(directory, segment_identity(store_id, entry));
    GLYPHA_REQUIRE(created.durable());
    GLYPHA_REQUIRE(created.file.has_value());
    return std::move(*created.file);
}

void append_record(glyphastore::DurableSegmentFile& file, std::uint64_t sequence, std::string_view key,
                   std::string_view value = {}, glyphastore::Opcode opcode = glyphastore::Opcode::put,
                   std::uint64_t expire_at_ns = 0, std::optional<std::uint64_t> stored_hash = std::nullopt) {
    const auto key_bytes = std::as_bytes(std::span{key});
    const auto value_bytes = std::as_bytes(std::span{value});
    const auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{sequence},
        .opcode = opcode,
        .type = glyphastore::ValueType::bytes,
        .flags = 0,
        .key_hash = stored_hash.value_or(glyphastore::hash_key(key)),
        .expire_at_ns = expire_at_ns,
        .key = key_bytes,
        .value = value_bytes,
    });
    GLYPHA_REQUIRE(encoded.has_value());
    GLYPHA_REQUIRE(file.append(*encoded).committed());
}

void create_private_file(const std::filesystem::path& path) {
    const auto descriptor = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    GLYPHA_REQUIRE(descriptor >= 0);
    GLYPHA_REQUIRE(::close(descriptor) == 0);
}

struct OneShotFilesystemFailure {
    glyphastore::FilesystemOperation target;
    glyphastore::ErrorCode code{glyphastore::ErrorCode::io_error};
    std::size_t target_occurrence{1};
    std::size_t occurrences{};
    bool fired{};

    static auto before(void* opaque, glyphastore::FilesystemOperation operation) -> glyphastore::Status {
        auto& state = *static_cast<OneShotFilesystemFailure*>(opaque);
        if (operation == state.target) {
            ++state.occurrences;
        }
        if (!state.fired && operation == state.target && state.occurrences == state.target_occurrence) {
            state.fired = true;
            return glyphastore::fail(state.code, "injected durable runtime filesystem failure");
        }
        return {};
    }
};

struct SyncThreadObserver {
    std::mutex mutex;
    std::thread::id sync_thread;
    std::vector<std::thread::id> sync_threads;

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        if (operation == glyphastore::FilesystemOperation::sync_record) {
            auto& observer = *static_cast<SyncThreadObserver*>(opaque);
            const std::lock_guard lock{observer.mutex};
            observer.sync_thread = std::this_thread::get_id();
            observer.sync_threads.push_back(observer.sync_thread);
        }
        return {};
    }
};

struct BatchBoundaryObserver {
    std::mutex mutex;
    std::size_t writes_since_sync{};
    std::size_t maximum_writes_before_sync{};
    std::size_t sync_count{};

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& observer = *static_cast<BatchBoundaryObserver*>(opaque);
        const std::lock_guard lock{observer.mutex};
        if (operation == glyphastore::FilesystemOperation::write_record) {
            ++observer.writes_since_sync;
        } else if (operation == glyphastore::FilesystemOperation::sync_record) {
            observer.maximum_writes_before_sync =
                std::max(observer.maximum_writes_before_sync, observer.writes_since_sync);
            observer.writes_since_sync = 0;
            ++observer.sync_count;
        }
        return {};
    }
};

struct RecordWriteObserver {
    std::mutex mutex;
    std::condition_variable written;
    bool record_written{};

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        if (operation == glyphastore::FilesystemOperation::write_record) {
            auto& observer = *static_cast<RecordWriteObserver*>(opaque);
            {
                const std::lock_guard lock{observer.mutex};
                observer.record_written = true;
            }
            observer.written.notify_all();
        }
        return {};
    }
};

struct RotationBudgetObserver {
    bool force_segment_full{true};
    std::uint64_t available_bytes{};

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& observer = *static_cast<RotationBudgetObserver*>(opaque);
        if (operation == glyphastore::FilesystemOperation::write_record && observer.force_segment_full) {
            observer.force_segment_full = false;
            return glyphastore::fail(glyphastore::ErrorCode::segment_full,
                                     "injected full Segment before rotation");
        }
        return {};
    }

    static auto available(void* opaque) -> glyphastore::Result<std::uint64_t> {
        return static_cast<RotationBudgetObserver*>(opaque)->available_bytes;
    }
};

auto recovery_manifest(const glyphastore::StoreId& store_id, std::uint32_t workers,
                       std::vector<glyphastore::ManifestSegmentEntry> segments) -> glyphastore::Manifest {
    const auto next_id = segments.empty() ? 1 : segments.back().segment_id.value + 1;
    return {
        .store_id = store_id,
        .manifest_generation = 1,
        .routing_algorithm = glyphastore::RoutingAlgorithm::fnv1a64_v1,
        .worker_count = workers,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{next_id},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = std::move(segments),
    };
}

auto owned_text(const glyphastore::OwnedValue& value) -> std::string {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

} // namespace

GLYPHA_TEST("durable recovery rebuilds partitioned visibility and Worker sequences") {
    RecoveryTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto alpha = key_for_worker(0, 2, "alpha");
    const auto gone = key_for_worker(0, 2, "gone");
    const auto expired = key_for_worker(0, 2, "expired");
    const std::string binary_prefix{"binary\0key", 10};
    const auto binary = key_for_worker(0, 2, binary_prefix);
    const auto beta = key_for_worker(1, 2, "beta");

    auto first = create_segment(*directory, store_id, entries[0]);
    append_record(first, 1, expired, "older-visible");
    append_record(first, 2, gone, "present");
    append_record(first, 3, gone, {}, glyphastore::Opcode::erase);
    append_record(first, 4, alpha, "old");
    append_record(first, 5, binary, "binary-value");
    GLYPHA_REQUIRE(first.seal().committed());

    auto second = create_segment(*directory, store_id, entries[1]);
    append_record(second, 6, alpha, "new");
    append_record(second, 7, expired, "stale", glyphastore::Opcode::put, 100);

    auto third = create_segment(*directory, store_id, entries[2]);
    append_record(third, 9, beta, "visible");
    GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());

    const auto recovered = glyphastore::recover_durable_state(*directory, 101);
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(recovered->segments.size() == 3);
    GLYPHA_REQUIRE(recovered->workers.size() == 2);
    GLYPHA_REQUIRE(recovered->stats.segments_scanned == 3);
    GLYPHA_REQUIRE(recovered->stats.rebuild.records_scanned == 8);
    GLYPHA_REQUIRE(recovered->stats.rebuild.records_visible == 3);
    GLYPHA_REQUIRE(recovered->stats.rebuild.tombstones == 1);
    GLYPHA_REQUIRE(recovered->stats.rebuild.expired == 1);
    GLYPHA_REQUIRE(recovered->stats.workers_requiring_rotation == 0);

    const auto alpha_ref = recovered->workers[0].index.find(alpha);
    GLYPHA_REQUIRE(alpha_ref.has_value());
    GLYPHA_REQUIRE(alpha_ref->segment_id.value == 2);
    GLYPHA_REQUIRE(alpha_ref->sequence.value == 6);
    GLYPHA_REQUIRE(!recovered->workers[0].index.find(gone).has_value());
    GLYPHA_REQUIRE(!recovered->workers[0].index.find(expired).has_value());
    GLYPHA_REQUIRE(recovered->workers[0].index.find(binary).has_value());
    GLYPHA_REQUIRE(recovered->workers[0].next_sequence.value == 8);
    GLYPHA_REQUIRE(recovered->workers[0].active_segment.value == 2);

    const auto beta_ref = recovered->workers[1].index.find(beta);
    GLYPHA_REQUIRE(beta_ref.has_value());
    GLYPHA_REQUIRE(beta_ref->sequence.value == 9);
    GLYPHA_REQUIRE(recovered->workers[1].next_sequence.value == 10);
    GLYPHA_REQUIRE(recovered->workers[1].active_segment.value == 3);
}

GLYPHA_TEST("recovery reports crash temporaries but rejects unlisted Segments without adoption") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.identity().segment_id == active.segment_id);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        create_private_file(temporary.path() / glyphastore::kManifestTemporaryFilename);
        create_private_file(temporary.path() /
                            ('.' + glyphastore::segment_filename(segment.identity()) + ".tmp"));

        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(recovered.has_value());
        GLYPHA_REQUIRE(recovered->namespace_audit.issues.size() == 2);
        GLYPHA_REQUIRE(recovered->namespace_audit.recovery_safe());
        GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kManifestTemporaryFilename));
    }
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.identity().generation == active.generation);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const glyphastore::SegmentHeaderIdentity orphan{
            .store_id = store_id,
            .segment_id = glyphastore::SegmentId{2},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
        };
        create_private_file(temporary.path() / glyphastore::segment_filename(orphan));

        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
        GLYPHA_REQUIRE(recovered.error().message.find("unlisted Segment") != std::string::npos);
        GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::segment_filename(orphan)));
    }
}

GLYPHA_TEST("recovery accepts only the documented sealed-active rotation transition") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(file.seal().committed());
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(recovered.has_value());
        GLYPHA_REQUIRE(recovered->workers[0].active_requires_rotation);
        GLYPHA_REQUIRE(recovered->stats.workers_requiring_rotation == 1);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        auto incorrectly_active = create_segment(*directory, store_id, entries[0]);
        auto active = create_segment(*directory, store_id, entries[1]);
        static_cast<void>(incorrectly_active);
        static_cast<void>(active);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery rejects missing and identity-mismatched manifest Segments") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto file_store_id = recovery_store_id();
        const auto manifest_store_id = recovery_store_id(std::byte{0x99});
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, file_store_id, active);
        static_cast<void>(file);
        GLYPHA_REQUIRE(
            directory->publish_manifest(recovery_manifest(manifest_store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery validates persisted key hashes and Worker routing") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        const auto key = key_for_worker(0, 1, "hash");
        append_record(file, 1, key, "value", glyphastore::Opcode::put, 0, glyphastore::hash_key(key) ^ 1U);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{1},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        auto wrong_owner = create_segment(*directory, store_id, entries[0]);
        auto other_active = create_segment(*directory, store_id, entries[1]);
        const auto key = key_for_worker(1, 2, "wrong-owner");
        append_record(wrong_owner, 1, key, "value");
        static_cast<void>(other_active);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery rejects equal winning sequences and exhausted Worker sequence space") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        const auto key = key_for_worker(0, 1, "duplicate");
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 5, key, "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 5, key, "second");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        append_record(file, std::numeric_limits<std::uint64_t>::max(), "last", "value");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::arithmetic_overflow);
    }
}

GLYPHA_TEST("recovery rejects overlapping sequence ranges across Worker Segments") {
    RecoveryTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    auto first = create_segment(*directory, store_id, entries[0]);
    append_record(first, 10, "first", "value");
    GLYPHA_REQUIRE(first.seal().committed());
    auto second = create_segment(*directory, store_id, entries[1]);
    append_record(second, 9, "different-key", "value");
    GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());

    const auto recovered = glyphastore::recover_durable_state(*directory);
    GLYPHA_REQUIRE(!recovered.has_value());
    GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(recovered.error().message.find("overlaps or reverses") != std::string::npos);
}

GLYPHA_TEST("durable runtime materializes recovered Indexes with bounded concurrent reads") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const std::string binary_key{"bin\0key", 7};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto sealed = create_segment(*directory, store_id, entries[0]);
        append_record(sealed, 1, "alpha", "one");
        append_record(sealed, 2, binary_key, "binary");
        GLYPHA_REQUIRE(sealed.seal().committed());
        auto active = create_segment(*directory, store_id, entries[1]);
        append_record(active, 3, "beta", "two");
        append_record(active, 4, "expired", "old", glyphastore::Opcode::put, 100);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->worker_count() == 1);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
    GLYPHA_REQUIRE((*runtime)->next_sequence(0).has_value());
    GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 5);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);

    const auto alpha = (*runtime)->get("alpha");
    const auto beta = (*runtime)->get("beta");
    const auto binary = (*runtime)->get(binary_key);
    GLYPHA_REQUIRE(alpha.has_value());
    GLYPHA_REQUIRE(beta.has_value());
    GLYPHA_REQUIRE(binary.has_value());
    GLYPHA_REQUIRE(owned_text(*alpha) == "one");
    GLYPHA_REQUIRE(owned_text(*beta) == "two");
    GLYPHA_REQUIRE(owned_text(*binary) == "binary");
    const auto expired = (*runtime)->get("expired", 100);
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);

    std::atomic_bool failed{};
    std::vector<std::thread> readers;
    for (std::size_t thread = 0; thread < 8; ++thread) {
        readers.emplace_back([&, thread] {
            for (std::size_t iteration = 0; iteration < 32; ++iteration) {
                const bool choose_alpha = (thread + iteration) % 2 == 0;
                const auto value = (*runtime)->get(choose_alpha ? "alpha" : "beta");
                if (!value || owned_text(*value) != (choose_alpha ? "one" : "two")) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE((*runtime)->healthy());

    const auto locked_again = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(!locked_again.has_value());
}

GLYPHA_TEST("blocked durable cold read does not block a mutation on the same Worker") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        append_record(segment, 1, "cold", "value");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingRecordRead blocked_read;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_read,
            .file_io = {.context = &blocked_read, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_read.arm();

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read_result;
    std::thread reader{[&] { read_result.emplace((*runtime)->get("cold")); }};
    GLYPHA_REQUIRE(blocked_read.wait_until_blocked());

    std::mutex completion_mutex;
    std::condition_variable completion;
    bool mutation_finished{};
    glyphastore::DurableMutationResult mutation;
    const std::string other_key{"other"};
    const std::string other_value{"new-value"};
    std::thread writer{[&] {
        mutation =
            (*runtime)->put(std::as_bytes(std::span{other_key}), std::as_bytes(std::span{other_value}));
        {
            const std::lock_guard lock{completion_mutex};
            mutation_finished = true;
        }
        completion.notify_one();
    }};

    bool mutation_completed_while_read_blocked{};
    {
        std::unique_lock lock{completion_mutex};
        mutation_completed_while_read_blocked =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return mutation_finished; });
    }
    blocked_read.release();
    writer.join();
    reader.join();

    GLYPHA_REQUIRE(mutation_completed_while_read_blocked);
    GLYPHA_REQUIRE(mutation.committed());
    GLYPHA_REQUIRE(read_result.has_value());
    GLYPHA_REQUIRE(read_result->has_value());
    GLYPHA_REQUIRE(owned_text(**read_result) == "value");
    const auto written = (*runtime)->get(other_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == other_value);
}

GLYPHA_TEST("durable cold read pin survives concurrent source retirement and relinearizes") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, "cold", "value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "second", "record");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_read;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_read,
            .file_io = {.context = &blocked_read, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_read.arm();

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read_result;
    std::thread reader{[&] { read_result.emplace((*runtime)->get("cold")); }};
    GLYPHA_REQUIRE(blocked_read.wait_until_blocked());

    std::mutex completion_mutex;
    std::condition_variable completion;
    bool compaction_finished{};
    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] {
        compaction = (*runtime)->compact_worker(0, 0);
        {
            const std::lock_guard lock{completion_mutex};
            compaction_finished = true;
        }
        completion.notify_one();
    }};
    bool retired_while_read_blocked{};
    {
        std::unique_lock lock{completion_mutex};
        retired_while_read_blocked =
            completion.wait_for(lock, std::chrono::seconds{5}, [&] { return compaction_finished; });
    }
    blocked_read.release();
    compactor.join();
    reader.join();

    GLYPHA_REQUIRE(retired_while_read_blocked);
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE(read_result.has_value());
    GLYPHA_REQUIRE(read_result->has_value());
    GLYPHA_REQUIRE(owned_text(**read_result) == "value");
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
}

GLYPHA_TEST("blocked durable compaction build permits same-Worker reads and mutations") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, "changing", "old");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "stable", "visible");
        append_record(second, 3, "erase-me", "present");
        append_record(second, 4, "ttl-key", "old-ttl");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());

    const std::string key{"changing"};
    const std::string replacement{"new"};
    std::optional<glyphastore::Result<glyphastore::OwnedValue>> stable;
    glyphastore::DurableMutationResult mutation;
    glyphastore::DurableMutationResult erased;
    glyphastore::DurableMutationResult ttl_updated;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool operations_finished{};
    std::thread operations{[&] {
        stable.emplace((*runtime)->get("stable"));
        mutation = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{replacement}));
        const std::string erased_key{"erase-me"};
        erased = (*runtime)->erase(std::as_bytes(std::span{erased_key}));
        const std::string ttl_key{"ttl-key"};
        const std::string ttl_value{"new-ttl"};
        ttl_updated =
            (*runtime)->put(std::as_bytes(std::span{ttl_key}), std::as_bytes(std::span{ttl_value}), 100);
        {
            const std::lock_guard lock{completion_mutex};
            operations_finished = true;
        }
        completion.notify_one();
    }};
    bool completed_during_build{};
    {
        std::unique_lock lock{completion_mutex};
        completed_during_build =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return operations_finished; });
    }

    blocked_build.release();
    operations.join();
    compactor.join();

    GLYPHA_REQUIRE(completed_during_build);
    GLYPHA_REQUIRE(stable.has_value());
    GLYPHA_REQUIRE(stable->has_value());
    GLYPHA_REQUIRE(owned_text(**stable) == "visible");
    GLYPHA_REQUIRE(mutation.committed());
    GLYPHA_REQUIRE(erased.committed());
    GLYPHA_REQUIRE(ttl_updated.committed());
    GLYPHA_REQUIRE(compaction.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto current = (*runtime)->get(key);
    GLYPHA_REQUIRE(current.has_value());
    GLYPHA_REQUIRE(owned_text(*current) == replacement);
    const auto erased_value = (*runtime)->get("erase-me");
    GLYPHA_REQUIRE(!erased_value.has_value());
    GLYPHA_REQUIRE(erased_value.error().code == glyphastore::ErrorCode::not_found);
    const auto ttl_visible = (*runtime)->get("ttl-key", 99);
    GLYPHA_REQUIRE(ttl_visible.has_value());
    GLYPHA_REQUIRE(owned_text(*ttl_visible) == "new-ttl");
    const auto ttl_expired = (*runtime)->get("ttl-key", 100);
    GLYPHA_REQUIRE(!ttl_expired.has_value());
    GLYPHA_REQUIRE(ttl_expired.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*runtime)->manifest() == recovery_manifest(store_id, 1, entries));
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
}

GLYPHA_TEST("blocked durable compaction makes an unrelated rotation fail fast") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{4},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto compacted_key = key_for_worker(0, 2, "compact-");
    const auto second_compacted_key = key_for_worker(0, 2, "compact-second-");
    const auto rotating_key = key_for_worker(1, 2, "rotate-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, compacted_key, "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, second_compacted_key, "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .before = &BlockingRecordRead::before,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.force_next_record_write_full();
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());

    glyphastore::DurableMutationResult rotation;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool rotation_finished{};
    std::thread writer{[&] {
        const std::string value{"value"};
        rotation = (*runtime)->put(std::as_bytes(std::span{rotating_key}), std::as_bytes(std::span{value}));
        {
            const std::lock_guard lock{completion_mutex};
            rotation_finished = true;
        }
        completion.notify_one();
    }};
    bool rotation_completed_during_build{};
    {
        std::unique_lock lock{completion_mutex};
        rotation_completed_during_build =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return rotation_finished; });
    }

    blocked_build.release();
    writer.join();
    compactor.join();

    GLYPHA_REQUIRE(rotation_completed_during_build);
    GLYPHA_REQUIRE(rotation.outcome == glyphastore::DurableMutationOutcome::not_committed);
    GLYPHA_REQUIRE(rotation.error.has_value());
    GLYPHA_REQUIRE(rotation.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 3);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
}

GLYPHA_TEST("compaction manifest sync holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{4},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto stable_key = key_for_worker(0, 2, "stable-");
    const auto second_key = key_for_worker(0, 2, "second-");
    const auto rejected_key = key_for_worker(0, 2, "rejected-");
    const auto other_worker_key = key_for_worker(1, 2, "other-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, stable_key, "stable-value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, second_key, "second-value");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingFilesystemOperation blocked_sync{glyphastore::FilesystemOperation::sync_manifest};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), glyphastore::FilesystemHooks{.context = &blocked_sync,
                                                       .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_sync.wait_until_blocked());

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read;
    glyphastore::DurableMutationResult rejected;
    glyphastore::DurableMutationResult other_worker;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool operations_finished{};
    std::thread operations{[&] {
        read.emplace((*runtime)->get(stable_key));
        const std::string rejected_value{"rejected"};
        rejected =
            (*runtime)->put(std::as_bytes(std::span{rejected_key}), std::as_bytes(std::span{rejected_value}));
        const std::string other_value{"other-value"};
        other_worker = (*runtime)->put(std::as_bytes(std::span{other_worker_key}),
                                       std::as_bytes(std::span{other_value}));
        {
            const std::lock_guard lock{completion_mutex};
            operations_finished = true;
        }
        completion.notify_one();
    }};
    bool completed_during_manifest_sync{};
    {
        std::unique_lock lock{completion_mutex};
        completed_during_manifest_sync =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return operations_finished; });
    }

    blocked_sync.release();
    operations.join();
    compactor.join();

    GLYPHA_REQUIRE(completed_during_manifest_sync);
    GLYPHA_REQUIRE(read.has_value());
    GLYPHA_REQUIRE(read->has_value());
    GLYPHA_REQUIRE(owned_text(**read) == "stable-value");
    GLYPHA_REQUIRE(rejected.outcome == glyphastore::DurableMutationOutcome::not_committed);
    GLYPHA_REQUIRE(rejected.error.has_value());
    GLYPHA_REQUIRE(rejected.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE(other_worker.committed());
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto durable_other = (*reopened)->get(other_worker_key);
    GLYPHA_REQUIRE(durable_other.has_value());
    GLYPHA_REQUIRE(owned_text(*durable_other) == "other-value");
}

GLYPHA_TEST("close during a blocked compaction build rolls back the old authority") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, "first", "value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "second", "value");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());
    const auto closed = (*runtime)->close();
    GLYPHA_REQUIRE(closed.has_value());

    blocked_build.release();
    compactor.join();
    GLYPHA_REQUIRE(compaction.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->manifest() == recovery_manifest(store_id, 1, entries));
    GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
    GLYPHA_REQUIRE((*reopened)->get("first").has_value());
    GLYPHA_REQUIRE((*reopened)->get("second").has_value());
}

GLYPHA_TEST("online compaction filesystem fault matrix reopens one clean authority") {
    struct FaultCase {
        glyphastore::FilesystemOperation operation;
        std::size_t occurrence{1};
    };
    const std::vector<FaultCase> faults{
        {glyphastore::FilesystemOperation::write_compaction_intent},
        {glyphastore::FilesystemOperation::sync_compaction_intent},
        {glyphastore::FilesystemOperation::rename_compaction_intent},
        {glyphastore::FilesystemOperation::sync_directory, 1},
        {glyphastore::FilesystemOperation::preallocate_segment},
        {glyphastore::FilesystemOperation::write_segment_header},
        {glyphastore::FilesystemOperation::sync_segment_file},
        {glyphastore::FilesystemOperation::rename_segment},
        {glyphastore::FilesystemOperation::sync_directory, 2},
        {glyphastore::FilesystemOperation::write_record, 1},
        {glyphastore::FilesystemOperation::write_record, 2},
        {glyphastore::FilesystemOperation::sync_record},
        {glyphastore::FilesystemOperation::write_commit_slot, 1},
        {glyphastore::FilesystemOperation::sync_commit_slot, 1},
        {glyphastore::FilesystemOperation::write_commit_slot, 2},
        {glyphastore::FilesystemOperation::sync_commit_slot, 2},
        {glyphastore::FilesystemOperation::write_manifest},
        {glyphastore::FilesystemOperation::sync_manifest},
        {glyphastore::FilesystemOperation::rename_manifest},
        {glyphastore::FilesystemOperation::sync_directory, 3},
        {glyphastore::FilesystemOperation::remove_compaction_segment, 1},
        {glyphastore::FilesystemOperation::remove_compaction_segment, 2},
        {glyphastore::FilesystemOperation::sync_directory, 4},
        {glyphastore::FilesystemOperation::remove_compaction_intent},
        {glyphastore::FilesystemOperation::sync_directory, 5},
    };
    for (const auto& fault : faults) {
        RecoveryTemporaryDirectory temporary;
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        {
            auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
            GLYPHA_REQUIRE(directory.has_value());
            auto first = create_segment(*directory, store_id, entries[0]);
            append_record(first, 1, "fault-first", "first-value");
            GLYPHA_REQUIRE(first.seal().committed());
            auto second = create_segment(*directory, store_id, entries[1]);
            append_record(second, 2, "fault-second", "second-value");
            GLYPHA_REQUIRE(second.seal().committed());
            static_cast<void>(create_segment(*directory, store_id, entries[2]));
            GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        }

        OneShotFilesystemFailure failure{.target = fault.operation, .target_occurrence = fault.occurrence};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(),
            glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
        GLYPHA_REQUIRE(runtime.has_value());
        const auto result = (*runtime)->compact_worker(0, 0);
        GLYPHA_REQUIRE(failure.fired);
        GLYPHA_REQUIRE(!result.compacted());
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(result.error->code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableCompactionOutcome::not_compacted ||
                       result.outcome == glyphastore::DurableCompactionOutcome::recovery_required);
        GLYPHA_REQUIRE((*runtime)->healthy() ==
                       (result.outcome == glyphastore::DurableCompactionOutcome::not_compacted));
        runtime->reset();

        auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(reopened.has_value());
        GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
        const auto first = (*reopened)->get("fault-first");
        const auto second = (*reopened)->get("fault-second");
        GLYPHA_REQUIRE(first.has_value());
        GLYPHA_REQUIRE(second.has_value());
        GLYPHA_REQUIRE(owned_text(*first) == "first-value");
        GLYPHA_REQUIRE(owned_text(*second) == "second-value");
    }
}

GLYPHA_TEST("durable runtime detects post-recovery Record corruption and remains fail-closed") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    glyphastore::RecordRef reference{};
    glyphastore::SegmentHeaderIdentity identity{};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        append_record(segment, 1, "stable", "value");
        const auto records = segment.scan_committed();
        GLYPHA_REQUIRE(records.has_value());
        GLYPHA_REQUIRE(records->size() == 1);
        reference = records->front();
        identity = segment.identity();
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->get("stable").has_value());

    const auto path = temporary.path() / glyphastore::segment_filename(identity);
    const auto descriptor = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    GLYPHA_REQUIRE(descriptor >= 0);
    const std::byte corruption{0xFF};
    const auto value_offset =
        static_cast<off_t>(reference.offset.value + glyphastore::kEncodedRecordHeaderSize + 6U);
    GLYPHA_REQUIRE(::pwrite(descriptor, &corruption, 1, value_offset) == 1);
    GLYPHA_REQUIRE(::close(descriptor) == 0);

    const auto corrupted = (*runtime)->get("stable");
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
    const auto after_failure = (*runtime)->get("stable");
    GLYPHA_REQUIRE(!after_failure.has_value());
    GLYPHA_REQUIRE(after_failure.error().code == glyphastore::ErrorCode::unavailable);
}

GLYPHA_TEST("durable runtime completes a sealed-active interrupted rotation") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.seal().committed());
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->manifest().segments[0].role == glyphastore::ManifestSegmentRole::sealed);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE(std::filesystem::exists(
        temporary.path() / glyphastore::segment_filename(segment_identity(store_id, active))));
}

GLYPHA_TEST("durable runtime adopts only the exact pristine prepared rotation Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto old = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(old.seal().committed());
        auto manifest = recovery_manifest(store_id, 1, {active});
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        const glyphastore::SegmentHeaderIdentity prepared{
            .store_id = store_id,
            .segment_id = manifest.next_segment_id,
            .generation = manifest.next_segment_generation,
            .owner_worker = active.owner_worker,
        };
        const auto replacement = glyphastore::DurableSegmentFile::create(*directory, prepared);
        GLYPHA_REQUIRE(replacement.durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->manifest().manifest_generation == 2);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
}

GLYPHA_TEST("durable runtime commits puts replacements erases and recovers them") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const std::string long_key(96, 'L');
    const auto long_bytes = std::as_bytes(std::span{long_key});
    const std::string first{"first"};
    const std::string second{"second"};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(runtime.has_value());
        const auto inserted = (*runtime)->put(long_bytes, std::as_bytes(std::span{first}), 500);
        GLYPHA_REQUIRE(inserted.committed());
        GLYPHA_REQUIRE(inserted.sequence->value == 1);
        GLYPHA_REQUIRE(owned_text(*(*runtime)->get(long_key, 499)) == "first");

        const auto replaced = (*runtime)->put(long_bytes, std::as_bytes(std::span{second}));
        GLYPHA_REQUIRE(replaced.committed());
        GLYPHA_REQUIRE(replaced.sequence->value == 2);
        GLYPHA_REQUIRE(owned_text(*(*runtime)->get(long_key)) == "second");

        const auto erased = (*runtime)->erase(long_bytes);
        GLYPHA_REQUIRE(erased.committed());
        GLYPHA_REQUIRE(erased.sequence->value == 3);
        const auto missing = (*runtime)->get(long_key);
        GLYPHA_REQUIRE(!missing.has_value());
        GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
        GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 4);
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 4);
    const auto missing = (*reopened)->get(long_key);
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("zero hot-cache budget falls back to pinned active-Segment reads for all value sizes") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 0;
    limits.max_hot_cache_bytes_per_worker = 0;
    limits.max_hot_cache_staging_bytes_per_worker = 0;
    limits.max_hot_cache_entries_per_worker = 0;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::array sizes{std::size_t{0}, std::size_t{64}, std::size_t{4096},
                           glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - 32U};
    for (std::size_t index = 0; index < sizes.size(); ++index) {
        const auto key = std::string{"cold-active-"} + std::to_string(index);
        const auto value = std::vector<std::byte>(sizes[index], static_cast<std::byte>(0x30U + index));
        GLYPHA_REQUIRE((*runtime)->put(std::as_bytes(std::span{key}), value).committed());
        const auto visible = (*runtime)->get(key);
        GLYPHA_REQUIRE(visible.has_value());
        GLYPHA_REQUIRE(visible->bytes == value);
    }

    const std::string overwrite_key{"cold-overwrite"};
    const std::string first{"first"};
    const std::string second{"second"};
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{overwrite_key}), std::as_bytes(std::span{first}))
                       .committed());
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{overwrite_key}), std::as_bytes(std::span{second}))
                       .committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(overwrite_key)) == second);
    GLYPHA_REQUIRE((*runtime)->erase(std::as_bytes(std::span{overwrite_key})).committed());
    GLYPHA_REQUIRE(!(*runtime)->get(overwrite_key).has_value());

    const std::string ttl_key{"cold-ttl"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{ttl_key}), std::as_bytes(std::span{first}), 100).committed());
    GLYPHA_REQUIRE((*runtime)->get(ttl_key, 99).has_value());
    const auto expired = (*runtime)->get(ttl_key, 100);
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
    // Repeated cold GETs must not keep the expired Index entry; a second GET is Index-miss only.
    const auto expired_again = (*runtime)->get(ttl_key, 100);
    GLYPHA_REQUIRE(!expired_again.has_value());
    GLYPHA_REQUIRE(expired_again.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE(expired_again.error().message == "key is not present");

    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].resident_bytes == 0);
    GLYPHA_REQUIRE(stats[0].staged_entries == 0);
    GLYPHA_REQUIRE(stats[0].staged_bytes == 0);
    GLYPHA_REQUIRE(stats[0].byte_budget == 0);
    GLYPHA_REQUIRE(stats[0].admission_bypasses == sizes.size() + 3U);
    // First TTL GET is a miss that validates expiry; the second is an Index miss (no cold I/O).
    GLYPHA_REQUIRE(stats[0].misses == sizes.size() + 3U);
}

GLYPHA_TEST("hot-cache accounting remains bounded across hit overwrite and erase") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 16U * 1024U;
    limits.max_hot_cache_bytes_per_worker = 16U * 1024U;
    limits.max_hot_cache_staging_bytes_per_worker = 8U * 1024U;
    limits.max_hot_cache_entries_per_worker = 1;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"bounded-hot"};
    const std::string first(64, 'a');
    const std::string second(64, 'b');
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{first})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == first);
    auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 1);
    GLYPHA_REQUIRE(stats[0].staged_entries == 0);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
    GLYPHA_REQUIRE(stats[0].hits == 1);

    // At the entry limit an overwrite may conservatively bypass admission; the
    // previous value must be evicted so the new authoritative Record is read cold.
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{second})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == second);
    stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].admission_bypasses == 1);
    GLYPHA_REQUIRE(stats[0].misses == 1);

    GLYPHA_REQUIRE((*runtime)->erase(std::as_bytes(std::span{key})).committed());
    stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].resident_bytes == 0);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
}

GLYPHA_TEST("durable runtime commits different Worker mutations concurrently") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        auto second = create_segment(*directory, store_id, entries[1]);
        GLYPHA_REQUIRE(first.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(second.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    const auto first_key = key_for_worker(0, 2, "parallel-a");
    const auto second_key = key_for_worker(1, 2, "parallel-b");
    std::atomic_bool failed{};
    std::thread first([&] {
        for (std::size_t iteration = 0; iteration < 8; ++iteration) {
            const auto value = std::string{"a-"} + std::to_string(iteration);
            if (!(*runtime)
                     ->put(std::as_bytes(std::span{first_key}), std::as_bytes(std::span{value}))
                     .committed()) {
                failed.store(true, std::memory_order_relaxed);
            }
        }
    });
    std::thread second([&] {
        for (std::size_t iteration = 0; iteration < 8; ++iteration) {
            const auto value = std::string{"b-"} + std::to_string(iteration);
            if (!(*runtime)
                     ->put(std::as_bytes(std::span{second_key}), std::as_bytes(std::span{value}))
                     .committed()) {
                failed.store(true, std::memory_order_relaxed);
            }
        }
    });
    first.join();
    second.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(first_key)) == "a-7");
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(second_key)) == "b-7");
    GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 9);
    GLYPHA_REQUIRE((*runtime)->next_sequence(1)->value == 9);
}

GLYPHA_TEST("rotation space preflight fails before sealing the active Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    RotationBudgetObserver observer{};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &observer,
                           .before = &RotationBudgetObserver::before,
                           .available_space_bytes = &RotationBudgetObserver::available});
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.reserved_free_bytes = 0;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string key{"rotation-budget"};
    const std::string value{"value"};
    const auto rejected = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
    GLYPHA_REQUIRE(!rejected.committed());
    GLYPHA_REQUIRE(rejected.error.has_value());
    GLYPHA_REQUIRE(rejected.error->code == glyphastore::ErrorCode::storage_exhausted);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 1);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.front().role == glyphastore::ManifestSegmentRole::active);
    runtime->reset();

    auto inspection = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(inspection.has_value());
    auto segment = glyphastore::DurableSegmentFile::open(*inspection, segment_identity(store_id, active),
                                                         glyphastore::SegmentFileOpenMode::read_only);
    GLYPHA_REQUIRE(segment.has_value());
    GLYPHA_REQUIRE(segment->selected_commit().commit.state == glyphastore::PersistedSegmentState::active);
}

GLYPHA_TEST("active rotation retires old-generation hot-cache accounting") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingRecordRead observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &observer, .before = &BlockingRecordRead::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string old_key{"old-hot"};
    const std::string new_key{"new-hot"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{old_key}), std::as_bytes(std::span{value})).committed());
    GLYPHA_REQUIRE((*runtime)->hot_cache_stats()[0].resident_entries == 1);

    observer.force_next_record_write_full();
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{new_key}), std::as_bytes(std::span{value})).committed());
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 1);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
    GLYPHA_REQUIRE((*runtime)->get(old_key).has_value());
    GLYPHA_REQUIRE((*runtime)->get(new_key).has_value());
}

GLYPHA_TEST("durable runtime rotates a full active Segment before committing the Record") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const std::string fill_key{"fill"};
    const std::string maximum_value(
        glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - fill_key.size(), 'x');
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        for (std::uint64_t sequence = 1; sequence <= 63; ++sequence) {
            append_record(segment, sequence, fill_key, maximum_value);
        }
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const std::string next_key{"next"};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(runtime.has_value());
        const auto committed =
            (*runtime)->put(std::as_bytes(std::span{next_key}), std::as_bytes(std::span{maximum_value}));
        GLYPHA_REQUIRE(committed.committed());
        GLYPHA_REQUIRE(committed.sequence->value == 64);
        GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
        GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
        const auto visible = (*runtime)->get(next_key);
        GLYPHA_REQUIRE(visible.has_value());
        GLYPHA_REQUIRE(visible->bytes.size() == maximum_value.size());

        const auto observation = (*runtime)->maintenance_observation(0);
        GLYPHA_REQUIRE(observation.has_value());
        GLYPHA_REQUIRE(observation->compaction_candidate_worker == 0);
        GLYPHA_REQUIRE(observation->candidate_sealed_record_bytes ==
                       63ULL * glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_live_record_bytes == glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_dead_record_bytes == 62ULL * glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_dead_byte_ratio_bp ==
                       static_cast<std::uint32_t>(62ULL * 10'000ULL / 63ULL));

        GLYPHA_REQUIRE((*runtime)
                           ->put(std::as_bytes(std::span{fill_key}), std::as_bytes(std::span{maximum_value}))
                           .committed());
        const auto overwritten = (*runtime)->maintenance_observation(0);
        GLYPHA_REQUIRE(overwritten.has_value());
        GLYPHA_REQUIRE(overwritten->candidate_live_record_bytes == 0);
        GLYPHA_REQUIRE(overwritten->candidate_dead_record_bytes ==
                       overwritten->candidate_sealed_record_bytes);
        GLYPHA_REQUIRE(overwritten->candidate_dead_byte_ratio_bp == 10'000);
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 66);
    GLYPHA_REQUIRE((*reopened)->get(next_key).has_value());
    GLYPHA_REQUIRE((*reopened)->get(fill_key).has_value());
}

GLYPHA_TEST("durable group closes a pending batch before rotating a full Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const std::string first_key{"one!"};
    const std::string second_key{"two!"};
    const std::string maximum_value(
        glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - first_key.size(), 'x');
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        for (std::uint64_t sequence = 1; sequence <= 62; ++sequence) {
            append_record(segment, sequence, "fill", maximum_value);
        }
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    SyncThreadObserver observer;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(),
            glyphastore::FilesystemHooks{.context = &observer, .before = &SyncThreadObserver::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
            std::move(*directory), 0,
            {.commit_sync = glyphastore::SegmentCommitSync::immediate,
             .sync_interval_ms = 50,
             .batch = glyphastore::DurableGroupConfig{.max_records = 2,
                                                      .max_bytes = 2U * glyphastore::kMaxNormalRecordSize,
                                                      .max_wait_ms = 50},
             .strict_ack = true});
        GLYPHA_REQUIRE(runtime.has_value());

        std::atomic committed{0};
        std::array<std::thread::id, 2> producer_threads{};
        const auto put = [&](const std::size_t producer, const std::string& key) {
            producer_threads[producer] = std::this_thread::get_id();
            const auto result =
                (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{maximum_value}));
            if (result.committed()) {
                committed.fetch_add(1);
            }
        };
        std::thread first{[&] { put(0, first_key); }};
        std::thread second{[&] { put(1, second_key); }};
        first.join();
        second.join();

        GLYPHA_REQUIRE(committed.load() == 2);
        GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
        GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 65);
        GLYPHA_REQUIRE((*runtime)->get(first_key).has_value());
        GLYPHA_REQUIRE((*runtime)->get(second_key).has_value());
        const std::lock_guard lock{observer.mutex};
        GLYPHA_REQUIRE(!observer.sync_threads.empty());
        GLYPHA_REQUIRE(std::ranges::none_of(observer.sync_threads, [&](const std::thread::id thread) {
            return thread == producer_threads[0] || thread == producer_threads[1];
        }));
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 65);
    GLYPHA_REQUIRE((*reopened)->get(first_key).has_value());
    GLYPHA_REQUIRE((*reopened)->get(second_key).has_value());
}

GLYPHA_TEST("one-Worker durable group commits on the dedicated commit executor") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    SyncThreadObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &SyncThreadObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 0;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true,
         .limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    std::array<std::thread::id, 2> producer_threads{};
    std::atomic committed{0};
    const auto put = [&](const std::size_t producer, const std::string key) {
        producer_threads[producer] = std::this_thread::get_id();
        const std::string value{"value"};
        if ((*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed()) {
            committed.fetch_add(1, std::memory_order_relaxed);
        }
    };
    std::thread first{put, 0, "first"};
    std::thread second{put, 1, "second"};
    first.join();
    second.join();

    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed) == 2);
    std::thread::id sync_thread;
    {
        const std::lock_guard lock{observer.mutex};
        sync_thread = observer.sync_thread;
    }
    GLYPHA_REQUIRE(sync_thread != std::thread::id{});
    GLYPHA_REQUIRE(sync_thread != producer_threads[0]);
    GLYPHA_REQUIRE(sync_thread != producer_threads[1]);
    GLYPHA_REQUIRE((*runtime)->get("first").has_value());
    GLYPHA_REQUIRE((*runtime)->get("second").has_value());
    const auto cache = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(cache[0].resident_entries == 0);
    GLYPHA_REQUIRE(cache[0].admission_bypasses == 2);
    GLYPHA_REQUIRE(cache[0].misses == 2);
}

GLYPHA_TEST("one-Worker commit executor bounds admission at the batch record limit") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BatchBoundaryObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &BatchBoundaryObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    static constexpr std::size_t kProducerCount = 32;
    std::barrier start{static_cast<std::ptrdiff_t>(kProducerCount + 1)};
    std::atomic committed{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (std::size_t producer = 0; producer < kProducerCount; ++producer) {
        producers.emplace_back([&, producer] {
            start.arrive_and_wait();
            const auto key = std::string{"key-"} + std::to_string(producer);
            const std::string value{"value"};
            if ((*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed()) {
                committed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    start.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }

    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed) == kProducerCount);
    const std::lock_guard lock{observer.mutex};
    GLYPHA_REQUIRE(observer.maximum_writes_before_sync == 2);
    GLYPHA_REQUIRE(observer.writes_since_sync == 0);
    GLYPHA_REQUIRE(observer.sync_count == kProducerCount / 2);
}

GLYPHA_TEST("explicit flush completes a partial sequenced durable group") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    RecordWriteObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &RecordWriteObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 32, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    std::atomic committed{false};
    std::thread producer{[&] {
        const std::string key{"partial"};
        const std::string value{"value"};
        committed.store(
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed(),
            std::memory_order_relaxed);
    }};
    {
        std::unique_lock lock{observer.mutex};
        GLYPHA_REQUIRE(observer.written.wait_for(lock, std::chrono::seconds{5},
                                                 [&] { return observer.record_written; }));
    }
    GLYPHA_REQUIRE((*runtime)->flush().has_value());
    producer.join();
    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE((*runtime)->get("partial").has_value());
}

GLYPHA_TEST("durable runtime reports an indeterminate slot sync and recovery resolves one boundary") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_commit_slot};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(
            temporary.path(), 0,
            glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
        GLYPHA_REQUIRE(runtime.has_value());
        const std::string key{"uncertain"};
        const std::string value{"value"};
        const auto result = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableMutationOutcome::indeterminate);
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(!(*runtime)->healthy());
    }
    GLYPHA_REQUIRE(failure.fired);

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    const auto next = (*recovered)->next_sequence(0)->value;
    GLYPHA_REQUIRE(next == 1 || next == 2);
    const auto resolved = (*recovered)->get("uncertain");
    if (next == 1) {
        GLYPHA_REQUIRE(!resolved.has_value());
        GLYPHA_REQUIRE(resolved.error().code == glyphastore::ErrorCode::not_found);
    } else {
        GLYPHA_REQUIRE(resolved.has_value());
        GLYPHA_REQUIRE(owned_text(*resolved) == "value");
    }
}

GLYPHA_TEST("durable mutation fault matrix preserves pre and post commit recovery oracles") {
    static constexpr std::array boundaries{
        glyphastore::FilesystemOperation::write_record,
        glyphastore::FilesystemOperation::sync_record,
        glyphastore::FilesystemOperation::write_commit_slot,
        glyphastore::FilesystemOperation::sync_commit_slot,
    };
    static constexpr std::array failures{
        glyphastore::ErrorCode::io_error,
        glyphastore::ErrorCode::storage_exhausted,
        glyphastore::ErrorCode::read_only_filesystem,
    };

    for (const auto boundary : boundaries) {
        for (const auto failure_code : failures) {
            RecoveryTemporaryDirectory temporary;
            const auto store_id = recovery_store_id();
            const glyphastore::ManifestSegmentEntry active{
                .segment_id = glyphastore::SegmentId{1},
                .generation = glyphastore::GenerationId{1},
                .owner_worker = glyphastore::WorkerId{0},
                .role = glyphastore::ManifestSegmentRole::active,
            };
            {
                auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
                GLYPHA_REQUIRE(directory.has_value());
                static_cast<void>(create_segment(*directory, store_id, active));
                GLYPHA_REQUIRE(
                    directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
            }

            OneShotFilesystemFailure failure{.target = boundary, .code = failure_code};
            {
                auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(
                    temporary.path(), 0,
                    glyphastore::FilesystemHooks{.context = &failure,
                                                 .before = &OneShotFilesystemFailure::before});
                GLYPHA_REQUIRE(runtime.has_value());
                const std::string key{"fault-matrix"};
                const std::string value{"value"};
                const auto result =
                    (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
                GLYPHA_REQUIRE(!result.committed());
                GLYPHA_REQUIRE(result.error.has_value());
                GLYPHA_REQUIRE(result.error->code == failure_code);
                GLYPHA_REQUIRE(result.outcome ==
                               (boundary == glyphastore::FilesystemOperation::sync_commit_slot
                                    ? glyphastore::DurableMutationOutcome::indeterminate
                                    : glyphastore::DurableMutationOutcome::not_committed));
            }
            GLYPHA_REQUIRE(failure.fired);

            auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
            GLYPHA_REQUIRE(recovered.has_value());
            const auto visible = (*recovered)->get("fault-matrix");
            if (boundary == glyphastore::FilesystemOperation::sync_commit_slot) {
                GLYPHA_REQUIRE(visible.has_value() ||
                               visible.error().code == glyphastore::ErrorCode::not_found);
                if (visible) {
                    GLYPHA_REQUIRE(owned_text(*visible) == "value");
                }
            } else {
                GLYPHA_REQUIRE(!visible.has_value());
                GLYPHA_REQUIRE(visible.error().code == glyphastore::ErrorCode::not_found);
            }
            GLYPHA_REQUIRE((*recovered)->verify_index().has_value());
        }
    }
}

GLYPHA_TEST("durable group flush failure wakes every batch waiter fail-closed") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    std::atomic outcomes{0};
    const auto put = [&](std::string key) {
        const std::string value{"value"};
        const auto result = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (result.outcome == glyphastore::DurableMutationOutcome::indeterminate) {
            outcomes.fetch_add(1);
        }
    };
    std::thread first{put, "first"};
    std::thread second{put, "second"};
    first.join();
    second.join();

    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(outcomes.load() == 2);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
}

GLYPHA_TEST("durable runtime close returns a sticky final flush failure") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::deferred, .sync_interval_ms = 60'000});
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string key{"close-failure"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed());

    const auto first = (*runtime)->close();
    GLYPHA_REQUIRE(!first.has_value());
    GLYPHA_REQUIRE(first.error().code == glyphastore::ErrorCode::io_error);
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
    const auto repeated = (*runtime)->close();
    GLYPHA_REQUIRE(!repeated.has_value());
    GLYPHA_REQUIRE(repeated.error().code == glyphastore::ErrorCode::io_error);
    const auto blocked = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
    GLYPHA_REQUIRE(blocked.outcome == glyphastore::DurableMutationOutcome::indeterminate);
}
