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
    bool fired{};

    static auto before(void* opaque, glyphastore::FilesystemOperation operation) -> glyphastore::Status {
        auto& state = *static_cast<OneShotFilesystemFailure*>(opaque);
        if (!state.fired && operation == state.target) {
            state.fired = true;
            return glyphastore::fail(glyphastore::ErrorCode::io_error,
                                     "injected durable runtime filesystem failure");
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
    append_record(first, 3, gone, "present");
    append_record(first, 4, gone, {}, glyphastore::Opcode::erase);
    append_record(first, 5, alpha, "new");
    append_record(first, 7, binary, "binary-value");
    GLYPHA_REQUIRE(first.seal().committed());

    auto second = create_segment(*directory, store_id, entries[1]);
    append_record(second, 2, alpha, "old");
    append_record(second, 6, expired, "stale", glyphastore::Opcode::put, 100);

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
    GLYPHA_REQUIRE(alpha_ref->segment_id.value == 1);
    GLYPHA_REQUIRE(alpha_ref->sequence.value == 5);
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
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::sequence_conflict);
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
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 65);
    GLYPHA_REQUIRE((*reopened)->get(next_key).has_value());
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
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
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
