#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/recovery.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
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
