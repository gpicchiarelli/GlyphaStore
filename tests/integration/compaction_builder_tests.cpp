#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/compaction_builder.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <cstdlib>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class CompactionBuildDirectory final {
  public:
    CompactionBuildDirectory() {
        auto pattern =
            (std::filesystem::temp_directory_path() / "glyphastore-compaction-build-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~CompactionBuildDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

struct CopyWriteFailure {
    bool enabled{};
    bool fired{};

    static auto before(void* context, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& failure = *static_cast<CopyWriteFailure*>(context);
        if (!failure.enabled || operation != glyphastore::FilesystemOperation::write_record) {
            return {};
        }
        failure.fired = true;
        return glyphastore::fail(glyphastore::ErrorCode::io_error,
                                 "injected durable compaction copy failure");
    }
};

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto build_manifest() -> glyphastore::Manifest {
    return {
        .store_id = {std::byte{0x41}, std::byte{0x42}, std::byte{0x43}},
        .manifest_generation = 21,
        .worker_count = 1,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{4},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments =
            {
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
            },
    };
}

auto identity(const glyphastore::Manifest& manifest, const glyphastore::ManifestSegmentEntry& entry)
    -> glyphastore::SegmentHeaderIdentity {
    return {.store_id = manifest.store_id,
            .segment_id = entry.segment_id,
            .generation = entry.generation,
            .owner_worker = entry.owner_worker};
}

struct TestRecord {
    std::uint64_t sequence{};
    glyphastore::Opcode opcode{glyphastore::Opcode::put};
    std::string_view key;
    std::string_view value;
    std::uint64_t expire_at_ns{};
    glyphastore::ValueType type{glyphastore::ValueType::bytes};
    std::uint32_t flags{};
};

auto create_records(glyphastore::DataDirectory& directory, const glyphastore::Manifest& manifest,
                    const glyphastore::ManifestSegmentEntry& entry, const std::span<const TestRecord> records)
    -> std::vector<glyphastore::RecordRef> {
    auto created = glyphastore::DurableSegmentFile::create(directory, identity(manifest, entry));
    GLYPHA_REQUIRE(created.durable());
    std::vector<glyphastore::RecordRef> references;
    references.reserve(records.size());
    for (const auto& record : records) {
        const glyphastore::RecordInput input{
            .sequence = glyphastore::SequenceNumber{record.sequence},
            .opcode = record.opcode,
            .type = record.type,
            .flags = record.flags,
            .key_hash = glyphastore::hash_key(record.key),
            .expire_at_ns = record.expire_at_ns,
            .key = bytes(record.key),
            .value = bytes(record.value),
        };
        const auto encoded = glyphastore::encode_record(input);
        GLYPHA_REQUIRE(encoded.has_value());
        const auto offset = created.file->selected_commit().commit.committed_end;
        GLYPHA_REQUIRE(created.file->append_record(*encoded).committed());
        references.push_back({.segment_id = entry.segment_id,
                              .offset = glyphastore::RecordOffset{offset},
                              .size = glyphastore::RecordSize{static_cast<std::uint32_t>(encoded->size())},
                              .sequence = input.sequence,
                              .generation = entry.generation});
    }
    if (entry.role == glyphastore::ManifestSegmentRole::sealed) {
        GLYPHA_REQUIRE(created.file->seal().committed());
    } else {
        GLYPHA_REQUIRE(
            created.file->flush_pending_commit(glyphastore::SegmentCommitSync::immediate).committed());
    }
    return references;
}

struct BuildFixture {
    glyphastore::Manifest manifest;
    glyphastore::Index index;
    glyphastore::RecordRef live_a;
    glyphastore::RecordRef expired;
    glyphastore::RecordRef replacement;
    glyphastore::RecordRef active;
};

auto create_build_fixture(glyphastore::DataDirectory& directory) -> BuildFixture {
    auto manifest = build_manifest();
    GLYPHA_REQUIRE(directory.publish_manifest(manifest).durable());
    const std::vector<TestRecord> first{
        {.sequence = 1, .key = "replacement", .value = "old"},
        {.sequence = 2, .key = "live-a", .value = "alpha", .type = glyphastore::ValueType::map, .flags = 17},
        {.sequence = 3, .key = "expired", .value = "stale", .expire_at_ns = 50},
    };
    const std::vector<TestRecord> second{
        {.sequence = 4, .key = "replacement", .value = "new"},
        {.sequence = 5, .key = "deleted", .value = "gone"},
        {.sequence = 6, .opcode = glyphastore::Opcode::erase, .key = "deleted", .value = {}},
    };
    const std::vector<TestRecord> active_records{
        {.sequence = 7, .key = "active", .value = "current"},
    };
    const auto first_refs = create_records(directory, manifest, manifest.segments[0], first);
    const auto second_refs = create_records(directory, manifest, manifest.segments[1], second);
    const auto active_refs = create_records(directory, manifest, manifest.segments[2], active_records);

    glyphastore::Index index;
    GLYPHA_REQUIRE(index.insert_or_assign("live-a", first_refs[1]).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign("expired", first_refs[2]).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign("replacement", second_refs[0]).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign("active", active_refs[0]).has_value());
    return {.manifest = std::move(manifest),
            .index = std::move(index),
            .live_a = first_refs[1],
            .expired = first_refs[2],
            .replacement = second_refs[0],
            .active = active_refs[0]};
}

auto text(const glyphastore::OwnedValue& value) -> std::string {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

} // namespace

GLYPHA_TEST("durable compaction builder copies exact visible Records and preserves v1 sequences") {
    CompactionBuildDirectory temporary;
    glyphastore::Manifest next;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto fixture = create_build_fixture(*directory);
        auto built = glyphastore::build_durable_worker_compaction(
            *directory, fixture.manifest, glyphastore::WorkerId{0}, fixture.index, 100);
        GLYPHA_REQUIRE(built.succeeded());
        GLYPHA_REQUIRE(built.prepared->plan.sources.size() == 2);
        GLYPHA_REQUIRE(built.prepared->plan.replacements.size() == 1);
        GLYPHA_REQUIRE(built.prepared->replacement_commits.size() == 1);
        GLYPHA_REQUIRE(built.prepared->stats.source_index_records_verified == 3);
        GLYPHA_REQUIRE(built.prepared->stats.records_copied == 2);
        GLYPHA_REQUIRE(built.prepared->stats.expired_records_dropped == 1);
        GLYPHA_REQUIRE(!built.prepared->index.find("expired").has_value());
        GLYPHA_REQUIRE(built.prepared->index.find("active") == fixture.active);
        const auto compacted_a = built.prepared->index.find("live-a");
        const auto compacted_replacement = built.prepared->index.find("replacement");
        GLYPHA_REQUIRE(compacted_a.has_value());
        GLYPHA_REQUIRE(compacted_replacement.has_value());
        GLYPHA_REQUIRE(compacted_a->sequence == fixture.live_a.sequence);
        GLYPHA_REQUIRE(compacted_replacement->sequence == fixture.replacement.sequence);
        GLYPHA_REQUIRE(compacted_a->generation == glyphastore::GenerationId{2});
        GLYPHA_REQUIRE(compacted_replacement->generation == glyphastore::GenerationId{2});
        GLYPHA_REQUIRE(directory->read_manifest().value() == fixture.manifest);
        GLYPHA_REQUIRE(directory->read_compaction_intent().has_value());
        next = built.prepared->plan.next_manifest;
        GLYPHA_REQUIRE(directory->publish_manifest(next).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest() == next);
    const auto live_a = (*runtime)->get("live-a", 100);
    const auto replacement = (*runtime)->get("replacement", 100);
    const auto active = (*runtime)->get("active", 100);
    GLYPHA_REQUIRE(live_a.has_value());
    GLYPHA_REQUIRE(replacement.has_value());
    GLYPHA_REQUIRE(active.has_value());
    GLYPHA_REQUIRE(text(*live_a) == "alpha");
    GLYPHA_REQUIRE(text(*replacement) == "new");
    GLYPHA_REQUIRE(text(*active) == "current");
    GLYPHA_REQUIRE(live_a->sequence == 2);
    GLYPHA_REQUIRE(replacement->sequence == 4);
    GLYPHA_REQUIRE(active->sequence == 7);
    GLYPHA_REQUIRE(!(*runtime)->get("expired", 100).has_value());
    GLYPHA_REQUIRE(!(*runtime)->get("deleted", 100).has_value());
}

GLYPHA_TEST("durable compaction builder prepares zero-output retirement without fabricating Records") {
    CompactionBuildDirectory temporary;
    glyphastore::Manifest next;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto fixture = create_build_fixture(*directory);
        glyphastore::Index active_only;
        GLYPHA_REQUIRE(active_only.insert_or_assign("active", fixture.active).has_value());
        auto built = glyphastore::build_durable_worker_compaction(*directory, fixture.manifest,
                                                                  glyphastore::WorkerId{0}, active_only, 100);
        GLYPHA_REQUIRE(built.succeeded());
        GLYPHA_REQUIRE(built.prepared->plan.replacements.empty());
        GLYPHA_REQUIRE(built.prepared->replacement_commits.empty());
        GLYPHA_REQUIRE(built.prepared->stats.records_copied == 0);
        GLYPHA_REQUIRE(built.prepared->index.find("active") == fixture.active);
        next = built.prepared->plan.next_manifest;
        GLYPHA_REQUIRE(directory->publish_manifest(next).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest() == next);
    GLYPHA_REQUIRE((*runtime)->get("active", 100).has_value());
    GLYPHA_REQUIRE(!(*runtime)->get("live-a", 100).has_value());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("durable compaction builder failure after intent is rolled back on reopen") {
    CompactionBuildDirectory temporary;
    const auto manifest = build_manifest();
    const glyphastore::ManifestSegmentEntry replacement{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{2},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::sealed,
    };
    CopyWriteFailure failure;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &CopyWriteFailure::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto fixture = create_build_fixture(*directory);
        failure.enabled = true;
        auto built = glyphastore::build_durable_worker_compaction(
            *directory, fixture.manifest, glyphastore::WorkerId{0}, fixture.index, 100);
        GLYPHA_REQUIRE(!built.succeeded());
        GLYPHA_REQUIRE(built.outcome == glyphastore::DurableCompactionBuildOutcome::recovery_required);
        GLYPHA_REQUIRE(built.error.has_value());
        GLYPHA_REQUIRE(failure.fired);
        GLYPHA_REQUIRE(directory->read_compaction_intent().has_value());
        GLYPHA_REQUIRE(std::filesystem::exists(
            temporary.path() / glyphastore::segment_filename(identity(manifest, replacement))));
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest() == manifest);
    GLYPHA_REQUIRE((*runtime)->get("live-a", 100).has_value());
    GLYPHA_REQUIRE((*runtime)->get("replacement", 100).has_value());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() /
                                            glyphastore::segment_filename(identity(manifest, replacement))));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}
