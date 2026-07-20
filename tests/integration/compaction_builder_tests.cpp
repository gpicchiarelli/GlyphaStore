#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/compaction_builder.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class ManualStoreClock final : public glyphastore::StoreClock {
  public:
    explicit ManualStoreClock(const std::uint64_t initial_now_ns) : now_ns_(initial_now_ns) {}

    [[nodiscard]] auto now_ns() const noexcept -> std::uint64_t override {
        return now_ns_.load(std::memory_order_acquire);
    }

    void set(const std::uint64_t now_ns) noexcept {
        now_ns_.store(now_ns, std::memory_order_release);
    }

  private:
    std::atomic_uint64_t now_ns_;
};

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

auto key_for_worker(const std::size_t worker, const std::size_t worker_count, const std::string_view prefix)
    -> std::string {
    for (std::size_t suffix = 0; suffix < 10'000; ++suffix) {
        auto candidate = std::string{prefix} + std::to_string(suffix);
        if (glyphastore::route_worker(candidate, worker_count) == worker) {
            return candidate;
        }
    }
    throw std::runtime_error("failed to construct a routed compaction test key");
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

GLYPHA_TEST("durable runtime installs and retires one Worker compaction atomically") {
    CompactionBuildDirectory temporary;
    const auto old = build_manifest();
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_build_fixture(*directory));
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string hot_key{"active-hot"};
    const std::string hot_value{"cached"};
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{hot_key}), std::as_bytes(std::span{hot_value}))
                       .committed());
    GLYPHA_REQUIRE((*runtime)->hot_cache_stats()[0].resident_entries == 1);
    const auto result = (*runtime)->compact_worker(0, 100);
    GLYPHA_REQUIRE(result.compacted());
    GLYPHA_REQUIRE(result.stats.source_index_records_verified == 2);
    GLYPHA_REQUIRE(result.stats.records_copied == 2);
    GLYPHA_REQUIRE(result.stats.expired_records_dropped == 0);
    const auto next = (*runtime)->manifest();
    GLYPHA_REQUIRE(next.manifest_generation == old.manifest_generation + 1U);
    GLYPHA_REQUIRE(next.segments.size() == 2);
    GLYPHA_REQUIRE(next.segments[0].segment_id == glyphastore::SegmentId{1});
    GLYPHA_REQUIRE(next.segments[0].generation == glyphastore::GenerationId{2});
    GLYPHA_REQUIRE(next.segments[1] == old.segments[2]);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().recovery_safe());
    GLYPHA_REQUIRE((*runtime)->verify_index().has_value());
    GLYPHA_REQUIRE(text(*(*runtime)->get("live-a", 100)) == "alpha");
    GLYPHA_REQUIRE(text(*(*runtime)->get("replacement", 100)) == "new");
    GLYPHA_REQUIRE(text(*(*runtime)->get("active", 100)) == "current");
    GLYPHA_REQUIRE(text(*(*runtime)->get(hot_key, 100)) == hot_value);
    GLYPHA_REQUIRE((*runtime)->hot_cache_stats()[0].resident_entries == 1);
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() /
                                            glyphastore::segment_filename(identity(old, old.segments[0]))));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() /
                                            glyphastore::segment_filename(identity(old, old.segments[1]))));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->manifest() == next);
    GLYPHA_REQUIRE((*reopened)->verify_index().has_value());
    GLYPHA_REQUIRE((*reopened)->get("live-a", 100)->sequence == 2);
    GLYPHA_REQUIRE((*reopened)->get("replacement", 100)->sequence == 4);
    GLYPHA_REQUIRE((*reopened)->get("active", 100)->sequence == 7);
    GLYPHA_REQUIRE((*reopened)->get(hot_key, 100)->sequence == 8);
}

GLYPHA_TEST("durable runtime fails closed when online compaction requires recovery") {
    CompactionBuildDirectory temporary;
    const auto old = build_manifest();
    CopyWriteFailure failure;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_build_fixture(*directory));
    }
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(
            temporary.path(), 100, {.context = &failure, .before = &CopyWriteFailure::before});
        GLYPHA_REQUIRE(runtime.has_value());
        failure.enabled = true;
        const auto result = (*runtime)->compact_worker(0, 100);
        GLYPHA_REQUIRE(!result.compacted());
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableCompactionOutcome::recovery_required);
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(failure.fired);
        GLYPHA_REQUIRE(!(*runtime)->healthy());
        GLYPHA_REQUIRE(!(*runtime)->get("active", 100).has_value());
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->manifest() == old);
    GLYPHA_REQUIRE((*reopened)->verify_index().has_value());
    GLYPHA_REQUIRE((*reopened)->get("live-a", 100).has_value());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("online compaction preserves another Worker's cached Segment after catalog compaction") {
    CompactionBuildDirectory temporary;
    const glyphastore::Manifest manifest{
        .store_id = {std::byte{0x51}, std::byte{0x52}, std::byte{0x53}},
        .manifest_generation = 9,
        .worker_count = 2,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{5},
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
                {.segment_id = glyphastore::SegmentId{4},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
    const auto first_key = key_for_worker(0, 2, "compact-first-");
    const auto second_key = key_for_worker(0, 2, "compact-second-");
    const auto active_key = key_for_worker(0, 2, "compact-active-");
    const auto other_key = key_for_worker(1, 2, "cached-other-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const std::vector<TestRecord> first{{.sequence = 1, .key = first_key, .value = "first"}};
        const std::vector<TestRecord> second{{.sequence = 2, .key = second_key, .value = "second"}};
        const std::vector<TestRecord> active{{.sequence = 3, .key = active_key, .value = "active"}};
        const std::vector<TestRecord> other{{.sequence = 1, .key = other_key, .value = "other"}};
        static_cast<void>(create_records(*directory, manifest, manifest.segments[0], first));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[1], second));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[2], active));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[3], other));
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path(), 100);
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE(text(*(*runtime)->get(other_key, 100)) == "other");
    const auto compacted = (*runtime)->compact_worker(0, 100);
    GLYPHA_REQUIRE(compacted.compacted());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 3);
    GLYPHA_REQUIRE(text(*(*runtime)->get(other_key, 100)) == "other");
    GLYPHA_REQUIRE(text(*(*runtime)->get(first_key, 100)) == "first");
    GLYPHA_REQUIRE(text(*(*runtime)->get(second_key, 100)) == "second");
    GLYPHA_REQUIRE((*runtime)->verify_index().has_value());
    const auto skipped = (*runtime)->compact_worker(1, 100);
    GLYPHA_REQUIRE(!skipped.compacted());
    GLYPHA_REQUIRE(skipped.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(skipped.error.has_value());
    GLYPHA_REQUIRE(skipped.error->code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE(text(*(*runtime)->get(other_key, 100)) == "other");
}

GLYPHA_TEST("public Store compact drops sealed Index-resident expired puts") {
    CompactionBuildDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_build_fixture(*directory));
    }

    // Recover before expiry so the Index still names the sealed expired put.
    const auto clock = std::make_shared<ManualStoreClock>(40);
    auto store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .clock = clock,
    });
    GLYPHA_REQUIRE(store.has_value());
    GLYPHA_REQUIRE((*store)->get("expired").has_value());

    // Advance time without a GET reclaim so sealed compaction owns the TTL drop.
    clock->set(100);
    const auto compacted = (*store)->compact();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->compacted);
    GLYPHA_REQUIRE(compacted->worker_index == 0);
    GLYPHA_REQUIRE(compacted->source_records_verified == 3);
    GLYPHA_REQUIRE(compacted->records_copied == 2);
    GLYPHA_REQUIRE(compacted->expired_records_dropped == 1);
    GLYPHA_REQUIRE(text(*(*store)->get("live-a")) == "alpha");
    GLYPHA_REQUIRE(text(*(*store)->get("replacement")) == "new");
    GLYPHA_REQUIRE(text(*(*store)->get("active")) == "current");
    const auto expired = (*store)->get("expired");
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*store)->verify_index().has_value());
    GLYPHA_REQUIRE((*store)->close().has_value());

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .clock = clock,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(text(*(*reopened)->get("live-a")) == "alpha");
    GLYPHA_REQUIRE(!(*reopened)->get("expired").has_value());
}

GLYPHA_TEST("public Store compacts one scheduled Worker and preserves restart visibility") {
    CompactionBuildDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_build_fixture(*directory));
    }

    auto store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(store.has_value());
    const auto compacted = (*store)->compact();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->compacted);
    GLYPHA_REQUIRE(compacted->worker_index == 0);
    GLYPHA_REQUIRE(compacted->source_records_verified == 2);
    GLYPHA_REQUIRE(compacted->records_copied == 2);
    GLYPHA_REQUIRE(compacted->expired_records_dropped == 0);
    GLYPHA_REQUIRE(text(*(*store)->get("live-a")) == "alpha");
    GLYPHA_REQUIRE(text(*(*store)->get("replacement")) == "new");
    GLYPHA_REQUIRE((*store)->verify_index().has_value());
    GLYPHA_REQUIRE((*store)->close().has_value());

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(text(*(*reopened)->get("live-a")) == "alpha");
    GLYPHA_REQUIRE(text(*(*reopened)->get("replacement")) == "new");
}

GLYPHA_TEST("public Store compaction scheduler advances one Worker per call") {
    CompactionBuildDirectory temporary;
    const glyphastore::Manifest manifest{
        .store_id = {std::byte{0x61}, std::byte{0x62}, std::byte{0x63}},
        .manifest_generation = 12,
        .worker_count = 2,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{7},
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
                {.segment_id = glyphastore::SegmentId{4},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{5},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{6},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
    const auto worker_zero_first = key_for_worker(0, 2, "schedule-zero-first-");
    const auto worker_zero_second = key_for_worker(0, 2, "schedule-zero-second-");
    const auto worker_one_first = key_for_worker(1, 2, "schedule-one-first-");
    const auto worker_one_second = key_for_worker(1, 2, "schedule-one-second-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const std::vector<TestRecord> zero_first{
            {.sequence = 1, .key = worker_zero_first, .value = "zero-first"}};
        const std::vector<TestRecord> zero_second{
            {.sequence = 2, .key = worker_zero_second, .value = "zero-second"}};
        const std::vector<TestRecord> one_first{
            {.sequence = 1, .key = worker_one_first, .value = "one-first"}};
        const std::vector<TestRecord> one_second{
            {.sequence = 2, .key = worker_one_second, .value = "one-second"}};
        static_cast<void>(create_records(*directory, manifest, manifest.segments[0], zero_first));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[1], zero_second));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[2], {}));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[3], one_first));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[4], one_second));
        static_cast<void>(create_records(*directory, manifest, manifest.segments[5], {}));
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
    }

    auto store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 2},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(store.has_value());
    const auto first = (*store)->compact();
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(first->compacted);
    GLYPHA_REQUIRE(first->worker_index == 0);
    const auto second = (*store)->compact();
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(second->compacted);
    GLYPHA_REQUIRE(second->worker_index == 1);
    const auto no_gain = (*store)->compact();
    GLYPHA_REQUIRE(no_gain.has_value());
    GLYPHA_REQUIRE(!no_gain->compacted);
    GLYPHA_REQUIRE(!no_gain->worker_index.has_value());
    GLYPHA_REQUIRE(text(*(*store)->get(worker_zero_first)) == "zero-first");
    GLYPHA_REQUIRE(text(*(*store)->get(worker_zero_second)) == "zero-second");
    GLYPHA_REQUIRE(text(*(*store)->get(worker_one_first)) == "one-first");
    GLYPHA_REQUIRE(text(*(*store)->get(worker_one_second)) == "one-second");
    GLYPHA_REQUIRE((*store)->verify_index().has_value());
}
