#include "glyphastore/persistence/recovery.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/segment_file.hpp"

#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace glyphastore {
namespace {

struct LatestRecord {
    RecordRef reference;
    std::uint64_t key_hash{};
    bool deleted{};
    bool expired{};
};

using LatestMap = std::unordered_map<std::string, LatestRecord>;

class RecoveryMemoryBudget final {
  public:
    explicit RecoveryMemoryBudget(const std::uint64_t maximum) : maximum_(maximum) {}

    [[nodiscard]] auto ensure_peak(const std::uint64_t additional) const -> Status {
        if (additional > maximum_ - retained_) {
            return fail(ErrorCode::resource_exhausted,
                        "durable recovery exceeds the configured memory budget");
        }
        return {};
    }

    [[nodiscard]] auto retain(const std::uint64_t additional) -> Status {
        if (auto available = ensure_peak(additional); !available) {
            return available;
        }
        retained_ += additional;
        return {};
    }

    [[nodiscard]] auto retain_repeated(const std::uint64_t unit, const std::size_t count) -> Status {
        if (unit != 0 && count > std::numeric_limits<std::uint64_t>::max() / unit) {
            return fail(ErrorCode::arithmetic_overflow, "durable recovery memory estimate overflow");
        }
        return retain(unit * static_cast<std::uint64_t>(count));
    }

  private:
    std::uint64_t maximum_{};
    std::uint64_t retained_{};
};

inline constexpr std::uint64_t kRecoveryBytesPerSegment = 256;
inline constexpr std::uint64_t kRecoveryBytesPerWorker = 1024;
inline constexpr std::uint64_t kRecoveryBytesPerKey = 256;

struct WorkerScanContext {
    WorkerId worker_id;
    std::size_t worker_count{};
    SegmentId segment_id;
    std::uint64_t now_ns{};
    SequenceNumber maximum_sequence{};
    LatestMap* latest{};
    RebuildStats* stats{};
    RecoveryMemoryBudget* memory_budget{};
};

auto segment_error(const ManifestSegmentEntry& entry, Error error) -> Unexpected {
    if (error.code == ErrorCode::not_found) {
        error.code = ErrorCode::corrupted_data;
    }
    error.message = "manifest Segment " + std::to_string(entry.segment_id.value) + ": " + error.message;
    return unexpected(std::move(error));
}

auto visit_recovery_record(void* opaque, const RecordRef& reference, const RecordView& record) -> Status {
    auto& context = *static_cast<WorkerScanContext*>(opaque);
    const auto computed_hash = hash_key(record.key);
    if (computed_hash != record.key_hash) {
        return fail(ErrorCode::corrupted_data,
                    "Segment " + std::to_string(context.segment_id.value) +
                        " contains a Record whose key hash does not match its key bytes");
    }
    if (route_worker(record.key_hash, context.worker_count) != context.worker_id.value) {
        return fail(ErrorCode::corrupted_data, "Segment " + std::to_string(context.segment_id.value) +
                                                   " contains a Record routed to another Worker");
    }

    ++context.stats->records_scanned;
    if (record.sequence.value > context.maximum_sequence.value) {
        context.maximum_sequence = record.sequence;
    }

    const auto key_bytes = static_cast<std::uint64_t>(record.key.size());
    if (key_bytes > (std::numeric_limits<std::uint64_t>::max() - kRecoveryBytesPerKey) / 2U) {
        return fail(ErrorCode::arithmetic_overflow, "durable recovery key memory estimate overflow");
    }
    const auto entry_bytes = kRecoveryBytesPerKey + key_bytes * 2U;
    if (auto memory = context.memory_budget->ensure_peak(entry_bytes); !memory) {
        return memory;
    }
    std::string key{record.key_string()};
    const auto current = context.latest->find(key);
    if (current != context.latest->end() &&
        current->second.reference.sequence.value == record.sequence.value) {
        return fail(ErrorCode::sequence_conflict,
                    "duplicate highest sequence for one key during durable recovery");
    }
    LatestRecord candidate{.reference = reference,
                           .key_hash = record.key_hash,
                           .deleted = record.opcode == Opcode::erase,
                           .expired = record.expired(context.now_ns)};
    if (current == context.latest->end()) {
        if (auto memory = context.memory_budget->retain(entry_bytes); !memory) {
            return memory;
        }
        context.latest->emplace(std::move(key), candidate);
    } else if (current->second.reference.sequence.value < record.sequence.value) {
        current->second = candidate;
    }
    return {};
}

auto validate_lifecycle(const ManifestSegmentEntry& entry, const SegmentCommit& commit,
                        bool& active_requires_rotation) -> Status {
    if (entry.role == ManifestSegmentRole::sealed) {
        if (commit.state != PersistedSegmentState::sealed) {
            return fail(ErrorCode::corrupted_data,
                        "manifest sealed Segment has an active persisted commit state");
        }
        return {};
    }
    if (commit.state == PersistedSegmentState::sealed) {
        active_requires_rotation = true;
    }
    return {};
}

} // namespace

auto recover_durable_state(DataDirectory& directory, const std::uint64_t now_ns,
                           const DurableResourceLimits& limits,
                           const DurableCompactionIntent* compaction_intent) -> Result<DurableRecoveryState> {
    if (auto valid = validate_durable_resource_limits(limits); !valid) {
        return unexpected(valid.error());
    }
    auto manifest = directory.read_manifest(limits.max_manifest_bytes);
    if (!manifest) {
        return unexpected(manifest.error());
    }
    if (auto resources = validate_durable_manifest_resources(*manifest, limits); !resources) {
        return unexpected(resources.error());
    }
    RecoveryMemoryBudget memory_budget{limits.max_recovery_memory_bytes};
    const auto manifest_memory = durable_manifest_bytes(manifest->segments.size());
    if (!manifest_memory) {
        return unexpected(manifest_memory.error());
    }
    if (auto memory = memory_budget.retain(*manifest_memory); !memory) {
        return unexpected(memory.error());
    }
    if (auto memory = memory_budget.retain_repeated(kRecoveryBytesPerSegment, manifest->segments.size());
        !memory) {
        return unexpected(memory.error());
    }
    if (auto memory = memory_budget.retain_repeated(kRecoveryBytesPerWorker, manifest->worker_count);
        !memory) {
        return unexpected(memory.error());
    }
    auto namespace_audit = audit_data_directory(directory, *manifest);
    if (!namespace_audit) {
        return unexpected(namespace_audit.error());
    }
    const auto safe =
        compaction_intent != nullptr
            ? validate_namespace_for_compaction_recovery(*namespace_audit, *manifest, *compaction_intent)
            : validate_namespace_for_recovery(*namespace_audit);
    if (!safe) {
        return unexpected(safe.error());
    }

    std::vector<std::size_t> worker_offsets(static_cast<std::size_t>(manifest->worker_count) + 1U);
    for (const auto& entry : manifest->segments) {
        ++worker_offsets[static_cast<std::size_t>(entry.owner_worker.value) + 1U];
    }
    for (std::size_t index = 1; index < worker_offsets.size(); ++index) {
        worker_offsets[index] += worker_offsets[index - 1U];
    }
    auto worker_cursors = worker_offsets;
    std::vector<std::size_t> entries_by_worker(manifest->segments.size());
    for (std::size_t index = 0; index < manifest->segments.size(); ++index) {
        const auto owner = manifest->segments[index].owner_worker.value;
        entries_by_worker[worker_cursors[owner]++] = index;
    }

    std::vector<RecoveredSegmentState> recovered_segments(manifest->segments.size());
    std::vector<RecoveredWorkerState> recovered_workers;
    recovered_workers.reserve(manifest->worker_count);
    DurableRecoveryStats recovery_stats{};

    for (std::size_t worker_index = 0; worker_index < manifest->worker_count; ++worker_index) {
        const auto worker_id = WorkerId{static_cast<std::uint32_t>(worker_index)};
        LatestMap latest;
        WorkerScanContext context{
            .worker_id = worker_id,
            .worker_count = manifest->worker_count,
            .now_ns = now_ns,
            .latest = &latest,
            .stats = &recovery_stats.rebuild,
            .memory_budget = &memory_budget,
        };
        SegmentId active_segment{};
        bool active_requires_rotation{};

        for (auto grouped_index = worker_offsets[worker_index];
             grouped_index < worker_offsets[worker_index + 1U]; ++grouped_index) {
            const auto catalog_index = entries_by_worker[grouped_index];
            const auto& entry = manifest->segments[catalog_index];
            const SegmentHeaderIdentity expected{
                .store_id = manifest->store_id,
                .segment_id = entry.segment_id,
                .generation = entry.generation,
                .owner_worker = entry.owner_worker,
            };
            auto file = DurableSegmentFile::open(directory, expected, SegmentFileOpenMode::read_only);
            if (!file) {
                return segment_error(entry, file.error());
            }

            bool segment_requires_rotation{};
            if (auto lifecycle =
                    validate_lifecycle(entry, file->selected_commit().commit, segment_requires_rotation);
                !lifecycle) {
                return segment_error(entry, lifecycle.error());
            }
            if (entry.role == ManifestSegmentRole::active) {
                active_segment = entry.segment_id;
                active_requires_rotation = segment_requires_rotation;
            }

            const auto& commit = file->selected_commit().commit;
            if (commit.record_count != 0 && context.maximum_sequence.value != 0 &&
                commit.first_sequence.value <= context.maximum_sequence.value) {
                return segment_error(
                    entry, Error{ErrorCode::corrupted_data,
                                 "committed sequence range overlaps or reverses a preceding Worker Segment"});
            }
            context.segment_id = entry.segment_id;
            if (auto scanned = file->visit_committed_records(&context, &visit_recovery_record); !scanned) {
                return segment_error(entry, scanned.error());
            }
            recovered_segments[catalog_index] = {
                .selected = file->selected_commit(),
            };
            ++recovery_stats.segments_scanned;
        }

        if (context.maximum_sequence.value == std::numeric_limits<std::uint64_t>::max()) {
            return fail(ErrorCode::arithmetic_overflow,
                        "Worker sequence space is exhausted during durable recovery");
        }

        const auto live_key_limit =
            durable_worker_live_key_limit(worker_index, manifest->worker_count, limits.max_live_keys);
        std::size_t visible_keys{};
        for (const auto& [key, record] : latest) {
            static_cast<void>(key);
            if (!record.deleted && !record.expired) {
                ++visible_keys;
            }
        }
        if (visible_keys > live_key_limit) {
            return fail(ErrorCode::resource_exhausted,
                        "durable recovery exceeds a Worker live-key budget partition");
        }

        Index index;
        if (auto reserved = index.reserve(latest.size()); !reserved) {
            return unexpected(reserved.error());
        }
        for (const auto& [key, record] : latest) {
            if (record.deleted) {
                ++recovery_stats.rebuild.tombstones;
                continue;
            }
            if (record.expired) {
                ++recovery_stats.rebuild.expired;
                continue;
            }
            const HashedKey hashed{.key = key, .hash = record.key_hash};
            if (auto inserted = index.insert_or_assign(hashed, record.reference); !inserted) {
                return unexpected(inserted.error());
            }
            ++recovery_stats.rebuild.records_visible;
        }

        if (active_requires_rotation) {
            ++recovery_stats.workers_requiring_rotation;
        }
        recovered_workers.push_back({
            .worker_id = worker_id,
            .index = std::move(index),
            .next_sequence = SequenceNumber{context.maximum_sequence.value + 1},
            .active_segment = active_segment,
            .active_requires_rotation = active_requires_rotation,
        });
    }

    return DurableRecoveryState{
        .manifest = std::move(*manifest),
        .namespace_audit = std::move(*namespace_audit),
        .segments = std::move(recovered_segments),
        .workers = std::move(recovered_workers),
        .stats = recovery_stats,
    };
}

} // namespace glyphastore
