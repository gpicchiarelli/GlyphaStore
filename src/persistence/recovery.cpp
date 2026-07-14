#include "glyphastore/persistence/recovery.hpp"

#include "glyphastore/core/key_hash.hpp"
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

struct WorkerScanContext {
    WorkerId worker_id;
    std::size_t worker_count{};
    SegmentId segment_id;
    std::uint64_t now_ns{};
    SequenceNumber maximum_sequence{};
    LatestMap* latest{};
    RebuildStats* stats{};
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

auto recover_durable_state(DataDirectory& directory, const std::uint64_t now_ns)
    -> Result<DurableRecoveryState> {
    auto manifest = directory.read_manifest();
    if (!manifest) {
        return unexpected(manifest.error());
    }
    auto namespace_audit = audit_data_directory(directory, *manifest);
    if (!namespace_audit) {
        return unexpected(namespace_audit.error());
    }
    if (auto safe = validate_namespace_for_recovery(*namespace_audit); !safe) {
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
            auto file = DurableSegmentFile::open(directory, expected);
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
