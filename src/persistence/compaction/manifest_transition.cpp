#include "glyphastore/persistence/compaction.hpp"

#include <limits>
#include <vector>

namespace glyphastore {

auto validate_durable_compaction_transition(const Manifest& current, const Manifest& next,
                                            const WorkerId worker_id) -> Result<std::size_t> {
    if (!encoded_manifest_size(current) || !encoded_manifest_size(next)) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition contains an invalid v1 manifest");
    }
    if (worker_id.value >= current.worker_count || current.store_id != next.store_id ||
        current.worker_count != next.worker_count || current.routing_algorithm != next.routing_algorithm ||
        current.routing_epoch != next.routing_epoch || current.next_segment_id != next.next_segment_id ||
        current.next_segment_generation != next.next_segment_generation ||
        current.manifest_generation == std::numeric_limits<std::uint64_t>::max() ||
        next.manifest_generation != current.manifest_generation + 1U) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition changes immutable catalog metadata");
    }

    std::vector<ManifestSegmentEntry> sources;
    for (const auto& entry : current.segments) {
        if (entry.owner_worker == worker_id && entry.role == ManifestSegmentRole::sealed) {
            sources.push_back(entry);
        }
    }
    if (sources.empty() || next.segments.size() > current.segments.size()) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition has no sealed source set or grows the catalog");
    }
    const auto retained_entries = current.segments.size() - sources.size();
    if (next.segments.size() < retained_entries) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition removes a non-source catalog entry");
    }
    const auto output_count = next.segments.size() - retained_entries;

    Manifest expected = current;
    expected.manifest_generation = next.manifest_generation;
    expected.segments.clear();
    expected.segments.reserve(next.segments.size());
    std::size_t replacements_remaining = output_count;
    for (const auto& entry : current.segments) {
        if (entry.owner_worker != worker_id || entry.role != ManifestSegmentRole::sealed) {
            expected.segments.push_back(entry);
            continue;
        }
        if (replacements_remaining == 0) {
            continue;
        }
        if (entry.generation.value == std::numeric_limits<std::uint32_t>::max()) {
            return fail(ErrorCode::arithmetic_overflow, "durable compaction Segment generation is exhausted");
        }
        auto replacement = entry;
        ++replacement.generation.value;
        expected.segments.push_back(replacement);
        --replacements_remaining;
    }
    if (expected != next) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction next manifest is not the canonical sealed-set replacement");
    }
    return output_count;
}

} // namespace glyphastore
