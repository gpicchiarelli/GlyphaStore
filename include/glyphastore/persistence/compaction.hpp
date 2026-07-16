#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/store/config.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace glyphastore {

// A v1 compaction replaces every sealed Segment owned by one Worker in one
// manifest generation. Replacements reuse the earliest source Segment IDs
// with incremented generations so their sequence ranges remain before the
// Worker's active Segment without changing the v1 manifest format.
struct DurableCompactionPlan {
    WorkerId worker_id{};
    Manifest next_manifest;
    std::vector<ManifestSegmentEntry> sources;
    std::vector<ManifestSegmentEntry> replacements;
    std::uint64_t temporary_bytes{};
    std::uint64_t reclaimed_bytes{};
};

[[nodiscard]] auto durable_compaction_output_segments(std::uint64_t live_encoded_bytes)
    -> Result<std::size_t>;

// Returns the deterministic replacement count when next is exactly the v1
// whole-Worker transition derived from current.
[[nodiscard]] auto validate_durable_compaction_transition(const Manifest& current, const Manifest& next,
                                                          WorkerId worker_id) -> Result<std::size_t>;

[[nodiscard]] auto plan_durable_worker_compaction(const Manifest& current, WorkerId worker_id,
                                                  std::size_t output_segment_count,
                                                  const DurableResourceLimits& limits)
    -> Result<DurableCompactionPlan>;

} // namespace glyphastore
