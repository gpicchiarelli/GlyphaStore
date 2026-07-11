#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <span>
#include <vector>

namespace glyphastore {

struct VacuumPolicy {
    double maximum_live_ratio{0.25};
    std::size_t maximum_segments_per_run{4};
};

struct VacuumStats {
    std::uint64_t records_copied{};
    std::uint64_t bytes_copied{};
    std::uint64_t old_segments{};
    std::uint64_t new_segments{};
};

struct VacuumResult {
    Index index;
    std::vector<SegmentPtr> segments;
    VacuumStats stats;
};

class VacuumPlanner final {
  public:
    explicit VacuumPlanner(VacuumPolicy policy = {}) : policy_(policy) {}
    [[nodiscard]] auto candidates(std::span<const SegmentPtr> segments) const -> std::vector<SegmentId>;

  private:
    VacuumPolicy policy_;
};

class VacuumBuilder final {
  public:
    [[nodiscard]] auto rebuild(const Index& current_index, std::span<const SegmentPtr> current_segments,
                               SegmentId first_new_segment, WorkerId owner = {}) const
        -> Result<VacuumResult>;
};

} // namespace glyphastore
