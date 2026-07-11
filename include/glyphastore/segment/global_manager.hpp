#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <unordered_map>
#include <vector>

namespace glyphastore {

// Global Segment Manager: catalog, scan order, and retirement pools for all Workers.
class GlobalSegmentManager final {
  public:
    explicit GlobalSegmentManager(SegmentId first_id = SegmentId{1});

    [[nodiscard]] auto allocate_active(WorkerId owner) -> SegmentPtr;
    [[nodiscard]] auto rotate_active(SegmentPtr active, WorkerId owner) -> Result<SegmentPtr>;
    [[nodiscard]] auto find(SegmentId id) const -> SegmentPtr;
    [[nodiscard]] auto segments() const noexcept -> const std::vector<SegmentPtr>& {
        return segments_;
    }
    [[nodiscard]] auto retired_pool() const noexcept -> const std::vector<SegmentId>& {
        return retired_pool_;
    }
    [[nodiscard]] auto try_retire(SegmentId id) -> Status;

  private:
    [[nodiscard]] auto register_segment(SegmentPtr segment) -> SegmentPtr;

    SegmentId next_id_;
    std::vector<SegmentPtr> segments_;
    std::unordered_map<SegmentId, SegmentPtr> catalog_;
    std::vector<SegmentId> retired_pool_;
};

} // namespace glyphastore
