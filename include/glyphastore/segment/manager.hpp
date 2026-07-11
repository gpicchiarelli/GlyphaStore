#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <unordered_map>
#include <vector>

namespace glyphastore {

class SegmentManager final {
  public:
    explicit SegmentManager(SegmentId first_id = SegmentId{1});

    [[nodiscard]] auto allocate_active(WorkerId owner) -> SegmentPtr;
    [[nodiscard]] auto rotate_active(SegmentPtr active, WorkerId owner) -> Result<SegmentPtr>;
    [[nodiscard]] auto find(SegmentId id) const -> SegmentPtr;
    [[nodiscard]] auto segments() const noexcept -> const std::vector<SegmentPtr>& {
        return segments_;
    }
    [[nodiscard]] auto try_retire(SegmentId id) -> Status;

  private:
    SegmentId next_id_;
    std::unordered_map<SegmentId, SegmentPtr> catalog_;
    std::vector<SegmentPtr> segments_;
};

} // namespace glyphastore
