#include "glyphastore/segment/manager.hpp"

namespace glyphastore {

SegmentManager::SegmentManager(SegmentId first_id) : next_id_(first_id) {}

auto SegmentManager::allocate_active(WorkerId owner) -> SegmentPtr {
    auto segment = std::make_shared<Segment>(next_id_, owner);
    ++next_id_.value;
    catalog_.emplace(segment->id(), segment);
    segments_.push_back(segment);
    return segment;
}

auto SegmentManager::rotate_active(SegmentPtr active, WorkerId owner) -> Result<SegmentPtr> {
    if (!active || active->state() != SegmentState::active) {
        return fail(ErrorCode::invalid_argument, "rotate requires an active segment");
    }
    if (auto sealed = active->seal(); !sealed) {
        return unexpected(sealed.error());
    }
    return allocate_active(owner);
}

auto SegmentManager::find(SegmentId id) const -> SegmentPtr {
    const auto it = catalog_.find(id);
    if (it == catalog_.end()) {
        return {};
    }
    return it->second;
}

auto SegmentManager::try_retire(SegmentId id) -> Status {
    const auto segment = find(id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "segment is not in the catalog");
    }
    if (auto retired = segment->retire(); !retired) {
        return retired;
    }
    catalog_.erase(id);
    return {};
}

} // namespace glyphastore
