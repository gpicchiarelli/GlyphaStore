#include "glyphastore/segment/global_manager.hpp"

namespace glyphastore {

GlobalSegmentManager::GlobalSegmentManager(const SegmentId first_id) : next_id_(first_id) {}

auto GlobalSegmentManager::register_segment(SegmentPtr segment) -> SegmentPtr {
    catalog_.emplace(segment->id(), segment);
    segments_.push_back(segment);
    return segment;
}

auto GlobalSegmentManager::allocate_active(const WorkerId owner) -> SegmentPtr {
    auto segment = register_segment(std::make_shared<Segment>(next_id_, owner));
    ++next_id_.value;
    return segment;
}

auto GlobalSegmentManager::rotate_active(SegmentPtr active, const WorkerId owner) -> Result<SegmentPtr> {
    if (!active || active->state() != SegmentState::active) {
        return fail(ErrorCode::invalid_argument, "rotate requires an active segment");
    }
    if (auto sealed = active->seal(); !sealed) {
        return unexpected(sealed.error());
    }
    auto segment = std::make_shared<Segment>(next_id_, owner);
    ++next_id_.value;
    return register_segment(std::move(segment));
}

auto GlobalSegmentManager::find(const SegmentId id) const -> SegmentPtr {
    const auto it = catalog_.find(id);
    if (it == catalog_.end()) {
        return {};
    }
    return it->second;
}

auto GlobalSegmentManager::try_retire(const SegmentId id) -> Status {
    const auto segment = find(id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "segment is not in the catalog");
    }
    if (auto retired = segment->retire(); !retired) {
        return retired;
    }
    catalog_.erase(id);
    retired_pool_.push_back(id);
    return {};
}

} // namespace glyphastore
