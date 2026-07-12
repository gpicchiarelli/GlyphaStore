#include "glyphastore/segment/global_manager.hpp"

#include <mutex>

namespace glyphastore {
namespace {

[[nodiscard]] auto find_in_catalog(const std::vector<SegmentPtr>& catalog, const SegmentId first_id,
                                   const SegmentId id) noexcept -> Segment* {
    if (id.value < first_id.value) {
        return nullptr;
    }
    const auto slot = id.value - first_id.value;
    if (slot >= catalog.size()) {
        return nullptr;
    }
    return catalog[slot].get();
}

} // namespace

GlobalSegmentManager::GlobalSegmentManager(const SegmentId first_id)
    : first_id_(first_id), next_id_(first_id) {}

auto GlobalSegmentManager::register_segment(SegmentPtr segment) -> SegmentPtr {
    const auto slot = segment->id().value - first_id_.value;
    if (catalog_.size() <= slot) {
        catalog_.resize(slot + 1U);
    }
    catalog_[slot] = segment;
    segments_.push_back(segment);
    return segment;
}

auto GlobalSegmentManager::allocate_active(const WorkerId owner) -> SegmentPtr {
    const std::lock_guard lock{mutex_};
    auto segment = register_segment(std::make_shared<Segment>(next_id_, owner));
    ++next_id_.value;
    return segment;
}

auto GlobalSegmentManager::rotate_active(SegmentPtr active, const WorkerId owner) -> Result<SegmentPtr> {
    const std::lock_guard lock{mutex_};
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

auto GlobalSegmentManager::find(const SegmentId id) const noexcept -> const Segment* {
    const std::lock_guard lock{mutex_};
    return find_in_catalog(catalog_, first_id_, id);
}

auto GlobalSegmentManager::find(const SegmentId id) noexcept -> Segment* {
    const std::lock_guard lock{mutex_};
    return find_in_catalog(catalog_, first_id_, id);
}

auto GlobalSegmentManager::segments() const -> std::vector<SegmentPtr> {
    const std::lock_guard lock{mutex_};
    return segments_;
}

auto GlobalSegmentManager::retired_pool() const -> std::vector<SegmentId> {
    const std::lock_guard lock{mutex_};
    return retired_pool_;
}

auto GlobalSegmentManager::try_retire(const SegmentId id) -> Status {
    const std::lock_guard lock{mutex_};
    auto* segment = find_in_catalog(catalog_, first_id_, id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "segment is not in the catalog");
    }
    if (auto retired = segment->retire(); !retired) {
        return retired;
    }
    const auto slot = id.value - first_id_.value;
    catalog_[slot].reset();
    retired_pool_.push_back(id);
    return {};
}

} // namespace glyphastore
