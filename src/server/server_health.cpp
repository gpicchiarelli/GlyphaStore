#include "server/server_health.hpp"

#include "glyphastore/server/server.hpp"
#include "store/store_internal.hpp"

namespace glyphastore::server {

auto ServerHealth::live(const bool started, const bool healthy) noexcept -> bool {
    return started && healthy;
}

auto ServerHealth::ready(const bool is_live, const bool stop_requested, const bool store_operational,
                         const MaintenanceSnapshot& maintenance) noexcept -> bool {
    if (!is_live) {
        return false;
    }
    if (stop_requested) {
        return false;
    }
    if (!store_operational) {
        return false;
    }
    if (maintenance.mutations_rejected) {
        return false;
    }
    if (maintenance.state == MaintenanceState::faulted && maintenance.last_error.has_value()) {
        return false;
    }
    return true;
}

auto Server::live() const noexcept -> bool {
    return ServerHealth::live(started_.load(std::memory_order_acquire), healthy());
}

auto Server::ready() const noexcept -> bool {
    return ServerHealth::ready(live(), stop_requested(), store_operational(), maintenance_snapshot());
}

auto Server::store_operational() const noexcept -> bool {
    return detail::StoreAccess::operational(*store_);
}

auto Server::maintenance_snapshot() const -> MaintenanceSnapshot {
    return store_->maintenance_snapshot();
}

} // namespace glyphastore::server
