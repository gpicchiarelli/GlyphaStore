#pragma once

#include "glyphastore/store/maintenance_types.hpp"

namespace glyphastore::server {

// Pure health predicates. Collection of inputs stays with Server; this type only
// answers live/ready from already-observed state.
class ServerHealth final {
  public:
    ServerHealth() = delete;

    [[nodiscard]] static auto live(bool started, bool healthy) noexcept -> bool;
    [[nodiscard]] static auto ready(bool is_live, bool stop_requested, bool store_operational,
                                    const MaintenanceSnapshot& maintenance) noexcept -> bool;
};

} // namespace glyphastore::server
