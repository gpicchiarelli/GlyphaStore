#pragma once

#include "glyphastore/persistence/durable_flush_coordinator.hpp"

#include <cstdint>
#include <mutex>

namespace glyphastore::detail {

// Build-tree-only bridge for deterministic generation-boundary tests.
class DurableFlushCoordinatorAccess final {
  public:
    static void set_flush_all_generation(DurableFlushCoordinator& coordinator,
                                         const std::uint64_t generation) {
        const std::lock_guard lock{coordinator.mutex_};
        coordinator.flush_all_generation_ = generation;
        coordinator.completed_generation_ = generation;
    }
};

} // namespace glyphastore::detail
