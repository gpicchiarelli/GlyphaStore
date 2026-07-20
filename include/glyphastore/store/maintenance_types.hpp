#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/store/config.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace glyphastore {

enum class MaintenanceState : std::uint8_t {
    stopped,
    idle,
    evaluating,
    compacting,
    suspended,
    draining,
    faulted,
};

enum class MaintenanceSkipReason : std::uint8_t {
    none,
    mode_disabled,
    mode_cooperative,
    no_gain,
    no_candidate,
    budget,
    store_closed,
    sequence_conflict,
    policy_deferred,
};

// Read-only physical observation for scheduling (no Worker Index access).
struct MaintenanceObservation {
    bool durable{};
    std::size_t segment_count{};
    std::size_t sealed_segment_count{};
    std::size_t max_segment_count{};
    std::uint64_t reserved_free_bytes{};
    std::optional<std::uint64_t> available_free_bytes{};
};

struct MaintenanceSnapshot {
    MaintenanceState state{MaintenanceState::stopped};
    bool thread_running{};
    MaintenanceMode mode{MaintenanceMode::cooperative};
    std::uint64_t evaluation_cycles{};
    std::uint64_t compact_attempts{};
    std::uint64_t compact_completed{};
    std::uint64_t skips{};
    std::uint64_t consecutive_no_gain{};
    std::uint64_t bytes_copied_window{};
    MaintenanceSkipReason last_skip_reason{MaintenanceSkipReason::none};
    MaintenanceObservation last_observation{};
    std::optional<Error> last_error{};
};

[[nodiscard]] auto validate_maintenance_config(const MaintenanceConfig& config) -> Status;

} // namespace glyphastore
