#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/server/abuse_limits.hpp"
#include "glyphastore/server/durable_mutation_executor.hpp"
#include "glyphastore/store/maintenance_types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore::server {

struct ExecutorStats final {
    std::size_t active_connections{};
    std::size_t adopted_connections{};
};

// Consistent point-in-time view used by ServerStatsReporter. Collection of
// these fields is separate from ASCII serialization.
struct ServerStatsSnapshot final {
    bool live{};
    bool ready{};
    bool store_operational{};
    std::vector<ExecutorStats> executors;
    MaintenanceSnapshot maintenance;
    std::vector<DurableMutationWorkerStats> mutations;
    std::vector<DurableBatchWorkerStats> batches;
    AbuseStats abuse{};
};

class ServerStatsReporter final {
  public:
    ServerStatsReporter() = delete;

    [[nodiscard]] static auto render(const ServerStatsSnapshot& snapshot, std::size_t maximum_bytes)
        -> Result<std::string>;

    [[nodiscard]] static auto maintenance_state_name(MaintenanceState state) noexcept -> std::string_view;
    [[nodiscard]] static auto maintenance_pressure_name(MaintenancePressureLevel level) noexcept
        -> std::string_view;
    [[nodiscard]] static auto maintenance_skip_reason_name(MaintenanceSkipReason reason) noexcept
        -> std::string_view;
};

} // namespace glyphastore::server
