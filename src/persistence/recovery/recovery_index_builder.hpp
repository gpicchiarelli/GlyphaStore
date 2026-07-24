#pragma once

#include "glyphastore/persistence/recovery.hpp"
#include "glyphastore/store/config.hpp"
#include "persistence/recovery/recovery_scanner.hpp"

namespace glyphastore::recovery {

class RecoveryIndexBuilder final {
  public:
    RecoveryIndexBuilder() = delete;

    [[nodiscard]] static auto build(WorkerId worker, std::size_t worker_index, std::size_t worker_count,
                                    const DurableResourceLimits& limits, WorkerScanResult&& scan,
                                    DurableRecoveryStats& recovery_stats)
        -> Result<RecoveredWorkerState>;
};

} // namespace glyphastore::recovery
