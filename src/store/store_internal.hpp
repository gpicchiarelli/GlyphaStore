#pragma once

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment.hpp"
#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/prepared_read.hpp"
#include "glyphastore/store/store.hpp"
#include "glyphastore/worker/worker.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace glyphastore::detail {

// Internal bridge for the native server and white-box tests. This header is
// never installed and is not part of the supported C++ API.
class StoreAccess final {
  public:
    struct PreparedGet final {
        std::optional<OwnedValue> value;
        std::optional<PreparedColdRead> cold;
    };

    [[nodiscard]] static auto get_owned(Store& store, std::size_t worker_index, const HashedKey& key,
                                        std::uint64_t now_ns) -> Result<OwnedValue>;
    // Returns an owning value/error when a GET can complete without file I/O;
    // an empty optional means the durable Record requires a pinned cold read.
    [[nodiscard]] static auto prepare_get_owned(Store& store, std::size_t worker_index, const HashedKey& key,
                                                std::uint64_t now_ns) -> Result<PreparedGet>;
    [[nodiscard]] static auto complete_get_owned(Store& store, std::size_t worker_index,
                                                 PreparedColdRead read,
                                                 const std::atomic_bool* cancelled = nullptr)
        -> Result<OwnedValue>;
    [[nodiscard]] static auto put(Store& store, std::size_t worker_index, const HashedKey& key,
                                  std::span<const std::byte> value, std::uint64_t expire_at_ns) -> Status;
    [[nodiscard]] static auto erase(Store& store, std::size_t worker_index, const HashedKey& key) -> Status;
    // Durable daemon path retaining the kernel's committed/not-committed/
    // indeterminate classification. This is required for a bounded internal
    // retry without inferring safety from a portable ErrorCode alone.
    [[nodiscard]] static auto put_durable(Store& store, std::size_t worker_index, const HashedKey& key,
                                          std::span<const std::byte> value, std::uint64_t expire_at_ns)
        -> DurableMutationResult;
    [[nodiscard]] static auto erase_durable(Store& store, std::size_t worker_index, const HashedKey& key)
        -> DurableMutationResult;
    // ErrorCode alone cannot authorize replay: committed and indeterminate
    // results are never retryable even if they carry sequence_conflict.
    [[nodiscard]] static auto should_retry_durable_mutation(const DurableMutationResult& result,
                                                            unsigned attempt) noexcept -> bool {
        return attempt == 0 && result.outcome == DurableMutationOutcome::not_committed && result.error &&
               result.error->code == ErrorCode::sequence_conflict;
    }
    [[nodiscard]] static auto is_durable(const Store& store) noexcept -> bool;
    [[nodiscard]] static auto worker_routing(const Store& store) noexcept -> WorkerRoutingState;
    [[nodiscard]] static auto batch_stats(const Store& store) -> std::vector<DurableBatchWorkerStats>;
    [[nodiscard]] static auto maintenance_controller(Store& store) noexcept -> MaintenanceController*;
    static void report_foreground_latency(Store& store, std::uint64_t latency_ns) noexcept;
    [[nodiscard]] static auto maintenance_mutations_rejected(const Store& store) noexcept -> bool;
    // True while the Store accepts new operations and any durable catalog remains healthy.
    [[nodiscard]] static auto operational(const Store& store) noexcept -> bool;

    [[nodiscard]] static auto worker(const Store& store, std::size_t index) noexcept -> const Worker&;
    [[nodiscard]] static auto segments(const Store& store) -> std::vector<SegmentPtr>;
    // Durable-only. Lexicographically sorted live Index keys for offline tools.
    [[nodiscard]] static auto snapshot_live_keys(Store& store) -> Result<std::vector<std::string>>;
    [[nodiscard]] static auto durable_manifest(const Store& store) -> Result<Manifest>;
};

} // namespace glyphastore::detail
