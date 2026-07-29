#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/bounded_spsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/latency_histogram.hpp"
#include "glyphastore/server/pair_read_generation.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <vector>

namespace glyphastore::server {

enum class MutationKind : std::uint8_t { put, erase };

struct MutationCompletion final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t admission_bytes{};
    std::uint32_t payload_slot{};
    std::optional<Error> error;
};

struct PairWriterStats final {
    std::size_t worker_index{};
    std::uint64_t reader_safe_epoch{};
    std::uint64_t writer_epoch{};
    std::size_t queue_depth{};
    std::size_t queued_bytes{};
    std::size_t maximum_queue_depth{};
    std::size_t maximum_queued_bytes{};
    std::size_t payload_slot_capacity{};
    std::size_t payload_slots_in_use{};
    std::size_t maximum_payload_slots_in_use{};
    std::size_t payload_arena_capacity_bytes{};
    std::size_t payload_arena_storage_bytes{};
    std::size_t payload_arena_bytes_in_use{};
    std::size_t maximum_payload_arena_bytes_in_use{};
    std::size_t payload_admission_bytes_in_use{};
    std::size_t maximum_payload_admission_bytes_in_use{};
    std::uint64_t payload_slot_full_total{};
    std::uint64_t payload_arena_full_total{};
    std::uint64_t payload_too_large_total{};
    std::uint64_t admitted{};
    std::uint64_t rejected{};
    std::uint64_t expired_before_store{};
    std::uint64_t completed{};
    std::uint64_t conflict_retries{};
    std::uint64_t conflict_retry_commits{};
    std::uint64_t total_queue_wait_ns{};
    std::uint64_t maximum_queue_wait_ns{};
    std::uint64_t total_service_ns{};
    std::uint64_t maximum_service_ns{};
    std::uint64_t read_catalog_revision{};
    std::uint64_t read_refresh_attempts{};
    std::uint64_t read_refresh_successes{};
    std::uint64_t read_refresh_failures{};
    std::uint64_t read_refresh_deferrals{};
    std::uint64_t generations_retired{};
    std::size_t retired_generation_count{};
    std::size_t delta_entries{};
    std::size_t delta_record_versions{};
    std::size_t delta_arena_record_bytes{};
    std::size_t delta_arena_key_bytes{};
    std::size_t delta_arena_key_storage_bytes{};
    bool read_merge_active{};
    std::size_t read_merge_post_entries{};
    std::uint64_t read_merge_starts{};
    std::uint64_t read_merge_completions{};
    std::uint64_t read_merge_failures{};
    std::uint64_t read_merge_backpressure{};
    std::uint64_t read_merge_slots_processed{};
    LatencyHistogram queue_wait_histogram{};
    LatencyHistogram service_histogram{};
};

struct MutationRequest final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t worker_index{};
    MutationKind kind{};
    std::span<const std::byte> key;
    std::uint64_t key_hash{};
    std::span<const std::byte> value;
    std::uint64_t expire_at_ns{};
    BoundedSpscQueue<MutationCompletion>* completions{};
    Wakeup* wakeup{};
};

struct PairReadMergeConfig final {
    std::size_t delta_entries{8'192};
    std::size_t maximum_post_entries{32'736};
    std::size_t quantum_slots{4'096};
};

// Paired Writer runtime. Every Store shard owns exactly one persistent Writer
// thread and one bounded SPSC mutation lane.
// The paired Reader is the sole producer, so the normal mutation path contains
// no mutex, condition variable, allocation for queue cells, or competing
// mutator. The same lane serves volatile and durable Stores.
class PairWriterPool final {
  public:
    static constexpr std::size_t kMaximumRetiredReadGenerations = 64;
    static constexpr std::size_t kMaximumReaderLeaseEpochs = kMaximumRetiredReadGenerations + 1U;
    // Stable cross-platform accounting charge. Queue cells and payload storage
    // are preallocated, but preserving a metadata charge keeps the configured
    // byte-admission contract independent of ABI-specific sizeof values.
    static constexpr std::size_t kMutationAdmissionOverheadBytes = 128;

    [[nodiscard]] static auto create(Store& store, std::size_t worker_count, std::size_t capacity_per_worker,
                                     std::size_t payload_bytes_per_worker,
                                     std::chrono::milliseconds maximum_queue_wait,
                                     PairReadMergeConfig read_merge = {})
        -> Result<std::unique_ptr<PairWriterPool>>;
    ~PairWriterPool();

    PairWriterPool(const PairWriterPool&) = delete;
    auto operator=(const PairWriterPool&) -> PairWriterPool& = delete;
    PairWriterPool(PairWriterPool&&) = delete;
    auto operator=(PairWriterPool&&) -> PairWriterPool& = delete;

    [[nodiscard]] auto start() -> Status;
    // Copies the borrowed request bytes into the lane's preallocated slot pool
    // before returning. nullopt means bounded admission rejected the request.
    [[nodiscard]] auto try_submit(const MutationRequest& request) noexcept -> std::optional<std::size_t>;
    // Reader-only after acquiring the matching completion. False is an
    // internal FIFO/lifetime violation and must fail the Reactor closed.
    [[nodiscard]] auto release_payload(std::size_t worker_index, std::uint32_t payload_slot) noexcept -> bool;
    [[nodiscard]] static auto mutation_admission_bytes(std::size_t key_bytes,
                                                       std::size_t value_bytes) noexcept
        -> std::optional<std::size_t>;
    void note_rejected(std::size_t worker_index) noexcept;
    [[nodiscard]] auto stats() const -> std::vector<PairWriterStats>;
    // Acquire one immutable view per Reader event-loop turn and report the
    // adopted epoch. The paired Writer retains that epoch until a later turn
    // reports quiescence; GET performs no refcount or lock operation.
    // minimum_leased_epoch is the oldest generation still borrowed by an
    // asynchronous Reader operation, or UINT64_MAX when none exists. The
    // Writer may reclaim only generations older than the resulting safe epoch.
    [[nodiscard]] auto adopt_read_generation(
        std::size_t worker_index,
        std::uint64_t minimum_leased_epoch = std::numeric_limits<std::uint64_t>::max()) const noexcept
        -> const PairReadGeneration*;
    // Reader-side atomic poll. A stale durable catalog wakes only the paired
    // Writer; snapshot construction and publication never execute on Reader.
    void request_read_refresh(std::size_t worker_index) noexcept;
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return healthy_.load(std::memory_order_acquire);
    }
    // Stops admission and drains every admitted mutation before returning.
    // nullopt waits unbounded. A set deadline expires remaining queued (pre-Store)
    // work as unavailable once it elapses (including a zero deadline, which arms
    // expiry immediately); in-flight Store mutations are never cancelled.
    // Returns unavailable if the deadline expired before workers finished.
    [[nodiscard]] auto stop_and_drain(std::optional<std::chrono::milliseconds> deadline = std::nullopt)
        -> Status;

  private:
    struct Lane;

    PairWriterPool(Store& store, std::size_t worker_count, std::size_t capacity_per_worker,
                   std::size_t payload_bytes_per_worker, std::chrono::milliseconds maximum_queue_wait,
                   std::vector<std::shared_ptr<const PairReadGeneration>> initial_generations,
                   std::vector<std::uint64_t> initial_catalog_revisions, PairReadMergeConfig read_merge);
    void run(std::size_t worker_index) noexcept;
    void note_worker_exit() noexcept;
    [[nodiscard]] auto begin_submission() noexcept -> bool;
    void finish_submission() noexcept;

    static constexpr auto kAdmissionClosed = std::size_t{1}
                                             << (std::numeric_limits<std::size_t>::digits - 1U);
    static constexpr auto kAdmissionCountMask = kAdmissionClosed - 1U;

    Store& store_;
    const std::chrono::milliseconds maximum_queue_wait_;
    const PairReadMergeConfig read_merge_config_;
    std::vector<std::unique_ptr<Lane>> lanes_;
    std::atomic_size_t active_workers_{};
    std::atomic_size_t admission_state_{};
    std::atomic_bool started_{};
    std::atomic_bool stopping_{};
    std::atomic_bool expire_remaining_{};
    std::atomic_bool healthy_{true};
};

} // namespace glyphastore::server
