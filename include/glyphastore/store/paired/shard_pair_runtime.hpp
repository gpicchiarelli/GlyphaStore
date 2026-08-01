#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/latency_histogram.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/paired/read_generation.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace glyphastore {
class Store;
}

namespace glyphastore::store::paired {

enum class MutationKind : std::uint8_t { put, erase };

// Opaque per-request context. The runtime never interprets it; glyphastored
// carries its encoded ConnectionToken here.
struct MutationContext final {
    std::uint64_t value{};
};

struct MutationOutcome final {
    MutationContext context;
    std::uint64_t request_id{};
    std::size_t admission_bytes{};
    std::uint32_t payload_slot{};
    std::optional<Error> error;
};

// Writer-side completion sink for asynchronous submissions. deliver() must be
// non-blocking; returning false reports an internal FIFO/lifetime violation and
// terminates the process because the mutation has already been linearized.
struct MutationSink final {
    void* completions{};
    void* wakeup{};
    bool (*deliver)(void* completions, MutationOutcome outcome) noexcept {};
    void (*notify)(void* wakeup) noexcept {};
};

// Asynchronous submission. Key and value bytes are copied into the lane's
// preallocated payload slot before try_submit() returns, so the producer may
// recycle its input buffer immediately.
struct AsyncMutationRequest final {
    std::size_t shard{};
    MutationKind kind{};
    MutationContext context{};
    std::uint64_t request_id{};
    std::span<const std::byte> key;
    std::uint64_t key_hash{};
    std::span<const std::byte> value;
    std::uint64_t expire_at_ns{};
    MutationSink sink{};
};

struct ShardPairStats final {
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
    // Synchronous embedded put/erase handoffs executed by this Writer.
    std::uint64_t sync_admitted{};
    LatencyHistogram queue_wait_histogram{};
    LatencyHistogram service_histogram{};
};

// Paired Reader/Writer runtime owned by the Store (ADR 0031, ADR 0032). Every
// Worker/shard owns exactly one persistent Writer thread, one immutable
// published read generation, and the bounded lanes that feed it:
//
//   * embedded put/erase hand a stack-resident node to the owning Writer and
//     wait for completion, so same-shard callers serialize on the Writer lane
//     instead of the Index mutex;
//   * glyphastored hands off borrowed frame bytes through the bounded SPSC lane
//     and receives asynchronous completions on its Reader.
//
// Generation authority lives here and nowhere else: hosts adopt a published
// generation, they never publish one.
class ShardPairRuntime final {
  public:
    static constexpr std::size_t kMaximumRetiredReadGenerations = 64;
    static constexpr std::size_t kMaximumReaderLeaseEpochs = kMaximumRetiredReadGenerations + 1U;
    // Stable cross-platform accounting charge. Queue cells and payload storage
    // are preallocated, but preserving a metadata charge keeps the configured
    // byte-admission contract independent of ABI-specific sizeof values.
    static constexpr std::size_t kMutationAdmissionOverheadBytes = 128;

    [[nodiscard]] static auto create(Store& store, const PairedConcurrencyConfig& config)
        -> Result<std::unique_ptr<ShardPairRuntime>>;
    ~ShardPairRuntime();

    ShardPairRuntime(const ShardPairRuntime&) = delete;
    auto operator=(const ShardPairRuntime&) -> ShardPairRuntime& = delete;
    ShardPairRuntime(ShardPairRuntime&&) = delete;
    auto operator=(ShardPairRuntime&&) -> ShardPairRuntime& = delete;

    [[nodiscard]] auto shard_count() const noexcept -> std::size_t {
        return lanes_.size();
    }
    [[nodiscard]] auto async_lane_enabled() const noexcept -> bool;
    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return healthy_.load(std::memory_order_acquire);
    }
    // Stops admission and drains every admitted mutation before returning.
    // nullopt waits unbounded. A set deadline expires remaining queued (pre-Store)
    // work as unavailable once it elapses (including a zero deadline, which arms
    // expiry immediately); in-flight Store mutations are never cancelled.
    // Returns unavailable if the deadline expired before Writers finished.
    [[nodiscard]] auto stop_and_drain(std::optional<std::chrono::milliseconds> deadline = std::nullopt)
        -> Status;

    // Synchronous embedded mutation. Callable from any thread: the caller's key
    // and value stay borrowed for the whole call, the owning Writer linearizes
    // and publishes, and only then is the caller released.
    [[nodiscard]] auto mutate(std::size_t shard, MutationKind kind, const HashedKey& key,
                              std::span<const std::byte> value, std::uint64_t expire_at_ns) -> Status;

    // Counted read lease for embedded readers. The lease keeps the adopted
    // generation alive: the Writer retires generations eagerly and frees them
    // only at an observed quiescent point with no lease outstanding.
    class ReadLease final {
      public:
        ReadLease() noexcept = default;
        ReadLease(const ShardPairRuntime& runtime, std::size_t shard) noexcept;
        ~ReadLease();

        ReadLease(const ReadLease&) = delete;
        auto operator=(const ReadLease&) -> ReadLease& = delete;
        ReadLease(ReadLease&&) = delete;
        auto operator=(ReadLease&&) -> ReadLease& = delete;

        explicit operator bool() const noexcept {
            return generation_ != nullptr;
        }
        [[nodiscard]] auto generation() const noexcept -> const PairReadGeneration* {
            return generation_;
        }

      private:
        const ShardPairRuntime* runtime_{};
        std::size_t shard_{};
        const PairReadGeneration* generation_{};
    };

    // Asynchronous host lane (glyphastored). Copies the borrowed request bytes
    // into the lane's preallocated slot pool before returning; nullopt means
    // bounded admission rejected the request.
    [[nodiscard]] auto try_submit(const AsyncMutationRequest& request) noexcept -> std::optional<std::size_t>;
    // Host-only after acquiring the matching completion. False is an internal
    // FIFO/lifetime violation and must fail the host closed.
    [[nodiscard]] auto release_payload(std::size_t shard, std::uint32_t payload_slot) noexcept -> bool;
    [[nodiscard]] static auto mutation_admission_bytes(std::size_t key_bytes,
                                                       std::size_t value_bytes) noexcept
        -> std::optional<std::size_t>;
    void note_rejected(std::size_t shard) noexcept;
    [[nodiscard]] auto stats() const -> std::vector<ShardPairStats>;
    // Acquire one immutable view per Reader event-loop turn and report the
    // adopted epoch. The paired Writer retains that epoch until a later turn
    // reports quiescence; GET performs no refcount or lock operation.
    // minimum_leased_epoch is the oldest generation still borrowed by an
    // asynchronous Reader operation, or UINT64_MAX when none exists. The
    // Writer may reclaim only generations older than the resulting safe epoch.
    // Requires PairedConcurrencyConfig::reader_epoch_lease.
    [[nodiscard]] auto adopt_read_generation(
        std::size_t shard,
        std::uint64_t minimum_leased_epoch = std::numeric_limits<std::uint64_t>::max()) const noexcept
        -> const PairReadGeneration*;
    // Reader-side atomic poll. A stale durable catalog wakes only the paired
    // Writer; snapshot construction and publication never execute on Reader.
    void request_read_refresh(std::size_t shard) noexcept;

  private:
    struct Lane;
    struct SyncMutation;

    ShardPairRuntime(Store& store, PairedConcurrencyConfig config,
                     std::vector<std::shared_ptr<const PairReadGeneration>> initial_generations,
                     std::vector<std::uint64_t> initial_catalog_revisions);

    void run(std::size_t shard) noexcept;
    void note_writer_exit() noexcept;
    [[nodiscard]] auto begin_submission() noexcept -> bool;
    void finish_submission() noexcept;
    void wake(Lane& lane) noexcept;

    static constexpr auto kAdmissionClosed = std::size_t{1}
                                             << (std::numeric_limits<std::size_t>::digits - 1U);
    static constexpr auto kAdmissionCountMask = kAdmissionClosed - 1U;

    Store& store_;
    const PairedConcurrencyConfig config_;
    const std::chrono::milliseconds maximum_queue_wait_;
    std::vector<std::unique_ptr<Lane>> lanes_;
    std::atomic_size_t active_writers_{};
    std::atomic_size_t admission_state_{};
    std::atomic_bool started_{};
    std::atomic_bool stopping_{};
    std::atomic_bool expire_remaining_{};
    std::atomic_bool healthy_{true};
};

} // namespace glyphastore::store::paired
