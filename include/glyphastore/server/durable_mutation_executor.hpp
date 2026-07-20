#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace glyphastore::server {

enum class DurableMutationKind : std::uint8_t { put, erase };

struct DurableMutationCompletion final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t admission_bytes{};
    std::optional<Error> error;
};

struct DurableMutationWorkerStats final {
    std::size_t worker_index{};
    std::size_t producer_threads{};
    std::size_t queue_depth{};
    std::size_t queued_bytes{};
    std::size_t maximum_queue_depth{};
    std::size_t maximum_queued_bytes{};
    std::uint64_t admitted{};
    std::uint64_t rejected{};
    std::uint64_t expired_before_store{};
    std::uint64_t completed{};
    std::uint64_t total_queue_wait_ns{};
    std::uint64_t maximum_queue_wait_ns{};
    std::uint64_t total_service_ns{};
    std::uint64_t maximum_service_ns{};
};

struct DurableMutationTask final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t worker_index{};
    DurableMutationKind kind{};
    std::string key;
    std::uint64_t key_hash{};
    std::vector<std::byte> value;
    std::uint64_t expire_at_ns{};
    std::size_t admission_bytes{};
    std::chrono::steady_clock::time_point admitted_at{};
    BoundedMpscQueue<DurableMutationCompletion>* completions{};
    Wakeup* wakeup{};
};

// One bounded FIFO lane per Store Worker. A lane mutex protects only queue
// admission/removal; durable Store work always runs after that mutex is
// released. Separate lanes prevent a slow Worker from serializing admission or
// execution for unrelated Workers.
class DurableMutationExecutor final {
  public:
    [[nodiscard]] static auto create(Store& store, std::size_t worker_count, std::size_t capacity_per_worker,
                                     std::size_t threads_per_worker,
                                     std::chrono::milliseconds maximum_queue_wait)
        -> Result<std::unique_ptr<DurableMutationExecutor>>;
    ~DurableMutationExecutor();

    DurableMutationExecutor(const DurableMutationExecutor&) = delete;
    auto operator=(const DurableMutationExecutor&) -> DurableMutationExecutor& = delete;
    DurableMutationExecutor(DurableMutationExecutor&&) = delete;
    auto operator=(DurableMutationExecutor&&) -> DurableMutationExecutor& = delete;

    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto try_submit(DurableMutationTask task) -> bool;
    void note_rejected(std::size_t worker_index) noexcept;
    [[nodiscard]] auto stats() const -> std::vector<DurableMutationWorkerStats>;
    // Stops admission and drains every admitted mutation before returning.
    // Zero deadline waits unbounded. A positive deadline expires remaining queued
    // (pre-Store) work as unavailable once it elapses; in-flight Store mutations
    // are never cancelled. Returns unavailable if the deadline expired.
    [[nodiscard]] auto stop_and_drain(std::chrono::milliseconds deadline = {}) -> Status;

  private:
    struct Lane;

    DurableMutationExecutor(Store& store, std::size_t worker_count, std::size_t capacity_per_worker,
                            std::size_t threads_per_worker, std::chrono::milliseconds maximum_queue_wait);
    void run(std::size_t worker_index) noexcept;
    void note_worker_exit() noexcept;

    Store& store_;
    const std::size_t threads_per_worker_;
    const std::chrono::milliseconds maximum_queue_wait_;
    std::vector<std::unique_ptr<Lane>> lanes_;
    std::mutex lifecycle_mutex_;
    std::condition_variable drained_;
    std::atomic_size_t active_workers_{};
    std::atomic_bool started_{};
    std::atomic_bool stopping_{};
    std::atomic_bool expire_remaining_{};
};

} // namespace glyphastore::server
