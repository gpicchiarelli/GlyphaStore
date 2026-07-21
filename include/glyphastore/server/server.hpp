#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/server/thread_affinity.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace glyphastore::server {

class Server final {
  public:
    // A missing Store Worker count inherits ReactorConfig::worker_count. An
    // explicit Store count must match because each executor owns one Worker.
    [[nodiscard]] static auto create(const ReactorConfig& config = {}, StoreConfig store_config = {})
        -> Result<std::unique_ptr<Server>>;
    ~Server();

    Server(const Server&) = delete;
    auto operator=(const Server&) -> Server& = delete;
    Server(Server&&) = delete;
    auto operator=(Server&&) -> Server& = delete;

    [[nodiscard]] auto start() -> Status;
    void request_stop() noexcept;
    [[nodiscard]] auto join() -> Status;

    // Primary port for callers: cleartext when listening cleartext, else TLS.
    [[nodiscard]] auto port() const noexcept -> std::uint16_t;
    [[nodiscard]] auto cleartext_port() const noexcept -> std::uint16_t;
    [[nodiscard]] auto tls_port() const noexcept -> std::uint16_t;
    [[nodiscard]] auto executor_count() const noexcept -> std::size_t {
        return reactors_.size();
    }
    [[nodiscard]] auto adopted_connections_per_executor() const -> std::vector<std::size_t>;
    [[nodiscard]] auto active_connections_per_executor() const -> std::vector<std::size_t>;
    [[nodiscard]] auto executor_affinity_results() const -> std::vector<ExecutorAffinityResult>;
    [[nodiscard]] auto durable_mutation_stats() const -> std::vector<DurableMutationWorkerStats>;
    [[nodiscard]] auto durable_batch_stats() const -> std::vector<DurableBatchWorkerStats>;
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return !failed_.load(std::memory_order_acquire);
    }

  private:
    Server(ReactorConfig config, std::unique_ptr<Store> store);
    void run(std::size_t executor_id) noexcept;

    ReactorConfig config_;
    std::unique_ptr<Store> store_;
    std::unique_ptr<DiskReadExecutor> disk_reads_;
    std::unique_ptr<DurableMutationExecutor> durable_mutations_;
    ConnectionHandoffMesh mesh_;
    std::vector<std::unique_ptr<Reactor>> reactors_;
    std::vector<std::thread> threads_;
    std::vector<ExecutorAffinityResult> affinity_results_;
    std::atomic<bool> stop_requested_{};
    std::atomic<bool> started_{};
    std::atomic<bool> failed_{};
    std::atomic<bool> shutdown_drain_timed_out_{};
    std::mutex failure_mutex_;
    std::optional<Error> failure_;
    std::mutex shutdown_mutex_;
    std::optional<std::chrono::steady_clock::time_point> shutdown_deadline_{};
};

} // namespace glyphastore::server
