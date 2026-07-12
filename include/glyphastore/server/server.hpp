#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/dispatcher.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/server/thread_affinity.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
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
    [[nodiscard]] static auto create(const ReactorConfig& config = {}) -> Result<std::unique_ptr<Server>>;
    ~Server();

    Server(const Server&) = delete;
    auto operator=(const Server&) -> Server& = delete;
    Server(Server&&) = delete;
    auto operator=(Server&&) -> Server& = delete;

    [[nodiscard]] auto start() -> Status;
    void request_stop() noexcept;
    [[nodiscard]] auto join() -> Status;

    [[nodiscard]] auto port() const noexcept -> std::uint16_t;
    [[nodiscard]] auto executor_count() const noexcept -> std::size_t {
        return reactors_.size();
    }
    [[nodiscard]] auto accepted_connections_per_executor() const -> std::vector<std::size_t>;
    [[nodiscard]] auto executor_affinity_results() const -> std::vector<ExecutorAffinityResult>;
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return !failed_.load(std::memory_order_acquire);
    }

  private:
    Server(ReactorConfig config, std::unique_ptr<Store> store);
    void run(std::size_t executor_id) noexcept;

    ReactorConfig config_;
    std::unique_ptr<Store> store_;
    DispatchMesh mesh_;
    std::vector<std::unique_ptr<Reactor>> reactors_;
    std::vector<std::thread> threads_;
    std::vector<ExecutorAffinityResult> affinity_results_;
    std::atomic<bool> stop_requested_{};
    std::atomic<bool> started_{};
    std::atomic<bool> failed_{};
    std::mutex failure_mutex_;
    std::optional<Error> failure_;
};

} // namespace glyphastore::server
