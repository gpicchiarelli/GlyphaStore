#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/prepared_read.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
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

struct DiskReadCompletion final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::optional<OwnedValue> value;
    std::optional<Error> error;
};

struct DiskReadTask final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t worker_index{};
    detail::PreparedColdRead read;
    std::shared_ptr<std::atomic_bool> cancelled;
    std::size_t maximum_value_bytes{};
    BoundedMpscQueue<DiskReadCompletion>* completions{};
    Wakeup* wakeup{};
};

// Bounded process-wide executor for durable cold reads. It is public only so
// Reactor and Server can be separately compiled; applications configure it
// through ReactorConfig and never need to instantiate it directly.
class DiskReadExecutor final {
  public:
    [[nodiscard]] static auto create(Store& store, std::size_t thread_count, std::size_t capacity)
        -> Result<std::unique_ptr<DiskReadExecutor>>;
    ~DiskReadExecutor();

    DiskReadExecutor(const DiskReadExecutor&) = delete;
    auto operator=(const DiskReadExecutor&) -> DiskReadExecutor& = delete;
    DiskReadExecutor(DiskReadExecutor&&) = delete;
    auto operator=(DiskReadExecutor&&) -> DiskReadExecutor& = delete;

    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto try_submit(DiskReadTask task) -> bool;
    void stop() noexcept;

  private:
    DiskReadExecutor(Store& store, std::size_t thread_count, std::size_t capacity);
    void run() noexcept;

    Store& store_;
    const std::size_t thread_count_;
    std::vector<std::optional<DiskReadTask>> queue_;
    std::size_t head_{};
    std::size_t tail_{};
    std::size_t size_{};
    std::mutex mutex_;
    std::condition_variable available_;
    bool started_{};
    bool stopping_{};
    std::vector<std::thread> threads_;
};

} // namespace glyphastore::server
