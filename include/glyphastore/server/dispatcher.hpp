#pragma once

#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/wakeup.hpp"
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

struct DispatchTask {
    RequestOpcode opcode{RequestOpcode::get};
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::uint64_t key_hash{};
    std::uint64_t expire_at_ns{};
    std::string key;
    std::vector<std::byte> value;
};

struct DispatchCompletion {
    ConnectionToken connection;
    ResponseStatus status{ResponseStatus::ok};
    std::uint64_t request_id{};
    std::vector<std::byte> value;
};

class WorkerDispatcher final {
  public:
    WorkerDispatcher(Store& store, Wakeup& wakeup, std::size_t inbox_capacity,
                     std::size_t completion_capacity);
    ~WorkerDispatcher();

    WorkerDispatcher(const WorkerDispatcher&) = delete;
    auto operator=(const WorkerDispatcher&) -> WorkerDispatcher& = delete;
    WorkerDispatcher(WorkerDispatcher&&) = delete;
    auto operator=(WorkerDispatcher&&) -> WorkerDispatcher& = delete;

    [[nodiscard]] auto try_submit(DispatchTask task) -> bool;
    [[nodiscard]] auto try_pop_completion() -> std::optional<DispatchCompletion>;
    [[nodiscard]] auto worker_count() const noexcept -> std::size_t {
        return executors_.size();
    }

  private:
    struct Executor {
        explicit Executor(std::size_t capacity) : inbox(capacity) {}

        BoundedMpscQueue<DispatchTask> inbox;
        std::atomic<std::size_t> pending{};
        std::mutex wait_mutex;
        std::condition_variable wait_condition;
        std::jthread thread;
    };

    void run(Executor& executor, std::stop_token stop);
    [[nodiscard]] auto execute(DispatchTask task) -> DispatchCompletion;
    void publish(DispatchCompletion completion, std::stop_token stop);

    Store& store_;
    Wakeup& wakeup_;
    BoundedMpscQueue<DispatchCompletion> completions_;
    std::vector<std::unique_ptr<Executor>> executors_;
};

} // namespace glyphastore::server
