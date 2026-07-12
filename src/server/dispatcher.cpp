#include "glyphastore/server/dispatcher.hpp"

#include "glyphastore/core/key_hash.hpp"

#include <chrono>
#include <span>
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto response_status(const Error& error) noexcept -> ResponseStatus {
    switch (error.code) {
    case ErrorCode::not_found:
        return ResponseStatus::not_found;
    case ErrorCode::invalid_argument:
    case ErrorCode::record_too_large:
        return ResponseStatus::invalid_request;
    default:
        return ResponseStatus::internal_error;
    }
}

} // namespace

WorkerDispatcher::WorkerDispatcher(Store& store, Wakeup& wakeup, const std::size_t inbox_capacity,
                                   const std::size_t completion_capacity)
    : store_(store), wakeup_(wakeup), completions_(completion_capacity) {
    executors_.reserve(store_.worker_count());
    for (std::size_t index = 0; index < store_.worker_count(); ++index) {
        auto executor = std::make_unique<Executor>(inbox_capacity);
        auto* executor_pointer = executor.get();
        executor->thread = std::jthread(
            [this, executor_pointer](const std::stop_token stop) { run(*executor_pointer, stop); });
        executors_.push_back(std::move(executor));
    }
}

WorkerDispatcher::~WorkerDispatcher() {
    for (auto& executor : executors_) {
        executor->thread.request_stop();
        executor->wait_condition.notify_one();
    }
    executors_.clear();
}

auto WorkerDispatcher::try_submit(DispatchTask task) -> bool {
    const auto owner = route_worker(task.key_hash, executors_.size());
    auto& executor = *executors_[owner];
    executor.pending.fetch_add(1U, std::memory_order_release);
    if (!executor.inbox.try_push(std::move(task))) {
        executor.pending.fetch_sub(1U, std::memory_order_relaxed);
        return false;
    }
    executor.wait_condition.notify_one();
    return true;
}

auto WorkerDispatcher::try_pop_completion() -> std::optional<DispatchCompletion> {
    return completions_.try_pop();
}

void WorkerDispatcher::run(Executor& executor, const std::stop_token stop) {
    while (!stop.stop_requested()) {
        auto task = executor.inbox.try_pop();
        if (task) {
            executor.pending.fetch_sub(1U, std::memory_order_relaxed);
            publish(execute(std::move(*task)), stop);
            continue;
        }
        std::unique_lock lock{executor.wait_mutex};
        executor.wait_condition.wait_for(lock, std::chrono::milliseconds{10}, [&] {
            return stop.stop_requested() || executor.pending.load(std::memory_order_acquire) != 0;
        });
    }
}

auto WorkerDispatcher::execute(DispatchTask task) -> DispatchCompletion {
    DispatchCompletion completion{.connection = task.connection, .request_id = task.request_id};
    const HashedKey key{.key = task.key, .hash = task.key_hash};
    switch (task.opcode) {
    case RequestOpcode::get: {
        auto record = store_.get(key, task.expire_at_ns);
        if (record) {
            completion.value.assign(record->value.begin(), record->value.end());
        } else {
            completion.status = response_status(record.error());
        }
        break;
    }
    case RequestOpcode::put: {
        const auto stored = store_.put(key, task.value, task.expire_at_ns);
        if (!stored) {
            completion.status = response_status(stored.error());
        }
        break;
    }
    case RequestOpcode::erase: {
        const auto erased = store_.erase(key);
        if (!erased) {
            completion.status = response_status(erased.error());
        }
        break;
    }
    case RequestOpcode::hello:
    case RequestOpcode::ping:
        completion.status = ResponseStatus::invalid_request;
        break;
    }
    return completion;
}

void WorkerDispatcher::publish(DispatchCompletion completion, const std::stop_token stop) {
    while (!stop.stop_requested()) {
        if (completions_.try_push(std::move(completion))) {
            static_cast<void>(wakeup_.notify());
            return;
        }
        std::this_thread::yield();
    }
}

} // namespace glyphastore::server
