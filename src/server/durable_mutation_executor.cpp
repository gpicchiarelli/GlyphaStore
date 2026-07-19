#include "glyphastore/server/durable_mutation_executor.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "store/store_internal.hpp"

#include <exception>
#include <utility>

namespace glyphastore::server {

struct DurableMutationExecutor::Lane final {
    explicit Lane(const std::size_t capacity) : queue(capacity) {}

    std::vector<std::optional<DurableMutationTask>> queue;
    std::size_t head{};
    std::size_t tail{};
    std::size_t size{};
    std::mutex mutex;
    std::condition_variable available;
    bool stopping{};
    std::thread thread;
};

DurableMutationExecutor::DurableMutationExecutor(Store& store, const std::size_t worker_count,
                                                 const std::size_t capacity_per_worker)
    : store_(store) {
    lanes_.reserve(worker_count);
    for (std::size_t worker = 0; worker < worker_count; ++worker) {
        lanes_.push_back(std::make_unique<Lane>(capacity_per_worker));
    }
}

DurableMutationExecutor::~DurableMutationExecutor() {
    stop_and_drain();
}

auto DurableMutationExecutor::create(Store& store, const std::size_t worker_count,
                                     const std::size_t capacity_per_worker)
    -> Result<std::unique_ptr<DurableMutationExecutor>> try {
    if (worker_count == 0 || worker_count != store.worker_count() || capacity_per_worker == 0) {
        return fail(ErrorCode::invalid_argument,
                    "durable mutation executor requires one nonempty lane per Store Worker");
    }
    return std::unique_ptr<DurableMutationExecutor>(
        new DurableMutationExecutor(store, worker_count, capacity_per_worker));
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "durable mutation executor allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "durable mutation executor construction failed");
}

auto DurableMutationExecutor::start() -> Status try {
    const std::lock_guard lifecycle_lock{lifecycle_mutex_};
    if (started_.load(std::memory_order_acquire)) {
        return fail(ErrorCode::invalid_argument, "durable mutation executor has already been started");
    }
    if (stopping_.load(std::memory_order_acquire)) {
        return fail(ErrorCode::unavailable, "durable mutation executor has been stopped");
    }
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        lanes_[worker]->thread = std::thread{[this, worker] { run(worker); }};
    }
    started_.store(true, std::memory_order_release);
    return {};
} catch (const std::exception& exception) {
    stop_and_drain();
    return fail(ErrorCode::io_error,
                std::string{"failed to start durable mutation executor: "} + exception.what());
} catch (...) {
    stop_and_drain();
    return fail(ErrorCode::io_error, "failed to start durable mutation executor");
}

auto DurableMutationExecutor::try_submit(DurableMutationTask task) -> bool {
    if (task.worker_index >= lanes_.size()) {
        return false;
    }
    auto& lane = *lanes_[task.worker_index];
    if (!started_.load(std::memory_order_acquire) || stopping_.load(std::memory_order_acquire)) {
        return false;
    }
    {
        const std::lock_guard lane_lock{lane.mutex};
        if (lane.stopping || lane.size == lane.queue.size()) {
            return false;
        }
        lane.queue[lane.tail].emplace(std::move(task));
        lane.tail = (lane.tail + 1U) % lane.queue.size();
        ++lane.size;
    }
    lane.available.notify_one();
    return true;
}

void DurableMutationExecutor::stop_and_drain() noexcept {
    {
        const std::lock_guard lifecycle_lock{lifecycle_mutex_};
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        for (auto& lane : lanes_) {
            {
                const std::lock_guard lane_lock{lane->mutex};
                lane->stopping = true;
            }
            lane->available.notify_all();
        }
    }
    for (auto& lane : lanes_) {
        if (lane->thread.joinable()) {
            lane->thread.join();
        }
    }
}

void DurableMutationExecutor::run(const std::size_t worker_index) noexcept {
    auto& lane = *lanes_[worker_index];
    while (true) {
        std::optional<DurableMutationTask> task;
        {
            std::unique_lock lane_lock{lane.mutex};
            lane.available.wait(lane_lock, [&lane] { return lane.stopping || lane.size != 0; });
            if (lane.size == 0) {
                return;
            }
            task.emplace(std::move(*lane.queue[lane.head]));
            lane.queue[lane.head].reset();
            lane.head = (lane.head + 1U) % lane.queue.size();
            --lane.size;
        }

        DurableMutationCompletion completion{.connection = task->connection,
                                             .request_id = task->request_id,
                                             .admission_bytes = task->admission_bytes};
        try {
            const HashedKey key{.key = task->key, .hash = task->key_hash};
            auto result =
                task->kind == DurableMutationKind::put
                    ? detail::StoreAccess::put(store_, worker_index, key, task->value, task->expire_at_ns)
                    : detail::StoreAccess::erase(store_, worker_index, key);
            if (!result) {
                completion.error.emplace(std::move(result.error()));
            }
        } catch (const std::bad_alloc&) {
            completion.error.emplace(ErrorCode::resource_exhausted, "durable mutation allocation failed");
        } catch (...) {
            completion.error.emplace(ErrorCode::internal_error, "durable mutation executor failure");
        }

        // Reactor admission caps all outstanding mutations at this ring's
        // capacity. Consequently every admitted request owns one completion
        // cell, including while shutdown drains after Reactor loops stop.
        if (!task->completions->try_push(std::move(completion))) {
            std::terminate();
        }
        static_cast<void>(task->wakeup->notify());
    }
}

} // namespace glyphastore::server
