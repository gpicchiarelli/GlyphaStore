#include "glyphastore/server/durable_mutation_executor.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto elapsed_ns(const std::chrono::steady_clock::time_point start,
                              const std::chrono::steady_clock::time_point end) noexcept -> std::uint64_t {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return count <= 0 ? 0U : static_cast<std::uint64_t>(count);
}

void saturating_add(std::uint64_t& destination, const std::uint64_t value) noexcept {
    destination = value > std::numeric_limits<std::uint64_t>::max() - destination
                      ? std::numeric_limits<std::uint64_t>::max()
                      : destination + value;
}

} // namespace

struct DurableMutationExecutor::Lane final {
    Lane(const std::size_t capacity, const std::size_t thread_count) : queue(capacity) {
        threads.reserve(thread_count);
    }

    std::vector<std::optional<DurableMutationTask>> queue;
    std::size_t head{};
    std::size_t tail{};
    std::size_t size{};
    mutable std::mutex mutex;
    std::condition_variable available;
    bool stopping{};
    std::vector<std::thread> threads;
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

DurableMutationExecutor::DurableMutationExecutor(Store& store, const std::size_t worker_count,
                                                 const std::size_t capacity_per_worker,
                                                 const std::size_t threads_per_worker,
                                                 const std::chrono::milliseconds maximum_queue_wait)
    : store_(store), threads_per_worker_(threads_per_worker), maximum_queue_wait_(maximum_queue_wait) {
    lanes_.reserve(worker_count);
    for (std::size_t worker = 0; worker < worker_count; ++worker) {
        lanes_.push_back(std::make_unique<Lane>(capacity_per_worker, threads_per_worker));
    }
}

DurableMutationExecutor::~DurableMutationExecutor() {
    static_cast<void>(stop_and_drain());
}

auto DurableMutationExecutor::create(Store& store, const std::size_t worker_count,
                                     const std::size_t capacity_per_worker,
                                     const std::size_t threads_per_worker,
                                     const std::chrono::milliseconds maximum_queue_wait)
    -> Result<std::unique_ptr<DurableMutationExecutor>> try {
    if (worker_count == 0 || worker_count != store.worker_count() || capacity_per_worker == 0 ||
        threads_per_worker == 0) {
        return fail(ErrorCode::invalid_argument,
                    "durable mutation executor requires one nonempty lane per Store Worker");
    }
    return std::unique_ptr<DurableMutationExecutor>(new DurableMutationExecutor(
        store, worker_count, capacity_per_worker, threads_per_worker, maximum_queue_wait));
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
        for (std::size_t thread = 0; thread < threads_per_worker_; ++thread) {
            lanes_[worker]->threads.emplace_back([this, worker] { run(worker); });
        }
    }
    active_workers_.store(lanes_.size() * threads_per_worker_, std::memory_order_release);
    started_.store(true, std::memory_order_release);
    return {};
} catch (const std::exception& exception) {
    static_cast<void>(stop_and_drain());
    return fail(ErrorCode::io_error,
                std::string{"failed to start durable mutation executor: "} + exception.what());
} catch (...) {
    static_cast<void>(stop_and_drain());
    return fail(ErrorCode::io_error, "failed to start durable mutation executor");
}

auto DurableMutationExecutor::try_submit(DurableMutationTask task) -> bool {
    if (task.worker_index >= lanes_.size()) {
        return false;
    }
    auto& lane = *lanes_[task.worker_index];
    if (!started_.load(std::memory_order_acquire) || stopping_.load(std::memory_order_acquire)) {
        const std::lock_guard lane_lock{lane.mutex};
        ++lane.rejected;
        return false;
    }
    {
        const std::lock_guard lane_lock{lane.mutex};
        if (lane.stopping || lane.size == lane.queue.size()) {
            ++lane.rejected;
            return false;
        }
        if (task.admission_bytes > std::numeric_limits<std::size_t>::max() - lane.queued_bytes) {
            ++lane.rejected;
            return false;
        }
        const auto admission_bytes = task.admission_bytes;
        task.admitted_at = std::chrono::steady_clock::now();
        lane.queue[lane.tail].emplace(std::move(task));
        lane.tail = (lane.tail + 1U) % lane.queue.size();
        ++lane.size;
        lane.queued_bytes += admission_bytes;
        ++lane.admitted;
        lane.maximum_queue_depth = std::max(lane.maximum_queue_depth, lane.size);
        lane.maximum_queued_bytes = std::max(lane.maximum_queued_bytes, lane.queued_bytes);
    }
    lane.available.notify_one();
    return true;
}

void DurableMutationExecutor::note_rejected(const std::size_t worker_index) noexcept {
    if (worker_index >= lanes_.size()) {
        return;
    }
    const std::lock_guard lane_lock{lanes_[worker_index]->mutex};
    ++lanes_[worker_index]->rejected;
}

auto DurableMutationExecutor::stats() const -> std::vector<DurableMutationWorkerStats> {
    std::vector<DurableMutationWorkerStats> result;
    result.reserve(lanes_.size());
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        const auto& lane = *lanes_[worker];
        const std::lock_guard lane_lock{lane.mutex};
        result.push_back({.worker_index = worker,
                          .producer_threads = threads_per_worker_,
                          .queue_depth = lane.size,
                          .queued_bytes = lane.queued_bytes,
                          .maximum_queue_depth = lane.maximum_queue_depth,
                          .maximum_queued_bytes = lane.maximum_queued_bytes,
                          .admitted = lane.admitted,
                          .rejected = lane.rejected,
                          .expired_before_store = lane.expired_before_store,
                          .completed = lane.completed,
                          .total_queue_wait_ns = lane.total_queue_wait_ns,
                          .maximum_queue_wait_ns = lane.maximum_queue_wait_ns,
                          .total_service_ns = lane.total_service_ns,
                          .maximum_service_ns = lane.maximum_service_ns});
    }
    return result;
}

void DurableMutationExecutor::note_worker_exit() noexcept {
    if (!started_.load(std::memory_order_acquire)) {
        return;
    }
    if (active_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        const std::lock_guard lifecycle_lock{lifecycle_mutex_};
        drained_.notify_all();
    }
}

auto DurableMutationExecutor::stop_and_drain(const std::chrono::milliseconds deadline) -> Status {
    {
        const std::lock_guard lifecycle_lock{lifecycle_mutex_};
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return {};
        }
        for (auto& lane : lanes_) {
            {
                const std::lock_guard lane_lock{lane->mutex};
                lane->stopping = true;
            }
            lane->available.notify_all();
        }
    }

    bool timed_out = false;
    if (deadline.count() > 0) {
        const auto deadline_at = std::chrono::steady_clock::now() + deadline;
        std::unique_lock lifecycle_lock{lifecycle_mutex_};
        while (active_workers_.load(std::memory_order_acquire) != 0 &&
               std::chrono::steady_clock::now() < deadline_at) {
            drained_.wait_until(lifecycle_lock, deadline_at);
        }
        timed_out = active_workers_.load(std::memory_order_acquire) != 0;
        if (timed_out) {
            expire_remaining_.store(true, std::memory_order_release);
            for (auto& lane : lanes_) {
                lane->available.notify_all();
            }
        }
    }

    for (auto& lane : lanes_) {
        for (auto& thread : lane->threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }
    if (timed_out) {
        return fail(ErrorCode::unavailable, "shutdown drain deadline exceeded");
    }
    return {};
}

void DurableMutationExecutor::run(const std::size_t worker_index) noexcept {
    auto& lane = *lanes_[worker_index];
    struct ExitGuard final {
        DurableMutationExecutor& executor;
        ~ExitGuard() {
            executor.note_worker_exit();
        }
    } exit_guard{*this};

    while (true) {
        std::optional<DurableMutationTask> task;
        std::uint64_t queue_wait_ns{};
        {
            std::unique_lock lane_lock{lane.mutex};
            lane.available.wait(lane_lock, [&] {
                return lane.stopping || lane.size != 0 ||
                       expire_remaining_.load(std::memory_order_acquire);
            });
            if (lane.size == 0) {
                return;
            }
            task.emplace(std::move(*lane.queue[lane.head]));
            lane.queue[lane.head].reset();
            lane.head = (lane.head + 1U) % lane.queue.size();
            --lane.size;
            if (task->admission_bytes > lane.queued_bytes) {
                std::terminate();
            }
            lane.queued_bytes -= task->admission_bytes;
            queue_wait_ns = elapsed_ns(task->admitted_at, std::chrono::steady_clock::now());
            saturating_add(lane.total_queue_wait_ns, queue_wait_ns);
            lane.maximum_queue_wait_ns = std::max(lane.maximum_queue_wait_ns, queue_wait_ns);
        }

        DurableMutationCompletion completion{.connection = task->connection,
                                             .request_id = task->request_id,
                                             .admission_bytes = task->admission_bytes};
        const bool force_expire = expire_remaining_.load(std::memory_order_acquire);
        const bool expired =
            force_expire ||
            (maximum_queue_wait_.count() != 0 &&
             queue_wait_ns >=
                 static_cast<std::uint64_t>(
                     std::chrono::duration_cast<std::chrono::nanoseconds>(maximum_queue_wait_).count()));
        const auto service_started = std::chrono::steady_clock::now();
        if (expired) {
            completion.error.emplace(ErrorCode::unavailable,
                                     force_expire ? "durable mutation abandoned after shutdown drain deadline"
                                                  : "durable mutation expired before Store execution");
        } else {
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
        }
        const auto service_ns = expired ? 0U : elapsed_ns(service_started, std::chrono::steady_clock::now());
        {
            const std::lock_guard lane_lock{lane.mutex};
            ++lane.completed;
            if (expired) {
                ++lane.expired_before_store;
            }
            saturating_add(lane.total_service_ns, service_ns);
            lane.maximum_service_ns = std::max(lane.maximum_service_ns, service_ns);
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
