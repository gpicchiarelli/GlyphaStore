#include "glyphastore/server/disk_read_executor.hpp"

#include "store/store_internal.hpp"

#include <exception>
#include <thread>
#include <utility>

namespace glyphastore::server {

struct DiskReadExecutor::Lane final {
    explicit Lane(const std::size_t capacity) : queue(capacity) {}

    BoundedSpscQueue<DiskReadTask> queue;
    alignas(128) std::atomic_uint64_t signal{};
    alignas(128) std::atomic_bool stopping{};
    std::thread thread;
};

DiskReadExecutor::DiskReadExecutor(Store& store, const std::size_t worker_count,
                                   const std::size_t capacity_per_worker)
    : store_(store) {
    lanes_.reserve(worker_count);
    for (std::size_t worker = 0; worker < worker_count; ++worker) {
        lanes_.push_back(std::make_unique<Lane>(capacity_per_worker));
    }
}

DiskReadExecutor::~DiskReadExecutor() {
    stop();
}

auto DiskReadExecutor::create(Store& store, const std::size_t worker_count,
                              const std::size_t capacity_per_worker)
    -> Result<std::unique_ptr<DiskReadExecutor>> try {
    if (worker_count == 0 || worker_count != store.worker_count() || capacity_per_worker == 0) {
        return fail(ErrorCode::invalid_argument,
                    "paired disk-read executor requires one nonempty SPSC lane per Store shard");
    }
    return std::unique_ptr<DiskReadExecutor>(new DiskReadExecutor(store, worker_count, capacity_per_worker));
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "disk-read executor allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "disk-read executor construction failed");
}

auto DiskReadExecutor::start() -> Status try {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return fail(ErrorCode::invalid_argument, "disk-read executor has already been started");
    }
    if (stopping_.load(std::memory_order_acquire)) {
        return fail(ErrorCode::unavailable, "disk-read executor has been stopped");
    }
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        lanes_[worker]->thread = std::thread{[this, worker] { run(worker); }};
    }
    return {};
} catch (const std::exception& exception) {
    stop();
    return fail(ErrorCode::io_error, std::string{"failed to start disk-read executor: "} + exception.what());
} catch (...) {
    stop();
    return fail(ErrorCode::io_error, "failed to start disk-read executor");
}

auto DiskReadExecutor::begin_submission() noexcept -> bool {
    const auto previous = admission_state_.fetch_add(1U, std::memory_order_acq_rel);
    if ((previous & kAdmissionClosed) == 0) {
        return true;
    }
    finish_submission();
    return false;
}

void DiskReadExecutor::finish_submission() noexcept {
    const auto previous = admission_state_.fetch_sub(1U, std::memory_order_acq_rel);
    if ((previous & kAdmissionClosed) != 0 && (previous & kAdmissionCountMask) == 1U) {
        admission_state_.notify_all();
    }
}

auto DiskReadExecutor::try_submit(DiskReadTask task) -> bool {
    if (task.worker_index >= lanes_.size() || !begin_submission()) {
        return false;
    }
    struct SubmissionGuard final {
        DiskReadExecutor& executor;
        ~SubmissionGuard() {
            executor.finish_submission();
        }
    } submission{*this};
    if (!started_.load(std::memory_order_acquire) || stopping_.load(std::memory_order_acquire)) {
        return false;
    }
    auto& lane = *lanes_[task.worker_index];
    if (lane.stopping.load(std::memory_order_acquire) || !lane.queue.try_push(std::move(task))) {
        return false;
    }
    lane.signal.fetch_add(1U, std::memory_order_release);
    lane.signal.notify_one();
    return true;
}

void DiskReadExecutor::stop() noexcept {
    if (!stopping_.exchange(true, std::memory_order_acq_rel)) {
        auto observed = admission_state_.fetch_or(kAdmissionClosed, std::memory_order_acq_rel);
        while ((observed & kAdmissionCountMask) != 0) {
            admission_state_.wait(observed, std::memory_order_acquire);
            observed = admission_state_.load(std::memory_order_acquire);
        }
        for (auto& lane : lanes_) {
            lane->stopping.store(true, std::memory_order_release);
            lane->signal.fetch_add(1U, std::memory_order_release);
            lane->signal.notify_one();
        }
    }
    for (auto& lane : lanes_) {
        if (lane->thread.joinable()) {
            lane->thread.join();
        }
        while (lane->queue.try_pop()) {
            // Reactor ownership has ended. Destroy every cancelled queued pin
            // before Store::close releases the durable directory lifetime.
        }
    }
}

void DiskReadExecutor::run(const std::size_t worker_index) noexcept {
    auto& lane = *lanes_[worker_index];
    while (true) {
        auto task = lane.queue.try_pop();
        if (!task) {
            if (lane.stopping.load(std::memory_order_acquire)) {
                return;
            }
            const auto observed = lane.signal.load(std::memory_order_acquire);
            task = lane.queue.try_pop();
            if (!task && !lane.stopping.load(std::memory_order_acquire)) {
                lane.signal.wait(observed, std::memory_order_acquire);
            }
            if (!task) {
                continue;
            }
        }

        DiskReadCompletion completion{.connection = task->connection, .request_id = task->request_id};
        try {
            auto result = task->cancelled->load(std::memory_order_acquire)
                              ? fail(ErrorCode::unavailable, "cold read was cancelled")
                              : detail::StoreAccess::complete_get_owned(
                                    store_, task->worker_index, std::move(task->read), task->cancelled.get());
            if (!result) {
                completion.error.emplace(std::move(result.error()));
            } else if (result->bytes.size() > task->maximum_value_bytes) {
                completion.error.emplace(ErrorCode::record_too_large,
                                         "cold-read response exceeds its connection budget");
            } else {
                completion.value.emplace(std::move(*result));
            }
        } catch (const std::bad_alloc&) {
            completion.error.emplace(ErrorCode::resource_exhausted, "cold-read allocation failed");
        } catch (...) {
            completion.error.emplace(ErrorCode::internal_error, "cold-read executor failure");
        }

        // Admission is capped by the destination Reactor queue capacity, so a
        // completion cell must exist for every accepted request.
        if (!task->completions->try_push(std::move(completion))) {
            std::terminate();
        }
        static_cast<void>(task->wakeup->notify());
    }
}

} // namespace glyphastore::server
