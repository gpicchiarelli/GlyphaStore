#include "glyphastore/server/disk_read_executor.hpp"

#include "store/store_internal.hpp"

#include <exception>
#include <limits>
#include <utility>

namespace glyphastore::server {

DiskReadExecutor::DiskReadExecutor(Store& store, const std::size_t thread_count, const std::size_t capacity)
    : store_(store), thread_count_(thread_count), queue_(capacity) {
    threads_.reserve(thread_count_);
}

DiskReadExecutor::~DiskReadExecutor() {
    stop();
}

auto DiskReadExecutor::create(Store& store, const std::size_t thread_count, const std::size_t capacity)
    -> Result<std::unique_ptr<DiskReadExecutor>> try {
    if (thread_count == 0 || capacity == 0) {
        return fail(ErrorCode::invalid_argument, "disk-read executor requires nonzero capacity and threads");
    }
    return std::unique_ptr<DiskReadExecutor>(new DiskReadExecutor(store, thread_count, capacity));
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "disk-read executor allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "disk-read executor construction failed");
}

auto DiskReadExecutor::start() -> Status try {
    const std::lock_guard lock{mutex_};
    if (started_) {
        return fail(ErrorCode::invalid_argument, "disk-read executor has already been started");
    }
    if (stopping_) {
        return fail(ErrorCode::unavailable, "disk-read executor has been stopped");
    }
    started_ = true;
    for (std::size_t index = 0; index < thread_count_; ++index) {
        threads_.emplace_back([this] { run(); });
    }
    return {};
} catch (const std::exception& exception) {
    stop();
    return fail(ErrorCode::io_error, std::string{"failed to start disk-read executor: "} + exception.what());
} catch (...) {
    stop();
    return fail(ErrorCode::io_error, "failed to start disk-read executor");
}

auto DiskReadExecutor::try_submit(DiskReadTask task) -> bool {
    {
        const std::lock_guard lock{mutex_};
        if (!started_ || stopping_ || size_ == queue_.size()) {
            return false;
        }
        queue_[tail_].emplace(std::move(task));
        tail_ = (tail_ + 1U) % queue_.size();
        ++size_;
    }
    available_.notify_one();
    return true;
}

void DiskReadExecutor::stop() noexcept {
    {
        const std::lock_guard lock{mutex_};
        if (stopping_) {
            return;
        }
        stopping_ = true;
        // Reactor loops have stopped, so queued requests are cancelled rather
        // than issuing new I/O. In-flight reads retain their Store operation
        // guard and generation pin until they complete.
        for (auto& task : queue_) {
            task.reset();
        }
        size_ = 0;
        head_ = 0;
        tail_ = 0;
    }
    available_.notify_all();
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
}

void DiskReadExecutor::run() noexcept {
    while (true) {
        std::optional<DiskReadTask> task;
        {
            std::unique_lock lock{mutex_};
            available_.wait(lock, [this] { return stopping_ || size_ != 0; });
            if (stopping_) {
                return;
            }
            task.emplace(std::move(*queue_[head_]));
            queue_[head_].reset();
            head_ = (head_ + 1U) % queue_.size();
            --size_;
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
