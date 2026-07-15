#include "glyphastore/persistence/durable_flush_coordinator.hpp"

#include <algorithm>

namespace glyphastore {

DurableFlushCoordinator::DurableFlushCoordinator(const std::uint32_t sync_interval_ms,
                                                 const std::uint32_t batch_max_wait_ms,
                                                 const bool periodic_sync_enabled,
                                                 const bool batch_timer_enabled, FlushCallback flush_callback)
    : sync_interval_ms_(sync_interval_ms), batch_max_wait_ms_(batch_max_wait_ms),
      periodic_sync_enabled_(periodic_sync_enabled), batch_timer_enabled_(batch_timer_enabled),
      flush_callback_(std::move(flush_callback)),
      worker_([this](const std::stop_token stop_token) { run(stop_token); }) {}

DurableFlushCoordinator::~DurableFlushCoordinator() {
    stop();
}

void DurableFlushCoordinator::request_flush() {
    flush_requested_.store(true, std::memory_order_release);
    wake_.notify_one();
}

void DurableFlushCoordinator::notify_batch_activity() {
    // Wake the coordinator so it can observe stop/forced-flush state without
    // moving the absolute timer deadline. Activity alone is not a flush
    // request: otherwise every append degenerates into an immediate fsync.
    wake_.notify_one();
}

auto DurableFlushCoordinator::flush_all_blocking() -> Status {
    if (!flush_callback_) {
        return {};
    }
    return flush_callback_();
}

void DurableFlushCoordinator::stop() {
    if (worker_.joinable()) {
        worker_.request_stop();
        wake_.notify_all();
        worker_.join();
    }
}

void DurableFlushCoordinator::run(const std::stop_token stop_token) {
    using clock = std::chrono::steady_clock;
    using duration = std::chrono::milliseconds;
    const auto batch_wait = batch_timer_enabled_ ? duration{batch_max_wait_ms_} : duration::max();
    const auto sync_wait = periodic_sync_enabled_ ? duration{sync_interval_ms_} : duration::max();
    const auto interval = std::min(batch_wait, sync_wait);
    auto next_deadline = clock::now() + interval;

    while (!stop_token.stop_requested()) {
        bool timed_out = false;
        {
            std::unique_lock lock{mutex_};
            wake_.wait_until(lock, next_deadline, [&] {
                return stop_token.stop_requested() || flush_requested_.load(std::memory_order_acquire);
            });
            timed_out = clock::now() >= next_deadline;
        }
        if (stop_token.stop_requested()) {
            break;
        }
        const bool forced = flush_requested_.exchange(false, std::memory_order_acq_rel);
        if (!timed_out && !forced) {
            continue;
        }
        if (timed_out) {
            next_deadline = clock::now() + interval;
        }
        static_cast<void>(flush_all_blocking());
    }
}

} // namespace glyphastore
