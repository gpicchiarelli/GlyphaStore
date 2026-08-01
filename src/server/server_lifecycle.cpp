#include "glyphastore/server/server.hpp"

#include <chrono>
#include <exception>
#include <string>
#include <utility>

namespace glyphastore::server {

auto Server::start() -> Status {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return fail(ErrorCode::invalid_argument, "server has already been started");
    }
    stop_requested_.store(false, std::memory_order_release);
    if (auto started = disk_reads_->start(); !started) {
        return started;
    }
    if (auto started = pair_writers_->start(); !started) {
        disk_reads_->stop();
        return started;
    }
    try {
        for (std::size_t executor = 0; executor < reactors_.size(); ++executor) {
            threads_.emplace_back([this, executor] { run(executor); });
        }
    } catch (const std::exception& exception) {
        request_stop();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        threads_.clear();
        static_cast<void>(pair_writers_->stop_and_drain());
        disk_reads_->stop();
        return fail(ErrorCode::io_error, std::string{"failed to start server executor: "} + exception.what());
    }
    return {};
}

void Server::request_stop() noexcept {
    if (stop_requested_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (config_.shutdown_drain_ms == 0) {
        return;
    }
    try {
        const std::lock_guard lock{shutdown_mutex_};
        if (!shutdown_deadline_.has_value()) {
            shutdown_deadline_ =
                std::chrono::steady_clock::now() + std::chrono::milliseconds{config_.shutdown_drain_ms};
        }
    } catch (...) {
        // Keep stop_requested set; unbounded drain is safer than failing stop.
    }
}

auto Server::join() -> Status {
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
    Status drained{};
    std::optional<std::chrono::milliseconds> remaining;
    if (config_.shutdown_drain_ms > 0) {
        const std::lock_guard lock{shutdown_mutex_};
        if (shutdown_deadline_.has_value()) {
            const auto left = *shutdown_deadline_ - std::chrono::steady_clock::now();
            remaining = left <= std::chrono::steady_clock::duration::zero()
                            ? std::chrono::milliseconds{0}
                            : std::chrono::duration_cast<std::chrono::milliseconds>(left);
        } else {
            remaining = std::chrono::milliseconds{config_.shutdown_drain_ms};
        }
    }
    drained = pair_writers_->stop_and_drain(remaining);
    if (disk_reads_) {
        disk_reads_->stop();
    }
    auto closed = store_->close();
    if (failure_) {
        return unexpected(*failure_);
    }
    if (shutdown_drain_timed_out_.load(std::memory_order_acquire) || !drained) {
        return fail(ErrorCode::unavailable, "shutdown drain deadline exceeded");
    }
    return closed;
}

void Server::run(const std::size_t executor_id) noexcept {
    affinity_results_[executor_id] = configure_executor_thread(executor_id, config_.executor_affinity);
    try {
        while (!stop_requested_.load(std::memory_order_acquire)) {
            auto status = reactors_[executor_id]->run_once(10);
            if (!status) {
                {
                    const std::lock_guard lock{failure_mutex_};
                    if (!failure_) {
                        failure_ = std::move(status.error());
                    }
                }
                failed_.store(true, std::memory_order_release);
                request_stop();
                return;
            }
        }
        auto& reactor = *reactors_[executor_id];
        reactor.stop_accepting();
        bool connection_drain_timed_out = false;
        while (!reactor.idle_for_shutdown()) {
            std::optional<std::chrono::steady_clock::time_point> deadline;
            {
                const std::lock_guard lock{shutdown_mutex_};
                deadline = shutdown_deadline_;
            }
            if (deadline.has_value() && std::chrono::steady_clock::now() >= *deadline) {
                connection_drain_timed_out = true;
                reactor.close_all_connections();
                break;
            }
            reactor.close_idle_connections();
            if (reactor.idle_for_shutdown()) {
                break;
            }
            auto status = reactor.run_once(10);
            if (!status) {
                {
                    const std::lock_guard lock{failure_mutex_};
                    if (!failure_) {
                        failure_ = std::move(status.error());
                    }
                }
                failed_.store(true, std::memory_order_release);
                request_stop();
                return;
            }
        }
        if (connection_drain_timed_out) {
            shutdown_drain_timed_out_.store(true, std::memory_order_release);
        }
    } catch (const std::exception& exception) {
        const std::lock_guard lock{failure_mutex_};
        if (!failure_) {
            failure_ = Error{ErrorCode::io_error,
                             std::string{"uncaught server executor exception: "} + exception.what()};
        }
        failed_.store(true, std::memory_order_release);
        request_stop();
    } catch (...) {
        const std::lock_guard lock{failure_mutex_};
        if (!failure_) {
            failure_ = Error{ErrorCode::io_error, "uncaught non-standard server executor exception"};
        }
        failed_.store(true, std::memory_order_release);
        request_stop();
    }
}

} // namespace glyphastore::server
