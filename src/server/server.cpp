#include "glyphastore/server/server.hpp"

#include "glyphastore/server/socket.hpp"

#include <exception>
#include <limits>
#include <string>
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto validate_config(const ReactorConfig& config) -> Status {
    constexpr std::size_t maximum_queue_capacity = std::size_t{1} << 30U;
    if (config.maximum_connections == 0 || config.worker_count == 0 || config.event_batch_size == 0 ||
        config.worker_inbox_capacity == 0 || config.completion_queue_capacity == 0 ||
        config.maximum_in_flight_per_connection == 0 || config.maximum_remote_tasks_per_cycle == 0 ||
        config.worker_inbox_capacity > maximum_queue_capacity ||
        config.completion_queue_capacity > maximum_queue_capacity ||
        config.maximum_connections > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::invalid_argument, "server capacity configuration is outside supported limits");
    }
    return {};
}

} // namespace

Server::Server(ReactorConfig config, std::unique_ptr<Store> store)
    : config_(std::move(config)), store_(std::move(store)),
      mesh_(store_->worker_count(), config_.worker_inbox_capacity, config_.completion_queue_capacity) {
    reactors_.reserve(store_->worker_count());
    threads_.reserve(store_->worker_count());
    affinity_results_.resize(store_->worker_count());
}

Server::~Server() {
    request_stop();
    static_cast<void>(join());
}

auto Server::create(const ReactorConfig& config) -> Result<std::unique_ptr<Server>> {
    if (auto valid = validate_config(config); !valid) {
        return unexpected(valid.error());
    }
    auto store = Store::open({.worker_config = {.explicit_count = config.worker_count}});
    if (!store) {
        return unexpected(store.error());
    }
    auto server = std::unique_ptr<Server>(new Server(config, std::move(*store)));
    const bool multiple_executors = server->store_->worker_count() > 1;
#if defined(__linux__)
    const bool kernel_distribution = config.reuse_port && multiple_executors;
#else
    const bool kernel_distribution = false;
#endif
    const bool explicit_distribution =
        config.distribute_connections && multiple_executors && !kernel_distribution;
    std::uint16_t shared_port = config.port;
    for (std::size_t executor = 0; executor < server->store_->worker_count(); ++executor) {
        TcpListener listener;
        if (executor == 0 || kernel_distribution) {
            auto bound = TcpListener::bind(config.bind_address, shared_port, 512, kernel_distribution);
            if (!bound) {
                return unexpected(bound.error());
            }
            listener = std::move(*bound);
            if (executor == 0) {
                shared_port = listener.port();
            }
        }
        auto endpoint_config = config;
        endpoint_config.handoff_accepted_connections = explicit_distribution && executor == 0;
        auto reactor =
            Reactor::create(endpoint_config, executor, std::move(listener), *server->store_, server->mesh_);
        if (!reactor) {
            return unexpected(reactor.error());
        }
        server->reactors_.push_back(std::move(*reactor));
    }
    return server;
}

auto Server::start() -> Status {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return fail(ErrorCode::invalid_argument, "server has already been started");
    }
    stop_requested_.store(false, std::memory_order_release);
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
        return fail(ErrorCode::io_error, std::string{"failed to start server executor: "} + exception.what());
    }
    return {};
}

void Server::request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
}

auto Server::join() -> Status {
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
    if (failure_) {
        return unexpected(*failure_);
    }
    return {};
}

auto Server::port() const noexcept -> std::uint16_t {
    return reactors_.empty() ? 0 : reactors_.front()->port();
}

auto Server::accepted_connections_per_executor() const -> std::vector<std::size_t> {
    std::vector<std::size_t> accepted;
    accepted.reserve(reactors_.size());
    for (const auto& reactor : reactors_) {
        accepted.push_back(reactor->accepted_connections());
    }
    return accepted;
}

auto Server::executor_affinity_results() const -> std::vector<ExecutorAffinityResult> {
    return affinity_results_;
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
