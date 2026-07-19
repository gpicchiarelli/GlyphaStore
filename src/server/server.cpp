#include "glyphastore/server/server.hpp"

#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/socket.hpp"

#include <algorithm>
#include <exception>
#include <limits>
#include <string>
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto validate_config(const ReactorConfig& config) -> Status {
    constexpr std::size_t maximum_queue_capacity = std::size_t{1} << 30U;
    if (config.maximum_connections == 0 || config.worker_count == 0 || config.event_batch_size == 0 ||
        config.connection_handoff_capacity == 0 || config.disk_read_queue_capacity == 0 ||
        config.durable_mutation_queue_capacity == 0 || config.durable_mutation_queue_bytes == 0 ||
        config.durable_group_mutation_concurrency == 0 ||
        config.connection_handoff_capacity > maximum_queue_capacity ||
        config.disk_read_queue_capacity > maximum_queue_capacity ||
        config.durable_mutation_queue_capacity > maximum_queue_capacity ||
        config.durable_group_mutation_concurrency > 32 ||
        config.disk_read_thread_count > kMaximumWorkerCount ||
        config.maximum_connections > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::invalid_argument, "server capacity configuration is outside supported limits");
    }
    if (config.worker_count > kMaximumWorkerCount) {
        return fail(ErrorCode::invalid_argument, "server worker count exceeds the supported maximum");
    }
    if (config.maximum_input_bytes < kRequestHeaderBytes ||
        config.maximum_output_bytes < kResponseHeaderBytes) {
        return fail(ErrorCode::invalid_argument, "server buffers are smaller than protocol headers");
    }
    return {};
}

} // namespace

Server::Server(ReactorConfig config, std::unique_ptr<Store> store)
    : config_(std::move(config)), store_(std::move(store)),
      mesh_(store_->worker_count(), config_.connection_handoff_capacity) {
    reactors_.reserve(store_->worker_count());
    threads_.reserve(store_->worker_count());
    affinity_results_.resize(store_->worker_count());
}

Server::~Server() {
    request_stop();
    static_cast<void>(join());
}

auto Server::create(const ReactorConfig& config, StoreConfig store_config)
    -> Result<std::unique_ptr<Server>> {
    if (auto valid = validate_config(config); !valid) {
        return unexpected(valid.error());
    }
    if (store_config.worker_config.explicit_count &&
        *store_config.worker_config.explicit_count != config.worker_count) {
        return fail(ErrorCode::invalid_argument, "Store worker count must match the server executor count");
    }
    store_config.worker_config.explicit_count = config.worker_count;
    const bool durable = store_config.storage_mode != StorageMode::volatile_memory;
    const auto mutation_threads_per_worker =
        store_config.storage_mode == StorageMode::durable_group
            ? std::max<std::size_t>(1U, std::min<std::size_t>(config.durable_group_mutation_concurrency,
                                                              store_config.durable_group.max_records))
            : 1U;
    constexpr std::size_t maximum_durable_mutation_threads = 1024;
    if (durable && config.worker_count > maximum_durable_mutation_threads / mutation_threads_per_worker) {
        return fail(ErrorCode::invalid_argument,
                    "durable mutation thread configuration exceeds the process limit");
    }
    auto store = Store::open(std::move(store_config));
    if (!store) {
        return unexpected(store.error());
    }
    auto server = std::unique_ptr<Server>(new Server(config, std::move(*store)));
    const auto disk_read_threads = config.disk_read_thread_count == 0
                                       ? std::min<std::size_t>(config.worker_count, 4U)
                                       : config.disk_read_thread_count;
    auto disk_reads =
        DiskReadExecutor::create(*server->store_, disk_read_threads, config.disk_read_queue_capacity);
    if (!disk_reads) {
        return unexpected(disk_reads.error());
    }
    server->disk_reads_ = std::move(*disk_reads);
    if (durable) {
        auto durable_mutations = DurableMutationExecutor::create(*server->store_, config.worker_count,
                                                                 config.durable_mutation_queue_capacity,
                                                                 mutation_threads_per_worker);
        if (!durable_mutations) {
            return unexpected(durable_mutations.error());
        }
        server->durable_mutations_ = std::move(*durable_mutations);
    }
#if defined(__linux__)
    const bool kernel_distribution = config.reuse_port && server->store_->worker_count() > 1;
#else
    const bool kernel_distribution = false;
#endif
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
        auto reactor = Reactor::create(config, executor, std::move(listener), *server->store_, server->mesh_,
                                       *server->disk_reads_, server->durable_mutations_.get());
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
    if (auto started = disk_reads_->start(); !started) {
        return started;
    }
    if (durable_mutations_) {
        if (auto started = durable_mutations_->start(); !started) {
            disk_reads_->stop();
            return started;
        }
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
        if (durable_mutations_) {
            durable_mutations_->stop_and_drain();
        }
        disk_reads_->stop();
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
    if (durable_mutations_) {
        durable_mutations_->stop_and_drain();
    }
    if (disk_reads_) {
        disk_reads_->stop();
    }
    auto closed = store_->close();
    if (failure_) {
        return unexpected(*failure_);
    }
    return closed;
}

auto Server::port() const noexcept -> std::uint16_t {
    return reactors_.empty() ? 0 : reactors_.front()->port();
}

auto Server::adopted_connections_per_executor() const -> std::vector<std::size_t> {
    std::vector<std::size_t> adopted;
    adopted.reserve(reactors_.size());
    for (const auto& reactor : reactors_) {
        adopted.push_back(reactor->adopted_connections());
    }
    return adopted;
}

auto Server::active_connections_per_executor() const -> std::vector<std::size_t> {
    std::vector<std::size_t> active;
    active.reserve(reactors_.size());
    for (const auto& reactor : reactors_) {
        active.push_back(reactor->active_connections());
    }
    return active;
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
