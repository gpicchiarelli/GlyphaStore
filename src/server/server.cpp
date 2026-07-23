#include "glyphastore/server/server.hpp"

#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/tls.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto probe_server_live(const void* context) noexcept -> bool {
    return static_cast<const Server*>(context)->live();
}

[[nodiscard]] auto probe_server_ready(const void* context) noexcept -> bool {
    return static_cast<const Server*>(context)->ready();
}

[[nodiscard]] auto probe_server_stats(const void* context, std::string& out) noexcept -> bool {
    try {
        auto report = static_cast<const Server*>(context)->stats_report();
        if (!report) {
            return false;
        }
        out = std::move(*report);
        return true;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] auto maintenance_state_name(const MaintenanceState state) noexcept -> std::string_view {
    switch (state) {
    case MaintenanceState::stopped:
        return "stopped";
    case MaintenanceState::idle:
        return "idle";
    case MaintenanceState::evaluating:
        return "evaluating";
    case MaintenanceState::compacting:
        return "compacting";
    case MaintenanceState::suspended:
        return "suspended";
    case MaintenanceState::draining:
        return "draining";
    case MaintenanceState::faulted:
        return "faulted";
    }
    return "unknown";
}

[[nodiscard]] auto maintenance_pressure_name(const MaintenancePressureLevel level) noexcept
    -> std::string_view {
    switch (level) {
    case MaintenancePressureLevel::none:
        return "none";
    case MaintenancePressureLevel::normal:
        return "normal";
    case MaintenancePressureLevel::pressure:
        return "pressure";
    case MaintenancePressureLevel::emergency:
        return "emergency";
    }
    return "unknown";
}

[[nodiscard]] auto maintenance_skip_reason_name(const MaintenanceSkipReason reason) noexcept
    -> std::string_view {
    switch (reason) {
    case MaintenanceSkipReason::none:
        return "none";
    case MaintenanceSkipReason::mode_disabled:
        return "mode_disabled";
    case MaintenanceSkipReason::mode_cooperative:
        return "mode_cooperative";
    case MaintenanceSkipReason::no_gain:
        return "no_gain";
    case MaintenanceSkipReason::no_candidate:
        return "no_candidate";
    case MaintenanceSkipReason::budget:
        return "budget";
    case MaintenanceSkipReason::store_closed:
        return "store_closed";
    case MaintenanceSkipReason::sequence_conflict:
        return "sequence_conflict";
    case MaintenanceSkipReason::policy_deferred:
        return "policy_deferred";
    case MaintenanceSkipReason::reclaim_threshold:
        return "reclaim_threshold";
    case MaintenanceSkipReason::copy_budget:
        return "copy_budget";
    }
    return "unknown";
}

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
    if (config.tls_port.has_value() && !config.tls.requested()) {
        return fail(ErrorCode::invalid_argument,
                    "tls_port requires TLS certificate configuration (--tls-cert/--tls-key)");
    }
    if (config.tls_port.has_value() && *config.tls_port != 0 && config.port != 0 &&
        *config.tls_port == config.port) {
        return fail(ErrorCode::invalid_argument,
                    "cleartext port and tls_port must differ (no opportunistic TLS on one endpoint)");
    }
    if (auto tls = validate_tls_config(config.tls); !tls) {
        return tls;
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
        auto durable_mutations = DurableMutationExecutor::create(
            *server->store_, config.worker_count, config.durable_mutation_queue_capacity,
            mutation_threads_per_worker, std::chrono::milliseconds{config.durable_mutation_queue_wait_ms});
        if (!durable_mutations) {
            return unexpected(durable_mutations.error());
        }
        server->durable_mutations_ = std::move(*durable_mutations);
    }
    std::shared_ptr<TlsContext> tls_context;
    if (config.tls.requested()) {
        auto created = TlsContext::create(config.tls);
        if (!created) {
            return unexpected(created.error());
        }
        tls_context = std::move(*created);
    }
#if defined(__linux__)
    const bool kernel_distribution = config.reuse_port && server->store_->worker_count() > 1;
#else
    const bool kernel_distribution = false;
#endif
    const bool dual_listen = config.tls.requested() && config.tls_port.has_value();
    const bool tls_only = config.tls.requested() && !dual_listen;
    const bool listen_cleartext = !tls_only;
    const bool listen_tls = tls_only || dual_listen;
    std::uint16_t shared_cleartext_port = config.port;
    std::uint16_t shared_tls_port = dual_listen ? *config.tls_port : config.port;
    const ServerLifecycleProbes lifecycle_probes{
        .live = probe_server_live,
        .ready = probe_server_ready,
        .stats = probe_server_stats,
        .context = server.get(),
    };
    for (std::size_t executor = 0; executor < server->store_->worker_count(); ++executor) {
        TcpListener cleartext_listener;
        TcpListener tls_listener;
        if (executor == 0 || kernel_distribution) {
            if (listen_cleartext) {
                auto bound =
                    TcpListener::bind(config.bind_address, shared_cleartext_port, 512, kernel_distribution);
                if (!bound) {
                    return unexpected(bound.error());
                }
                cleartext_listener = std::move(*bound);
                if (executor == 0) {
                    shared_cleartext_port = cleartext_listener.port();
                }
            }
            if (listen_tls) {
                auto bound =
                    TcpListener::bind(config.bind_address, shared_tls_port, 512, kernel_distribution);
                if (!bound) {
                    return unexpected(bound.error());
                }
                tls_listener = std::move(*bound);
                if (executor == 0) {
                    shared_tls_port = tls_listener.port();
                }
            }
        }
        auto reactor =
            Reactor::create(config, executor, std::move(cleartext_listener), std::move(tls_listener),
                            *server->store_, server->mesh_, *server->disk_reads_,
                            server->durable_mutations_.get(), lifecycle_probes, tls_context);
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
            static_cast<void>(durable_mutations_->stop_and_drain());
        }
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
    if (durable_mutations_) {
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
        drained = durable_mutations_->stop_and_drain(remaining);
    }
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

auto Server::port() const noexcept -> std::uint16_t {
    if (reactors_.empty()) {
        return 0;
    }
    const auto cleartext = reactors_.front()->cleartext_port();
    return cleartext != 0 ? cleartext : reactors_.front()->tls_port();
}

auto Server::cleartext_port() const noexcept -> std::uint16_t {
    return reactors_.empty() ? 0 : reactors_.front()->cleartext_port();
}

auto Server::tls_port() const noexcept -> std::uint16_t {
    return reactors_.empty() ? 0 : reactors_.front()->tls_port();
}

auto Server::live() const noexcept -> bool {
    return started_.load(std::memory_order_acquire) && healthy();
}

auto Server::ready() const noexcept -> bool {
    if (!live()) {
        return false;
    }
    if (stop_requested()) {
        return false;
    }
    if (!store_operational()) {
        return false;
    }
    const auto snapshot = maintenance_snapshot();
    if (snapshot.mutations_rejected) {
        return false;
    }
    if (snapshot.state == MaintenanceState::faulted && snapshot.last_error.has_value()) {
        return false;
    }
    return true;
}

auto Server::store_operational() const noexcept -> bool {
    return detail::StoreAccess::operational(*store_);
}

auto Server::maintenance_snapshot() const -> MaintenanceSnapshot {
    return store_->maintenance_snapshot();
}

auto Server::first_failure() const -> std::optional<Error> {
    const std::lock_guard lock{failure_mutex_};
    return failure_;
}

auto Server::stats_report() const -> Result<std::string> {
    if (!live()) {
        return fail(ErrorCode::unavailable, "server is not live");
    }
    try {
        constexpr std::size_t kStatsBudgetBytes = 256U * 1024U;
        std::string out;
        out.reserve(4096);
        out += "GlyphaStore/stats\n";
#ifndef GLYPHASTORE_VERSION
#define GLYPHASTORE_VERSION "dev"
#endif
        out += "version=";
        out += GLYPHASTORE_VERSION;
        out += '\n';
        out += "live=1\n";
        out += ready() ? "ready=1\n" : "ready=0\n";
        out += "executors=";
        out += std::to_string(reactors_.size());
        out += '\n';

        std::uint64_t connections_active{};
        std::uint64_t connections_adopted{};
        for (const auto& reactor : reactors_) {
            connections_active += reactor->active_connections();
            connections_adopted += reactor->adopted_connections();
        }
        out += "connections_active=";
        out += std::to_string(connections_active);
        out += '\n';
        out += "connections_adopted=";
        out += std::to_string(connections_adopted);
        out += '\n';

        const auto maintenance = store_->maintenance_snapshot();
        out += "maintenance_state=";
        out += maintenance_state_name(maintenance.state);
        out += '\n';
        out += "maintenance_pressure=";
        out += maintenance_pressure_name(maintenance.pressure);
        out += '\n';
        out += "mutations_rejected=";
        out += maintenance.mutations_rejected ? "1\n" : "0\n";
        out += "compact_attempts=";
        out += std::to_string(maintenance.compact_attempts);
        out += '\n';
        out += "compact_completed=";
        out += std::to_string(maintenance.compact_completed);
        out += '\n';
        out += "useful_compactions=";
        out += std::to_string(maintenance.useful_compactions);
        out += '\n';
        out += "maintenance_skips=";
        out += std::to_string(maintenance.skips);
        out += '\n';
        out += "maintenance_consecutive_no_gain=";
        out += std::to_string(maintenance.consecutive_no_gain);
        out += '\n';
        out += "maintenance_last_skip_reason=";
        out += maintenance_skip_reason_name(maintenance.last_skip_reason);
        out += '\n';
        out += "maintenance_last_no_gain_source_records_verified=";
        out += std::to_string(maintenance.last_no_gain_source_records_verified);
        out += '\n';
        out += "maintenance_last_no_gain_source_bytes_verified=";
        out += std::to_string(maintenance.last_no_gain_source_bytes_verified);
        out += '\n';
        out += "maintenance_last_no_gain_expired_records_dropped=";
        out += std::to_string(maintenance.last_no_gain_expired_records_dropped);
        out += '\n';
        out += "maintenance_total_no_gain_source_records_verified=";
        out += std::to_string(maintenance.total_no_gain_source_records_verified);
        out += '\n';
        out += "maintenance_total_no_gain_source_bytes_verified=";
        out += std::to_string(maintenance.total_no_gain_source_bytes_verified);
        out += '\n';
        out += "maintenance_total_no_gain_expired_records_dropped=";
        out += std::to_string(maintenance.total_no_gain_expired_records_dropped);
        out += '\n';
        out += "maintenance_sequence_conflicts=";
        out += std::to_string(maintenance.sequence_conflicts);
        out += '\n';
        out += "durable_rotation_attempts=";
        out += std::to_string(maintenance.rotation.attempts);
        out += "\ndurable_rotations_committed=";
        out += std::to_string(maintenance.rotation.committed);
        out += "\ndurable_rotation_compaction_waits=";
        out += std::to_string(maintenance.rotation.compaction_waits);
        out += "\ndurable_rotation_final_record_commit_attempts=";
        out += std::to_string(maintenance.rotation.final_record_commit_attempts);
        out += "\ndurable_rotation_final_record_commits=";
        out += std::to_string(maintenance.rotation.final_record_commits);
        out += "\ndurable_rotation_last_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.last_publication_wait_duration_ns);
        out += "\ndurable_rotation_total_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.total_publication_wait_duration_ns);
        out += "\ndurable_rotation_maximum_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.maximum_publication_wait_duration_ns);
        out += "\ndurable_rotation_last_seal_ns=";
        out += std::to_string(maintenance.rotation.last_seal_duration_ns);
        out += "\ndurable_rotation_total_seal_ns=";
        out += std::to_string(maintenance.rotation.total_seal_duration_ns);
        out += "\ndurable_rotation_maximum_seal_ns=";
        out += std::to_string(maintenance.rotation.maximum_seal_duration_ns);
        out += "\ndurable_rotation_last_create_ns=";
        out += std::to_string(maintenance.rotation.last_create_duration_ns);
        out += "\ndurable_rotation_total_create_ns=";
        out += std::to_string(maintenance.rotation.total_create_duration_ns);
        out += "\ndurable_rotation_maximum_create_ns=";
        out += std::to_string(maintenance.rotation.maximum_create_duration_ns);
        out += "\ndurable_rotation_last_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.last_manifest_publication_duration_ns);
        out += "\ndurable_rotation_total_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.total_manifest_publication_duration_ns);
        out += "\ndurable_rotation_maximum_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.maximum_manifest_publication_duration_ns);
        out += "\ndurable_rotation_last_execution_ns=";
        out += std::to_string(maintenance.rotation.last_execution_duration_ns);
        out += "\ndurable_rotation_total_execution_ns=";
        out += std::to_string(maintenance.rotation.total_execution_duration_ns);
        out += "\ndurable_rotation_maximum_execution_ns=";
        out += std::to_string(maintenance.rotation.maximum_execution_duration_ns);
        out += "\ndurable_rotation_last_total_ns=";
        out += std::to_string(maintenance.rotation.last_total_duration_ns);
        out += "\ndurable_rotation_total_ns=";
        out += std::to_string(maintenance.rotation.total_duration_ns);
        out += "\ndurable_rotation_maximum_total_ns=";
        out += std::to_string(maintenance.rotation.maximum_total_duration_ns);
        out += "\ndurable_rotation_last_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.last_final_record_commit_duration_ns);
        out += "\ndurable_rotation_total_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.total_final_record_commit_duration_ns);
        out += "\ndurable_rotation_maximum_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.maximum_final_record_commit_duration_ns);
        out += '\n';
        out += "maintenance_candidate_worker=";
        if (maintenance.last_observation.compaction_candidate_worker) {
            out += std::to_string(*maintenance.last_observation.compaction_candidate_worker);
        } else {
            out += "none";
        }
        out += "\nmaintenance_candidate_sealed_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_sealed_record_bytes);
        out += "\nmaintenance_candidate_live_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_live_record_bytes);
        out += "\nmaintenance_candidate_dead_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_dead_record_bytes);
        out += "\nmaintenance_candidate_dead_byte_ratio_bp=";
        if (maintenance.last_observation.candidate_dead_byte_ratio_bp) {
            out += std::to_string(*maintenance.last_observation.candidate_dead_byte_ratio_bp);
        } else {
            out += "none";
        }
        out += "\nmaintenance_candidate_scheduling_dead_byte_ratio_bp=";
        if (maintenance.last_observation.candidate_scheduling_dead_byte_ratio_bp) {
            out += std::to_string(*maintenance.last_observation.candidate_scheduling_dead_byte_ratio_bp);
        } else {
            out += "none";
        }
        out += "\nmaintenance_unread_ttl_probe_performed=";
        out += maintenance.last_observation.unread_ttl_probe_performed ? "1" : "0";
        out += "\nmaintenance_candidate_unread_expired_sealed_record_count=";
        out += std::to_string(maintenance.last_observation.candidate_unread_expired_sealed_record_count);
        out += "\nmaintenance_candidate_unread_expired_sealed_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_unread_expired_sealed_record_bytes);
        out += '\n';

        for (const auto& lane : durable_mutation_stats()) {
            out += "lane[";
            out += std::to_string(lane.worker_index);
            out += "].queue_depth=";
            out += std::to_string(lane.queue_depth);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].admitted=";
            out += std::to_string(lane.admitted);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].rejected=";
            out += std::to_string(lane.rejected);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].expired_before_store=";
            out += std::to_string(lane.expired_before_store);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].completed=";
            out += std::to_string(lane.completed);
            out += '\n';
            if (out.size() > kStatsBudgetBytes) {
                return fail(ErrorCode::resource_exhausted, "stats report exceeds the bounded size budget");
            }
        }
        for (const auto& batch : durable_batch_stats()) {
            out += "batch[";
            out += std::to_string(batch.worker_id.value);
            out += "].enabled=";
            out += batch.enabled ? "1" : "0";
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].pending_records=";
            out += std::to_string(batch.pending_records);
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].committed_batches=";
            out += std::to_string(batch.committed_batches);
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].failed_batches=";
            out += std::to_string(batch.failed_batches);
            out += '\n';
            if (out.size() > kStatsBudgetBytes) {
                return fail(ErrorCode::resource_exhausted, "stats report exceeds the bounded size budget");
            }
        }
        return out;
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, {});
    }
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

auto Server::durable_mutation_stats() const -> std::vector<DurableMutationWorkerStats> {
    return durable_mutations_ ? durable_mutations_->stats() : std::vector<DurableMutationWorkerStats>{};
}

auto Server::durable_batch_stats() const -> std::vector<DurableBatchWorkerStats> {
    return detail::StoreAccess::batch_stats(*store_);
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
