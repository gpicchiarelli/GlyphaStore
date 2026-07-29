#include "glyphastore/server/server.hpp"

#include "server/server_builder.hpp"
#include "server/server_runtime.hpp"
#include "server/server_stats.hpp"
#include "store/store_internal.hpp"

#include <string>
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

} // namespace

Server::Server(ReactorConfig config, ServerRuntime&& runtime)
    : config_(std::move(config)), store_(std::move(runtime.store)),
      disk_reads_(std::move(runtime.disk_reads)), pair_writers_(std::move(runtime.pair_writers)),
      mesh_(std::move(runtime.mesh)), reactors_(std::move(runtime.reactors)) {
    threads_.reserve(store_->worker_count());
    affinity_results_.resize(store_->worker_count());
}

Server::~Server() {
    request_stop();
    static_cast<void>(join());
}

auto Server::create(const ReactorConfig& config, StoreConfig store_config)
    -> Result<std::unique_ptr<Server>> {
    ServerBuilder builder{config, std::move(store_config)};
    auto runtime = builder.build();
    if (!runtime) {
        return unexpected(runtime.error());
    }
    auto server = std::unique_ptr<Server>{new Server{config, std::move(*runtime)}};
    const ServerLifecycleProbes lifecycle_probes{
        .live = probe_server_live,
        .ready = probe_server_ready,
        .stats = probe_server_stats,
        .context = server.get(),
    };
    auto reactors = builder.create_reactors(*server->store_, server->mesh_, *server->disk_reads_,
                                            *server->pair_writers_, lifecycle_probes);
    if (!reactors) {
        return unexpected(reactors.error());
    }
    server->reactors_ = std::move(*reactors);
    return server;
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

auto Server::unix_socket_path() const noexcept -> std::string {
    if (reactors_.empty()) {
        return {};
    }
    return std::string{reactors_.front()->unix_socket_path()};
}

auto Server::first_failure() const -> std::optional<Error> {
    const std::lock_guard lock{failure_mutex_};
    return failure_;
}

auto Server::stats_report() const -> Result<std::string> {
    if (!live()) {
        return fail(ErrorCode::unavailable, "server is not live");
    }
    ServerStatsSnapshot snapshot{
        .live = true,
        .ready = ready(),
        .store_operational = store_operational(),
        .maintenance = store_->maintenance_snapshot(),
        .mutations = pair_writer_stats(),
        .batches = durable_batch_stats(),
    };
    snapshot.executors.reserve(reactors_.size());
    for (const auto& reactor : reactors_) {
        snapshot.executors.push_back(ExecutorStats{
            .active_connections = reactor->active_connections(),
            .adopted_connections = reactor->adopted_connections(),
            .output_scatter_responses = reactor->output_scatter_responses(),
            .output_scatter_bytes = reactor->output_scatter_bytes(),
            .output_scatter_partial_writes = reactor->output_scatter_partial_writes(),
            .output_scatter_completions = reactor->output_scatter_completions(),
        });
    }
    if (!reactors_.empty()) {
        snapshot.abuse = reactors_.front()->abuse_stats();
        snapshot.security_audit = reactors_.front()->security_audit_stats();
    }
    snapshot.tls_enabled = config_.tls.requested();
    snapshot.tls_mtls = config_.tls.mtls_enabled();
    snapshot.tls_crl = config_.tls.crl_enabled();
    snapshot.tls_ocsp_fail_closed = config_.tls.ocsp_fail_closed;
    snapshot.authz_enabled = config_.authz.enabled();
    snapshot.authz_principals = config_.authz.size();
    constexpr std::size_t kStatsBudgetBytes = 256U * 1024U;
    return ServerStatsReporter::render(snapshot, kStatsBudgetBytes);
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

auto Server::pair_writer_stats() const -> std::vector<PairWriterStats> {
    return pair_writers_->stats();
}

auto Server::durable_batch_stats() const -> std::vector<DurableBatchWorkerStats> {
    return detail::StoreAccess::batch_stats(*store_);
}

} // namespace glyphastore::server
