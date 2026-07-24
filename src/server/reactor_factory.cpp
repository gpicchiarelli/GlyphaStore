#include "server/reactor_factory.hpp"

#include "glyphastore/server/socket.hpp"

#include <utility>

namespace glyphastore::server {

auto ReactorFactory::create_all(const ReactorConfig& config, Store& store, ConnectionHandoffMesh& mesh,
                                DiskReadExecutor& disk_reads, DurableMutationExecutor* durable_mutations,
                                const ServerLifecycleProbes lifecycle_probes,
                                std::shared_ptr<TlsContext> tls_context)
    -> Result<std::vector<std::unique_ptr<Reactor>>> {
#if defined(__linux__)
    const bool kernel_distribution = config.reuse_port && store.worker_count() > 1;
#else
    const bool kernel_distribution = false;
#endif
    const bool dual_listen = config.tls.requested() && config.tls_port.has_value();
    const bool tls_only = config.tls.requested() && !dual_listen;
    const bool listen_cleartext = !tls_only;
    const bool listen_tls = tls_only || dual_listen;
    std::uint16_t shared_cleartext_port = config.port;
    std::uint16_t shared_tls_port = dual_listen ? *config.tls_port : config.port;

    std::vector<std::unique_ptr<Reactor>> reactors;
    reactors.reserve(store.worker_count());
    for (std::size_t executor = 0; executor < store.worker_count(); ++executor) {
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
            Reactor::create(config, executor, std::move(cleartext_listener), std::move(tls_listener), store,
                            mesh, disk_reads, durable_mutations, lifecycle_probes, tls_context);
        if (!reactor) {
            return unexpected(reactor.error());
        }
        reactors.push_back(std::move(*reactor));
    }
    return reactors;
}

} // namespace glyphastore::server
