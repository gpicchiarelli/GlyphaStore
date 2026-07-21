#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/durable_mutation_executor.hpp"
#include "glyphastore/server/poller.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/tls.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace glyphastore::server {

struct ServerLifecycleProbes final {
    bool (*live)(const void* context) noexcept{};
    bool (*ready)(const void* context) noexcept{};
    const void* context{};
};

struct ReactorConfig {
    std::string bind_address{"127.0.0.1"};
    std::uint16_t port{7379};
    std::size_t maximum_connections{4096};
    std::size_t worker_count{1};
    std::size_t event_batch_size{256};
    std::size_t maximum_input_bytes{4U * 1024U * 1024U};
    std::size_t maximum_output_bytes{4U * 1024U * 1024U};
    std::size_t connection_handoff_capacity{4096};
    bool reuse_port{true};
    bool executor_affinity{};
    // Zero selects min(worker_count, 4). Request and per-Reactor completion
    // rings are fixed-capacity and reject excess cold reads as overloaded.
    std::size_t disk_read_thread_count{};
    std::size_t disk_read_queue_capacity{256};
    // Per-Worker durable mutation lane and completion-ring capacity. Volatile
    // stores do not create mutation lanes and pay no asynchronous hop.
    std::size_t durable_mutation_queue_capacity{256};
    std::size_t durable_mutation_queue_bytes{16U * 1024U * 1024U};
    // Used only by durable-group. Multiple producers are required for a batch
    // to accumulate while earlier producers await strict acknowledgement.
    std::size_t durable_group_mutation_concurrency{4};
    // Zero disables expiry. Once Store execution begins the mutation always
    // runs to a classified completion and is never cancelled by this limit.
    std::uint32_t durable_mutation_queue_wait_ms{1000};
    // Bound how long join() waits after stop for connection drain and durable
    // mutation lanes. Zero means wait unbounded. The same deadline starts when
    // request_stop() is first observed: listeners stop accepting, idle
    // connections close, in-flight responses may still flush, then remaining
    // queued (not yet in Store) mutations complete as unavailable. In-flight
    // Store work is never cancelled and still drains before Store::close().
    // A timed-out drain makes join() return unavailable (fail-closed).
    std::uint32_t shutdown_drain_ms{30'000};
    // When tls.requested() and tls_port is unset: TLS-only on `port` (no cleartext).
    // When tls.requested() and tls_port is set: cleartext on `port`, TLS on *tls_port
    // (ADR 0020 dual listeners; never opportunistic TLS on one endpoint).
    std::optional<std::uint16_t> tls_port{};
    TlsConfig tls{};
};

class Reactor final {
  public:
    // cleartext_listener and/or tls_listener may be unbound (descriptor < 0).
    // A bound TLS listener requires a non-null tls context.
    [[nodiscard]] static auto create(const ReactorConfig& config, std::size_t executor_id,
                                     TcpListener cleartext_listener, TcpListener tls_listener, Store& store,
                                     ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                                     DurableMutationExecutor* durable_mutations,
                                     ServerLifecycleProbes lifecycle_probes = {},
                                     std::shared_ptr<TlsContext> tls = {})
        -> Result<std::unique_ptr<Reactor>>;

    Reactor(const Reactor&) = delete;
    auto operator=(const Reactor&) -> Reactor& = delete;
    Reactor(Reactor&&) = delete;
    auto operator=(Reactor&&) -> Reactor& = delete;

    [[nodiscard]] auto run_once(int timeout_ms) -> Status;
    // Remove listeners from the poller and close them. Existing connections stay
    // open until drained or force-closed. Safe only on the owning executor thread.
    void stop_accepting() noexcept;
    // Close connections with no in-flight request and no pending output. Used
    // while shutting down so idle clients cannot hold the process open.
    void close_idle_connections() noexcept;
    // Force-close every connection. Outstanding async completions are still
    // accounted when they arrive (stale generation discards the response).
    void close_all_connections() noexcept;
    [[nodiscard]] auto idle_for_shutdown() const noexcept -> bool {
        return active_connections_.load(std::memory_order_relaxed) == 0 &&
               disk_reads_outstanding_ == 0 && durable_mutations_outstanding_ == 0;
    }
    // Cleartext listen port, or 0 when this reactor has no cleartext listener.
    [[nodiscard]] auto cleartext_port() const noexcept -> std::uint16_t {
        return listener_.descriptor() >= 0 ? listener_.port() : 0;
    }
    // TLS listen port, or 0 when this reactor has no TLS listener.
    [[nodiscard]] auto tls_port() const noexcept -> std::uint16_t {
        return tls_listener_.descriptor() >= 0 ? tls_listener_.port() : 0;
    }
    // Backward-compatible primary port: cleartext if present, otherwise TLS.
    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        const auto cleartext = cleartext_port();
        return cleartext != 0 ? cleartext : tls_port();
    }
    [[nodiscard]] auto executor_id() const noexcept -> std::size_t {
        return executor_id_;
    }
    [[nodiscard]] auto active_connections() const noexcept -> std::size_t {
        return active_connections_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] auto adopted_connections() const noexcept -> std::size_t {
        return adopted_connections_;
    }

  private:
    struct Connection {
        SocketHandle socket;
        std::unique_ptr<TlsSession> tls;
        std::uint32_t generation{1};
        std::vector<std::byte> input;
        std::size_t input_offset{};
        std::vector<std::byte> output;
        std::size_t output_offset{};
        std::optional<std::uint32_t> bound_worker;
        bool initialized{};
        bool peer_read_closed{};
        bool write_armed{};
        // At most one asynchronous Store request per connection preserves wire
        // response order and prevents one pipelined client monopolizing a lane.
        bool request_in_flight{};
        std::shared_ptr<std::atomic_bool> read_cancellation;
    };

    Reactor(ReactorConfig config, std::size_t executor_id, TcpListener cleartext_listener,
            TcpListener tls_listener, Poller poller, Wakeup wakeup, Store& store,
            ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
            DurableMutationExecutor* durable_mutations, ServerLifecycleProbes lifecycle_probes,
            std::shared_ptr<TlsContext> tls);

    [[nodiscard]] auto accept_ready(bool tls_endpoint) -> Status;
    [[nodiscard]] auto adopt_connection(ConnectionHandoff handoff) -> Status;
    [[nodiscard]] auto read_ready(ConnectionToken token) -> Status;
    [[nodiscard]] auto write_ready(ConnectionToken token) -> Status;
    [[nodiscard]] auto process_frames(ConnectionToken token) -> Status;
    [[nodiscard]] auto bind_connection(ConnectionToken token, const RequestView& request) -> Status;
    [[nodiscard]] auto transfer_connection(ConnectionToken token, std::size_t target_worker) -> Status;
    [[nodiscard]] auto dispatch_request(ConnectionToken token, const RequestView& request,
                                        std::uint64_t& cached_now_ns) -> Status;
    [[nodiscard]] auto execute_local(ConnectionToken token, const RequestView& request,
                                     std::uint64_t key_hash, std::uint64_t& cached_now_ns) -> Status;
    [[nodiscard]] auto process_messages() -> Status;
    [[nodiscard]] auto process_handoffs() -> Status;
    [[nodiscard]] auto process_disk_read_completions() -> Status;
    [[nodiscard]] auto process_durable_mutation_completions() -> Status;
    [[nodiscard]] auto update_connection_interest(ConnectionToken token) -> Status;
    [[nodiscard]] auto queue_response(ConnectionToken token, const ResponseView& response) -> Status;
    [[nodiscard]] auto connection(ConnectionToken token) noexcept -> Connection*;
    void close_connection(ConnectionToken token) noexcept;

    static constexpr std::uint64_t kListenerToken = std::numeric_limits<std::uint64_t>::max();
    static constexpr std::uint64_t kMessageToken = kListenerToken - 1U;
    static constexpr std::uint64_t kTlsListenerToken = kListenerToken - 2U;
    static constexpr std::uint64_t kRoutingEpoch = 1;

    ReactorConfig config_;
    std::size_t executor_id_{};
    TcpListener listener_;
    TcpListener tls_listener_;
    Poller poller_;
    Wakeup wakeup_;
    Store& store_;
    ConnectionHandoffMesh& mesh_;
    DiskReadExecutor& disk_reads_;
    DurableMutationExecutor* durable_mutations_{};
    ServerLifecycleProbes lifecycle_probes_{};
    std::shared_ptr<TlsContext> tls_;
    BoundedMpscQueue<DiskReadCompletion> disk_read_completions_;
    BoundedMpscQueue<DurableMutationCompletion> durable_mutation_completions_;
    std::vector<Connection> connections_;
    std::vector<std::uint32_t> free_slots_;
    std::vector<IoEvent> events_;
    std::atomic<std::size_t> active_connections_{};
    std::size_t adopted_connections_{};
    std::size_t disk_reads_outstanding_{};
    std::size_t durable_mutations_outstanding_{};
    std::size_t durable_mutation_bytes_outstanding_{};
    bool shutting_down_{};
};

} // namespace glyphastore::server
