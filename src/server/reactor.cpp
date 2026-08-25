#include "glyphastore/server/reactor.hpp"

#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/hot_path_phases.hpp"
#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/server/peercred.hpp"
#include "server/reactor_detail.hpp"
#include "store/store_internal.hpp"
#include "system_error.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/uio.h>
#include <utility>
#include <vector>

namespace glyphastore::server {

Reactor::Reactor(ReactorConfig config, const std::size_t executor_id, TcpListener cleartext_listener,
                 TcpListener tls_listener, UnixListener unix_listener, Poller poller, Wakeup wakeup,
                 Store& store, ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                 PairWriterPool& pair_writers, ServerLifecycleProbes lifecycle_probes,
                 std::shared_ptr<TlsContext> tls, std::shared_ptr<AbuseController> abuse,
                 std::shared_ptr<SecurityAudit> security_audit)
    : config_(std::move(config)), executor_id_(executor_id), listener_(std::move(cleartext_listener)),
      tls_listener_(std::move(tls_listener)), unix_listener_(std::move(unix_listener)),
      poller_(std::move(poller)), wakeup_(std::move(wakeup)), store_(store),
      worker_routing_(detail::StoreAccess::worker_routing(store)), mesh_(mesh), disk_reads_(disk_reads),
      pair_writers_(pair_writers), lifecycle_probes_(lifecycle_probes), tls_(std::move(tls)),
      abuse_(std::move(abuse)), security_audit_(std::move(security_audit)),
      disk_read_completions_(config_.disk_read_queue_capacity),
      mutation_completions_(config_.durable_mutation_queue_capacity),
      read_cancellation_epochs_(std::make_unique<std::atomic_uint64_t[]>(config_.maximum_connections)),
      connections_(config_.maximum_connections), events_(config_.event_batch_size) {
    durable_store_ = detail::StoreAccess::is_durable(store_);
    local_read_generation_ = pair_writers_.adopt_read_generation(executor_id_, minimum_cold_read_epoch());
    free_slots_.reserve(config_.maximum_connections);
    for (std::size_t slot = config_.maximum_connections; slot > 0; --slot) {
        free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
    }
}

auto Reactor::create(const ReactorConfig& config, const std::size_t executor_id,
                     TcpListener cleartext_listener, TcpListener tls_listener, UnixListener unix_listener,
                     Store& store, ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                     PairWriterPool& pair_writers, ServerLifecycleProbes lifecycle_probes,
                     std::shared_ptr<TlsContext> tls, std::shared_ptr<AbuseController> abuse,
                     std::shared_ptr<SecurityAudit> security_audit) -> Result<std::unique_ptr<Reactor>> {
    if (executor_id >= mesh.size() || executor_id >= store.worker_count()) {
        return fail(ErrorCode::invalid_argument, "reactor executor id is outside the Worker mesh");
    }
    if (tls_listener.descriptor() >= 0 && !tls) {
        return fail(ErrorCode::invalid_argument, "TLS listener requires a TLS context");
    }
    auto poller = Poller::create();
    if (!poller) {
        return unexpected(poller.error());
    }
    auto wakeup = Wakeup::create();
    if (!wakeup) {
        return unexpected(wakeup.error());
    }
    if (!abuse && config.abuse.any_enabled()) {
        abuse = std::make_shared<AbuseController>(config.abuse);
    }
    if (!security_audit && (config.security_audit_events || config.authz.enabled() ||
                            (tls && tls->mtls_enabled()) || config.unix_peercred)) {
        security_audit = std::make_shared<SecurityAudit>(config.security_audit_events, config.quiet);
    }
    auto reactor = std::unique_ptr<Reactor>(new Reactor(
        config, executor_id, std::move(cleartext_listener), std::move(tls_listener), std::move(unix_listener),
        std::move(*poller), std::move(*wakeup), store, mesh, disk_reads, pair_writers, lifecycle_probes,
        std::move(tls), std::move(abuse), std::move(security_audit)));
    if (reactor->listener_.descriptor() >= 0) {
        if (auto added =
                reactor->poller_.add(reactor->listener_.descriptor(), kListenerToken, IoInterest::read);
            !added) {
            return unexpected(added.error());
        }
    }
    if (reactor->tls_listener_.descriptor() >= 0) {
        if (auto added = reactor->poller_.add(reactor->tls_listener_.descriptor(), kTlsListenerToken,
                                              IoInterest::read);
            !added) {
            return unexpected(added.error());
        }
    }
    if (reactor->unix_listener_.descriptor() >= 0) {
        if (auto added = reactor->poller_.add(reactor->unix_listener_.descriptor(), kUnixListenerToken,
                                              IoInterest::read);
            !added) {
            return unexpected(added.error());
        }
    }
    if (auto added = reactor->poller_.add(reactor->wakeup_.descriptor(), kMessageToken, IoInterest::read);
        !added) {
        return unexpected(added.error());
    }
    mesh.register_wakeup(executor_id, reactor->wakeup_);
    return reactor;
}

auto Reactor::connection(const ConnectionToken token) noexcept -> Connection* {
    if (token.slot >= connections_.size()) {
        return nullptr;
    }
    auto& candidate = connections_[token.slot];
    if (!candidate.socket.valid() || candidate.generation != token.generation) {
        return nullptr;
    }
    return &candidate;
}

auto Reactor::has_pending_output(const Connection& connection) noexcept -> bool {
    return connection.output_offset < connection.output.size() || connection.output_lease.has_value();
}

auto Reactor::pending_output_bytes(const Connection& connection) noexcept -> std::size_t {
    auto pending = connection.output.size() - connection.output_offset;
    if (connection.output_lease) {
        pending += connection.output_lease->header.size() - connection.output_lease->header_offset;
        pending += connection.output_lease->value.bytes.size() - connection.output_lease->value_offset;
    }
    return pending;
}

auto Reactor::acquire_cold_read_lease(const std::uint64_t epoch) noexcept -> bool {
    ReadLeaseEpoch* free{};
    for (auto& lease : cold_read_leases_) {
        if (lease.uses != 0 && lease.epoch == epoch) {
            if (lease.uses == std::numeric_limits<std::size_t>::max()) {
                return false;
            }
            ++lease.uses;
            return true;
        }
        if (lease.uses == 0 && free == nullptr) {
            free = &lease;
        }
    }
    if (free == nullptr) {
        return false;
    }
    free->epoch = epoch;
    free->uses = 1;
    return true;
}

auto Reactor::release_cold_read_lease(const std::uint64_t epoch) noexcept -> bool {
    for (auto& lease : cold_read_leases_) {
        if (lease.uses == 0 || lease.epoch != epoch) {
            continue;
        }
        --lease.uses;
        if (lease.uses == 0) {
            lease.epoch = std::numeric_limits<std::uint64_t>::max();
        }
        return true;
    }
    return false;
}

auto Reactor::minimum_cold_read_epoch() const noexcept -> std::uint64_t {
    auto minimum = std::numeric_limits<std::uint64_t>::max();
    for (const auto& lease : cold_read_leases_) {
        if (lease.uses != 0) {
            minimum = std::min(minimum, lease.epoch);
        }
    }
    return minimum;
}

void Reactor::close_connection(const ConnectionToken token) noexcept {
    auto* current = connection(token);
    if (current == nullptr) {
        return;
    }
    static_cast<void>(poller_.remove(current->socket.descriptor()));
    if (current->cold_read_in_flight) {
        read_cancellation_epochs_[token.slot].fetch_add(1U, std::memory_order_release);
    }
    current->tls.reset();
    current->socket.reset();
    current->principal.clear();
    current->capabilities = Capability::none;
    current->key_prefix.clear();
    current->input.clear();
    current->input_offset = 0;
    current->output.clear();
    current->output_offset = 0;
    current->output_lease.reset();
    current->bound_worker.reset();
    current->initialized = false;
    current->peer_read_closed = false;
    current->write_armed = false;
    current->pipelined_store_input_observed = false;
    current->request_in_flight = false;
    current->cold_read_in_flight = false;
    current->close_after_flush = false;
    current->last_activity = {};
    current->partial_request_since = {};
    current->in_flight_since = {};
    current->connection_rate_window_start_ns = 0;
    current->connection_rate_used = 0;
    ++current->generation;
    if (current->generation == 0) {
        current->generation = 1;
    }
    free_slots_.push_back(token.slot);
    --active_connections_;
}

void Reactor::stop_accepting() noexcept {
    shutting_down_ = true;
    if (listener_.descriptor() >= 0) {
        static_cast<void>(poller_.remove(listener_.descriptor()));
        listener_ = TcpListener{};
    }
    if (tls_listener_.descriptor() >= 0) {
        static_cast<void>(poller_.remove(tls_listener_.descriptor()));
        tls_listener_ = TcpListener{};
    }
    if (unix_listener_.descriptor() >= 0) {
        static_cast<void>(poller_.remove(unix_listener_.descriptor()));
        unix_listener_ = UnixListener{};
    }
}

void Reactor::close_idle_connections() noexcept {
    for (std::uint32_t slot = 0; slot < connections_.size(); ++slot) {
        auto& candidate = connections_[slot];
        if (!candidate.socket.valid()) {
            continue;
        }
        if (candidate.request_in_flight || has_pending_output(candidate) ||
            candidate.input_offset < candidate.input.size()) {
            continue;
        }
        close_connection(ConnectionToken{.slot = slot, .generation = candidate.generation});
    }
}

void Reactor::close_all_connections() noexcept {
    for (std::uint32_t slot = 0; slot < connections_.size(); ++slot) {
        auto& candidate = connections_[slot];
        if (!candidate.socket.valid()) {
            continue;
        }
        const ConnectionToken token{.slot = slot, .generation = candidate.generation};
        // Shutdown-deadline hard close: best-effort flush of any decided response still
        // buffered (same posture as in-flight request timeout). Join still reports drain
        // timeout; this only avoids discarding ACKs the kernel can accept right now.
        if (has_pending_output(candidate)) {
            static_cast<void>(write_ready(token));
            if (connection(token) == nullptr) {
                continue;
            }
        }
        close_connection(token);
    }
    reject_pending_handoffs();
}

void Reactor::reject_pending_handoffs() noexcept {
    while (auto handoff = mesh_.try_pop(executor_id_)) {
        // Accept-flood fillers without BIND OK may silently close; BIND cells must
        // surface OVERLOADED (same contract as adopt-slot exhaustion).
        if (handoff->bound_worker.has_value() || !handoff->output.empty()) {
            reject_orphaned_handoff(std::move(*handoff));
        }
    }
}

auto Reactor::accept_ready(const bool tls_endpoint) -> Status {
    auto& listener = tls_endpoint ? tls_listener_ : listener_;
    const auto now = std::chrono::steady_clock::now();
    while (true) {
        auto accepted = listener.accept();
        if (!accepted) {
            // EMFILE/ENFILE/configure_* failures must not stop the executor.
            // EAGAIN/EINTR are already empty optionals from TcpListener::accept.
            return {};
        }
        if (!accepted->has_value()) {
            return {};
        }
        if (abuse_ && !abuse_->try_admit_accept(now)) {
            // Drop the peer without adopting; SocketHandle closes on destroy.
            continue;
        }
        ConnectionHandoff handoff{.socket = std::move(**accepted), .last_activity = now};
        if (tls_endpoint) {
            if (!tls_) {
                continue;
            }
            auto session = tls_->accept_socket(handoff.socket.descriptor());
            if (!session) {
                if (security_audit_) {
                    const auto& message = session.error().message;
                    if (tls_->mtls_enabled()) {
                        security_audit_->auth_deny(message);
                    } else {
                        security_audit_->tls_error(message);
                    }
                }
                // Fail closed for this peer; keep accepting other connections.
                continue;
            }
            handoff.tls = std::move(*session);
            if (tls_->mtls_enabled()) {
                handoff.principal = std::string{handoff.tls->peer_principal()};
                if (handoff.principal.empty()) {
                    if (security_audit_) {
                        security_audit_->auth_deny("missing_principal");
                    }
                    continue;
                }
                if (security_audit_) {
                    security_audit_->auth_accept(handoff.principal);
                }
            }
        }
        if (config_.authz.enabled()) {
            const auto grant = config_.authz.grant_for(handoff.principal);
            handoff.capabilities = grant.capabilities;
            handoff.key_prefix = grant.key_prefix;
        }
        if (auto adopted = adopt_connection(std::move(handoff)); !adopted) {
            return adopted;
        }
    }
}

auto Reactor::accept_unix_ready() -> Status {
    const auto now = std::chrono::steady_clock::now();
    while (true) {
        auto accepted = unix_listener_.accept();
        if (!accepted) {
            // Same isolation as TCP accept: one peer/setup glitch must not stop the process.
            return {};
        }
        if (!accepted->has_value()) {
            return {};
        }
        if (abuse_ && !abuse_->try_admit_accept(now)) {
            continue;
        }
        ConnectionHandoff handoff{.socket = std::move(**accepted), .last_activity = now};
        if (config_.unix_peercred) {
            auto credentials = peer_credentials(handoff.socket.descriptor());
            if (!credentials) {
                if (security_audit_) {
                    security_audit_->auth_deny(credentials.error().message);
                }
                if (config_.unix_peercred_required) {
                    continue;
                }
            } else {
                handoff.principal = peercred_principal(*credentials);
                if (security_audit_) {
                    security_audit_->auth_accept(handoff.principal);
                }
            }
        } else if (config_.unix_peercred_required) {
            if (security_audit_) {
                security_audit_->auth_deny("unix_peercred_required");
            }
            continue;
        }
        if (config_.authz.enabled()) {
            const auto grant = config_.authz.grant_for(handoff.principal);
            handoff.capabilities = grant.capabilities;
            handoff.key_prefix = grant.key_prefix;
        }
        if (auto adopted = adopt_connection(std::move(handoff)); !adopted) {
            return adopted;
        }
    }
}

auto Reactor::adopt_connection(ConnectionHandoff handoff) -> Status {
    if (handoff.peer_read_closed && handoff.input.empty() && handoff.output.empty()) {
        return {};
    }
    if (free_slots_.empty()) {
        // Accept-flood keeps silent drop. BIND handoffs carry buffered OK / bound_worker
        // and must not destroy a definitive status with the socket.
        if (handoff.bound_worker.has_value() || !handoff.output.empty()) {
            reject_orphaned_handoff(std::move(handoff));
        }
        return {};
    }
    const auto slot = free_slots_.back();
    free_slots_.pop_back();
    auto& current = connections_[slot];
    current.socket = std::move(handoff.socket);
    if (config_.accepted_socket_send_buffer_bytes != 0) {
        const auto bytes = static_cast<int>(config_.accepted_socket_send_buffer_bytes);
        if (::setsockopt(current.socket.descriptor(), SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
            const bool bind_handoff = handoff.bound_worker.has_value() || !handoff.output.empty();
            handoff.socket = std::move(current.socket);
            free_slots_.push_back(slot);
            if (bind_handoff) {
                reject_orphaned_handoff(std::move(handoff));
            }
            return {};
        }
    }
    current.tls = std::move(handoff.tls);
    current.principal = std::move(handoff.principal);
    current.capabilities = handoff.capabilities;
    current.key_prefix = std::move(handoff.key_prefix);
    current.input = std::move(handoff.input);
    current.input_offset = 0;
    current.output = std::move(handoff.output);
    current.output_offset = 0;
    current.output_lease.reset();
    current.bound_worker = handoff.bound_worker;
    current.initialized = handoff.initialized;
    current.peer_read_closed = handoff.peer_read_closed;
    current.write_armed = has_pending_output(current);
    current.pipelined_store_input_observed = false;
    current.request_in_flight = false;
    current.cold_read_in_flight = false;
    current.close_after_flush = false;
    current.last_activity = handoff.last_activity.time_since_epoch().count() == 0
                                ? std::chrono::steady_clock::now()
                                : handoff.last_activity;
    current.partial_request_since = handoff.partial_request_since;
    current.in_flight_since = {};
    current.connection_rate_window_start_ns = handoff.connection_rate_window_start_ns;
    current.connection_rate_used = handoff.connection_rate_used;
    const ConnectionToken token{.slot = slot, .generation = current.generation};
    auto interest = IoInterest::none;
    if (!current.peer_read_closed) {
        interest = interest | IoInterest::read;
    }
    if (has_pending_output(current)) {
        interest = interest | IoInterest::write;
    }
    if (auto added = poller_.add(current.socket.descriptor(), token.encode(), interest); !added) {
        ConnectionHandoff rejected{.socket = std::move(current.socket),
                                   .tls = std::move(current.tls),
                                   .principal = std::move(current.principal),
                                   .capabilities = current.capabilities,
                                   .key_prefix = std::move(current.key_prefix),
                                   .input = std::move(current.input),
                                   .output = std::move(current.output),
                                   .bound_worker = current.bound_worker,
                                   .initialized = current.initialized,
                                   .peer_read_closed = current.peer_read_closed,
                                   .last_activity = current.last_activity,
                                   .partial_request_since = current.partial_request_since,
                                   .connection_rate_window_start_ns = current.connection_rate_window_start_ns,
                                   .connection_rate_used = current.connection_rate_used};
        current.capabilities = Capability::none;
        current.bound_worker.reset();
        current.initialized = false;
        current.peer_read_closed = false;
        current.write_armed = false;
        current.close_after_flush = false;
        free_slots_.push_back(slot);
        if (rejected.bound_worker.has_value() || !rejected.output.empty()) {
            reject_orphaned_handoff(std::move(rejected));
        }
        return {};
    }
    ++active_connections_;
    ++adopted_connections_;
    // write_ready flushes BIND OK then processes residual input only after the
    // decided bytes fully drain (EAGAIN-safe). Do not call process_frames while
    // output remains — a trailing decode error would close and discard BIND OK.
    if (auto* adopted = connection(token);
        adopted != nullptr &&
        (has_pending_output(*adopted) || adopted->input_offset < adopted->input.size())) {
        if (auto flushed = write_ready(token); !flushed) {
            close_connection(token);
        }
    }
    // TLS accept/handoff: application records may already sit in OpenSSL's buffer
    // after SSL_accept (TLS 1.3 coalescing) with no fresh edge-triggered readable
    // event. Drain once before returning to the poller — same class as WANT_READ
    // stranded-ACK. Connection-local failures must not fail the accept loop.
    if (auto* adopted = connection(token); adopted != nullptr && adopted->tls && !adopted->peer_read_closed) {
        if (auto drained = read_ready(token); !drained) {
            close_connection(token);
        }
    }
    return {};
}

void Reactor::reject_orphaned_handoff(ConnectionHandoff handoff,
                                      const std::optional<std::uint64_t> request_id_override) noexcept {
    if (!handoff.socket.valid()) {
        return;
    }
    std::uint64_t request_id = request_id_override.value_or(0);
    if (!request_id_override.has_value() && !handoff.output.empty()) {
        if (auto decoded = decode_response(handoff.output)) {
            request_id = decoded->frame.request_id;
        }
    }
    const ResponseView overloaded{
        .status = ResponseStatus::overloaded,
        .request_id = request_id,
        .owner_worker = handoff.bound_worker.value_or(kNoWorker),
        .worker_count = static_cast<std::uint32_t>(mesh_.size()),
        .routing_epoch = kRoutingEpoch,
    };
    auto encoded = encode_response(overloaded);
    if (!encoded) {
        return;
    }
    std::size_t offset = 0;
    while (offset < encoded->size()) {
        if (handoff.tls) {
            auto written = handoff.tls->write(encoded->data() + offset, encoded->size() - offset);
            if (!written || written->kind != TlsIoKind::ok || written->bytes == 0) {
                break;
            }
            offset += written->bytes;
            continue;
        }
        const auto sent = ::send(handoff.socket.descriptor(), encoded->data() + offset,
                                 encoded->size() - offset, reactor_detail::send_flags());
        if (sent <= 0) {
            break;
        }
        offset += static_cast<std::size_t>(sent);
    }
}

auto Reactor::queue_response(const ConnectionToken token, const ResponseView& response) -> Status {
    GS_PHASE_TCP(encode);
    auto* current = connection(token);
    if (current == nullptr) {
        return fail(ErrorCode::not_found, "response targets a stale connection");
    }
    auto encoded_size = encoded_response_size(response);
    if (!encoded_size) {
        return unexpected(encoded_size.error());
    }
    if (current->output_lease) {
        return fail(ErrorCode::corrupted_data,
                    "contiguous response cannot overtake an active scatter output lease");
    }
    const auto pending = pending_output_bytes(*current);
    if (*encoded_size > config_.maximum_output_bytes ||
        pending > config_.maximum_output_bytes - *encoded_size) {
        return fail(ErrorCode::record_too_large, "connection output high watermark exceeded");
    }
    try {
        if (glyphastore::fault::consume_fail(glyphastore::fault::Site::response_queue)) {
            throw std::bad_alloc{};
        }
        if (current->output_offset > 0) {
            current->output.erase(current->output.begin(),
                                  current->output.begin() +
                                      static_cast<std::ptrdiff_t>(current->output_offset));
            current->output_offset = 0;
        }
        const auto output_offset = current->output.size();
        current->output.resize(output_offset + *encoded_size);
        auto destination = std::span<std::byte>{current->output}.subspan(output_offset, *encoded_size);
        if (auto encoded = encode_response(destination, response); !encoded) {
            current->output.resize(output_offset);
            return unexpected(encoded.error());
        }
        if (abuse_ && !current->principal.empty() && !response.value.empty()) {
            abuse_->record_principal_response_bytes(current->principal, response.value.size(),
                                                    std::chrono::steady_clock::now());
        }
        return {};
    } catch (const std::bad_alloc&) {
        // Per-connection isolation: do not escalate to executor fail-stop. Mutation
        // completions already close without inventing OVERLOADED (may be committed).
        return fail(ErrorCode::resource_exhausted, "response queue allocation failed");
    }
}

auto Reactor::queue_owned_response(const ConnectionToken token, ResponseView response, OwnedValue value)
    -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return fail(ErrorCode::not_found, "owned response targets a stale connection");
    }
    response.value = value.view();
    // One lease deliberately provides one bounded slow-output pin. If another
    // request is already buffered, keep the contiguous path so a pipelined
    // connection can overlap its next cold read instead of being serialized by
    // the lease. A future bounded multi-extent queue may remove this trade-off.
    const auto has_buffered_follow_up = current->input_offset < current->input.size();
    if (current->tls || value.bytes.size() < reactor_detail::kMinimumScatterValueBytes ||
        has_buffered_follow_up || current->pipelined_store_input_observed) {
        return queue_response(token, response);
    }
    if (current->output_lease) {
        return fail(ErrorCode::corrupted_data, "connection already owns a scatter output lease");
    }
    auto encoded_size = encoded_response_size(response);
    if (!encoded_size) {
        return unexpected(encoded_size.error());
    }
    const auto pending = pending_output_bytes(*current);
    if (*encoded_size > config_.maximum_output_bytes ||
        pending > config_.maximum_output_bytes - *encoded_size) {
        return fail(ErrorCode::record_too_large, "connection output high watermark exceeded");
    }
    try {
        if (glyphastore::fault::consume_fail(glyphastore::fault::Site::response_queue)) {
            throw std::bad_alloc{};
        }
        if (current->output_offset > 0) {
            current->output.erase(current->output.begin(),
                                  current->output.begin() +
                                      static_cast<std::ptrdiff_t>(current->output_offset));
            current->output_offset = 0;
        }
        LeasedOutput leased;
        if (auto encoded = encode_response_header(leased.header, response); !encoded) {
            return unexpected(encoded.error());
        }
        const auto value_bytes = value.bytes.size();
        leased.value = std::move(value);
        current->output_lease.emplace(std::move(leased));
        output_scatter_responses_.fetch_add(1U, std::memory_order_relaxed);
        output_scatter_bytes_.fetch_add(value_bytes, std::memory_order_relaxed);
        if (abuse_ && !current->principal.empty()) {
            abuse_->record_principal_response_bytes(current->principal, value_bytes,
                                                    std::chrono::steady_clock::now());
        }
        return {};
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "owned response queue allocation failed");
    }
}

auto Reactor::update_connection_interest(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    auto interest = IoInterest::none;
    if (!current->peer_read_closed && !current->output_lease) {
        interest = interest | IoInterest::read;
    }
    if (has_pending_output(*current)) {
        interest = interest | IoInterest::write;
    }
    auto modified = [&] {
        GS_PHASE_TCP(poller_update);
        return poller_.modify(current->socket.descriptor(), token.encode(), interest);
    }();
    if (!modified) {
        return modified;
    }
    current->write_armed = has_interest(interest, IoInterest::write);
    return {};
}

auto Reactor::process_handoffs() -> Status {
    while (auto handoff = mesh_.try_pop(executor_id_)) {
        if (auto adopted = adopt_connection(std::move(*handoff)); !adopted) {
            return adopted;
        }
    }
    return {};
}

auto Reactor::read_ready(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    std::array<std::byte, 16U * 1024U> buffer{};
    while (true) {
        std::size_t received_size = 0;
        if (current->tls) {
            auto received = current->tls->read(buffer.data(), buffer.size());
            if (!received) {
                return unexpected(received.error());
            }
            if (received->kind == TlsIoKind::want_read || received->kind == TlsIoKind::want_write ||
                received->kind == TlsIoKind::would_block) {
                // SSL_write may be waiting on a readable TLS record. Retry decided
                // output before returning to the edge-triggered poller.
                if (has_pending_output(*current)) {
                    return write_ready(token);
                }
                return {};
            }
            if (received->kind == TlsIoKind::closed) {
                current->peer_read_closed = true;
                if (!has_pending_output(*current) && !current->request_in_flight &&
                    current->input_offset >= current->input.size()) {
                    close_connection(token);
                    return {};
                }
                return write_ready(token);
            }
            received_size = received->bytes;
        } else {
            const auto received = ::recv(current->socket.descriptor(), buffer.data(), buffer.size(), 0);
            if (received > 0) {
                received_size = static_cast<std::size_t>(received);
            } else if (received == 0) {
                current->peer_read_closed = true;
                if (!has_pending_output(*current) && !current->request_in_flight &&
                    current->input_offset >= current->input.size()) {
                    close_connection(token);
                    return {};
                }
                return write_ready(token);
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return {};
            } else if (errno == EINTR) {
                continue;
            } else {
                return system_error("recv");
            }
        }
        const auto buffered = current->input.size() - current->input_offset;
        // Isolate input-buffer failures to this connection. Returning Status error
        // lets run_once hard-close and discard any already-decided output; throwing
        // fail-stops the executor. Drain decided bytes first when present.
        const auto isolate_input_failure = [&]() -> Status {
            current = connection(token);
            if (current == nullptr) {
                return {};
            }
            if (has_pending_output(*current)) {
                current->close_after_flush = true;
                current->input.clear();
                current->input_offset = 0;
                return write_ready(token);
            }
            close_connection(token);
            return {};
        };
        if (received_size > config_.maximum_input_bytes ||
            buffered > config_.maximum_input_bytes - received_size) {
            return isolate_input_failure();
        }
        try {
            if (glyphastore::fault::consume_fail(glyphastore::fault::Site::input_buffer)) {
                throw std::bad_alloc{};
            }
            prepare_input_append(*current, received_size);
            current->input.insert(current->input.end(), buffer.begin(),
                                  buffer.begin() + static_cast<std::ptrdiff_t>(received_size));
        } catch (const std::bad_alloc&) {
            return isolate_input_failure();
        }
        touch_activity(*current, std::chrono::steady_clock::now());
        if (auto processed = process_frames(token); !processed) {
            // Mirror write_ready: a decided response (abuse OVERLOADED / authz deny)
            // may already be queued before a trailing decode failure. Closing here
            // discarded that signal as silent EOF and undermined rate-limit feedback.
            current = connection(token);
            if (current != nullptr && has_pending_output(*current)) {
                current->close_after_flush = true;
                current->input.clear();
                current->input_offset = 0;
                return write_ready(token);
            }
            return processed;
        }
        current = connection(token);
        if (current == nullptr) {
            return {};
        }
        if (has_pending_output(*current)) {
            if (auto flushed = write_ready(token); !flushed) {
                return flushed;
            }
            current = connection(token);
            if (current == nullptr) {
                return {};
            }
        }
    }
}

void Reactor::prepare_input_append(Connection& current, const std::size_t additional_bytes) {
    if (current.input_offset == 0 || additional_bytes == 0) {
        return;
    }
    const auto physical_room = current.input.capacity() - current.input.size();
    const bool would_reallocate = additional_bytes > physical_room;
    const bool would_cross_limit = current.input.size() > config_.maximum_input_bytes - additional_bytes;
    if (!would_reallocate && !would_cross_limit) {
        return;
    }
    const auto remaining = current.input.size() - current.input_offset;
    current.input.erase(current.input.begin(),
                        current.input.begin() + static_cast<std::ptrdiff_t>(current.input_offset));
    current.input_offset = 0;
    input_buffer_compactions_.fetch_add(1U, std::memory_order_relaxed);
    input_buffer_bytes_moved_.fetch_add(remaining, std::memory_order_relaxed);
}

auto Reactor::write_ready(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    for (;;) {
        while (has_pending_output(*current)) {
            std::size_t written_size{};
            std::size_t requested_size{};
            bool would_block{};
            const bool scatter = current->output_lease.has_value();
            if (current->tls) {
                if (scatter) {
                    return fail(ErrorCode::corrupted_data, "TLS connection owns an invalid scatter lease");
                }
                const auto* data = current->output.data() + current->output_offset;
                requested_size = current->output.size() - current->output_offset;
                auto written = [&] {
                    GS_PHASE_TCP(socket_write);
                    return current->tls->write(data, requested_size);
                }();
                if (!written) {
                    return unexpected(written.error());
                }
                if (written->kind == TlsIoKind::want_read) {
                    // Opportunistic SSL_read: OpenSSL may need to consume a control
                    // record before SSL_write can proceed. Do not rely solely on an
                    // edge-triggered readable notification (especially after half-close
                    // previously armed write-only interest).
                    std::array<std::byte, 16U * 1024U> tls_scratch{};
                    auto received = current->tls->read(tls_scratch.data(), tls_scratch.size());
                    if (received && received->kind == TlsIoKind::ok && received->bytes > 0) {
                        const auto buffered = current->input.size() - current->input_offset;
                        if (received->bytes <= config_.maximum_input_bytes &&
                            buffered <= config_.maximum_input_bytes - received->bytes) {
                            try {
                                prepare_input_append(*current, received->bytes);
                                current->input.insert(current->input.end(), tls_scratch.begin(),
                                                      tls_scratch.begin() +
                                                          static_cast<std::ptrdiff_t>(received->bytes));
                            } catch (const std::bad_alloc&) {
                                current->close_after_flush = true;
                                current->input.clear();
                                current->input_offset = 0;
                            }
                        }
                        continue;
                    }
                    if (received && received->kind == TlsIoKind::closed) {
                        current->peer_read_closed = true;
                    }
                    // Always keep read interest for WANT_READ, even after half-close.
                    const auto interest = IoInterest::read | IoInterest::write;
                    auto modified = [&] {
                        GS_PHASE_TCP(poller_update);
                        return poller_.modify(current->socket.descriptor(), token.encode(), interest);
                    }();
                    if (!modified) {
                        return modified;
                    }
                    current->write_armed = true;
                    // One immediate retry covers fail-once injection and sockets that
                    // are already readable/writable without a fresh ET edge. A second
                    // WANT_READ with no drain progress returns to the poller below.
                    written = [&] {
                        GS_PHASE_TCP(socket_write);
                        return current->tls->write(data, requested_size);
                    }();
                    if (!written) {
                        return unexpected(written.error());
                    }
                    if (written->kind == TlsIoKind::ok) {
                        written_size = written->bytes;
                    } else if (written->kind == TlsIoKind::closed) {
                        return fail(ErrorCode::io_error, "TLS write closed by peer");
                    } else if (tls_io_blocked(written->kind)) {
                        would_block = true;
                    } else {
                        return fail(ErrorCode::io_error, "TLS write returned an unexpected status");
                    }
                } else if (tls_io_blocked(written->kind)) {
                    would_block = true;
                } else if (written->kind == TlsIoKind::closed) {
                    return fail(ErrorCode::io_error, "TLS write closed by peer");
                } else {
                    written_size = written->bytes;
                }
                if (would_block) {
                    // want_write / would_block / persistent want_read: keep write armed;
                    // keep read unless half-closed *and* OpenSSL only asked for write.
                    const bool needs_read =
                        tls_io_needs_read(written->kind) || (!current->peer_read_closed && !scatter);
                    if (current->write_armed && !needs_read) {
                        return {};
                    }
                    const auto interest =
                        needs_read ? (IoInterest::read | IoInterest::write) : IoInterest::write;
                    auto modified = [&] {
                        GS_PHASE_TCP(poller_update);
                        return poller_.modify(current->socket.descriptor(), token.encode(), interest);
                    }();
                    if (!modified) {
                        return modified;
                    }
                    current->write_armed = true;
                    return {};
                }
            } else if (!scatter) {
                const auto* data = current->output.data() + current->output_offset;
                requested_size = current->output.size() - current->output_offset;
                const auto written = [&] {
                    GS_PHASE_TCP(socket_write);
                    return ::send(current->socket.descriptor(), data, requested_size,
                                  reactor_detail::send_flags());
                }();
                if (written > 0) {
                    written_size = static_cast<std::size_t>(written);
                } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    would_block = true;
                } else if (written < 0 && errno == EINTR) {
                    continue;
                } else if (written == 0) {
                    return fail(ErrorCode::io_error, "socket send made no progress");
                } else {
                    return system_error("send");
                }
            } else {
                std::array<iovec, 3> vectors{};
                std::size_t count{};
                const auto append = [&](const std::byte* data, const std::size_t size) {
                    if (size == 0) {
                        return;
                    }
                    vectors[count++] = {.iov_base = const_cast<std::byte*>(data), .iov_len = size};
                    requested_size += size;
                };
                if (current->output_offset < current->output.size()) {
                    append(current->output.data() + current->output_offset,
                           current->output.size() - current->output_offset);
                }
                auto& lease = *current->output_lease;
                append(lease.header.data() + lease.header_offset, lease.header.size() - lease.header_offset);
                append(lease.value.bytes.data() + lease.value_offset,
                       lease.value.bytes.size() - lease.value_offset);
                msghdr message{};
                message.msg_iov = vectors.data();
                message.msg_iovlen = static_cast<decltype(message.msg_iovlen)>(count);
                const auto written = [&] {
                    GS_PHASE_TCP(socket_write);
                    return ::sendmsg(current->socket.descriptor(), &message, reactor_detail::send_flags());
                }();
                if (written > 0) {
                    written_size = static_cast<std::size_t>(written);
                } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    would_block = true;
                } else if (written < 0 && errno == EINTR) {
                    continue;
                } else if (written == 0) {
                    return fail(ErrorCode::io_error, "socket sendmsg made no progress");
                } else {
                    return system_error("sendmsg");
                }
            }
            if (would_block) {
                if (scatter) {
                    output_scatter_partial_writes_.fetch_add(1U, std::memory_order_relaxed);
                }
                if (current->write_armed) {
                    return {};
                }
                const auto interest = current->peer_read_closed || scatter
                                          ? IoInterest::write
                                          : IoInterest::read | IoInterest::write;
                auto modified = [&] {
                    GS_PHASE_TCP(poller_update);
                    return poller_.modify(current->socket.descriptor(), token.encode(), interest);
                }();
                if (!modified) {
                    return modified;
                }
                current->write_armed = true;
                return {};
            }
            if (scatter && written_size < requested_size) {
                output_scatter_partial_writes_.fetch_add(1U, std::memory_order_relaxed);
            }

            auto remaining = written_size;
            const auto consume = [&](std::size_t& offset, const std::size_t size) {
                const auto available = size - offset;
                const auto consumed = std::min(available, remaining);
                offset += consumed;
                remaining -= consumed;
            };
            consume(current->output_offset, current->output.size());
            if (current->output_offset == current->output.size()) {
                current->output.clear();
                current->output_offset = 0;
            }
            if (current->output_lease) {
                consume(current->output_lease->header_offset, current->output_lease->header.size());
                consume(current->output_lease->value_offset, current->output_lease->value.bytes.size());
                if (current->output_lease->header_offset == current->output_lease->header.size() &&
                    current->output_lease->value_offset == current->output_lease->value.bytes.size()) {
                    current->output_lease.reset();
                    output_scatter_completions_.fetch_add(1U, std::memory_order_relaxed);
                }
            }
            if (remaining != 0) {
                return fail(ErrorCode::corrupted_data, "socket write exceeded queued output extents");
            }
        }

        touch_activity(*current, std::chrono::steady_clock::now());
        if (current->close_after_flush && !has_pending_output(*current) && !current->request_in_flight) {
            close_connection(token);
            return {};
        }
        // Drain pipelined input before half-close teardown. Client SHUT_WR means
        // "done sending" — responses already decided (or still buffered) must flush,
        // and frames already received must still execute. Closing here used to drop
        // residual input and, via callers that process_frames on EAGAIN, could also
        // discard a buffered ACK when a trailing decode failed.
        if (!current->request_in_flight && current->input_offset < current->input.size()) {
            if (auto processed = process_frames(token); !processed) {
                // A decided response may have been queued earlier in this turn
                // (e.g. abuse OVERLOADED) before a trailing decode failed. Drain it.
                current = connection(token);
                if (current != nullptr && has_pending_output(*current)) {
                    current->close_after_flush = true;
                    current->input.clear();
                    current->input_offset = 0;
                    continue;
                }
                return processed;
            }
            current = connection(token);
            if (current == nullptr) {
                return {};
            }
            if (has_pending_output(*current)) {
                continue;
            }
        }
        if (current->peer_read_closed && !current->request_in_flight && !has_pending_output(*current) &&
            current->input_offset >= current->input.size()) {
            close_connection(token);
            return {};
        }
        if (current->request_in_flight) {
            // A synchronous flush entered from read_ready can drain output
            // without ever arming writable interest. The socket is already
            // registered readable, so a read -> read modify would be a pure
            // syscall. Half-closed or write-armed connections still require
            // the full reconciliation below.
            if (!current->write_armed && !current->peer_read_closed) {
                return {};
            }
            return update_connection_interest(token);
        }
        if (!current->write_armed) {
            return {};
        }
        auto modified = [&] {
            GS_PHASE_TCP(poller_update);
            return poller_.modify(current->socket.descriptor(), token.encode(), IoInterest::read);
        }();
        if (!modified) {
            return modified;
        }
        current->write_armed = false;
        return {};
    }
}

auto Reactor::run_once(const int timeout_ms) -> Status {
    pair_writers_.request_read_refresh(executor_id_);
    local_read_generation_ = pair_writers_.adopt_read_generation(executor_id_, minimum_cold_read_epoch());
    if (!local_read_generation_) {
        return fail(ErrorCode::unavailable, "paired read generation is unavailable");
    }
    const auto now = std::chrono::steady_clock::now();
    enforce_timeouts(now);
    if (auto messages = process_messages(); !messages) {
        return messages;
    }
    const auto wait_ms = next_timeout_ms(now);
    const auto effective_timeout =
        timeout_ms < 0 ? wait_ms : (wait_ms < 0 ? timeout_ms : std::min(timeout_ms, wait_ms));
    auto ready = poller_.wait(events_, effective_timeout);
    if (!ready) {
        return unexpected(ready.error());
    }
    for (std::size_t index = 0; index < *ready; ++index) {
        const auto event = events_[index];
        if (event.token == kMessageToken) {
            if (auto messages = process_messages(); !messages) {
                return messages;
            }
            continue;
        }
        if (event.token == kListenerToken) {
            if (auto accepted = accept_ready(false); !accepted) {
                return accepted;
            }
            continue;
        }
        if (event.token == kTlsListenerToken) {
            if (auto accepted = accept_ready(true); !accepted) {
                return accepted;
            }
            continue;
        }
        if (event.token == kUnixListenerToken) {
            if (auto accepted = accept_unix_ready(); !accepted) {
                return accepted;
            }
            continue;
        }
        const auto token = ConnectionToken::decode(event.token);
        if (has_flag(event.flags, IoFlags::readable)) {
            if (auto read = read_ready(token); !read) {
                close_connection(token);
                continue;
            }
        }
        if (connection(token) != nullptr && has_flag(event.flags, IoFlags::writable)) {
            if (auto written = write_ready(token); !written) {
                close_connection(token);
                continue;
            }
        }
        // Hangup before raw error: EPOLLERR|EPOLLRDHUP and kqueue EV_EOF(+EV_ERROR)
        // co-report. The old error-first hard-close skipped hangup drain and could
        // discard decided bytes left after a partial writable flush (EAGAIN).
        if (auto* current = connection(token); current != nullptr && has_flag(event.flags, IoFlags::hangup)) {
            current->peer_read_closed = true;
            if (!has_pending_output(*current) && !current->request_in_flight &&
                current->input_offset >= current->input.size()) {
                close_connection(token);
            } else if (auto drained = write_ready(token); !drained) {
                close_connection(token);
            }
            continue;
        }
        if (connection(token) != nullptr && has_flag(event.flags, IoFlags::error)) {
            auto* current = connection(token);
            int so_error = 0;
            if (current != nullptr && current->socket.valid()) {
                socklen_t length = sizeof(so_error);
                if (::getsockopt(current->socket.descriptor(), SOL_SOCKET, SO_ERROR, &so_error, &length) !=
                    0) {
                    so_error = errno;
                }
            }
            if (so_error == 0) {
                // Spurious co-report without a sticky socket error — do not discard
                // decided output; wait for writable/hangup.
                continue;
            }
            // Terminal SO_ERROR: best-effort one more drain, then release the slot.
            if (current != nullptr &&
                (has_pending_output(*current) || current->input_offset < current->input.size())) {
                static_cast<void>(write_ready(token));
            }
            if (connection(token) != nullptr) {
                close_connection(token);
            }
            continue;
        }
    }
    enforce_timeouts(std::chrono::steady_clock::now());
    return {};
}

void Reactor::touch_activity(Connection& current, const std::chrono::steady_clock::time_point now) noexcept {
    current.last_activity = now;
}

void Reactor::enforce_timeouts(const std::chrono::steady_clock::time_point now) noexcept {
    if (!abuse_) {
        return;
    }
    const auto idle_ms = abuse_->limits().idle_timeout_ms;
    const auto request_ms = abuse_->limits().request_timeout_ms;
    if (idle_ms == 0 && request_ms == 0) {
        return;
    }
    for (std::uint32_t slot = 0; slot < connections_.size(); ++slot) {
        auto& candidate = connections_[slot];
        if (!candidate.socket.valid()) {
            continue;
        }
        bool timed_out = false;
        bool request_timeout = false;
        if (request_ms != 0) {
            const auto budget = std::chrono::milliseconds{request_ms};
            if (candidate.partial_request_since.time_since_epoch().count() != 0 &&
                now - candidate.partial_request_since >= budget) {
                timed_out = true;
                request_timeout = true;
            } else if (candidate.request_in_flight &&
                       candidate.in_flight_since.time_since_epoch().count() != 0 &&
                       now - candidate.in_flight_since >= budget) {
                timed_out = true;
                request_timeout = true;
            }
        }
        if (!timed_out && idle_ms != 0 && !candidate.request_in_flight && !has_pending_output(candidate) &&
            candidate.input_offset >= candidate.input.size() &&
            candidate.last_activity.time_since_epoch().count() != 0 &&
            now - candidate.last_activity >= std::chrono::milliseconds{idle_ms}) {
            timed_out = true;
        }
        if (!timed_out) {
            continue;
        }
        if (request_timeout) {
            abuse_->note_request_timeout_closed();
        } else {
            abuse_->note_idle_closed();
        }
        const ConnectionToken token{.slot = slot, .generation = candidate.generation};
        if (request_timeout && has_pending_output(candidate) && !candidate.request_in_flight) {
            // Partial-frame timeout must not discard a prior decided response still
            // blocked on EAGAIN (idle timeout already requires !has_pending_output).
            candidate.close_after_flush = true;
            candidate.input.clear();
            candidate.input_offset = 0;
            candidate.partial_request_since = {};
            static_cast<void>(write_ready(token));
            continue;
        }
        if (request_timeout && has_pending_output(candidate)) {
            // In-flight timeout: best-effort flush of any prior decided bytes, then hard
            // close (admitted work continues; stale completions are dropped).
            static_cast<void>(write_ready(token));
            if (connection(token) == nullptr) {
                continue;
            }
        }
        close_connection(token);
    }
}

auto Reactor::next_timeout_ms(const std::chrono::steady_clock::time_point now) const noexcept -> int {
    if (!abuse_) {
        return -1;
    }
    const auto idle_ms = abuse_->limits().idle_timeout_ms;
    const auto request_ms = abuse_->limits().request_timeout_ms;
    if (idle_ms == 0 && request_ms == 0) {
        return -1;
    }
    std::optional<std::chrono::steady_clock::duration> soonest;
    const auto consider = [&](const std::chrono::steady_clock::time_point since,
                              const std::uint32_t budget_ms) {
        if (budget_ms == 0 || since.time_since_epoch().count() == 0) {
            return;
        }
        const auto deadline = since + std::chrono::milliseconds{budget_ms};
        const auto remaining = deadline - now;
        if (!soonest.has_value() || remaining < *soonest) {
            soonest = remaining;
        }
    };
    for (const auto& candidate : connections_) {
        if (!candidate.socket.valid()) {
            continue;
        }
        consider(candidate.partial_request_since, request_ms);
        if (candidate.request_in_flight) {
            consider(candidate.in_flight_since, request_ms);
        }
        if (!candidate.request_in_flight && !has_pending_output(candidate) &&
            candidate.input_offset >= candidate.input.size()) {
            consider(candidate.last_activity, idle_ms);
        }
    }
    if (!soonest.has_value()) {
        return -1;
    }
    if (*soonest <= std::chrono::steady_clock::duration::zero()) {
        return 0;
    }
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(*soonest).count();
    if (millis > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(millis);
}

} // namespace glyphastore::server
