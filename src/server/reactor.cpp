#include "glyphastore/server/reactor.hpp"

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
        close_connection(ConnectionToken{.slot = slot, .generation = candidate.generation});
    }
}

auto Reactor::accept_ready(const bool tls_endpoint) -> Status {
    auto& listener = tls_endpoint ? tls_listener_ : listener_;
    const auto now = std::chrono::steady_clock::now();
    while (true) {
        auto accepted = listener.accept();
        if (!accepted) {
            return unexpected(accepted.error());
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
            return unexpected(accepted.error());
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
    if (free_slots_.empty()) {
        return {};
    }
    if (handoff.peer_read_closed && handoff.input.empty() && handoff.output.empty()) {
        return {};
    }
    const auto slot = free_slots_.back();
    free_slots_.pop_back();
    auto& current = connections_[slot];
    current.socket = std::move(handoff.socket);
    if (config_.accepted_socket_send_buffer_bytes != 0) {
        const auto bytes = static_cast<int>(config_.accepted_socket_send_buffer_bytes);
        if (::setsockopt(current.socket.descriptor(), SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
            current.socket.reset();
            free_slots_.push_back(slot);
            return system_error("Reactor SO_SNDBUF");
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
        current.socket.reset();
        free_slots_.push_back(slot);
        return added;
    }
    ++active_connections_;
    ++adopted_connections_;
    if (!current.input.empty()) {
        if (auto processed = process_frames(token); !processed) {
            close_connection(token);
            return {};
        }
    }
    if (auto* adopted = connection(token); adopted != nullptr && has_pending_output(*adopted)) {
        if (auto flushed = write_ready(token); !flushed) {
            close_connection(token);
        }
    }
    return {};
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
    if (current->output_offset > 0) {
        current->output.erase(current->output.begin(),
                              current->output.begin() + static_cast<std::ptrdiff_t>(current->output_offset));
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
}

auto Reactor::queue_owned_response(const ConnectionToken token, ResponseView response, OwnedValue value)
    -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return fail(ErrorCode::not_found, "owned response targets a stale connection");
    }
    response.value = value.bytes;
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
    if (current->output_offset > 0) {
        current->output.erase(current->output.begin(),
                              current->output.begin() + static_cast<std::ptrdiff_t>(current->output_offset));
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
    if (auto modified = poller_.modify(current->socket.descriptor(), token.encode(), interest); !modified) {
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
            if (received->kind == TlsIoKind::would_block) {
                return {};
            }
            if (received->kind == TlsIoKind::closed) {
                current->peer_read_closed = true;
                if (!has_pending_output(*current) && !current->request_in_flight) {
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
                if (!has_pending_output(*current) && !current->request_in_flight) {
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
        if (received_size > config_.maximum_input_bytes ||
            buffered > config_.maximum_input_bytes - received_size) {
            return fail(ErrorCode::record_too_large, "connection input high watermark exceeded");
        }
        current->input.insert(current->input.end(), buffer.begin(),
                              buffer.begin() + static_cast<std::ptrdiff_t>(received_size));
        touch_activity(*current, std::chrono::steady_clock::now());
        if (auto processed = process_frames(token); !processed) {
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

auto Reactor::write_ready(const ConnectionToken token) -> Status {
    GS_PHASE_TCP(write);
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
                auto written = current->tls->write(data, requested_size);
                if (!written) {
                    return unexpected(written.error());
                }
                if (written->kind == TlsIoKind::would_block) {
                    would_block = true;
                } else if (written->kind == TlsIoKind::closed) {
                    return fail(ErrorCode::io_error, "TLS write closed by peer");
                } else {
                    written_size = written->bytes;
                }
            } else if (!scatter) {
                const auto* data = current->output.data() + current->output_offset;
                requested_size = current->output.size() - current->output_offset;
                const auto written =
                    ::send(current->socket.descriptor(), data, requested_size, reactor_detail::send_flags());
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
                const auto written =
                    ::sendmsg(current->socket.descriptor(), &message, reactor_detail::send_flags());
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
                if (auto modified = poller_.modify(current->socket.descriptor(), token.encode(), interest);
                    !modified) {
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
        if (current->peer_read_closed && !current->request_in_flight) {
            close_connection(token);
            return {};
        }
        if (!current->request_in_flight && current->input_offset < current->input.size()) {
            if (auto processed = process_frames(token); !processed) {
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
        if (current->request_in_flight) {
            return update_connection_interest(token);
        }
        if (!current->write_armed) {
            return {};
        }
        if (auto modified = poller_.modify(current->socket.descriptor(), token.encode(), IoInterest::read);
            !modified) {
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
        if (connection(token) != nullptr && has_flag(event.flags, IoFlags::error)) {
            close_connection(token);
            continue;
        }
        if (auto* current = connection(token); current != nullptr && has_flag(event.flags, IoFlags::hangup)) {
            current->peer_read_closed = true;
            if (!has_pending_output(*current) && !current->request_in_flight) {
                close_connection(token);
            } else if (auto modified = update_connection_interest(token); !modified) {
                close_connection(token);
            }
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
        close_connection(ConnectionToken{.slot = slot, .generation = candidate.generation});
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
