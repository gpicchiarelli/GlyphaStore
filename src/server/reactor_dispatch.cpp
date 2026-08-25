#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/hot_path_phases.hpp"
#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/server/reactor.hpp"
#include "server/reactor_detail.hpp"
#include "store/store_internal.hpp"
#include "system_error.hpp"

#include <chrono>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore::server {

auto Reactor::process_frames(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    if (current->close_after_flush) {
        // Handoff-reject path: drain OVERLOADED only — do not accept BIND/Store frames.
        current->input.clear();
        current->input_offset = 0;
        return {};
    }
    std::uint64_t cached_now_ns{};
    // Sample the reactor clock once per process_frames turn (not per request).
    const auto now = std::chrono::steady_clock::now();
    while (!current->request_in_flight && !current->output_lease &&
           current->input_offset < current->input.size()) {
        const std::span<const std::byte> available{current->input.data() + current->input_offset,
                                                   current->input.size() - current->input_offset};
        DecodedFrame<RequestView> decoded{};
        {
            GS_PHASE_TCP(decode);
            auto parsed = decode_request(available);
            if (!parsed) {
                return unexpected(parsed.error());
            }
            decoded = std::move(*parsed);
        }
        if (!decoded.complete) {
            if (current->partial_request_since.time_since_epoch().count() == 0) {
                current->partial_request_since = now;
            }
            break;
        }
        current->partial_request_since = {};
        if (shutting_down_ && decoded.frame.opcode != RequestOpcode::health &&
            decoded.frame.opcode != RequestOpcode::ready && decoded.frame.opcode != RequestOpcode::stats) {
            // Connection drain refuses new work; lifecycle probes may still observe live/ready/stats.
            ResponseView refused{.status = ResponseStatus::overloaded,
                                 .request_id = decoded.frame.request_id,
                                 .owner_worker = static_cast<std::uint32_t>(executor_id_),
                                 .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                 .routing_epoch = kRoutingEpoch};
            if (auto queued = queue_response(token, refused); !queued) {
                return queued;
            }
            current->input_offset += decoded.consumed;
            continue;
        }
        if (!authorize_request(config_.authz, current->capabilities, current->key_prefix,
                               decoded.frame.opcode, decoded.frame.key)) {
            if (security_audit_) {
                const auto reason = authorize_opcode(config_.authz, current->capabilities,
                                                     decoded.frame.opcode, current->key_prefix)
                                        ? "prefix_denied"
                                        : "capability_denied";
                security_audit_->authz_deny(current->principal, request_opcode_name(decoded.frame.opcode),
                                            reason);
            }
            ResponseView denied{.status = ResponseStatus::permission_denied,
                                .request_id = decoded.frame.request_id,
                                .owner_worker = static_cast<std::uint32_t>(executor_id_),
                                .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                .routing_epoch = kRoutingEpoch};
            if (auto queued = queue_response(token, denied); !queued) {
                return queued;
            }
            current->input_offset += decoded.consumed;
            continue;
        }
        // Lifecycle probes stay exempt from request/bandwidth quotas so HEALTH/READY/STATS remain
        // observable under overload (same posture as shutdown drain).
        const bool lifecycle_probe = decoded.frame.opcode == RequestOpcode::health ||
                                     decoded.frame.opcode == RequestOpcode::ready ||
                                     decoded.frame.opcode == RequestOpcode::stats;
        if (!lifecycle_probe && abuse_) {
            if (!abuse_->try_admit_connection_request(current->connection_rate_window_start_ns,
                                                      current->connection_rate_used, now)) {
                ResponseView refused{.status = ResponseStatus::overloaded,
                                     .request_id = decoded.frame.request_id,
                                     .owner_worker = static_cast<std::uint32_t>(executor_id_),
                                     .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                     .routing_epoch = kRoutingEpoch};
                if (auto queued = queue_response(token, refused); !queued) {
                    return queued;
                }
                current->input_offset += decoded.consumed;
                continue;
            }
            const auto request_bytes = decoded.frame.key.size() + decoded.frame.value.size();
            if (!abuse_->try_admit_principal(current->principal, request_bytes, now)) {
                ResponseView refused{.status = ResponseStatus::overloaded,
                                     .request_id = decoded.frame.request_id,
                                     .owner_worker = static_cast<std::uint32_t>(executor_id_),
                                     .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                     .routing_epoch = kRoutingEpoch};
                if (auto queued = queue_response(token, refused); !queued) {
                    return queued;
                }
                current->input_offset += decoded.consumed;
                continue;
            }
        }
        if (decoded.frame.opcode == RequestOpcode::bind_worker) {
            current->input_offset += decoded.consumed;
            touch_activity(*current, now);
            return bind_connection(token, decoded.frame);
        }

        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = decoded.frame.request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        bool immediate_response = true;
        std::vector<std::byte> init_identity;
        switch (decoded.frame.opcode) {
        case RequestOpcode::init:
            // Do not mark initialized until identity bytes exist — a throw after
            // the flag left the connection half-initialized and fail-stopped the
            // executor under memory pressure.
            try {
                if (glyphastore::fault::consume_fail(glyphastore::fault::Site::init_identity)) {
                    throw std::bad_alloc{};
                }
                init_identity = encode_init_identity_value(get_worker_routing());
                response.value = init_identity;
                current->initialized = true;
            } catch (const std::bad_alloc&) {
                response.status = ResponseStatus::overloaded;
            }
            break;
        case RequestOpcode::ping:
            response.value = decoded.frame.value;
            break;
        case RequestOpcode::health:
            if (lifecycle_probes_.live != nullptr && lifecycle_probes_.live(lifecycle_probes_.context)) {
                response.value = reactor_detail::bytes("GlyphaStore/live");
            } else {
                response.status = ResponseStatus::internal_error;
            }
            break;
        case RequestOpcode::ready:
            if (lifecycle_probes_.ready != nullptr && lifecycle_probes_.ready(lifecycle_probes_.context)) {
                response.value = reactor_detail::bytes("GlyphaStore/ready");
            } else {
                response.status = ResponseStatus::internal_error;
            }
            break;
        case RequestOpcode::stats: {
            if (lifecycle_probes_.stats == nullptr) {
                response.status = ResponseStatus::internal_error;
                break;
            }
            try {
                std::string report;
                if (!lifecycle_probes_.stats(lifecycle_probes_.context, report)) {
                    response.status = ResponseStatus::internal_error;
                    break;
                }
                const auto value_budget = config_.maximum_output_bytes > kResponseHeaderBytes
                                              ? config_.maximum_output_bytes - kResponseHeaderBytes
                                              : std::size_t{0};
                if (report.size() > value_budget || report.size() + kResponseHeaderBytes > kMaxFrameBytes) {
                    response.status = ResponseStatus::overloaded;
                    break;
                }
                response.value = std::as_bytes(std::span<const char>{report.data(), report.size()});
                if (auto queued = queue_response(token, response); !queued) {
                    return queued;
                }
                immediate_response = false;
            } catch (const std::bad_alloc&) {
                response.status = ResponseStatus::overloaded;
            }
            break;
        }
        case RequestOpcode::backup: {
            // Wire v2: BACKUP is Bound-state only (not a pre-INIT lifecycle probe).
            // Unbound BACKUP used to run the synchronous fenced path on any fresh
            // connection when authz was disabled.
            if (!current->initialized || !current->bound_worker.has_value()) {
                response.status = ResponseStatus::not_bound;
                break;
            }
            if (lifecycle_probes_.backup == nullptr) {
                response.status = ResponseStatus::unsupported;
                break;
            }
            try {
                const auto destination = std::string_view{
                    reinterpret_cast<const char*>(decoded.frame.key.data()), decoded.frame.key.size()};
                const auto value_budget = config_.maximum_output_bytes > kResponseHeaderBytes
                                              ? config_.maximum_output_bytes - kResponseHeaderBytes
                                              : std::size_t{0};
                // Refuse before the fenced copy when the OK report cannot fit. OVERLOADED
                // after a successful backup would falsely claim known-not-committed.
                const auto estimated = reactor_detail::backup_ok_report_max_bytes(destination.size());
                if (estimated > value_budget || estimated + kResponseHeaderBytes > kMaxFrameBytes) {
                    response.status = ResponseStatus::overloaded;
                    break;
                }
                std::string report;
                if (!lifecycle_probes_.backup(const_cast<void*>(lifecycle_probes_.context), destination,
                                              report)) {
                    response.status = ResponseStatus::internal_error;
                    response.value = std::as_bytes(std::span<const char>{report.data(), report.size()});
                    if (auto queued = queue_response(token, response); !queued) {
                        return queued;
                    }
                    immediate_response = false;
                    break;
                }
                if (report.size() > value_budget || report.size() + kResponseHeaderBytes > kMaxFrameBytes) {
                    // Backup already committed. Never OVERLOADED (rejected / not-committed).
                    // Prefer a minimal OK that fits; otherwise INTERNAL_ERROR (reconcile).
                    constexpr std::string_view kMinimalOk = "status=ok\n";
                    if (kMinimalOk.size() <= value_budget &&
                        kMinimalOk.size() + kResponseHeaderBytes <= kMaxFrameBytes) {
                        response.value = reactor_detail::bytes(kMinimalOk);
                    } else {
                        response.status = ResponseStatus::internal_error;
                    }
                } else {
                    response.value = std::as_bytes(std::span<const char>{report.data(), report.size()});
                }
                if (auto queued = queue_response(token, response); !queued) {
                    return queued;
                }
                immediate_response = false;
            } catch (const std::bad_alloc&) {
                // May have crossed the backup fence; do not claim known-not-committed.
                response.status = ResponseStatus::internal_error;
            }
            break;
        }
        case RequestOpcode::get:
        case RequestOpcode::put:
        case RequestOpcode::erase:
            if (available.size() > decoded.consumed) {
                current->pipelined_store_input_observed = true;
            }
            immediate_response = false;
            if (auto dispatched = dispatch_request(token, decoded.frame, cached_now_ns); !dispatched) {
                return dispatched;
            }
            break;
        case RequestOpcode::bind_worker:
            break;
        }
        if (immediate_response) {
            if (auto queued = queue_response(token, response); !queued) {
                return queued;
            }
        }
        current->input_offset += decoded.consumed;
        touch_activity(*current, now);
        if (current->request_in_flight) {
            current->in_flight_since = now;
            break;
        }
    }
    if (current->input_offset == current->input.size()) {
        current->input.clear();
        current->input_offset = 0;
        if (!current->request_in_flight) {
            current->partial_request_since = {};
        }
    }
    return {};
}

auto Reactor::bind_connection(const ConnectionToken token, const RequestView& request) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    ResponseView response{.status = ResponseStatus::ok,
                          .request_id = request.request_id,
                          .owner_worker = request.target_worker,
                          .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                          .routing_epoch = kRoutingEpoch};
    if (!current->initialized || current->bound_worker.has_value() || request.target_worker >= mesh_.size()) {
        response.status = ResponseStatus::invalid_request;
        response.owner_worker = current->bound_worker.value_or(kNoWorker);
        return queue_response(token, response);
    }
    current->bound_worker = request.target_worker;
    if (auto queued = queue_response(token, response); !queued) {
        current->bound_worker.reset();
        return queued;
    }
    if (request.target_worker == executor_id_) {
        // write_ready flushes BIND OK then parses residual input only after drain.
        // Calling process_frames while EAGAIN still holds decided bytes lets a
        // trailing decode error close the socket and discard BIND OK.
        return write_ready(token);
    }
    return transfer_connection(token, request.target_worker, request.request_id);
}

auto Reactor::transfer_connection(const ConnectionToken token, const std::size_t target_worker,
                                  const std::uint64_t request_id) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    if (current->output_lease) {
        return fail(ErrorCode::corrupted_data, "connection handoff cannot transfer a scatter output lease");
    }
    if (auto removed = poller_.remove(current->socket.descriptor()); !removed) {
        // Must not move a live socket that is still registered here — dual-poller
        // ownership spins the source executor on stale tokens after handoff.
        current->bound_worker.reset();
        current->output.clear();
        current->output_offset = 0;
        current->output_lease.reset();
        const ResponseView overloaded{.status = ResponseStatus::overloaded,
                                      .request_id = request_id,
                                      .owner_worker = static_cast<std::uint32_t>(target_worker),
                                      .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                      .routing_epoch = kRoutingEpoch};
        if (auto queued = queue_response(token, overloaded); queued) {
            current->close_after_flush = true;
            current->input.clear();
            current->input_offset = 0;
            if (auto flushed = write_ready(token); !flushed) {
                close_connection(token);
            }
            return {};
        }
        close_connection(token);
        return {};
    }
    if (current->input_offset > 0) {
        current->input.erase(current->input.begin(),
                             current->input.begin() + static_cast<std::ptrdiff_t>(current->input_offset));
        current->input_offset = 0;
    }
    if (current->output_offset > 0) {
        current->output.erase(current->output.begin(),
                              current->output.begin() + static_cast<std::ptrdiff_t>(current->output_offset));
        current->output_offset = 0;
    }
    ConnectionHandoff handoff{.socket = std::move(current->socket),
                              .tls = std::move(current->tls),
                              .principal = std::move(current->principal),
                              .capabilities = current->capabilities,
                              .key_prefix = std::move(current->key_prefix),
                              .input = std::move(current->input),
                              .output = std::move(current->output),
                              .bound_worker = static_cast<std::uint32_t>(target_worker),
                              .initialized = current->initialized,
                              .peer_read_closed = current->peer_read_closed,
                              .last_activity = current->last_activity,
                              .partial_request_since = current->partial_request_since,
                              .connection_rate_window_start_ns = current->connection_rate_window_start_ns,
                              .connection_rate_used = current->connection_rate_used};
    current->principal.clear();
    current->capabilities = Capability::none;
    current->key_prefix.clear();
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
    // Slot stays occupied until enqueue succeeds. A full handoff queue must not
    // destroy the socket after BIND already buffered OK — restore, replace with
    // OVERLOADED, flush best-effort, then close (spec: close on enqueue failure).
    if (!mesh_.try_handoff(target_worker, std::move(handoff))) {
        current->socket = std::move(handoff.socket);
        current->tls = std::move(handoff.tls);
        current->principal = std::move(handoff.principal);
        current->capabilities = handoff.capabilities;
        current->key_prefix = std::move(handoff.key_prefix);
        current->input = std::move(handoff.input);
        current->output.clear();
        current->output_offset = 0;
        current->output_lease.reset();
        current->bound_worker.reset();
        current->initialized = handoff.initialized;
        current->peer_read_closed = handoff.peer_read_closed;
        current->last_activity = handoff.last_activity;
        current->partial_request_since = handoff.partial_request_since;
        current->connection_rate_window_start_ns = handoff.connection_rate_window_start_ns;
        current->connection_rate_used = handoff.connection_rate_used;
        if (auto added = poller_.add(current->socket.descriptor(), token.encode(), IoInterest::read);
            !added) {
            reject_orphaned_handoff(
                ConnectionHandoff{.socket = std::move(current->socket),
                                  .tls = std::move(current->tls),
                                  .bound_worker = static_cast<std::uint32_t>(target_worker),
                                  .initialized = current->initialized},
                request_id);
            // Socket is no longer on the connection / poller; finish slot teardown.
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
            return {};
        }
        const ResponseView overloaded{.status = ResponseStatus::overloaded,
                                      .request_id = request_id,
                                      .owner_worker = static_cast<std::uint32_t>(target_worker),
                                      .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                      .routing_epoch = kRoutingEpoch};
        if (auto queued = queue_response(token, overloaded); queued) {
            current->close_after_flush = true;
            current->input.clear();
            current->input_offset = 0;
            if (auto flushed = write_ready(token); !flushed) {
                close_connection(token);
            }
            // Pending OVERLOADED stays until writable drain; do not fail the
            // read path (that would close and clear the buffered status).
            return {};
        }
        close_connection(token);
        return {};
    }
    ++current->generation;
    if (current->generation == 0) {
        current->generation = 1;
    }
    free_slots_.push_back(token.slot);
    --active_connections_;
    return {};
}

auto Reactor::dispatch_request(const ConnectionToken token, const RequestView& request,
                               std::uint64_t& cached_now_ns) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    const auto key = reactor_detail::key_text(request.key);
    std::uint64_t key_hash{};
    std::size_t owner{};
    {
        GS_PHASE_TCP(route);
        key_hash = hash_key_routing(key, worker_routing_);
        owner = route_worker(key_hash, mesh_.size());
    }
    if (!current->bound_worker.has_value()) {
        return queue_response(token, {.status = ResponseStatus::not_bound,
                                      .request_id = request.request_id,
                                      .owner_worker = static_cast<std::uint32_t>(owner),
                                      .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                      .routing_epoch = kRoutingEpoch});
    }
    if (*current->bound_worker != executor_id_) {
        return fail(ErrorCode::corrupted_data, "bound connection is owned by the wrong Reactor");
    }
    if (owner != executor_id_) {
        return queue_response(token, {.status = ResponseStatus::wrong_owner,
                                      .request_id = request.request_id,
                                      .owner_worker = static_cast<std::uint32_t>(owner),
                                      .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                      .routing_epoch = kRoutingEpoch});
    }
    return execute_local(token, request, key_hash, cached_now_ns);
}

auto Reactor::execute_local(const ConnectionToken token, const RequestView& request,
                            const std::uint64_t key_hash, std::uint64_t& cached_now_ns) -> Status {
    const auto key_string = reactor_detail::key_text(request.key);
    const HashedKey key{.key = key_string, .hash = key_hash};
    ResponseView response{.status = ResponseStatus::ok,
                          .request_id = request.request_id,
                          .owner_worker = static_cast<std::uint32_t>(executor_id_),
                          .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                          .routing_epoch = kRoutingEpoch};
    OwnedValue owned_response;
    // process_frames is entered only from read_ready or write_ready. The read
    // path already owns readable interest and flushes queued output; the write
    // path reconciles interest after process_frames returns. Async submissions
    // therefore leave the next poller transition to their caller—modifying it
    // here would be overwritten before any intervening poller wait.
    GS_PHASE_TCP_NAMED(store_phase, store_op);
    switch (request.opcode) {
    case RequestOpcode::get: {
        if (cached_now_ns == 0) {
            cached_now_ns = reactor_detail::current_time_ns();
        }
        // Online backup fence: temporary refusal — OVERLOADED, not INTERNAL_ERROR
        // (OperationGuard fail is bare unavailable without StoreAccess rewrite).
        if (!detail::StoreAccess::admissions_open(store_)) {
            response.status = ResponseStatus::overloaded;
            break;
        }
        if (!local_read_generation_) {
            response.status = ResponseStatus::overloaded;
            break;
        }
        if (!durable_store_) {
            auto record = local_read_generation_->get(key, cached_now_ns);
            if (!record) {
                response.status = reactor_detail::response_status(record.error());
            } else {
                owned_response = std::move(*record);
                response.value = owned_response.view();
            }
            break;
        }
        auto published = local_read_generation_->prepare_durable(key);
        if (!published) {
            response.status = reactor_detail::response_status(published.error());
            break;
        }
        auto record = detail::StoreAccess::prepare_published_durable_get(
            store_, executor_id_, std::move(*published), cached_now_ns);
        if (!record) {
            response.status = reactor_detail::response_status(record.error());
        } else if (record->value) {
            owned_response = std::move(*record->value);
            response.value = owned_response.view();
        } else {
            auto* current = connection(token);
            if (current == nullptr) {
                return {};
            }
            if (disk_reads_outstanding_ >= config_.disk_read_queue_capacity) {
                response.status = ResponseStatus::overloaded;
                break;
            }
            const auto value_budget = config_.maximum_output_bytes - kResponseHeaderBytes;
            if (!record->cold) {
                response.status = ResponseStatus::internal_error;
                break;
            }
            const auto generation_epoch = local_read_generation_->epoch();
            if (!acquire_cold_read_lease(generation_epoch)) {
                response.status = ResponseStatus::overloaded;
                break;
            }
            const auto& cancellation_epoch = read_cancellation_epochs_[token.slot];
            DiskReadTask task{.connection = token,
                              .request_id = request.request_id,
                              .generation_epoch = generation_epoch,
                              .worker_index = executor_id_,
                              .read = std::move(*record->cold),
                              .cancellation =
                                  {
                                      .epoch = &cancellation_epoch,
                                      .expected = cancellation_epoch.load(std::memory_order_relaxed),
                                  },
                              .maximum_value_bytes = value_budget,
                              .completions = &disk_read_completions_,
                              .wakeup = &wakeup_};
            ++disk_reads_outstanding_;
            if (!disk_reads_.try_submit(std::move(task))) {
                --disk_reads_outstanding_;
                if (!release_cold_read_lease(generation_epoch)) {
                    return fail(ErrorCode::corrupted_data, "cold-read lease accounting underflow");
                }
                response.status = ResponseStatus::overloaded;
                break;
            }
            current->request_in_flight = true;
            current->cold_read_in_flight = true;
            return {};
        }
        break;
    }
    case RequestOpcode::put: {
        auto* current = connection(token);
        if (current == nullptr) {
            return {};
        }
        if (detail::StoreAccess::maintenance_mutations_rejected(store_)) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        // Online backup (and close) fence: reject before enqueue. Writer OperationGuard
        // failure is unavailable → wire INTERNAL_ERROR without rewrite on some paths;
        // known-not-committed must be OVERLOADED (mirror maintenance emergency).
        if (!detail::StoreAccess::admissions_open(store_)) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        const auto admission_bytes =
            PairWriterPool::mutation_admission_bytes(request.key.size(), request.value.size());
        if (!admission_bytes || *admission_bytes > config_.durable_mutation_queue_bytes) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        const auto admitted = pair_writers_.try_submit({
            .connection = token,
            .request_id = request.request_id,
            .worker_index = executor_id_,
            .kind = MutationKind::put,
            .key = request.key,
            .key_hash = key_hash,
            .value = request.value,
            .expire_at_ns = request.expire_at_ns,
            .completions = &mutation_completions_,
            .wakeup = &wakeup_,
        });
        if (!admitted) {
            response.status = ResponseStatus::overloaded;
            break;
        }
        ++mutations_outstanding_;
        mutation_bytes_outstanding_ += *admitted;
        current->request_in_flight = true;
        return {};
    } break;
    case RequestOpcode::erase: {
        auto* current = connection(token);
        if (current == nullptr) {
            return {};
        }
        if (detail::StoreAccess::maintenance_mutations_rejected(store_)) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        if (!detail::StoreAccess::admissions_open(store_)) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        const auto admission_bytes = PairWriterPool::mutation_admission_bytes(request.key.size(), 0U);
        if (!admission_bytes || *admission_bytes > config_.durable_mutation_queue_bytes) {
            pair_writers_.note_rejected(executor_id_);
            response.status = ResponseStatus::overloaded;
            break;
        }
        const auto admitted = pair_writers_.try_submit({
            .connection = token,
            .request_id = request.request_id,
            .worker_index = executor_id_,
            .kind = MutationKind::erase,
            .key = request.key,
            .key_hash = key_hash,
            .completions = &mutation_completions_,
            .wakeup = &wakeup_,
        });
        if (!admitted) {
            response.status = ResponseStatus::overloaded;
            break;
        }
        ++mutations_outstanding_;
        mutation_bytes_outstanding_ += *admitted;
        current->request_in_flight = true;
        return {};
    } break;
    case RequestOpcode::init:
    case RequestOpcode::ping:
    case RequestOpcode::health:
    case RequestOpcode::ready:
    case RequestOpcode::stats:
    case RequestOpcode::backup:
    case RequestOpcode::bind_worker:
        response.status = ResponseStatus::invalid_request;
        break;
    }
    GS_PHASE_FINISH(store_phase);
    return queue_response(token, response);
}

auto Reactor::process_messages() -> Status {
    if (auto drained = wakeup_.drain(); !drained) {
        return drained;
    }
    if (auto completed = process_disk_read_completions(); !completed) {
        return completed;
    }
    if (auto completed = process_mutation_completions(); !completed) {
        return completed;
    }
    return process_handoffs();
}

auto Reactor::process_disk_read_completions() -> Status {
    while (auto completion = disk_read_completions_.try_pop()) {
        if (disk_reads_outstanding_ == 0) {
            return fail(ErrorCode::corrupted_data, "disk-read completion accounting underflow");
        }
        --disk_reads_outstanding_;
        if (!release_cold_read_lease(completion->generation_epoch)) {
            return fail(ErrorCode::corrupted_data, "cold-read completion released an unknown epoch");
        }
        local_read_generation_ = pair_writers_.adopt_read_generation(executor_id_, minimum_cold_read_epoch());
        if (!local_read_generation_) {
            return fail(ErrorCode::unavailable, "paired read generation is unavailable");
        }
        auto* current = connection(completion->connection);
        if (current == nullptr) {
            continue;
        }
        if (!current->request_in_flight) {
            return fail(ErrorCode::corrupted_data, "unexpected disk-read completion");
        }
        current->request_in_flight = false;
        current->in_flight_since = {};
        current->cold_read_in_flight = false;
        touch_activity(*current, std::chrono::steady_clock::now());
        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = completion->request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        Status queued;
        if (completion->error) {
            response.status = reactor_detail::response_status(*completion->error);
            queued = queue_response(completion->connection, response);
        } else if (completion->value) {
            queued = queue_owned_response(completion->connection, response, std::move(*completion->value));
        } else {
            response.status = ResponseStatus::internal_error;
            queued = queue_response(completion->connection, response);
        }
        if (!queued) {
            close_connection(completion->connection);
            continue;
        }
        // Flush the decided response; write_ready processes pipelined input only
        // after output fully drains. process_frames-on-EAGAIN discarded ACKs.
        if (auto flushed = write_ready(completion->connection); !flushed) {
            close_connection(completion->connection);
        }
    }
    return {};
}

auto Reactor::process_mutation_completions() -> Status {
    while (auto completion = mutation_completions_.try_pop()) {
        // Writer publishes with release before enqueueing the completion. Load
        // with acquire before allowing the same connection to parse a following
        // GET, establishing acknowledged PUT -> visible GET.
        local_read_generation_ = pair_writers_.adopt_read_generation(executor_id_, minimum_cold_read_epoch());
        if (!local_read_generation_) {
            return fail(ErrorCode::unavailable, "paired read generation is unavailable");
        }
        if (mutations_outstanding_ == 0) {
            return fail(ErrorCode::corrupted_data, "mutation completion accounting underflow");
        }
        --mutations_outstanding_;
        if (completion->admission_bytes > mutation_bytes_outstanding_) {
            return fail(ErrorCode::corrupted_data, "mutation byte accounting underflow");
        }
        mutation_bytes_outstanding_ -= completion->admission_bytes;
        if (!pair_writers_.release_payload(executor_id_, completion->payload_slot)) {
            // Out-of-order only while drain expiry is armed: abandoned queued work
            // may complete ahead of an earlier in-flight Store mutation.
            if (!pair_writers_.expire_remaining_armed()) {
                return fail(ErrorCode::corrupted_data, "mutation payload completion violated FIFO ownership");
            }
            deferred_mutation_payloads_.push_back(completion->payload_slot);
        } else {
            flush_deferred_mutation_payloads();
        }
        auto* current = connection(completion->connection);
        if (current == nullptr) {
            continue;
        }
        if (!current->request_in_flight || current->cold_read_in_flight) {
            return fail(ErrorCode::corrupted_data, "unexpected mutation completion");
        }
        const bool output_was_pending = has_pending_output(*current);
        current->request_in_flight = false;
        current->in_flight_since = {};
        touch_activity(*current, std::chrono::steady_clock::now());
        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = completion->request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        if (completion->error) {
            response.status = reactor_detail::response_status(*completion->error);
        }
        if (auto queued = queue_response(completion->connection, response); !queued) {
            close_connection(completion->connection);
            continue;
        }
        // Resume already-buffered frames after the completed mutation, before
        // flushing its decided response. At most one new asynchronous request
        // can be admitted; its Writer work may overlap this socket drain while
        // response bytes remain ordered in the connection output buffer.
        if (!output_was_pending && current->input_offset < current->input.size()) {
            if (auto resumed = process_frames(completion->connection); !resumed) {
                current = connection(completion->connection);
                if (current == nullptr || !has_pending_output(*current)) {
                    close_connection(completion->connection);
                    continue;
                }
                // A later malformed frame must not discard responses already
                // decided earlier in the pipeline.
                current->close_after_flush = true;
                current->input.clear();
                current->input_offset = 0;
            }
        }
        if (auto flushed = write_ready(completion->connection); !flushed) {
            close_connection(completion->connection);
        }
    }
    return {};
}

void Reactor::flush_deferred_mutation_payloads() noexcept {
    while (!deferred_mutation_payloads_.empty()) {
        if (!pair_writers_.release_payload(executor_id_, deferred_mutation_payloads_.front())) {
            return;
        }
        deferred_mutation_payloads_.erase(deferred_mutation_payloads_.begin());
    }
}

} // namespace glyphastore::server
