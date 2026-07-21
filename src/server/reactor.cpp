#include "glyphastore/server/reactor.hpp"

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
#include <utility>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto key_text(const std::span<const std::byte> value) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

[[nodiscard]] auto response_status(const Error& error) noexcept -> ResponseStatus {
    switch (error.code) {
    case ErrorCode::not_found:
        return ResponseStatus::not_found;
    case ErrorCode::invalid_argument:
    case ErrorCode::record_too_large:
        return ResponseStatus::invalid_request;
    case ErrorCode::resource_exhausted:
    case ErrorCode::storage_exhausted:
    case ErrorCode::file_too_large:
    case ErrorCode::descriptor_exhausted:
    case ErrorCode::read_only_filesystem:
    case ErrorCode::unavailable:
    case ErrorCode::sequence_conflict:
        return ResponseStatus::overloaded;
    default:
        return ResponseStatus::internal_error;
    }
}

[[nodiscard]] auto current_time_ns() noexcept -> std::uint64_t {
    const auto elapsed = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
}

[[nodiscard]] auto send_flags() noexcept -> int {
#if defined(__linux__)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

} // namespace

Reactor::Reactor(ReactorConfig config, const std::size_t executor_id, TcpListener cleartext_listener,
                 TcpListener tls_listener, Poller poller, Wakeup wakeup, Store& store,
                 ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                 DurableMutationExecutor* durable_mutations, ServerLifecycleProbes lifecycle_probes,
                 std::shared_ptr<TlsContext> tls)
    : config_(std::move(config)), executor_id_(executor_id), listener_(std::move(cleartext_listener)),
      tls_listener_(std::move(tls_listener)), poller_(std::move(poller)), wakeup_(std::move(wakeup)),
      store_(store), mesh_(mesh), disk_reads_(disk_reads), durable_mutations_(durable_mutations),
      lifecycle_probes_(lifecycle_probes), tls_(std::move(tls)),
      disk_read_completions_(config_.disk_read_queue_capacity),
      durable_mutation_completions_(config_.durable_mutation_queue_capacity),
      connections_(config_.maximum_connections), events_(config_.event_batch_size) {
    free_slots_.reserve(config_.maximum_connections);
    for (std::size_t slot = config_.maximum_connections; slot > 0; --slot) {
        free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
    }
}

auto Reactor::create(const ReactorConfig& config, const std::size_t executor_id,
                     TcpListener cleartext_listener, TcpListener tls_listener, Store& store,
                     ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                     DurableMutationExecutor* durable_mutations, ServerLifecycleProbes lifecycle_probes,
                     std::shared_ptr<TlsContext> tls)
    -> Result<std::unique_ptr<Reactor>> {
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
    auto reactor = std::unique_ptr<Reactor>(
        new Reactor(config, executor_id, std::move(cleartext_listener), std::move(tls_listener),
                    std::move(*poller), std::move(*wakeup), store, mesh, disk_reads, durable_mutations,
                    lifecycle_probes, std::move(tls)));
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

void Reactor::close_connection(const ConnectionToken token) noexcept {
    auto* current = connection(token);
    if (current == nullptr) {
        return;
    }
    static_cast<void>(poller_.remove(current->socket.descriptor()));
    if (current->read_cancellation) {
        current->read_cancellation->store(true, std::memory_order_release);
    }
    current->tls.reset();
    current->socket.reset();
    current->input.clear();
    current->input_offset = 0;
    current->output.clear();
    current->output_offset = 0;
    current->bound_worker.reset();
    current->initialized = false;
    current->peer_read_closed = false;
    current->write_armed = false;
    current->request_in_flight = false;
    current->read_cancellation.reset();
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
}

void Reactor::close_idle_connections() noexcept {
    for (std::uint32_t slot = 0; slot < connections_.size(); ++slot) {
        auto& candidate = connections_[slot];
        if (!candidate.socket.valid()) {
            continue;
        }
        if (candidate.request_in_flight || candidate.output_offset < candidate.output.size() ||
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
    while (true) {
        auto accepted = listener.accept();
        if (!accepted) {
            return unexpected(accepted.error());
        }
        if (!accepted->has_value()) {
            return {};
        }
        ConnectionHandoff handoff{.socket = std::move(**accepted)};
        if (tls_endpoint) {
            if (!tls_) {
                continue;
            }
            auto session = tls_->accept_socket(handoff.socket.descriptor());
            if (!session) {
                // Fail closed for this peer; keep accepting other connections.
                continue;
            }
            handoff.tls = std::move(*session);
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
    current.tls = std::move(handoff.tls);
    current.input = std::move(handoff.input);
    current.input_offset = 0;
    current.output = std::move(handoff.output);
    current.output_offset = 0;
    current.bound_worker = handoff.bound_worker;
    current.initialized = handoff.initialized;
    current.peer_read_closed = handoff.peer_read_closed;
    current.write_armed = !current.output.empty();
    current.request_in_flight = false;
    current.read_cancellation.reset();
    const ConnectionToken token{.slot = slot, .generation = current.generation};
    auto interest = IoInterest::none;
    if (!current.peer_read_closed) {
        interest = interest | IoInterest::read;
    }
    if (!current.output.empty()) {
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
    if (auto* adopted = connection(token);
        adopted != nullptr && adopted->output_offset < adopted->output.size()) {
        if (auto flushed = write_ready(token); !flushed) {
            close_connection(token);
        }
    }
    return {};
}

auto Reactor::queue_response(const ConnectionToken token, const ResponseView& response) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return fail(ErrorCode::not_found, "response targets a stale connection");
    }
    auto encoded_size = encoded_response_size(response);
    if (!encoded_size) {
        return unexpected(encoded_size.error());
    }
    const auto pending = current->output.size() - current->output_offset;
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
    return {};
}

auto Reactor::process_frames(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    std::uint64_t cached_now_ns{};
    while (!current->request_in_flight && current->input_offset < current->input.size()) {
        const std::span<const std::byte> available{current->input.data() + current->input_offset,
                                                   current->input.size() - current->input_offset};
        auto decoded = decode_request(available);
        if (!decoded) {
            return unexpected(decoded.error());
        }
        if (!decoded->complete) {
            break;
        }
        if (shutting_down_ && decoded->frame.opcode != RequestOpcode::health &&
            decoded->frame.opcode != RequestOpcode::ready &&
            decoded->frame.opcode != RequestOpcode::stats) {
            // Connection drain refuses new work; lifecycle probes may still observe live/ready/stats.
            ResponseView refused{.status = ResponseStatus::overloaded,
                                 .request_id = decoded->frame.request_id,
                                 .owner_worker = static_cast<std::uint32_t>(executor_id_),
                                 .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                                 .routing_epoch = kRoutingEpoch};
            if (auto queued = queue_response(token, refused); !queued) {
                return queued;
            }
            current->input_offset += decoded->consumed;
            continue;
        }
        if (decoded->frame.opcode == RequestOpcode::bind_worker) {
            current->input_offset += decoded->consumed;
            return bind_connection(token, decoded->frame);
        }

        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = decoded->frame.request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        bool immediate_response = true;
        switch (decoded->frame.opcode) {
        case RequestOpcode::init:
            current->initialized = true;
            response.value = bytes("GlyphaStore/2");
            break;
        case RequestOpcode::ping:
            response.value = decoded->frame.value;
            break;
        case RequestOpcode::health:
            if (lifecycle_probes_.live != nullptr &&
                lifecycle_probes_.live(lifecycle_probes_.context)) {
                response.value = bytes("GlyphaStore/live");
            } else {
                response.status = ResponseStatus::internal_error;
            }
            break;
        case RequestOpcode::ready:
            if (lifecycle_probes_.ready != nullptr &&
                lifecycle_probes_.ready(lifecycle_probes_.context)) {
                response.value = bytes("GlyphaStore/ready");
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
                if (report.size() > value_budget ||
                    report.size() + kResponseHeaderBytes > kMaxFrameBytes) {
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
        case RequestOpcode::get:
        case RequestOpcode::put:
        case RequestOpcode::erase:
            immediate_response = false;
            if (auto dispatched = dispatch_request(token, decoded->frame, cached_now_ns); !dispatched) {
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
        current->input_offset += decoded->consumed;
        if (current->request_in_flight) {
            break;
        }
    }
    if (current->input_offset == current->input.size()) {
        current->input.clear();
        current->input_offset = 0;
    } else if (current->input_offset > 0) {
        current->input.erase(current->input.begin(),
                             current->input.begin() + static_cast<std::ptrdiff_t>(current->input_offset));
        current->input_offset = 0;
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
        return queued;
    }
    if (request.target_worker == executor_id_) {
        return process_frames(token);
    }
    return transfer_connection(token, request.target_worker);
}

auto Reactor::transfer_connection(const ConnectionToken token, const std::size_t target_worker) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    static_cast<void>(poller_.remove(current->socket.descriptor()));
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
                              .input = std::move(current->input),
                              .output = std::move(current->output),
                              .bound_worker = static_cast<std::uint32_t>(target_worker),
                              .initialized = current->initialized,
                              .peer_read_closed = current->peer_read_closed};
    current->bound_worker.reset();
    current->initialized = false;
    current->peer_read_closed = false;
    current->write_armed = false;
    current->request_in_flight = false;
    current->read_cancellation.reset();
    ++current->generation;
    if (current->generation == 0) {
        current->generation = 1;
    }
    free_slots_.push_back(token.slot);
    --active_connections_;
    static_cast<void>(mesh_.try_handoff(target_worker, std::move(handoff)));
    return {};
}

auto Reactor::dispatch_request(const ConnectionToken token, const RequestView& request,
                               std::uint64_t& cached_now_ns) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    const auto key = key_text(request.key);
    const auto key_hash = hash_key(key);
    const auto owner = route_worker(key_hash, mesh_.size());
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
    const auto key_string = key_text(request.key);
    const HashedKey key{.key = key_string, .hash = key_hash};
    ResponseView response{.status = ResponseStatus::ok,
                          .request_id = request.request_id,
                          .owner_worker = static_cast<std::uint32_t>(executor_id_),
                          .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                          .routing_epoch = kRoutingEpoch};
    OwnedValue owned_response;
    switch (request.opcode) {
    case RequestOpcode::get: {
        if (cached_now_ns == 0) {
            cached_now_ns = current_time_ns();
        }
        auto record = detail::StoreAccess::prepare_get_owned(store_, executor_id_, key, cached_now_ns);
        if (!record) {
            response.status = response_status(record.error());
        } else if (record->value) {
            owned_response = std::move(*record->value);
            response.value = owned_response.bytes;
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
            std::shared_ptr<std::atomic_bool> cancellation;
            try {
                cancellation = std::make_shared<std::atomic_bool>(false);
            } catch (const std::bad_alloc&) {
                response.status = ResponseStatus::overloaded;
                break;
            }
            DiskReadTask task{.connection = token,
                              .request_id = request.request_id,
                              .worker_index = executor_id_,
                              .read = std::move(*record->cold),
                              .cancelled = cancellation,
                              .maximum_value_bytes = value_budget,
                              .completions = &disk_read_completions_,
                              .wakeup = &wakeup_};
            ++disk_reads_outstanding_;
            if (!disk_reads_.try_submit(std::move(task))) {
                --disk_reads_outstanding_;
                response.status = ResponseStatus::overloaded;
                break;
            }
            current->request_in_flight = true;
            current->read_cancellation = std::move(cancellation);
            return update_connection_interest(token);
        }
        break;
    }
    case RequestOpcode::put:
        if (detail::StoreAccess::is_durable(store_)) {
            auto* current = connection(token);
            if (current == nullptr) {
                return {};
            }
            if (detail::StoreAccess::maintenance_mutations_rejected(store_)) {
                if (durable_mutations_) {
                    durable_mutations_->note_rejected(executor_id_);
                }
                response.status = ResponseStatus::overloaded;
                break;
            }
            if (durable_mutations_ == nullptr ||
                durable_mutations_outstanding_ >= config_.durable_mutation_queue_capacity) {
                if (durable_mutations_) {
                    durable_mutations_->note_rejected(executor_id_);
                }
                response.status = ResponseStatus::overloaded;
                break;
            }
            if (request.key.size() > config_.durable_mutation_queue_bytes ||
                request.value.size() > config_.durable_mutation_queue_bytes - request.key.size()) {
                durable_mutations_->note_rejected(executor_id_);
                response.status = ResponseStatus::overloaded;
                break;
            }
            try {
                DurableMutationTask task{
                    .connection = token,
                    .request_id = request.request_id,
                    .worker_index = executor_id_,
                    .kind = DurableMutationKind::put,
                    .key = std::string{key_string},
                    .key_hash = key_hash,
                    .value = std::vector<std::byte>{request.value.begin(), request.value.end()},
                    .expire_at_ns = request.expire_at_ns,
                    .completions = &durable_mutation_completions_,
                    .wakeup = &wakeup_,
                };
                task.admission_bytes =
                    sizeof(DurableMutationTask) + task.key.capacity() + task.value.capacity();
                if (task.admission_bytes > config_.durable_mutation_queue_bytes ||
                    durable_mutation_bytes_outstanding_ >
                        config_.durable_mutation_queue_bytes - task.admission_bytes) {
                    durable_mutations_->note_rejected(executor_id_);
                    response.status = ResponseStatus::overloaded;
                    break;
                }
                const auto admission_bytes = task.admission_bytes;
                ++durable_mutations_outstanding_;
                durable_mutation_bytes_outstanding_ += admission_bytes;
                if (!durable_mutations_->try_submit(std::move(task))) {
                    --durable_mutations_outstanding_;
                    durable_mutation_bytes_outstanding_ -= admission_bytes;
                    response.status = ResponseStatus::overloaded;
                    break;
                }
            } catch (const std::bad_alloc&) {
                durable_mutations_->note_rejected(executor_id_);
                response.status = ResponseStatus::overloaded;
                break;
            }
            current->request_in_flight = true;
            return update_connection_interest(token);
        }
        if (auto stored =
                detail::StoreAccess::put(store_, executor_id_, key, request.value, request.expire_at_ns);
            !stored) {
            response.status = response_status(stored.error());
        }
        break;
    case RequestOpcode::erase:
        if (detail::StoreAccess::is_durable(store_)) {
            auto* current = connection(token);
            if (current == nullptr) {
                return {};
            }
            if (detail::StoreAccess::maintenance_mutations_rejected(store_)) {
                if (durable_mutations_) {
                    durable_mutations_->note_rejected(executor_id_);
                }
                response.status = ResponseStatus::overloaded;
                break;
            }
            if (durable_mutations_ == nullptr ||
                durable_mutations_outstanding_ >= config_.durable_mutation_queue_capacity) {
                if (durable_mutations_) {
                    durable_mutations_->note_rejected(executor_id_);
                }
                response.status = ResponseStatus::overloaded;
                break;
            }
            if (request.key.size() > config_.durable_mutation_queue_bytes) {
                durable_mutations_->note_rejected(executor_id_);
                response.status = ResponseStatus::overloaded;
                break;
            }
            try {
                DurableMutationTask task{
                    .connection = token,
                    .request_id = request.request_id,
                    .worker_index = executor_id_,
                    .kind = DurableMutationKind::erase,
                    .key = std::string{key_string},
                    .key_hash = key_hash,
                    .completions = &durable_mutation_completions_,
                    .wakeup = &wakeup_,
                };
                task.admission_bytes = sizeof(DurableMutationTask) + task.key.capacity();
                if (task.admission_bytes > config_.durable_mutation_queue_bytes ||
                    durable_mutation_bytes_outstanding_ >
                        config_.durable_mutation_queue_bytes - task.admission_bytes) {
                    durable_mutations_->note_rejected(executor_id_);
                    response.status = ResponseStatus::overloaded;
                    break;
                }
                const auto admission_bytes = task.admission_bytes;
                ++durable_mutations_outstanding_;
                durable_mutation_bytes_outstanding_ += admission_bytes;
                if (!durable_mutations_->try_submit(std::move(task))) {
                    --durable_mutations_outstanding_;
                    durable_mutation_bytes_outstanding_ -= admission_bytes;
                    response.status = ResponseStatus::overloaded;
                    break;
                }
            } catch (const std::bad_alloc&) {
                durable_mutations_->note_rejected(executor_id_);
                response.status = ResponseStatus::overloaded;
                break;
            }
            current->request_in_flight = true;
            return update_connection_interest(token);
        }
        if (auto erased = detail::StoreAccess::erase(store_, executor_id_, key); !erased) {
            response.status = response_status(erased.error());
        }
        break;
    case RequestOpcode::init:
    case RequestOpcode::ping:
    case RequestOpcode::health:
    case RequestOpcode::ready:
    case RequestOpcode::stats:
    case RequestOpcode::bind_worker:
        response.status = ResponseStatus::invalid_request;
        break;
    }
    return queue_response(token, response);
}

auto Reactor::process_messages() -> Status {
    if (auto drained = wakeup_.drain(); !drained) {
        return drained;
    }
    if (auto completed = process_disk_read_completions(); !completed) {
        return completed;
    }
    if (auto completed = process_durable_mutation_completions(); !completed) {
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
        auto* current = connection(completion->connection);
        if (current == nullptr) {
            continue;
        }
        if (!current->request_in_flight) {
            return fail(ErrorCode::corrupted_data, "unexpected disk-read completion");
        }
        current->request_in_flight = false;
        current->read_cancellation.reset();
        OwnedValue owned;
        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = completion->request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        if (completion->error) {
            response.status = response_status(*completion->error);
        } else if (completion->value) {
            owned = std::move(*completion->value);
            response.value = owned.bytes;
        } else {
            response.status = ResponseStatus::internal_error;
        }
        if (auto queued = queue_response(completion->connection, response); !queued) {
            close_connection(completion->connection);
            continue;
        }
        if (auto processed = process_frames(completion->connection); !processed) {
            close_connection(completion->connection);
            continue;
        }
        if (connection(completion->connection) != nullptr) {
            if (auto flushed = write_ready(completion->connection); !flushed) {
                close_connection(completion->connection);
            }
        }
    }
    return {};
}

auto Reactor::process_durable_mutation_completions() -> Status {
    while (auto completion = durable_mutation_completions_.try_pop()) {
        if (durable_mutations_outstanding_ == 0) {
            return fail(ErrorCode::corrupted_data, "durable-mutation completion accounting underflow");
        }
        --durable_mutations_outstanding_;
        if (completion->admission_bytes > durable_mutation_bytes_outstanding_) {
            return fail(ErrorCode::corrupted_data, "durable-mutation byte accounting underflow");
        }
        durable_mutation_bytes_outstanding_ -= completion->admission_bytes;
        auto* current = connection(completion->connection);
        if (current == nullptr) {
            continue;
        }
        if (!current->request_in_flight || current->read_cancellation) {
            return fail(ErrorCode::corrupted_data, "unexpected durable-mutation completion");
        }
        current->request_in_flight = false;
        ResponseView response{.status = ResponseStatus::ok,
                              .request_id = completion->request_id,
                              .owner_worker = static_cast<std::uint32_t>(executor_id_),
                              .worker_count = static_cast<std::uint32_t>(mesh_.size()),
                              .routing_epoch = kRoutingEpoch};
        if (completion->error) {
            response.status = response_status(*completion->error);
        }
        if (auto queued = queue_response(completion->connection, response); !queued) {
            close_connection(completion->connection);
            continue;
        }
        if (auto processed = process_frames(completion->connection); !processed) {
            close_connection(completion->connection);
            continue;
        }
        if (connection(completion->connection) != nullptr) {
            if (auto flushed = write_ready(completion->connection); !flushed) {
                close_connection(completion->connection);
            }
        }
    }
    return {};
}

auto Reactor::update_connection_interest(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    auto interest = IoInterest::none;
    if (!current->peer_read_closed) {
        interest = interest | IoInterest::read;
    }
    if (current->output_offset < current->output.size()) {
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
                if (current->output_offset == current->output.size() && !current->request_in_flight) {
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
                if (current->output_offset == current->output.size() && !current->request_in_flight) {
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
        if (auto processed = process_frames(token); !processed) {
            return processed;
        }
        current = connection(token);
        if (current == nullptr) {
            return {};
        }
        if (current->output_offset < current->output.size()) {
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
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    while (current->output_offset < current->output.size()) {
        const auto* data = current->output.data() + current->output_offset;
        const auto remaining = current->output.size() - current->output_offset;
        std::size_t written_size = 0;
        bool would_block = false;
        if (current->tls) {
            auto written = current->tls->write(data, remaining);
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
        } else {
            const auto written = ::send(current->socket.descriptor(), data, remaining, send_flags());
            if (written > 0) {
                written_size = static_cast<std::size_t>(written);
            } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                would_block = true;
            } else if (written < 0 && errno == EINTR) {
                continue;
            } else {
                return system_error("send");
            }
        }
        if (would_block) {
            if (current->write_armed) {
                return {};
            }
            const auto interest =
                current->peer_read_closed ? IoInterest::write : IoInterest::read | IoInterest::write;
            if (auto modified = poller_.modify(current->socket.descriptor(), token.encode(), interest);
                !modified) {
                return modified;
            }
            current->write_armed = true;
            return {};
        }
        current->output_offset += written_size;
    }
    current->output.clear();
    current->output_offset = 0;
    if (current->peer_read_closed && !current->request_in_flight) {
        close_connection(token);
        return {};
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

auto Reactor::run_once(const int timeout_ms) -> Status {
    if (auto messages = process_messages(); !messages) {
        return messages;
    }
    auto ready = poller_.wait(events_, timeout_ms);
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
            if (current->output_offset == current->output.size() && !current->request_in_flight) {
                close_connection(token);
            } else if (auto modified = update_connection_interest(token); !modified) {
                close_connection(token);
            }
        }
    }
    return {};
}

} // namespace glyphastore::server
