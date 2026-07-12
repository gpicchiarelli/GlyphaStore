#include "glyphastore/server/reactor.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>
#include <sys/socket.h>

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
    default:
        return ResponseStatus::internal_error;
    }
}

[[nodiscard]] auto current_time_ns() noexcept -> std::uint64_t {
    const auto elapsed = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
}

[[nodiscard]] auto system_error(const char* operation) -> Unexpected {
    return fail(ErrorCode::io_error, std::string{operation} + ": " + std::strerror(errno));
}

[[nodiscard]] auto send_flags() noexcept -> int {
#if defined(__linux__)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

} // namespace

Reactor::Reactor(ReactorConfig config, const std::size_t executor_id, TcpListener listener, Poller poller,
                 Wakeup wakeup, Store& store, DispatchMesh& mesh)
    : config_(std::move(config)), executor_id_(executor_id), listener_(std::move(listener)),
      poller_(std::move(poller)), wakeup_(std::move(wakeup)), store_(store), mesh_(mesh),
      connections_(config_.maximum_connections), events_(config_.event_batch_size) {
    free_slots_.reserve(config_.maximum_connections);
    for (std::size_t slot = config_.maximum_connections; slot > 0; --slot) {
        free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
    }
}

auto Reactor::create(const ReactorConfig& config, const std::size_t executor_id, TcpListener listener,
                     Store& store, DispatchMesh& mesh) -> Result<std::unique_ptr<Reactor>> {
    if (executor_id >= mesh.size() || executor_id >= store.worker_count()) {
        return fail(ErrorCode::invalid_argument, "reactor executor id is outside the Worker mesh");
    }
    auto poller = Poller::create();
    if (!poller) {
        return unexpected(poller.error());
    }
    auto wakeup = Wakeup::create();
    if (!wakeup) {
        return unexpected(wakeup.error());
    }
    auto reactor = std::unique_ptr<Reactor>(new Reactor(config, executor_id, std::move(listener),
                                                        std::move(*poller), std::move(*wakeup), store, mesh));
    if (reactor->listener_.descriptor() >= 0) {
        if (auto added =
                reactor->poller_.add(reactor->listener_.descriptor(), kListenerToken, IoInterest::read);
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
    current->socket.reset();
    current->input.clear();
    current->input_offset = 0;
    current->output.clear();
    current->output_offset = 0;
    current->in_flight = 0;
    current->peer_read_closed = false;
    ++current->generation;
    if (current->generation == 0) {
        current->generation = 1;
    }
    free_slots_.push_back(token.slot);
    --active_connections_;
}

auto Reactor::accept_ready() -> Status {
    while (true) {
        auto accepted = listener_.accept();
        if (!accepted) {
            return unexpected(accepted.error());
        }
        if (!accepted->has_value()) {
            return {};
        }
        auto socket = std::move(**accepted);
        const auto target =
            config_.handoff_accepted_connections ? next_accept_executor_++ % mesh_.size() : executor_id_;
        if (target != executor_id_) {
            static_cast<void>(mesh_.try_handoff(target, std::move(socket)));
            continue;
        }
        if (auto adopted = adopt_connection(std::move(socket)); !adopted) {
            return adopted;
        }
    }
}

auto Reactor::adopt_connection(SocketHandle socket) -> Status {
    if (free_slots_.empty()) {
        return {};
    }
    const auto slot = free_slots_.back();
    free_slots_.pop_back();
    auto& current = connections_[slot];
    current.socket = std::move(socket);
    current.input.clear();
    current.input.reserve(4096);
    current.output.clear();
    current.output.reserve(4096);
    current.in_flight = 0;
    current.peer_read_closed = false;
    const ConnectionToken token{.slot = slot, .generation = current.generation};
    if (auto added = poller_.add(current.socket.descriptor(), token.encode(), IoInterest::read); !added) {
        current.socket.reset();
        free_slots_.push_back(slot);
        return added;
    }
    ++active_connections_;
    ++accepted_connections_;
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
    const bool enable_write_notifications = pending == 0;
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
    if (enable_write_notifications) {
        const auto interest =
            current->peer_read_closed ? IoInterest::write : IoInterest::read | IoInterest::write;
        return poller_.modify(current->socket.descriptor(), token.encode(), interest);
    }
    return {};
}

auto Reactor::process_frames(const ConnectionToken token) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    while (current->input_offset < current->input.size()) {
        const std::span<const std::byte> available{current->input.data() + current->input_offset,
                                                   current->input.size() - current->input_offset};
        auto decoded = decode_request(available);
        if (!decoded) {
            return unexpected(decoded.error());
        }
        if (!decoded->complete) {
            break;
        }
        ResponseView response{.status = ResponseStatus::ok, .request_id = decoded->frame.request_id};
        bool immediate_response = true;
        switch (decoded->frame.opcode) {
        case RequestOpcode::hello:
            response.value = bytes("GlyphaStore/1");
            break;
        case RequestOpcode::ping:
            response.value = decoded->frame.value;
            break;
        case RequestOpcode::get: {
            immediate_response = false;
            if (auto dispatched = dispatch_request(token, decoded->frame); !dispatched) {
                return dispatched;
            }
            break;
        }
        case RequestOpcode::put: {
            immediate_response = false;
            if (auto dispatched = dispatch_request(token, decoded->frame); !dispatched) {
                return dispatched;
            }
            break;
        }
        case RequestOpcode::erase: {
            immediate_response = false;
            if (auto dispatched = dispatch_request(token, decoded->frame); !dispatched) {
                return dispatched;
            }
            break;
        }
        }
        if (immediate_response) {
            if (auto queued = queue_response(token, response); !queued) {
                return queued;
            }
        }
        current->input_offset += decoded->consumed;
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

auto Reactor::dispatch_request(const ConnectionToken token, const RequestView& request) -> Status {
    auto* current = connection(token);
    if (current == nullptr) {
        return {};
    }
    const auto key = key_text(request.key);
    const auto key_hash = hash_key(key);
    const auto owner = route_worker(key_hash, mesh_.size());
    if (owner == executor_id_) {
        return execute_local(token, request, key_hash);
    }
    if (current->in_flight >= config_.maximum_in_flight_per_connection) {
        return queue_response(token,
                              {.status = ResponseStatus::overloaded, .request_id = request.request_id});
    }
    DispatchTask task{
        .opcode = request.opcode,
        .origin_executor = executor_id_,
        .connection = token,
        .request_id = request.request_id,
        .key_hash = key_hash,
        .expire_at_ns = request.opcode == RequestOpcode::get ? current_time_ns() : request.expire_at_ns,
        .key = std::string{key},
        .value = std::vector<std::byte>{request.value.begin(), request.value.end()},
    };
    if (!mesh_.try_submit(owner, std::move(task))) {
        return queue_response(token,
                              {.status = ResponseStatus::overloaded, .request_id = request.request_id});
    }
    ++current->in_flight;
    return {};
}

auto Reactor::execute_local(const ConnectionToken token, const RequestView& request,
                            const std::uint64_t key_hash) -> Status {
    const auto key_string = key_text(request.key);
    const HashedKey key{.key = key_string, .hash = key_hash};
    ResponseView response{.status = ResponseStatus::ok, .request_id = request.request_id};
    switch (request.opcode) {
    case RequestOpcode::get: {
        auto record = store_.get_owned(executor_id_, key, current_time_ns());
        if (record) {
            response.value = record->value;
        } else {
            response.status = response_status(record.error());
        }
        break;
    }
    case RequestOpcode::put: {
        if (auto stored = store_.put_owned(executor_id_, key, request.value, request.expire_at_ns); !stored) {
            response.status = response_status(stored.error());
        }
        break;
    }
    case RequestOpcode::erase: {
        if (auto erased = store_.erase_owned(executor_id_, key); !erased) {
            response.status = response_status(erased.error());
        }
        break;
    }
    case RequestOpcode::hello:
    case RequestOpcode::ping:
        response.status = ResponseStatus::invalid_request;
        break;
    }
    return queue_response(token, response);
}

auto Reactor::execute_remote(DispatchTask task) -> DispatchCompletion {
    DispatchCompletion completion{.connection = task.connection, .request_id = task.request_id};
    const HashedKey key{.key = task.key, .hash = task.key_hash};
    switch (task.opcode) {
    case RequestOpcode::get: {
        auto record = store_.get_owned(executor_id_, key, task.expire_at_ns);
        if (record) {
            completion.value.assign(record->value.begin(), record->value.end());
        } else {
            completion.status = response_status(record.error());
        }
        break;
    }
    case RequestOpcode::put: {
        if (auto stored = store_.put_owned(executor_id_, key, task.value, task.expire_at_ns); !stored) {
            completion.status = response_status(stored.error());
        }
        break;
    }
    case RequestOpcode::erase: {
        if (auto erased = store_.erase_owned(executor_id_, key); !erased) {
            completion.status = response_status(erased.error());
        }
        break;
    }
    case RequestOpcode::hello:
    case RequestOpcode::ping:
        completion.status = ResponseStatus::invalid_request;
        break;
    }
    return completion;
}

auto Reactor::process_completions() -> Status {
    while (auto completion = mesh_.try_pop_completion(executor_id_)) {
        auto* current = connection(completion->connection);
        if (current == nullptr) {
            continue;
        }
        if (current->in_flight == 0) {
            return fail(ErrorCode::corrupted_data, "completion has no matching in-flight request");
        }
        --current->in_flight;
        if (auto queued = queue_response(completion->connection, {.status = completion->status,
                                                                  .request_id = completion->request_id,
                                                                  .value = completion->value});
            !queued) {
            close_connection(completion->connection);
        }
    }
    return {};
}

auto Reactor::process_remote_tasks() -> Status {
    if (deferred_completion_) {
        if (!mesh_.try_complete(deferred_completion_->first, std::move(deferred_completion_->second))) {
            return {};
        }
        deferred_completion_.reset();
    }
    std::size_t processed{};
    while (processed < config_.maximum_remote_tasks_per_cycle) {
        auto task = mesh_.try_pop_task(executor_id_);
        if (!task) {
            return {};
        }
        const auto origin = task->origin_executor;
        auto completion = execute_remote(std::move(*task));
        if (!mesh_.try_complete(origin, std::move(completion))) {
            deferred_completion_.emplace(origin, std::move(completion));
            return {};
        }
        ++processed;
    }
    return wakeup_.notify();
}

auto Reactor::process_messages() -> Status {
    if (auto drained = wakeup_.drain(); !drained) {
        return drained;
    }
    if (auto handoffs = process_handoffs(); !handoffs) {
        return handoffs;
    }
    if (auto completions = process_completions(); !completions) {
        return completions;
    }
    return process_remote_tasks();
}

auto Reactor::process_handoffs() -> Status {
    while (auto socket = mesh_.try_pop_handoff(executor_id_)) {
        if (auto adopted = adopt_connection(std::move(*socket)); !adopted) {
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
        const auto received = ::recv(current->socket.descriptor(), buffer.data(), buffer.size(), 0);
        if (received > 0) {
            const auto received_size = static_cast<std::size_t>(received);
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
            continue;
        }
        if (received == 0) {
            current->peer_read_closed = true;
            if (current->output_offset == current->output.size() && current->in_flight == 0) {
                close_connection(token);
                return {};
            }
            const auto interest =
                current->output_offset == current->output.size() ? IoInterest::none : IoInterest::write;
            return poller_.modify(current->socket.descriptor(), token.encode(), interest);
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return {};
        }
        if (errno == EINTR) {
            continue;
        }
        return system_error("recv");
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
        const auto written = ::send(current->socket.descriptor(), data, remaining, send_flags());
        if (written > 0) {
            current->output_offset += static_cast<std::size_t>(written);
            continue;
        }
        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return {};
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return system_error("send");
    }
    current->output.clear();
    current->output_offset = 0;
    if (current->peer_read_closed && current->in_flight == 0) {
        close_connection(token);
        return {};
    }
    const auto interest = current->peer_read_closed ? IoInterest::none : IoInterest::read;
    return poller_.modify(current->socket.descriptor(), token.encode(), interest);
}

auto Reactor::run_once(const int timeout_ms) -> Status {
    if (auto messages = process_messages(); !messages) {
        return messages;
    }
    const auto effective_timeout = deferred_completion_.has_value() ? 0 : timeout_ms;
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
            if (auto accepted = accept_ready(); !accepted) {
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
            if (current->output_offset == current->output.size() && current->in_flight == 0) {
                close_connection(token);
            } else {
                const auto interest =
                    current->output_offset == current->output.size() ? IoInterest::none : IoInterest::write;
                if (auto modified = poller_.modify(current->socket.descriptor(), token.encode(), interest);
                    !modified) {
                    close_connection(token);
                }
            }
        }
    }
    return {};
}

} // namespace glyphastore::server
