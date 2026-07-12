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

Reactor::Reactor(ReactorConfig config, TcpListener listener, Poller poller, Wakeup wakeup,
                 std::unique_ptr<Store> store)
    : config_(std::move(config)), listener_(std::move(listener)), poller_(std::move(poller)),
      wakeup_(std::move(wakeup)), store_(std::move(store)),
      dispatcher_(*store_, wakeup_, config_.worker_inbox_capacity, config_.completion_queue_capacity),
      connections_(config_.maximum_connections), events_(config_.event_batch_size) {
    free_slots_.reserve(config_.maximum_connections);
    for (std::size_t slot = config_.maximum_connections; slot > 0; --slot) {
        free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
    }
}

auto Reactor::create(const ReactorConfig& config) -> Result<std::unique_ptr<Reactor>> {
    constexpr std::size_t maximum_queue_capacity = std::size_t{1} << 30U;
    if (config.maximum_connections == 0 || config.worker_count == 0 || config.event_batch_size == 0 ||
        config.worker_inbox_capacity == 0 || config.completion_queue_capacity == 0 ||
        config.maximum_in_flight_per_connection == 0 ||
        config.worker_inbox_capacity > maximum_queue_capacity ||
        config.completion_queue_capacity > maximum_queue_capacity ||
        config.maximum_connections > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::invalid_argument,
                    "reactor capacity configuration is outside supported limits");
    }
    auto listener = TcpListener::bind(config.bind_address, config.port);
    if (!listener) {
        return unexpected(listener.error());
    }
    auto poller = Poller::create();
    if (!poller) {
        return unexpected(poller.error());
    }
    auto wakeup = Wakeup::create();
    if (!wakeup) {
        return unexpected(wakeup.error());
    }
    auto store = Store::open({.worker_config = {.explicit_count = config.worker_count}});
    if (!store) {
        return unexpected(store.error());
    }
    auto reactor = std::unique_ptr<Reactor>(
        new Reactor(config, std::move(*listener), std::move(*poller), std::move(*wakeup), std::move(*store)));
    if (auto added = reactor->poller_.add(reactor->listener_.descriptor(), kListenerToken, IoInterest::read);
        !added) {
        return unexpected(added.error());
    }
    if (auto added = reactor->poller_.add(reactor->wakeup_.descriptor(), kCompletionToken, IoInterest::read);
        !added) {
        return unexpected(added.error());
    }
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
        if (free_slots_.empty()) {
            continue;
        }
        const auto slot = free_slots_.back();
        free_slots_.pop_back();
        auto& current = connections_[slot];
        current.socket = std::move(**accepted);
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
    }
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
    if (current->in_flight >= config_.maximum_in_flight_per_connection) {
        return queue_response(token,
                              {.status = ResponseStatus::overloaded, .request_id = request.request_id});
    }
    const auto key = key_text(request.key);
    DispatchTask task{
        .opcode = request.opcode,
        .connection = token,
        .request_id = request.request_id,
        .key_hash = hash_key(key),
        .expire_at_ns = request.opcode == RequestOpcode::get ? current_time_ns() : request.expire_at_ns,
        .key = std::string{key},
        .value = std::vector<std::byte>{request.value.begin(), request.value.end()},
    };
    if (!dispatcher_.try_submit(std::move(task))) {
        return queue_response(token,
                              {.status = ResponseStatus::overloaded, .request_id = request.request_id});
    }
    ++current->in_flight;
    return {};
}

auto Reactor::process_completions() -> Status {
    if (auto drained = wakeup_.drain(); !drained) {
        return drained;
    }
    while (auto completion = dispatcher_.try_pop_completion()) {
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
    auto ready = poller_.wait(events_, timeout_ms);
    if (!ready) {
        return unexpected(ready.error());
    }
    for (std::size_t index = 0; index < *ready; ++index) {
        const auto event = events_[index];
        if (event.token == kCompletionToken) {
            if (auto completed = process_completions(); !completed) {
                return completed;
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
