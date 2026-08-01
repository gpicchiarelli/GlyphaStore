#include "experimental/paired_reactor.hpp"

#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/poller.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "server/system_error.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <sys/uio.h>
#include <utility>
#include <vector>

namespace glyphastore::experimental {
namespace {

using server::ConnectionToken;
using server::IoFlags;
using server::IoInterest;
using server::RequestOpcode;
using server::RequestView;
using server::ResponseStatus;
using server::ResponseView;

inline constexpr std::uint64_t kListenerToken = std::numeric_limits<std::uint64_t>::max();
inline constexpr std::uint64_t kWakeupToken = kListenerToken - 1U;
inline constexpr std::uint64_t kRoutingEpoch = 1;
inline constexpr std::size_t kMutationCredits = VolatileShardPairPrototype::kQueueCapacity;
inline constexpr std::size_t kMaximumOutputFrames = 128;

[[nodiscard]] auto key_text(const std::span<const std::byte> key) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(key.data()), key.size()};
}

[[nodiscard]] auto now_ns() noexcept -> std::uint64_t {
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

struct OutputFrame final {
    std::array<std::byte, server::kResponseHeaderBytes> header{};
    const std::byte* value{};
    std::size_t value_size{};
    std::size_t header_offset{};
    std::size_t value_offset{};
    bool generation_borrow{};

    [[nodiscard]] auto remaining() const noexcept -> std::size_t {
        return header.size() - header_offset + value_size - value_offset;
    }
};

struct PendingMutation final {
    ConnectionToken connection{};
    std::uint64_t internal_request{};
    std::uint64_t client_request{};
    bool active{};
};

} // namespace

struct PairedReactorPrototype::Impl final {
    struct Connection final {
        server::SocketHandle socket;
        std::uint32_t generation{1};
        std::vector<std::byte> input;
        std::size_t input_offset{};
        std::vector<OutputFrame> output;
        std::size_t output_head{};
        std::size_t output_count{};
        std::size_t output_bytes{};
        PrototypeReadPin output_pin;
        bool initialized{};
        bool bound{};
        bool request_in_flight{};
        bool mutation_blocked{};
        bool peer_read_closed{};
        bool write_armed{};
    };

    Impl(PairedReactorPrototypeConfig reactor_config, server::TcpListener bound_listener,
         server::Poller event_poller, server::Wakeup completion_wakeup)
        : config(std::move(reactor_config)), listener(std::move(bound_listener)),
          poller(std::move(event_poller)), wakeup(std::move(completion_wakeup)),
          connections(config.maximum_connections), events(config.event_batch_size),
          init_identity(encode_init_identity_value({})) {
        free_slots.reserve(config.maximum_connections);
        for (std::size_t index = config.maximum_connections; index > 0; --index) {
            free_slots.push_back(static_cast<std::uint32_t>(index - 1U));
        }
        for (auto& connection : connections) {
            connection.input.reserve(std::min(config.maximum_input_bytes, std::size_t{16U * 1024U}));
            connection.output.resize(config.output_frames_per_connection);
        }
    }

    ~Impl() {
        stop_accepting();
        close_all_connections();
        if (pair) {
            pair->stop_and_drain();
        }
    }

    static void notify_completion(void* context) noexcept {
        static_cast<void>(static_cast<server::Wakeup*>(context)->notify());
    }

    [[nodiscard]] auto initialize_pair() -> Status {
        auto created = VolatileShardPairPrototype::create(
            config.maximum_value_bytes, config.merge_delta_entries, config.writer_batch,
            PrototypeCompletionNotifier{.context = &wakeup, .notify = &notify_completion});
        if (!created) {
            return unexpected(created.error());
        }
        pair = std::move(*created);
        return {};
    }

    [[nodiscard]] auto connection(const ConnectionToken token) noexcept -> Connection* {
        if (token.slot >= connections.size()) {
            return nullptr;
        }
        auto& candidate = connections[token.slot];
        if (!candidate.socket.valid() || candidate.generation != token.generation) {
            return nullptr;
        }
        return &candidate;
    }

    void reset_output(Connection& current) noexcept {
        current.output_pin.reset();
        for (std::size_t index = 0; index < current.output_count; ++index) {
            current.output[(current.output_head + index) % current.output.size()] = {};
        }
        current.output_head = 0;
        current.output_count = 0;
        current.output_bytes = 0;
    }

    void close_connection(const ConnectionToken token) noexcept {
        auto* current = connection(token);
        if (current == nullptr) {
            return;
        }
        static_cast<void>(poller.remove(current->socket.descriptor()));
        current->socket.reset();
        current->input.clear();
        current->input_offset = 0;
        reset_output(*current);
        current->initialized = false;
        current->bound = false;
        current->request_in_flight = false;
        current->mutation_blocked = false;
        current->peer_read_closed = false;
        current->write_armed = false;
        ++current->generation;
        if (current->generation == 0) {
            current->generation = 1;
        }
        free_slots.push_back(token.slot);
        --active_connections;
        ++closed_connections;
    }

    [[nodiscard]] auto has_generation_output(const Connection& current) const noexcept -> bool {
        for (std::size_t index = 0; index < current.output_count; ++index) {
            const auto& frame = current.output[(current.output_head + index) % current.output.size()];
            if (frame.generation_borrow && frame.value_offset < frame.value_size) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto update_interest(const ConnectionToken token) -> Status {
        auto* current = connection(token);
        if (current == nullptr) {
            return {};
        }
        auto interest = IoInterest::none;
        if (current->output_count != 0) {
            interest = interest | IoInterest::write;
            current->write_armed = true;
        } else if (!current->peer_read_closed && !current->request_in_flight && !current->mutation_blocked) {
            interest = interest | IoInterest::read;
            current->write_armed = false;
        }
        return poller.modify(current->socket.descriptor(), token.encode(), interest);
    }

    [[nodiscard]] auto queue_response(Connection& current, const ResponseView& response,
                                      const bool generation_borrow = false) -> Status {
        if (current.output_count == current.output.size()) {
            return fail(ErrorCode::resource_exhausted, "paired Reactor output frame ring is full");
        }
        auto encoded_size = server::encoded_response_size(response);
        if (!encoded_size || *encoded_size > config.maximum_output_bytes ||
            current.output_bytes > config.maximum_output_bytes - *encoded_size) {
            return fail(ErrorCode::resource_exhausted, "paired Reactor output byte budget exceeded");
        }
        const auto index = (current.output_head + current.output_count) % current.output.size();
        auto& frame = current.output[index];
        frame = {.value = response.value.data(),
                 .value_size = response.value.size(),
                 .generation_borrow = generation_borrow};
        if (auto encoded = server::encode_response_header(frame.header, response); !encoded) {
            frame = {};
            return unexpected(encoded.error());
        }
        ++current.output_count;
        current.output_bytes += *encoded_size;
        ++responses;
        response_bytes += *encoded_size;
        return {};
    }

    void consume_written(Connection& current, std::size_t bytes) noexcept {
        while (bytes != 0 && current.output_count != 0) {
            auto& frame = current.output[current.output_head];
            const auto header_remaining = frame.header.size() - frame.header_offset;
            const auto from_header = std::min(bytes, header_remaining);
            frame.header_offset += from_header;
            current.output_bytes -= from_header;
            bytes -= from_header;
            if (bytes != 0 && frame.header_offset == frame.header.size()) {
                const auto value_remaining = frame.value_size - frame.value_offset;
                const auto from_value = std::min(bytes, value_remaining);
                frame.value_offset += from_value;
                current.output_bytes -= from_value;
                bytes -= from_value;
            }
            if (frame.remaining() == 0) {
                frame = {};
                current.output_head = (current.output_head + 1U) % current.output.size();
                --current.output_count;
            }
        }
        if (!has_generation_output(current)) {
            current.output_pin.reset();
        }
    }

    [[nodiscard]] auto flush_output(const ConnectionToken token) -> Status {
        auto* current = connection(token);
        if (current == nullptr || current->output_count == 0) {
            return {};
        }
        std::array<iovec, kMaximumOutputFrames * 2U> vectors{};
        std::size_t vector_count = 0;
        for (std::size_t index = 0; index < current->output_count; ++index) {
            const auto& frame = current->output[(current->output_head + index) % current->output.size()];
            if (frame.header_offset < frame.header.size()) {
                vectors[vector_count++] = {
                    .iov_base = const_cast<std::byte*>(frame.header.data() + frame.header_offset),
                    .iov_len = frame.header.size() - frame.header_offset};
            }
            if (frame.value_offset < frame.value_size) {
                vectors[vector_count++] = {.iov_base =
                                               const_cast<std::byte*>(frame.value + frame.value_offset),
                                           .iov_len = frame.value_size - frame.value_offset};
            }
        }
        msghdr message{};
        message.msg_iov = vectors.data();
        message.msg_iovlen = static_cast<decltype(msghdr::msg_iovlen)>(vector_count);
        ++writev_calls;
        const auto written = ::sendmsg(current->socket.descriptor(), &message, send_flags());
        if (written < 0 && errno == EINTR) {
            return flush_output(token);
        }
        if (written < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return server::system_error("paired Reactor sendmsg");
        }
        if (written > 0) {
            consume_written(*current, static_cast<std::size_t>(written));
        }
        if (current->output_count != 0) {
            ++partial_writes;
            if (has_generation_output(*current) && !current->output_pin) {
                current->output_pin = pair->pin_read_generation();
                ++slow_output_pins;
            }
            return update_interest(token);
        }
        current->output_head = 0;
        if (current->peer_read_closed && !current->request_in_flight) {
            close_connection(token);
            return {};
        }
        return update_interest(token);
    }

    void compact_input(Connection& current) {
        if (current.input_offset == 0 || current.output_count != 0) {
            return;
        }
        if (current.input_offset == current.input.size()) {
            current.input.clear();
        } else {
            current.input.erase(current.input.begin(),
                                current.input.begin() + static_cast<std::ptrdiff_t>(current.input_offset));
        }
        current.input_offset = 0;
    }

    [[nodiscard]] auto submit_mutation(const ConnectionToken token, Connection& current,
                                       const RequestView& request) -> PrototypeSubmitStatus {
        auto internal = next_internal_request++;
        if (internal == 0) {
            internal = next_internal_request++;
        }
        auto& pending = pending_mutations[internal % pending_mutations.size()];
        if (pending.active) {
            return PrototypeSubmitStatus::queue_full;
        }
        const auto status =
            request.opcode == RequestOpcode::put
                ? pair->try_submit_put(internal, key_text(request.key), request.value, request.expire_at_ns)
                : pair->try_submit_erase(internal, key_text(request.key));
        if (status != PrototypeSubmitStatus::submitted) {
            return status;
        }
        pending = {.connection = token,
                   .internal_request = internal,
                   .client_request = request.request_id,
                   .active = true};
        current.request_in_flight = true;
        current.mutation_blocked = false;
        ++mutations_submitted;
        return status;
    }

    [[nodiscard]] auto process_frames(const ConnectionToken token) -> Status {
        auto* current = connection(token);
        if (current == nullptr || current->output_count != 0 || current->request_in_flight) {
            return {};
        }
        while (current->input_offset < current->input.size() &&
               current->output_count < current->output.size()) {
            const auto available = std::span<const std::byte>{current->input}.subspan(current->input_offset);
            auto decoded = server::decode_request(
                available, std::min(config.maximum_input_bytes, server::kMaxFrameBytes));
            if (!decoded) {
                return unexpected(decoded.error());
            }
            if (!decoded->complete) {
                break;
            }
            const auto& request = decoded->frame;
            ++requests;
            ResponseView response{.request_id = request.request_id,
                                  .owner_worker = 0,
                                  .worker_count = 1,
                                  .routing_epoch = kRoutingEpoch};
            bool consume = true;
            switch (request.opcode) {
            case RequestOpcode::init:
                current->initialized = true;
                response.value = init_identity;
                break;
            case RequestOpcode::bind_worker:
                if (!current->initialized || current->bound || request.target_worker != 0) {
                    response.status = ResponseStatus::invalid_request;
                } else {
                    current->bound = true;
                }
                break;
            case RequestOpcode::ping:
                response.value = request.value;
                break;
            case RequestOpcode::health:
            case RequestOpcode::ready:
                response.value = request.opcode == RequestOpcode::health
                                     ? std::as_bytes(std::span{"GlyphaStore/live", 16})
                                     : std::as_bytes(std::span{"GlyphaStore/ready", 17});
                break;
            case RequestOpcode::stats:
            case RequestOpcode::backup:
                response.status = ResponseStatus::unsupported;
                break;
            case RequestOpcode::get: {
                if (!current->bound) {
                    response.status = ResponseStatus::not_bound;
                    break;
                }
                ++gets;
                const auto found = pair->get(key_text(request.key), now_ns());
                if (!found) {
                    response.status = ResponseStatus::not_found;
                } else {
                    response.value = found->value;
                }
                if (auto queued = queue_response(*current, response, found.has_value()); !queued) {
                    return queued;
                }
                current->input_offset += decoded->consumed;
                continue;
            }
            case RequestOpcode::put:
            case RequestOpcode::erase: {
                if (!current->bound) {
                    response.status = ResponseStatus::not_bound;
                    break;
                }
                if (current->output_bytes > config.maximum_output_bytes - server::kResponseHeaderBytes) {
                    consume = false;
                    break;
                }
                const auto submitted = submit_mutation(token, *current, request);
                if (submitted == PrototypeSubmitStatus::submitted) {
                    current->input_offset += decoded->consumed;
                    return update_interest(token);
                }
                if (submitted == PrototypeSubmitStatus::queue_full) {
                    current->mutation_blocked = true;
                    ++mutation_backpressure;
                    consume = false;
                    return update_interest(token);
                }
                response.status = submitted == PrototypeSubmitStatus::stopped
                                      ? ResponseStatus::overloaded
                                      : ResponseStatus::invalid_request;
                break;
            }
            }
            if (!consume) {
                break;
            }
            if (auto queued = queue_response(*current, response); !queued) {
                return queued;
            }
            current->input_offset += decoded->consumed;
        }
        return update_interest(token);
    }

    [[nodiscard]] auto read_ready(const ConnectionToken token) -> Status {
        auto* current = connection(token);
        if (current == nullptr) {
            return {};
        }
        std::array<std::byte, 16U * 1024U> buffer{};
        while (current->output_count == 0 && !current->request_in_flight && !current->mutation_blocked) {
            const auto received = ::recv(current->socket.descriptor(), buffer.data(), buffer.size(), 0);
            if (received > 0) {
                const auto amount = static_cast<std::size_t>(received);
                const auto buffered = current->input.size() - current->input_offset;
                if (amount > config.maximum_input_bytes || buffered > config.maximum_input_bytes - amount) {
                    return fail(ErrorCode::record_too_large, "paired Reactor input budget exceeded");
                }
                current->input.insert(current->input.end(), buffer.begin(),
                                      buffer.begin() + static_cast<std::ptrdiff_t>(amount));
                if (auto processed = process_frames(token); !processed) {
                    return processed;
                }
                current = connection(token);
                if (current == nullptr || current->output_count != 0 || current->request_in_flight ||
                    current->mutation_blocked) {
                    break;
                }
                continue;
            }
            if (received == 0) {
                current->peer_read_closed = true;
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            return server::system_error("paired Reactor recv");
        }
        if (current != nullptr && current->output_count != 0) {
            return flush_output(token);
        }
        if (current != nullptr && current->peer_read_closed && !current->request_in_flight) {
            close_connection(token);
        }
        return {};
    }

    [[nodiscard]] auto accept_ready() -> Status {
        while (true) {
            auto accepted = listener.accept();
            if (!accepted) {
                return unexpected(accepted.error());
            }
            if (!accepted->has_value()) {
                return {};
            }
            if (free_slots.empty()) {
                continue;
            }
            const auto slot = free_slots.back();
            free_slots.pop_back();
            auto& current = connections[slot];
            current.socket = std::move(**accepted);
            if (config.accepted_socket_send_buffer_bytes != 0) {
                const auto bytes = static_cast<int>(config.accepted_socket_send_buffer_bytes);
                if (::setsockopt(current.socket.descriptor(), SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) !=
                    0) {
                    current.socket.reset();
                    free_slots.push_back(slot);
                    return server::system_error("paired Reactor SO_SNDBUF");
                }
            }
            const ConnectionToken token{.slot = slot, .generation = current.generation};
            if (auto added = poller.add(current.socket.descriptor(), token.encode(), IoInterest::read);
                !added) {
                current.socket.reset();
                free_slots.push_back(slot);
                return added;
            }
            ++active_connections;
            ++accepted_connections;
        }
    }

    [[nodiscard]] auto process_completions() -> Status {
        std::array<PrototypeCompletion, kMutationCredits> completed{};
        std::size_t count = 0;
        while (count < completed.size()) {
            auto completion = pair->try_pop_completion();
            if (!completion) {
                break;
            }
            completed[count++] = std::move(*completion);
        }
        if (count == 0) {
            return {};
        }

        // Every completion pop acquires the Writer's queue publication, which
        // is sequenced after its ReadGeneration release-store. One subsequent
        // adoption therefore observes at least the newest generation covered
        // by this drained completion batch. Per-completion seq-cst turns would
        // add cache-line traffic without strengthening the ACK contract.
        pair->adopt_publication();
        for (const auto& completion : std::span{completed}.first(count)) {
            auto& pending = pending_mutations[completion.request_id % pending_mutations.size()];
            if (!pending.active || pending.internal_request != completion.request_id) {
                return fail(ErrorCode::corrupted_data, "paired Reactor completion credit mismatch");
            }
            const auto token = pending.connection;
            const auto client_request = pending.client_request;
            pending = {};
            ++mutation_completions;
            auto* current = connection(token);
            if (current == nullptr) {
                continue;
            }
            current->request_in_flight = false;
            ResponseView response{.status = completion.error.has_value() ? ResponseStatus::overloaded
                                                                         : ResponseStatus::ok,
                                  .request_id = client_request,
                                  .owner_worker = 0,
                                  .worker_count = 1,
                                  .routing_epoch = kRoutingEpoch};
            if (auto queued = queue_response(*current, response); !queued) {
                close_connection(token);
            }
        }
        return {};
    }

    [[nodiscard]] auto service_connections() -> Status {
        for (std::size_t slot = 0; slot < connections.size(); ++slot) {
            auto& current = connections[slot];
            if (!current.socket.valid()) {
                continue;
            }
            const ConnectionToken token{.slot = static_cast<std::uint32_t>(slot),
                                        .generation = current.generation};
            if (current.output_count != 0) {
                if (auto flushed = flush_output(token); !flushed) {
                    close_connection(token);
                }
                continue;
            }
            compact_input(current);
            if (!current.request_in_flight && current.input_offset < current.input.size()) {
                current.mutation_blocked = false;
                if (auto processed = process_frames(token); !processed) {
                    close_connection(token);
                    continue;
                }
                if (auto* live = connection(token); live != nullptr && live->output_count != 0) {
                    if (auto flushed = flush_output(token); !flushed) {
                        close_connection(token);
                    }
                }
            }
        }
        return {};
    }

    [[nodiscard]] auto run_once(const int timeout_ms) -> Status {
        pair->adopt_publication();
        if (auto completed = process_completions(); !completed) {
            return completed;
        }
        if (auto serviced = service_connections(); !serviced) {
            return serviced;
        }
        auto ready = poller.wait(events, timeout_ms);
        if (!ready) {
            return unexpected(ready.error());
        }
        for (std::size_t index = 0; index < *ready; ++index) {
            const auto event = events[index];
            if (event.token == kListenerToken) {
                if (auto accepted = accept_ready(); !accepted) {
                    return accepted;
                }
                continue;
            }
            if (event.token == kWakeupToken) {
                if (auto drained = wakeup.drain(); !drained) {
                    return drained;
                }
                if (auto completed = process_completions(); !completed) {
                    return completed;
                }
                continue;
            }
            const auto token = ConnectionToken::decode(event.token);
            if (server::has_flag(event.flags, IoFlags::readable)) {
                if (auto read = read_ready(token); !read) {
                    close_connection(token);
                    continue;
                }
            }
            if (connection(token) != nullptr && server::has_flag(event.flags, IoFlags::writable)) {
                if (auto written = flush_output(token); !written) {
                    close_connection(token);
                    continue;
                }
            }
            if (connection(token) != nullptr && server::has_flag(event.flags, IoFlags::error)) {
                close_connection(token);
                continue;
            }
            if (auto* current = connection(token);
                current != nullptr && server::has_flag(event.flags, IoFlags::hangup)) {
                current->peer_read_closed = true;
                if (current->output_count == 0 && !current->request_in_flight) {
                    close_connection(token);
                }
            }
        }
        if (auto completed = process_completions(); !completed) {
            return completed;
        }
        return service_connections();
    }

    void stop_accepting() noexcept {
        if (listener.descriptor() >= 0) {
            static_cast<void>(poller.remove(listener.descriptor()));
            listener = server::TcpListener{};
        }
    }

    void close_all_connections() noexcept {
        for (std::size_t slot = 0; slot < connections.size(); ++slot) {
            if (connections[slot].socket.valid()) {
                close_connection(ConnectionToken{.slot = static_cast<std::uint32_t>(slot),
                                                 .generation = connections[slot].generation});
            }
        }
    }

    [[nodiscard]] auto local_stats() const noexcept -> PairedReactorPrototypeStats {
        return {.accepted_connections = accepted_connections,
                .closed_connections = closed_connections,
                .requests = requests,
                .gets = gets,
                .mutations_submitted = mutations_submitted,
                .mutation_completions = mutation_completions,
                .mutation_backpressure = mutation_backpressure,
                .responses = responses,
                .response_bytes = response_bytes,
                .writev_calls = writev_calls,
                .partial_writes = partial_writes,
                .slow_output_pins = slow_output_pins,
                .active_connections = active_connections};
    }

    PairedReactorPrototypeConfig config;
    server::TcpListener listener;
    server::Poller poller;
    server::Wakeup wakeup;
    std::unique_ptr<VolatileShardPairPrototype> pair;
    std::vector<Connection> connections;
    std::vector<std::uint32_t> free_slots;
    std::vector<server::IoEvent> events;
    std::vector<std::byte> init_identity;
    std::array<PendingMutation, kMutationCredits> pending_mutations{};
    std::uint64_t next_internal_request{1};
    std::uint64_t accepted_connections{};
    std::uint64_t closed_connections{};
    std::uint64_t requests{};
    std::uint64_t gets{};
    std::uint64_t mutations_submitted{};
    std::uint64_t mutation_completions{};
    std::uint64_t mutation_backpressure{};
    std::uint64_t responses{};
    std::uint64_t response_bytes{};
    std::uint64_t writev_calls{};
    std::uint64_t partial_writes{};
    std::uint64_t slow_output_pins{};
    std::size_t active_connections{};
};

auto PairedReactorPrototype::create(PairedReactorPrototypeConfig config)
    -> Result<std::unique_ptr<PairedReactorPrototype>> {
    if (config.maximum_connections == 0 || config.event_batch_size == 0 ||
        config.maximum_input_bytes < server::kRequestHeaderBytes ||
        config.maximum_output_bytes < server::kResponseHeaderBytes ||
        config.output_frames_per_connection == 0 ||
        config.output_frames_per_connection > kMaximumOutputFrames || config.maximum_value_bytes == 0 ||
        config.accepted_socket_send_buffer_bytes >
            static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
        config.maximum_value_bytes > server::kMaxFrameBytes - server::kResponseHeaderBytes) {
        return fail(ErrorCode::invalid_argument, "invalid paired Reactor prototype limits");
    }
    auto listener = server::TcpListener::bind("127.0.0.1", 0, 512, false);
    if (!listener) {
        return unexpected(listener.error());
    }
    auto poller = server::Poller::create();
    if (!poller) {
        return unexpected(poller.error());
    }
    auto wakeup = server::Wakeup::create();
    if (!wakeup) {
        return unexpected(wakeup.error());
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*poller),
                                           std::move(*wakeup));
        if (auto added = impl->poller.add(impl->listener.descriptor(), kListenerToken, IoInterest::read);
            !added) {
            return unexpected(added.error());
        }
        if (auto added = impl->poller.add(impl->wakeup.descriptor(), kWakeupToken, IoInterest::read);
            !added) {
            return unexpected(added.error());
        }
        if (auto initialized = impl->initialize_pair(); !initialized) {
            return unexpected(initialized.error());
        }
        return std::unique_ptr<PairedReactorPrototype>{new PairedReactorPrototype{std::move(impl)}};
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "paired Reactor prototype allocation failed");
    }
}

PairedReactorPrototype::PairedReactorPrototype(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)), published_stats_(impl_->local_stats()),
      published_pair_stats_(impl_->pair->stats()) {}

PairedReactorPrototype::~PairedReactorPrototype() = default;

auto PairedReactorPrototype::run_once(const int timeout_ms) -> Status {
    auto status = impl_->run_once(timeout_ms);
    const auto reactor_stats = impl_->local_stats();
    const auto pair_stats = impl_->pair->stats();
    {
        const std::lock_guard lock{stats_mutex_};
        published_stats_ = reactor_stats;
        published_pair_stats_ = pair_stats;
    }
    return status;
}

void PairedReactorPrototype::stop_accepting() noexcept {
    impl_->stop_accepting();
}

void PairedReactorPrototype::close_all_connections() noexcept {
    impl_->close_all_connections();
}

auto PairedReactorPrototype::port() const noexcept -> std::uint16_t {
    return impl_->listener.port();
}

auto PairedReactorPrototype::idle() const noexcept -> bool {
    return stats().active_connections == 0;
}

auto PairedReactorPrototype::stats() const noexcept -> PairedReactorPrototypeStats {
    const std::lock_guard lock{stats_mutex_};
    return published_stats_;
}

auto PairedReactorPrototype::pair_stats() const noexcept -> PrototypePairStats {
    const std::lock_guard lock{stats_mutex_};
    return published_pair_stats_;
}

} // namespace glyphastore::experimental
