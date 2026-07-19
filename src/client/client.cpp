#include "glyphastore/client/client.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/server/protocol.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <variant>

namespace glyphastore::client {
namespace {

using Clock = std::chrono::steady_clock;

class Socket final {
  public:
    Socket() = default;
    explicit Socket(const int descriptor) noexcept : descriptor_(descriptor) {}
    ~Socket() {
        reset();
    }

    Socket(Socket&& other) noexcept : descriptor_(std::exchange(other.descriptor_, -1)) {}
    auto operator=(Socket&& other) noexcept -> Socket& {
        if (this != &other) {
            reset();
            descriptor_ = std::exchange(other.descriptor_, -1);
        }
        return *this;
    }
    Socket(const Socket&) = delete;
    auto operator=(const Socket&) -> Socket& = delete;

    [[nodiscard]] auto get() const noexcept -> int {
        return descriptor_;
    }
    [[nodiscard]] explicit operator bool() const noexcept {
        return descriptor_ >= 0;
    }
    void reset() noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
            descriptor_ = -1;
        }
    }

  private:
    int descriptor_{-1};
};

struct OwnedResponse {
    server::ResponseStatus status{server::ResponseStatus::internal_error};
    std::uint64_t request_id{};
    std::uint32_t owner_worker{server::kNoWorker};
    std::uint32_t worker_count{};
    std::uint64_t routing_epoch{};
    std::vector<std::byte> value;
};

struct ExchangeFailure {
    Error error;
    std::size_t request_bytes_sent{};
};

using ExchangeResult = std::variant<OwnedResponse, ExchangeFailure>;

[[nodiscard]] auto system_error(const ErrorCode code, const std::string_view operation) -> Error {
    return {code, std::string{operation} + ": " + std::strerror(errno)};
}

[[nodiscard]] auto wait_for(const int descriptor, const short events, const Clock::time_point deadline)
    -> Status {
    for (;;) {
        const auto now = Clock::now();
        if (now >= deadline) {
            return fail(ErrorCode::unavailable, "TCP operation timed out");
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const auto bounded_timeout =
            std::clamp<std::int64_t>(remaining.count(), 1, std::numeric_limits<int>::max());
        const auto timeout = static_cast<int>(bounded_timeout);
        pollfd event{.fd = descriptor, .events = events, .revents = 0};
        const auto result = ::poll(&event, 1, timeout);
        if (result > 0) {
            if ((event.revents & events) != 0) {
                return {};
            }
            return fail(ErrorCode::io_error, "TCP connection closed while waiting for I/O");
        }
        if (result == 0) {
            return fail(ErrorCode::unavailable, "TCP operation timed out");
        }
        if (errno != EINTR) {
            return unexpected(system_error(ErrorCode::io_error, "poll"));
        }
    }
}

[[nodiscard]] auto connect_socket(const ClientConfig& config) -> Result<Socket> {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* addresses{};
    const auto service = std::to_string(config.port);
    const auto resolved = ::getaddrinfo(config.host.c_str(), service.c_str(), &hints, &addresses);
    if (resolved != 0) {
        return fail(ErrorCode::unavailable,
                    std::string{"cannot resolve server address: "} + ::gai_strerror(resolved));
    }
    const std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard{addresses, &::freeaddrinfo};
    Error last_error{ErrorCode::unavailable, "no server address could be connected"};
    const auto deadline = Clock::now() + std::chrono::milliseconds{config.connect_timeout_ms};
    for (auto* address = addresses; address != nullptr; address = address->ai_next) {
        Socket socket{::socket(address->ai_family, address->ai_socktype, address->ai_protocol)};
        if (!socket) {
            last_error = system_error(ErrorCode::io_error, "socket");
            continue;
        }
        const auto flags = ::fcntl(socket.get(), F_GETFL, 0);
        if (flags < 0 || ::fcntl(socket.get(), F_SETFL, flags | O_NONBLOCK) < 0 ||
            ::fcntl(socket.get(), F_SETFD, FD_CLOEXEC) < 0) {
            last_error = system_error(ErrorCode::io_error, "fcntl");
            continue;
        }
        const int enabled = 1;
        static_cast<void>(::setsockopt(socket.get(), IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)));
#if defined(SO_NOSIGPIPE)
        static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)));
#endif
        if (::connect(socket.get(), address->ai_addr, address->ai_addrlen) == 0) {
            return socket;
        }
        if (errno != EINPROGRESS) {
            last_error = system_error(ErrorCode::unavailable, "connect");
            continue;
        }
        auto ready = wait_for(socket.get(), POLLOUT, deadline);
        if (!ready) {
            last_error = ready.error();
            continue;
        }
        int socket_error{};
        socklen_t socket_error_size = sizeof(socket_error);
        if (::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_size) != 0) {
            last_error = system_error(ErrorCode::io_error, "getsockopt");
            continue;
        }
        if (socket_error != 0) {
            last_error = {ErrorCode::unavailable, std::string{"connect: "} + std::strerror(socket_error)};
            continue;
        }
        return socket;
    }
    return unexpected(std::move(last_error));
}

[[nodiscard]] auto load_u32(const std::span<const std::byte> input) noexcept -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[index])) << (index * 8U);
    }
    return value;
}

[[nodiscard]] auto send_frame(const int descriptor, const std::span<const std::byte> frame,
                              const Clock::time_point deadline)
    -> std::variant<std::size_t, ExchangeFailure> {
    std::size_t sent{};
    while (sent < frame.size()) {
#if defined(MSG_NOSIGNAL)
        constexpr int send_flags = MSG_NOSIGNAL;
#else
        constexpr int send_flags = 0;
#endif
        const auto count = ::send(descriptor, frame.data() + sent, frame.size() - sent, send_flags);
        if (count > 0) {
            sent += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            auto ready = wait_for(descriptor, POLLOUT, deadline);
            if (ready) {
                continue;
            }
            return ExchangeFailure{ready.error(), sent};
        }
        return ExchangeFailure{system_error(ErrorCode::io_error, "send"), sent};
    }
    return sent;
}

[[nodiscard]] auto receive_exact(const int descriptor, const std::span<std::byte> output,
                                 const Clock::time_point deadline) -> Status {
    std::size_t received{};
    while (received < output.size()) {
        const auto count = ::recv(descriptor, output.data() + received, output.size() - received, 0);
        if (count > 0) {
            received += static_cast<std::size_t>(count);
            continue;
        }
        if (count == 0) {
            return fail(ErrorCode::io_error, "server closed the TCP connection");
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            auto ready = wait_for(descriptor, POLLIN, deadline);
            if (ready) {
                continue;
            }
            return ready;
        }
        return unexpected(system_error(ErrorCode::io_error, "receive"));
    }
    return {};
}

[[nodiscard]] auto receive_response(Socket& socket, const ClientConfig& config,
                                    const Clock::time_point deadline, const std::size_t request_bytes_sent)
    -> ExchangeResult {
    std::array<std::byte, sizeof(std::uint32_t)> size_bytes{};
    if (auto received = receive_exact(socket.get(), size_bytes, deadline); !received) {
        return ExchangeFailure{received.error(), request_bytes_sent};
    }
    const auto frame_size = static_cast<std::size_t>(load_u32(size_bytes));
    if (frame_size < server::kResponseHeaderBytes || frame_size > config.maximum_frame_bytes) {
        return ExchangeFailure{{ErrorCode::corrupted_data, "server response size is outside client limits"},
                               request_bytes_sent};
    }
    std::vector<std::byte> frame(frame_size);
    std::ranges::copy(size_bytes, frame.begin());
    if (auto received =
            receive_exact(socket.get(), std::span<std::byte>{frame}.subspan(size_bytes.size()), deadline);
        !received) {
        return ExchangeFailure{received.error(), request_bytes_sent};
    }
    auto decoded = server::decode_response(frame, config.maximum_frame_bytes);
    if (!decoded || !decoded->complete || decoded->consumed != frame.size()) {
        return ExchangeFailure{{ErrorCode::corrupted_data, "server returned an invalid response frame"},
                               request_bytes_sent};
    }
    return OwnedResponse{.status = decoded->frame.status,
                         .request_id = decoded->frame.request_id,
                         .owner_worker = decoded->frame.owner_worker,
                         .worker_count = decoded->frame.worker_count,
                         .routing_epoch = decoded->frame.routing_epoch,
                         .value = {decoded->frame.value.begin(), decoded->frame.value.end()}};
}

[[nodiscard]] auto exchange(Socket& socket, const std::span<const std::byte> request,
                            const ClientConfig& config) -> ExchangeResult {
    const auto deadline = Clock::now() + std::chrono::milliseconds{config.request_timeout_ms};
    auto sent = send_frame(socket.get(), request, deadline);
    if (const auto* failure = std::get_if<ExchangeFailure>(&sent)) {
        return *failure;
    }
    const auto request_bytes_sent = std::get<std::size_t>(sent);
    return receive_response(socket, config, deadline, request_bytes_sent);
}

[[nodiscard]] auto response_error(const server::ResponseStatus status) -> Error {
    switch (status) {
    case server::ResponseStatus::invalid_request:
        return {ErrorCode::invalid_argument, "server rejected the request as invalid"};
    case server::ResponseStatus::unsupported:
        return {ErrorCode::invalid_argument, "server does not support the request"};
    case server::ResponseStatus::internal_error:
        return {ErrorCode::internal_error, "server reported an internal error"};
    case server::ResponseStatus::not_found:
        return {ErrorCode::not_found, "key was not found"};
    case server::ResponseStatus::overloaded:
        return {ErrorCode::resource_exhausted, "server is overloaded"};
    case server::ResponseStatus::wrong_owner:
        return {ErrorCode::corrupted_data, "server rejected the client's worker routing"};
    case server::ResponseStatus::not_bound:
        return {ErrorCode::unavailable, "server connection is not bound to a worker"};
    case server::ResponseStatus::ok:
        break;
    }
    return {ErrorCode::internal_error, "unexpected successful response mapping"};
}

[[nodiscard]] auto as_bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto is_mutation(const PipelineOpcode opcode) noexcept -> bool {
    return opcode == PipelineOpcode::put || opcode == PipelineOpcode::erase;
}

[[nodiscard]] auto is_supported(const PipelineOpcode opcode) noexcept -> bool {
    return opcode == PipelineOpcode::get || opcode == PipelineOpcode::put || opcode == PipelineOpcode::erase;
}

[[nodiscard]] auto wire_opcode(const PipelineOpcode opcode) noexcept -> server::RequestOpcode {
    switch (opcode) {
    case PipelineOpcode::get:
        return server::RequestOpcode::get;
    case PipelineOpcode::put:
        return server::RequestOpcode::put;
    case PipelineOpcode::erase:
        return server::RequestOpcode::erase;
    }
    return server::RequestOpcode::get;
}

struct WorkerConnection {
    explicit WorkerConnection(const std::uint32_t index) : worker(index) {
        input.reserve(64U * 1024U);
    }

    void reset() noexcept {
        socket.reset();
        input.clear();
        input_offset = 0;
    }

    std::uint32_t worker{};
    std::mutex mutex;
    Socket socket;
    std::vector<std::byte> input;
    std::size_t input_offset{};
};

[[nodiscard]] auto receive_buffered_response(WorkerConnection& connection, const ClientConfig& config,
                                             const Clock::time_point deadline,
                                             const std::size_t request_bytes_sent) -> ExchangeResult {
    for (;;) {
        const auto available = connection.input.size() - connection.input_offset;
        if (available >= sizeof(std::uint32_t)) {
            const std::span<const std::byte> pending{connection.input.data() + connection.input_offset,
                                                     available};
            const auto frame_size = static_cast<std::size_t>(load_u32(pending));
            if (frame_size < server::kResponseHeaderBytes || frame_size > config.maximum_frame_bytes) {
                return ExchangeFailure{
                    {ErrorCode::corrupted_data, "server response size is outside client limits"},
                    request_bytes_sent};
            }
            if (available >= frame_size) {
                auto decoded = server::decode_response(pending.first(frame_size), config.maximum_frame_bytes);
                if (!decoded || !decoded->complete || decoded->consumed != frame_size) {
                    return ExchangeFailure{
                        {ErrorCode::corrupted_data, "server returned an invalid response frame"},
                        request_bytes_sent};
                }
                OwnedResponse response{.status = decoded->frame.status,
                                       .request_id = decoded->frame.request_id,
                                       .owner_worker = decoded->frame.owner_worker,
                                       .worker_count = decoded->frame.worker_count,
                                       .routing_epoch = decoded->frame.routing_epoch,
                                       .value = {decoded->frame.value.begin(), decoded->frame.value.end()}};
                connection.input_offset += frame_size;
                if (connection.input_offset == connection.input.size()) {
                    connection.input.clear();
                    connection.input_offset = 0;
                }
                return response;
            }
        }

        if (connection.input_offset > 0) {
            connection.input.erase(connection.input.begin(),
                                   connection.input.begin() +
                                       static_cast<std::ptrdiff_t>(connection.input_offset));
            connection.input_offset = 0;
        }
        std::array<std::byte, 64U * 1024U> chunk;
        const auto count = ::recv(connection.socket.get(), chunk.data(), chunk.size(), 0);
        if (count > 0) {
            connection.input.insert(connection.input.end(), chunk.begin(),
                                    chunk.begin() + static_cast<std::ptrdiff_t>(count));
            continue;
        }
        if (count == 0) {
            return ExchangeFailure{{ErrorCode::io_error, "server closed the TCP connection"},
                                   request_bytes_sent};
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            auto ready = wait_for(connection.socket.get(), POLLIN, deadline);
            if (ready) {
                continue;
            }
            return ExchangeFailure{ready.error(), request_bytes_sent};
        }
        return ExchangeFailure{system_error(ErrorCode::io_error, "receive"), request_bytes_sent};
    }
}

struct Metadata {
    std::uint32_t worker_count{};
    std::uint64_t routing_epoch{};
};

} // namespace

class Client::Impl final {
  public:
    explicit Impl(ClientConfig config) : config_(std::move(config)) {}

    [[nodiscard]] auto initialize() -> Status {
        if (config_.host.empty() || config_.port == 0 || config_.connect_timeout_ms == 0 ||
            config_.request_timeout_ms == 0 || config_.maximum_frame_bytes < server::kResponseHeaderBytes ||
            config_.maximum_frame_bytes > server::kMaxFrameBytes || config_.maximum_pipeline_requests == 0 ||
            config_.maximum_pipeline_bytes < server::kRequestHeaderBytes) {
            return fail(ErrorCode::invalid_argument, "client configuration is outside protocol limits");
        }
        auto first = std::make_unique<WorkerConnection>(0);
        auto discovered = bootstrap(*first, std::nullopt);
        if (!discovered) {
            return unexpected(discovered.error());
        }
        worker_count_ = discovered->worker_count;
        routing_epoch_ = discovered->routing_epoch;
        connections_.reserve(worker_count_);
        connections_.push_back(std::move(first));
        const Metadata expected{worker_count_, routing_epoch_};
        for (std::uint32_t worker = 1; worker < worker_count_; ++worker) {
            auto connection = std::make_unique<WorkerConnection>(worker);
            auto connected = bootstrap(*connection, expected);
            if (!connected) {
                close();
                return unexpected(connected.error());
            }
            connections_.push_back(std::move(connection));
        }
        return {};
    }

    [[nodiscard]] auto get(const std::span<const std::byte> key) -> Result<std::vector<std::byte>> {
        return read(server::RequestOpcode::get, key, {});
    }

    [[nodiscard]] auto ping(const std::span<const std::byte> payload) -> Result<std::vector<std::byte>> {
        return read(server::RequestOpcode::ping, {}, payload);
    }

    [[nodiscard]] auto mutate(const server::RequestOpcode opcode, const std::span<const std::byte> key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
        -> MutationResult {
        if (!healthy_.load(std::memory_order_acquire)) {
            return rejected({ErrorCode::unavailable, "client is closed or routing metadata changed"});
        }
        const auto worker = worker_for(key);
        auto& connection = *connections_[worker];
        const std::lock_guard lock{connection.mutex};
        if (!healthy_.load(std::memory_order_acquire)) {
            return rejected({ErrorCode::unavailable, "client closed before mutation admission"});
        }
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (auto connected = ensure_connected(connection); !connected) {
                return rejected(connected.error());
            }
            const auto request_id = next_request_id();
            auto encoded = server::encode_request({.opcode = opcode,
                                                   .request_id = request_id,
                                                   .expire_at_ns = expire_at_ns,
                                                   .key = key,
                                                   .value = value});
            if (!encoded) {
                return rejected(encoded.error());
            }
            if (encoded->size() > config_.maximum_frame_bytes) {
                return rejected({ErrorCode::record_too_large, "request exceeds the configured frame limit"});
            }
            auto result = exchange(connection.socket, *encoded, config_);
            if (auto* failure = std::get_if<ExchangeFailure>(&result)) {
                connection.reset();
                if (failure->request_bytes_sent == 0 && attempt == 0) {
                    continue;
                }
                return indeterminate(failure->error);
            }
            auto& response = std::get<OwnedResponse>(result);
            if (auto valid = validate_response(response, request_id, worker); !valid) {
                connection.reset();
                return indeterminate(valid.error());
            }
            if (response.status == server::ResponseStatus::ok) {
                return {.outcome = MutationOutcome::committed};
            }
            auto error = response_error(response.status);
            if (response.status == server::ResponseStatus::internal_error) {
                return indeterminate(std::move(error));
            }
            if (response.status == server::ResponseStatus::wrong_owner ||
                response.status == server::ResponseStatus::not_bound) {
                healthy_.store(false, std::memory_order_release);
            }
            return rejected(std::move(error));
        }
        return rejected({ErrorCode::unavailable, "could not send mutation"});
    }

    [[nodiscard]] auto execute_pipeline(const std::span<const PipelineRequest> requests)
        -> Result<std::vector<PipelineResponse>> {
        if (requests.empty()) {
            return std::vector<PipelineResponse>{};
        }
        if (!healthy_.load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "client is closed or routing metadata changed");
        }
        if (requests.size() > config_.maximum_pipeline_requests) {
            return fail(ErrorCode::resource_exhausted, "pipeline exceeds the configured request limit");
        }

        struct EncodedRequest {
            std::uint64_t request_id{};
            std::size_t begin{};
            std::size_t size{};
        };

        const auto worker = worker_for(requests.front().key);
        std::vector<PipelineResponse> responses(requests.size());
        std::vector<EncodedRequest> metadata;
        metadata.reserve(requests.size());
        std::size_t output_size{};
        for (const auto& request : requests) {
            if (!is_supported(request.opcode)) {
                return fail(ErrorCode::invalid_argument, "pipeline request contains an invalid opcode");
            }
            if (worker_for(request.key) != worker) {
                return fail(ErrorCode::invalid_argument, "every pipeline key must route to the same Worker");
            }
            if ((request.opcode == PipelineOpcode::get || request.opcode == PipelineOpcode::erase) &&
                (!request.value.empty() || request.put_options.expire_at_ns != 0)) {
                return fail(ErrorCode::invalid_argument,
                            "GET and ERASE pipeline requests cannot carry PUT fields");
            }
            auto frame_size = server::encoded_request_size({.opcode = wire_opcode(request.opcode),
                                                            .expire_at_ns = request.put_options.expire_at_ns,
                                                            .key = request.key,
                                                            .value = request.value});
            if (!frame_size) {
                return unexpected(frame_size.error());
            }
            if (*frame_size > config_.maximum_frame_bytes ||
                *frame_size > config_.maximum_pipeline_bytes - output_size) {
                return fail(ErrorCode::record_too_large,
                            "pipeline exceeds a configured frame or aggregate byte limit");
            }
            metadata.push_back({.begin = output_size, .size = *frame_size});
            output_size += *frame_size;
        }

        std::vector<std::byte> output(output_size);
        for (std::size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            auto& encoded = metadata[index];
            encoded.request_id = next_request_id();
            auto written =
                server::encode_request(std::span<std::byte>{output}.subspan(encoded.begin, encoded.size),
                                       {.opcode = wire_opcode(request.opcode),
                                        .request_id = encoded.request_id,
                                        .expire_at_ns = request.put_options.expire_at_ns,
                                        .key = request.key,
                                        .value = request.value});
            if (!written) {
                return unexpected(written.error());
            }
        }

        auto& connection = *connections_[worker];
        const std::lock_guard lock{connection.mutex};
        if (!healthy_.load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "client closed before pipeline admission");
        }
        if (auto connected = ensure_connected(connection); !connected) {
            return unexpected(connected.error());
        }

        const auto mark_unresolved = [&](const std::size_t first, const Error& error,
                                         const std::size_t bytes_sent) {
            for (std::size_t index = first; index < requests.size(); ++index) {
                const auto mutation_may_have_arrived =
                    is_mutation(requests[index].opcode) && bytes_sent > metadata[index].begin;
                responses[index].outcome =
                    mutation_may_have_arrived ? PipelineOutcome::indeterminate : PipelineOutcome::failed;
                responses[index].error = error;
            }
        };

        const auto deadline = Clock::now() + std::chrono::milliseconds{config_.request_timeout_ms};
        auto sent = send_frame(connection.socket.get(), output, deadline);
        if (const auto* failure = std::get_if<ExchangeFailure>(&sent)) {
            connection.reset();
            mark_unresolved(0, failure->error, failure->request_bytes_sent);
            return responses;
        }
        const auto bytes_sent = std::get<std::size_t>(sent);
        for (std::size_t index = 0; index < requests.size(); ++index) {
            ExchangeResult result = ExchangeFailure{{ErrorCode::resource_exhausted, {}}, bytes_sent};
            try {
                result = receive_buffered_response(connection, config_, deadline, bytes_sent);
            } catch (const std::bad_alloc&) {
                connection.reset();
                mark_unresolved(
                    index,
                    {ErrorCode::resource_exhausted, "allocation failed while receiving pipeline responses"},
                    bytes_sent);
                return responses;
            }
            if (const auto* failure = std::get_if<ExchangeFailure>(&result)) {
                connection.reset();
                mark_unresolved(index, failure->error, bytes_sent);
                return responses;
            }
            auto& response = std::get<OwnedResponse>(result);
            if (auto valid = validate_response(response, metadata[index].request_id, worker); !valid) {
                connection.reset();
                mark_unresolved(index, valid.error(), bytes_sent);
                return responses;
            }
            if (response.status == server::ResponseStatus::ok) {
                responses[index].outcome = PipelineOutcome::succeeded;
                responses[index].value = std::move(response.value);
                continue;
            }
            auto error = response_error(response.status);
            responses[index].outcome = is_mutation(requests[index].opcode) &&
                                               response.status == server::ResponseStatus::internal_error
                                           ? PipelineOutcome::indeterminate
                                           : PipelineOutcome::failed;
            responses[index].error = std::move(error);
            if (response.status == server::ResponseStatus::wrong_owner ||
                response.status == server::ResponseStatus::not_bound) {
                healthy_.store(false, std::memory_order_release);
            }
        }
        return responses;
    }

    [[nodiscard]] auto worker_count() const noexcept -> std::uint32_t {
        return worker_count_;
    }
    [[nodiscard]] auto worker_for_key(const std::span<const std::byte> key) const noexcept -> std::uint32_t {
        return worker_for(key);
    }
    [[nodiscard]] auto routing_epoch() const noexcept -> std::uint64_t {
        return routing_epoch_;
    }
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return healthy_.load(std::memory_order_acquire);
    }

    void close() noexcept {
        healthy_.store(false, std::memory_order_release);
        for (auto& connection : connections_) {
            const std::lock_guard lock{connection->mutex};
            connection->reset();
        }
    }

  private:
    [[nodiscard]] auto bootstrap(WorkerConnection& connection, const std::optional<Metadata> expected)
        -> Result<Metadata> {
        auto opened = connect_socket(config_);
        if (!opened) {
            return unexpected(opened.error());
        }
        connection.reset();
        connection.socket = std::move(*opened);
        const auto init_id = next_request_id();
        auto init = server::encode_request({.opcode = server::RequestOpcode::init, .request_id = init_id});
        if (!init) {
            return unexpected(init.error());
        }
        auto initialized = exchange(connection.socket, *init, config_);
        if (const auto* failure = std::get_if<ExchangeFailure>(&initialized)) {
            connection.reset();
            return unexpected(failure->error);
        }
        const auto& response = std::get<OwnedResponse>(initialized);
        constexpr std::string_view identity{"GlyphaStore/2"};
        if (response.status != server::ResponseStatus::ok || response.request_id != init_id ||
            response.value.size() != identity.size() ||
            !std::ranges::equal(response.value, as_bytes(identity)) || response.worker_count == 0 ||
            response.worker_count > 256 || response.routing_epoch == 0) {
            connection.reset();
            return fail(ErrorCode::corrupted_data, "server INIT response is not valid protocol v2 metadata");
        }
        const Metadata metadata{response.worker_count, response.routing_epoch};
        if (expected && (metadata.worker_count != expected->worker_count ||
                         metadata.routing_epoch != expected->routing_epoch)) {
            connection.reset();
            return fail(ErrorCode::unavailable,
                        "server routing metadata changed during connection bootstrap");
        }
        const auto bind_id = next_request_id();
        auto bind = server::encode_request({.opcode = server::RequestOpcode::bind_worker,
                                            .request_id = bind_id,
                                            .target_worker = connection.worker});
        if (!bind) {
            return unexpected(bind.error());
        }
        auto bound = exchange(connection.socket, *bind, config_);
        if (const auto* failure = std::get_if<ExchangeFailure>(&bound)) {
            connection.reset();
            return unexpected(failure->error);
        }
        const auto& bind_response = std::get<OwnedResponse>(bound);
        if (bind_response.status != server::ResponseStatus::ok || bind_response.request_id != bind_id ||
            bind_response.owner_worker != connection.worker ||
            bind_response.worker_count != metadata.worker_count ||
            bind_response.routing_epoch != metadata.routing_epoch) {
            connection.reset();
            return fail(ErrorCode::corrupted_data, "server BIND_WORKER response is inconsistent");
        }
        return metadata;
    }

    [[nodiscard]] auto ensure_connected(WorkerConnection& connection) -> Status {
        if (connection.socket) {
            return {};
        }
        auto connected = bootstrap(connection, Metadata{worker_count_, routing_epoch_});
        if (!connected) {
            return unexpected(connected.error());
        }
        return {};
    }

    [[nodiscard]] auto read(const server::RequestOpcode opcode, const std::span<const std::byte> key,
                            const std::span<const std::byte> value) -> Result<std::vector<std::byte>> {
        if (!healthy_.load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "client is closed or routing metadata changed");
        }
        const auto worker = opcode == server::RequestOpcode::ping ? 0U : worker_for(key);
        auto& connection = *connections_[worker];
        const std::lock_guard lock{connection.mutex};
        if (!healthy_.load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "client closed before read admission");
        }
        Error last_error{ErrorCode::unavailable, "request was not attempted"};
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (auto connected = ensure_connected(connection); !connected) {
                last_error = connected.error();
                continue;
            }
            const auto request_id = next_request_id();
            auto encoded = server::encode_request(
                {.opcode = opcode, .request_id = request_id, .key = key, .value = value});
            if (!encoded) {
                return unexpected(encoded.error());
            }
            if (encoded->size() > config_.maximum_frame_bytes) {
                return fail(ErrorCode::record_too_large, "request exceeds the configured frame limit");
            }
            auto result = exchange(connection.socket, *encoded, config_);
            if (auto* failure = std::get_if<ExchangeFailure>(&result)) {
                last_error = failure->error;
                connection.reset();
                continue;
            }
            auto& response = std::get<OwnedResponse>(result);
            if (auto valid = validate_response(response, request_id, worker); !valid) {
                connection.reset();
                return unexpected(valid.error());
            }
            if (response.status != server::ResponseStatus::ok) {
                if (response.status == server::ResponseStatus::wrong_owner ||
                    response.status == server::ResponseStatus::not_bound) {
                    healthy_.store(false, std::memory_order_release);
                }
                return unexpected(response_error(response.status));
            }
            return std::move(response.value);
        }
        return unexpected(std::move(last_error));
    }

    [[nodiscard]] auto validate_response(const OwnedResponse& response, const std::uint64_t request_id,
                                         const std::uint32_t worker) -> Status {
        if (response.request_id != request_id) {
            return fail(ErrorCode::corrupted_data, "server response request ID does not match");
        }
        if (response.worker_count != worker_count_ || response.routing_epoch != routing_epoch_) {
            healthy_.store(false, std::memory_order_release);
            return fail(ErrorCode::unavailable, "server routing metadata changed");
        }
        if (response.owner_worker != worker && response.status != server::ResponseStatus::wrong_owner) {
            healthy_.store(false, std::memory_order_release);
            return fail(ErrorCode::corrupted_data, "server response came from the wrong worker");
        }
        return {};
    }

    [[nodiscard]] auto worker_for(const std::span<const std::byte> key) const noexcept -> std::uint32_t {
        return static_cast<std::uint32_t>(hash_key(key) % worker_count_);
    }

    [[nodiscard]] auto next_request_id() noexcept -> std::uint64_t {
        auto id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
        if (id == 0) {
            id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
        }
        return id;
    }

    [[nodiscard]] static auto rejected(Error error) -> MutationResult {
        return {.outcome = MutationOutcome::rejected, .error = std::move(error)};
    }
    [[nodiscard]] static auto indeterminate(Error error) -> MutationResult {
        return {.outcome = MutationOutcome::indeterminate, .error = std::move(error)};
    }

    ClientConfig config_;
    std::vector<std::unique_ptr<WorkerConnection>> connections_;
    std::uint32_t worker_count_{};
    std::uint64_t routing_epoch_{};
    std::atomic<std::uint64_t> next_request_id_{1};
    std::atomic<bool> healthy_{true};
};

Client::Client(std::unique_ptr<Impl> implementation) noexcept : implementation_(std::move(implementation)) {}

auto Client::connect(ClientConfig config) -> Result<Client> {
    try {
        auto implementation = std::make_unique<Impl>(std::move(config));
        if (auto initialized = implementation->initialize(); !initialized) {
            return unexpected(initialized.error());
        }
        return Client{std::move(implementation)};
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    } catch (const std::exception& exception) {
        return fail(ErrorCode::internal_error,
                    std::string{"client initialization failed: "} + exception.what());
    }
}

Client::~Client() = default;
Client::Client(Client&&) noexcept = default;
auto Client::operator=(Client&&) noexcept -> Client& = default;

auto Client::get(const std::span<const std::byte> key) -> Result<std::vector<std::byte>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->get(key);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    }
}

auto Client::get(const std::string_view key) -> Result<std::vector<std::byte>> {
    return get(as_bytes(key));
}

auto Client::ping(const std::span<const std::byte> payload) -> Result<std::vector<std::byte>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->ping(payload);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    }
}

auto Client::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                 const PutOptions options) -> MutationResult {
    try {
        if (!implementation_) {
            return {.outcome = MutationOutcome::rejected,
                    .error = Error{ErrorCode::unavailable, "client was moved from"}};
        }
        return implementation_->mutate(server::RequestOpcode::put, key, value, options.expire_at_ns);
    } catch (const std::bad_alloc&) {
        return {.outcome = MutationOutcome::rejected,
                .error = Error{ErrorCode::resource_exhausted, "client allocation failed"}};
    }
}

auto Client::put(const std::string_view key, const std::string_view value, const PutOptions options)
    -> MutationResult {
    return put(as_bytes(key), as_bytes(value), options);
}

auto Client::erase(const std::span<const std::byte> key) -> MutationResult {
    try {
        if (!implementation_) {
            return {.outcome = MutationOutcome::rejected,
                    .error = Error{ErrorCode::unavailable, "client was moved from"}};
        }
        return implementation_->mutate(server::RequestOpcode::erase, key, {}, 0);
    } catch (const std::bad_alloc&) {
        return {.outcome = MutationOutcome::rejected,
                .error = Error{ErrorCode::resource_exhausted, "client allocation failed"}};
    }
}

auto Client::erase(const std::string_view key) -> MutationResult {
    return erase(as_bytes(key));
}

auto Client::execute_pipeline(const std::span<const PipelineRequest> requests)
    -> Result<std::vector<PipelineResponse>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->execute_pipeline(requests);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "pipeline allocation failed before completion");
    }
}

auto Client::worker_for(const std::span<const std::byte> key) const noexcept -> std::uint32_t {
    return implementation_ ? implementation_->worker_for_key(key) : 0;
}

auto Client::worker_for(const std::string_view key) const noexcept -> std::uint32_t {
    return worker_for(as_bytes(key));
}

auto Client::worker_count() const noexcept -> std::uint32_t {
    return implementation_ ? implementation_->worker_count() : 0;
}

auto Client::routing_epoch() const noexcept -> std::uint64_t {
    return implementation_ ? implementation_->routing_epoch() : 0;
}

auto Client::healthy() const noexcept -> bool {
    return implementation_ && implementation_->healthy();
}

void Client::close() noexcept {
    if (implementation_) {
        implementation_->close();
    }
}

} // namespace glyphastore::client
