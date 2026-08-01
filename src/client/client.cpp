#include "glyphastore/client/client.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/tls.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <future>
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
#include <vector>

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

struct WorkerConnection {
    explicit WorkerConnection(const std::uint32_t index) : worker(index) {
        input.reserve(64U * 1024U);
    }

    void reset() noexcept {
        tls.reset();
        socket.reset();
        input.clear();
        input_offset = 0;
    }

    std::uint32_t worker{};
    std::mutex mutex;
    Socket socket;
    std::unique_ptr<server::TlsSession> tls;
    std::vector<std::byte> input;
    std::size_t input_offset{};
};

[[nodiscard]] auto send_frame(WorkerConnection& connection, const std::span<const std::byte> frame,
                              const Clock::time_point deadline)
    -> std::variant<std::size_t, ExchangeFailure> {
    std::size_t sent{};
    while (sent < frame.size()) {
        if (connection.tls) {
            auto written = connection.tls->write(frame.data() + sent, frame.size() - sent);
            if (!written) {
                return ExchangeFailure{written.error(), sent};
            }
            if (written->kind == server::TlsIoKind::would_block) {
                auto ready =
                    wait_for(connection.socket.get(), static_cast<short>(POLLIN | POLLOUT), deadline);
                if (ready) {
                    continue;
                }
                return ExchangeFailure{ready.error(), sent};
            }
            if (written->kind == server::TlsIoKind::closed) {
                return ExchangeFailure{{ErrorCode::io_error, "server closed the TLS connection"}, sent};
            }
            sent += written->bytes;
            continue;
        }
#if defined(MSG_NOSIGNAL)
        constexpr int send_flags = MSG_NOSIGNAL;
#else
        constexpr int send_flags = 0;
#endif
        const auto count =
            ::send(connection.socket.get(), frame.data() + sent, frame.size() - sent, send_flags);
        if (count > 0) {
            sent += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            auto ready = wait_for(connection.socket.get(), POLLOUT, deadline);
            if (ready) {
                continue;
            }
            return ExchangeFailure{ready.error(), sent};
        }
        return ExchangeFailure{system_error(ErrorCode::io_error, "send"), sent};
    }
    return sent;
}

[[nodiscard]] auto receive_exact(WorkerConnection& connection, const std::span<std::byte> output,
                                 const Clock::time_point deadline) -> Status {
    std::size_t received{};
    while (received < output.size()) {
        if (connection.tls) {
            auto read = connection.tls->read(output.data() + received, output.size() - received);
            if (!read) {
                return unexpected(read.error());
            }
            if (read->kind == server::TlsIoKind::would_block) {
                auto ready =
                    wait_for(connection.socket.get(), static_cast<short>(POLLIN | POLLOUT), deadline);
                if (ready) {
                    continue;
                }
                return ready;
            }
            if (read->kind == server::TlsIoKind::closed) {
                return fail(ErrorCode::io_error, "server closed the TLS connection");
            }
            received += read->bytes;
            continue;
        }
        const auto count =
            ::recv(connection.socket.get(), output.data() + received, output.size() - received, 0);
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
            auto ready = wait_for(connection.socket.get(), POLLIN, deadline);
            if (ready) {
                continue;
            }
            return ready;
        }
        return unexpected(system_error(ErrorCode::io_error, "receive"));
    }
    return {};
}

[[nodiscard]] auto receive_response(WorkerConnection& connection, const ClientConfig& config,
                                    const Clock::time_point deadline, const std::size_t request_bytes_sent)
    -> ExchangeResult {
    std::array<std::byte, sizeof(std::uint32_t)> size_bytes{};
    if (auto received = receive_exact(connection, size_bytes, deadline); !received) {
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
            receive_exact(connection, std::span<std::byte>{frame}.subspan(size_bytes.size()), deadline);
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

[[nodiscard]] auto exchange(WorkerConnection& connection, const std::span<const std::byte> request,
                            const ClientConfig& config, const Clock::time_point deadline) -> ExchangeResult {
    auto sent = send_frame(connection, request, deadline);
    if (const auto* failure = std::get_if<ExchangeFailure>(&sent)) {
        return *failure;
    }
    const auto request_bytes_sent = std::get<std::size_t>(sent);
    return receive_response(connection, config, deadline, request_bytes_sent);
}

[[nodiscard]] auto exchange(WorkerConnection& connection, const std::span<const std::byte> request,
                            const ClientConfig& config) -> ExchangeResult {
    const auto deadline = Clock::now() + std::chrono::milliseconds{config.request_timeout_ms};
    return exchange(connection, request, config, deadline);
}

[[nodiscard]] auto category_for(const ErrorCode code) -> std::string {
    switch (code) {
    case ErrorCode::invalid_argument:
    case ErrorCode::record_too_large:
        return "invalid_argument";
    case ErrorCode::not_found:
        return "not_found";
    case ErrorCode::resource_exhausted:
        return "overloaded";
    case ErrorCode::unavailable:
        return "unavailable";
    case ErrorCode::io_error:
        return "transport";
    case ErrorCode::corrupted_data:
        return "protocol";
    case ErrorCode::internal_error:
        return "internal";
    default:
        return "internal";
    }
}

[[nodiscard]] auto retryability_for(const std::string_view category, const bool mutation_sent,
                                    const bool indeterminate) -> std::string {
    if (indeterminate) {
        return "reconcile_first";
    }
    if (category == "invalid_argument" && !mutation_sent) {
        return "never";
    }
    if (category == "transport" && !mutation_sent) {
        return "same_request";
    }
    if (category == "overloaded") {
        // Wire OVERLOADED collapses admission pressure and durable capacity exhaustion
        // (including maintenance emergency). Do not advertise success-seeking retry.
        return "never";
    }
    if (category == "permission_denied") {
        return "never";
    }
    if (category == "not_found") {
        return "new_attempt";
    }
    if (category == "unavailable") {
        return "never";
    }
    return mutation_sent ? "reconcile_first" : "new_attempt";
}

[[nodiscard]] auto
enrich_error(Error error, const std::string_view operation, const std::optional<std::uint64_t> request_id,
             const std::optional<std::uint32_t> worker, const std::optional<std::uint64_t> routing_epoch,
             const std::size_t bytes_sent = 0, const std::optional<std::uint16_t> wire_status = std::nullopt,
             const bool mutation = false, const bool indeterminate = false) -> Error {
    if (error.category.empty()) {
        error.category = category_for(error.code);
    }
    error.operation = std::string{operation};
    error.request_id = request_id;
    error.worker = worker;
    error.routing_epoch = routing_epoch;
    error.bytes_sent = bytes_sent;
    if (wire_status.has_value()) {
        error.wire_status = wire_status;
    }
    if (mutation) {
        error.retryability = retryability_for(error.category, bytes_sent > 0, indeterminate);
        if (bytes_sent > 0 && error.category == "transport") {
            error.retryability = "reconcile_first";
        }
    } else {
        error.retryability = retryability_for(error.category, false, false);
    }
    return error;
}

[[nodiscard]] auto response_error(const server::ResponseStatus status) -> Error {
    Error error;
    switch (status) {
    case server::ResponseStatus::invalid_request:
        error = {ErrorCode::invalid_argument, "server rejected the request as invalid"};
        break;
    case server::ResponseStatus::unsupported:
        error = {ErrorCode::invalid_argument, "server does not support the request"};
        break;
    case server::ResponseStatus::internal_error:
        error = {ErrorCode::internal_error, "server reported an internal error"};
        break;
    case server::ResponseStatus::not_found:
        error = {ErrorCode::not_found, "key was not found"};
        break;
    case server::ResponseStatus::overloaded:
        error = {ErrorCode::resource_exhausted, "server is overloaded"};
        break;
    case server::ResponseStatus::wrong_owner:
        error = {ErrorCode::corrupted_data, "server rejected the client's worker routing"};
        break;
    case server::ResponseStatus::not_bound:
        error = {ErrorCode::unavailable, "server connection is not bound to a worker"};
        break;
    case server::ResponseStatus::permission_denied:
        error = {ErrorCode::invalid_argument, "server denied the request"};
        error.category = "permission_denied";
        error.retryability = "never";
        error.wire_status = static_cast<std::uint16_t>(status);
        return error;
    case server::ResponseStatus::ok:
        return {ErrorCode::internal_error, "unexpected successful response mapping"};
    }
    error.category = category_for(error.code);
    error.wire_status = static_cast<std::uint16_t>(status);
    error.retryability =
        retryability_for(error.category, false, status == server::ResponseStatus::internal_error);
    return error;
}

[[nodiscard]] auto as_bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto is_mutation(const PipelineOpcode opcode) noexcept -> bool {
    return opcode == PipelineOpcode::put || opcode == PipelineOpcode::erase;
}

[[nodiscard]] auto operation_name(const server::RequestOpcode opcode) noexcept -> std::string_view {
    switch (opcode) {
    case server::RequestOpcode::init:
        return "init";
    case server::RequestOpcode::ping:
        return "ping";
    case server::RequestOpcode::get:
        return "get";
    case server::RequestOpcode::put:
        return "put";
    case server::RequestOpcode::erase:
        return "erase";
    case server::RequestOpcode::bind_worker:
        return "bind_worker";
    case server::RequestOpcode::health:
        return "health";
    case server::RequestOpcode::ready:
        return "ready";
    case server::RequestOpcode::stats:
        return "stats";
    case server::RequestOpcode::backup:
        return "backup";
    }
    return "unknown";
}

[[nodiscard]] auto operation_name(const PipelineOpcode opcode) noexcept -> std::string_view {
    switch (opcode) {
    case PipelineOpcode::get:
        return "get";
    case PipelineOpcode::put:
        return "put";
    case PipelineOpcode::erase:
        return "erase";
    }
    return "unknown";
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
        if (connection.tls) {
            auto read = connection.tls->read(chunk.data(), chunk.size());
            if (!read) {
                return ExchangeFailure{read.error(), request_bytes_sent};
            }
            if (read->kind == server::TlsIoKind::would_block) {
                auto ready =
                    wait_for(connection.socket.get(), static_cast<short>(POLLIN | POLLOUT), deadline);
                if (ready) {
                    continue;
                }
                return ExchangeFailure{ready.error(), request_bytes_sent};
            }
            if (read->kind == server::TlsIoKind::closed) {
                return ExchangeFailure{{ErrorCode::io_error, "server closed the TLS connection"},
                                       request_bytes_sent};
            }
            connection.input.insert(connection.input.end(), chunk.begin(),
                                    chunk.begin() + static_cast<std::ptrdiff_t>(read->bytes));
            continue;
        }
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
    WorkerRoutingState routing{};
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
        if (config_.tls.enable) {
            server::ClientTlsConfig tls_config{
                .enable = true,
                .ca_file = config_.tls.ca_file,
                .certificate_file = config_.tls.cert_file,
                .private_key_file = config_.tls.key_file,
                .server_name = config_.tls.server_name.empty() ? config_.host : config_.tls.server_name,
                .insecure_skip_verify = config_.tls.insecure_skip_verify,
                .handshake_timeout_ms = config_.connect_timeout_ms,
            };
            auto context = server::TlsContext::create_client(tls_config);
            if (!context) {
                return unexpected(context.error());
            }
            tls_context_ = std::move(*context);
            tls_server_name_ = tls_config.server_name;
        }
        auto first = std::make_unique<WorkerConnection>(0);
        auto discovered = bootstrap(*first, std::nullopt);
        if (!discovered) {
            return unexpected(discovered.error());
        }
        worker_count_ = discovered->worker_count;
        routing_epoch_ = discovered->routing_epoch;
        routing_ = discovered->routing;
        connections_.reserve(worker_count_);
        connections_.push_back(std::move(first));
        const Metadata expected{worker_count_, routing_epoch_, routing_};
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

    [[nodiscard]] auto get(const std::span<const std::byte> key, const RequestOptions options)
        -> Result<std::vector<std::byte>> {
        return read(server::RequestOpcode::get, key, {}, options);
    }

    [[nodiscard]] auto ping(const std::span<const std::byte> payload, const RequestOptions options)
        -> Result<std::vector<std::byte>> {
        return read(server::RequestOpcode::ping, {}, payload, options);
    }

    [[nodiscard]] auto backup(const std::string_view destination, const RequestOptions options)
        -> Result<std::vector<std::byte>> {
        if (destination.empty()) {
            return fail(ErrorCode::invalid_argument, "backup destination must be non-empty");
        }
        return read(server::RequestOpcode::backup, as_bytes(destination), {}, options);
    }

    [[nodiscard]] auto resolve_deadline(const RequestOptions options) const -> Result<Clock::time_point> {
        const auto timeout_ms = options.timeout.has_value()
                                    ? static_cast<std::uint32_t>(options.timeout->count())
                                    : config_.request_timeout_ms;
        if (timeout_ms == 0 || (options.timeout.has_value() && options.timeout->count() <= 0)) {
            return fail(ErrorCode::invalid_argument, "request timeout must be positive");
        }
        return Clock::now() + std::chrono::milliseconds{timeout_ms};
    }

    [[nodiscard]] auto mutate(const server::RequestOpcode opcode, const std::span<const std::byte> key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns,
                              const RequestOptions options) -> MutationResult {
        const auto operation = opcode == server::RequestOpcode::put ? "put" : "erase";
        if (!healthy_.load(std::memory_order_acquire)) {
            return rejected(enrich_error(
                {ErrorCode::unavailable, "client is closed or routing metadata changed"}, operation,
                std::nullopt, std::nullopt, std::nullopt, 0, std::nullopt, true, false));
        }
        auto deadline = resolve_deadline(options);
        if (!deadline) {
            return rejected(enrich_error(deadline.error(), operation, std::nullopt, std::nullopt,
                                         std::nullopt, 0, std::nullopt, true, false));
        }
        const auto worker = worker_for(key);
        auto& connection = *connections_[worker];
        const std::lock_guard lock{connection.mutex};
        if (!healthy_.load(std::memory_order_acquire)) {
            return rejected(enrich_error({ErrorCode::unavailable, "client closed before mutation admission"},
                                         operation, std::nullopt, worker, routing_epoch_, 0, std::nullopt,
                                         true, false));
        }
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (auto connected = ensure_connected(connection); !connected) {
                return rejected(enrich_error(connected.error(), operation, std::nullopt, worker,
                                             routing_epoch_, 0, std::nullopt, true, false));
            }
            const auto request_id = next_request_id();
            auto encoded = server::encode_request({.opcode = opcode,
                                                   .request_id = request_id,
                                                   .expire_at_ns = expire_at_ns,
                                                   .key = key,
                                                   .value = value});
            if (!encoded) {
                return rejected(enrich_error(encoded.error(), operation, request_id, worker, routing_epoch_,
                                             0, std::nullopt, true, false));
            }
            if (encoded->size() > config_.maximum_frame_bytes) {
                return rejected(enrich_error(
                    {ErrorCode::record_too_large, "request exceeds the configured frame limit"}, operation,
                    request_id, worker, routing_epoch_, 0, std::nullopt, true, false));
            }
            auto result = exchange(connection, *encoded, config_, *deadline);
            if (auto* failure = std::get_if<ExchangeFailure>(&result)) {
                connection.reset();
                if (failure->request_bytes_sent == 0) {
                    if (attempt == 0) {
                        continue;
                    }
                    return rejected(enrich_error(failure->error, operation, request_id, worker,
                                                 routing_epoch_, 0, std::nullopt, true, false));
                }
                return indeterminate(enrich_error(failure->error, operation, request_id, worker,
                                                  routing_epoch_, failure->request_bytes_sent, std::nullopt,
                                                  true, true));
            }
            auto& response = std::get<OwnedResponse>(result);
            if (auto valid = validate_response(response, request_id, worker); !valid) {
                connection.reset();
                return indeterminate(enrich_error(valid.error(), operation, request_id, worker,
                                                  routing_epoch_, encoded->size(), std::nullopt, true, true));
            }
            if (response.status == server::ResponseStatus::ok) {
                if (!response.value.empty()) {
                    connection.reset();
                    return indeterminate(enrich_error(
                        {ErrorCode::corrupted_data, "mutation response value must be empty"}, operation,
                        request_id, worker, routing_epoch_, encoded->size(), std::nullopt, true, true));
                }
                return {.outcome = MutationOutcome::committed};
            }
            auto error =
                enrich_error(response_error(response.status), operation, request_id, worker, routing_epoch_,
                             encoded->size(), static_cast<std::uint16_t>(response.status), true,
                             response.status == server::ResponseStatus::internal_error);
            if (response.status == server::ResponseStatus::internal_error) {
                return indeterminate(std::move(error));
            }
            if (response.status == server::ResponseStatus::wrong_owner ||
                response.status == server::ResponseStatus::not_bound) {
                healthy_.store(false, std::memory_order_release);
            }
            return rejected(std::move(error));
        }
        return rejected(enrich_error({ErrorCode::unavailable, "could not send mutation"}, operation,
                                     std::nullopt, worker, routing_epoch_, 0, std::nullopt, true, false));
    }

    [[nodiscard]] auto execute_pipeline(const std::span<const PipelineRequest> requests,
                                        const RequestOptions options)
        -> Result<std::vector<PipelineResponse>> {
        auto deadline = resolve_deadline(options);
        if (!deadline) {
            return unexpected(deadline.error());
        }
        return execute_pipeline(requests, *deadline);
    }

    [[nodiscard]] auto execute_pipeline(const std::span<const PipelineRequest> requests,
                                        const Clock::time_point deadline)
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

        const auto mark_unresolved = [&](const std::size_t first, Error error, const std::size_t bytes_sent) {
            for (std::size_t index = first; index < requests.size(); ++index) {
                const auto mutation_may_have_arrived =
                    is_mutation(requests[index].opcode) && bytes_sent > metadata[index].begin;
                responses[index].outcome =
                    mutation_may_have_arrived ? PipelineOutcome::indeterminate : PipelineOutcome::failed;
                responses[index].error = enrich_error(
                    error, operation_name(requests[index].opcode), metadata[index].request_id, worker,
                    routing_epoch_,
                    bytes_sent > metadata[index].begin ? bytes_sent - metadata[index].begin : 0,
                    error.wire_status, is_mutation(requests[index].opcode), mutation_may_have_arrived);
            }
        };

        auto sent = send_frame(connection, output, deadline);
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
                if (is_mutation(requests[index].opcode) && !response.value.empty()) {
                    connection.reset();
                    mark_unresolved(index,
                                    {ErrorCode::corrupted_data, "mutation response value must be empty"},
                                    bytes_sent);
                    return responses;
                }
                responses[index].outcome = PipelineOutcome::succeeded;
                responses[index].value = std::move(response.value);
                continue;
            }
            const auto indeterminate = is_mutation(requests[index].opcode) &&
                                       response.status == server::ResponseStatus::internal_error;
            responses[index].outcome =
                indeterminate ? PipelineOutcome::indeterminate : PipelineOutcome::failed;
            responses[index].error =
                enrich_error(response_error(response.status), operation_name(requests[index].opcode),
                             metadata[index].request_id, worker, routing_epoch_,
                             bytes_sent - metadata[index].begin, static_cast<std::uint16_t>(response.status),
                             is_mutation(requests[index].opcode), indeterminate);
            if (response.status == server::ResponseStatus::wrong_owner ||
                response.status == server::ResponseStatus::not_bound) {
                healthy_.store(false, std::memory_order_release);
            }
        }
        return responses;
    }

    [[nodiscard]] auto execute_batch(const std::span<const PipelineRequest> requests,
                                     const RequestOptions options) -> Result<std::vector<PipelineResponse>> {
        if (requests.empty()) {
            return std::vector<PipelineResponse>{};
        }
        if (!healthy_.load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "client is closed or routing metadata changed");
        }
        auto deadline = resolve_deadline(options);
        if (!deadline) {
            return unexpected(deadline.error());
        }

        struct WorkerJob {
            std::vector<PipelineRequest> requests;
            std::vector<std::size_t> original_indices;
        };

        std::vector<WorkerJob> jobs(worker_count_);
        for (std::size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            if (!is_supported(request.opcode)) {
                return fail(ErrorCode::invalid_argument, "batch request contains an invalid opcode");
            }
            if ((request.opcode == PipelineOpcode::get || request.opcode == PipelineOpcode::erase) &&
                (!request.value.empty() || request.put_options.expire_at_ns != 0)) {
                return fail(ErrorCode::invalid_argument,
                            "GET and ERASE batch requests cannot carry PUT fields");
            }
            const auto worker = worker_for(request.key);
            if (worker >= worker_count_) {
                return fail(ErrorCode::internal_error, "batch routing produced an invalid Worker");
            }
            auto& job = jobs[worker];
            if (job.requests.size() >= config_.maximum_pipeline_requests) {
                return fail(ErrorCode::resource_exhausted,
                            "batch exceeds the configured per-Worker request limit");
            }
            job.requests.push_back(request);
            job.original_indices.push_back(index);
        }

        std::vector<PipelineResponse> responses(requests.size());
        struct ActiveJob {
            WorkerJob* job{};
            std::future<Result<std::vector<PipelineResponse>>> future;
        };
        std::vector<ActiveJob> active;
        active.reserve(worker_count_);

        const auto shared_deadline = *deadline;
        const auto run_job = [this,
                              shared_deadline](WorkerJob& job) -> Result<std::vector<PipelineResponse>> {
            return execute_pipeline(job.requests, shared_deadline);
        };

        for (auto& job : jobs) {
            if (job.requests.empty()) {
                continue;
            }
            if (active.empty() && std::none_of(jobs.begin(), jobs.end(), [&](const WorkerJob& other) {
                    return &other != &job && !other.requests.empty();
                })) {
                auto executed = run_job(job);
                if (!executed) {
                    for (const auto index : job.original_indices) {
                        responses[index].outcome = PipelineOutcome::failed;
                        responses[index].error = executed.error();
                    }
                    return responses;
                }
                for (std::size_t offset = 0; offset < job.original_indices.size(); ++offset) {
                    responses[job.original_indices[offset]] = std::move((*executed)[offset]);
                }
                return responses;
            }
            active.push_back({
                .job = &job,
                .future = std::async(std::launch::async, run_job, std::ref(job)),
            });
        }

        for (auto& item : active) {
            auto executed = item.future.get();
            if (!executed) {
                for (const auto index : item.job->original_indices) {
                    responses[index].outcome = PipelineOutcome::failed;
                    responses[index].error = executed.error();
                }
                continue;
            }
            for (std::size_t offset = 0; offset < item.job->original_indices.size(); ++offset) {
                responses[item.job->original_indices[offset]] = std::move((*executed)[offset]);
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
        if (tls_context_) {
            auto session = tls_context_->connect_socket(connection.socket.get(), tls_server_name_);
            if (!session) {
                connection.reset();
                return unexpected(session.error());
            }
            connection.tls = std::move(*session);
        }
        const auto init_id = next_request_id();
        auto init = server::encode_request({.opcode = server::RequestOpcode::init, .request_id = init_id});
        if (!init) {
            return unexpected(init.error());
        }
        auto initialized = exchange(connection, *init, config_);
        if (const auto* failure = std::get_if<ExchangeFailure>(&initialized)) {
            connection.reset();
            return unexpected(failure->error);
        }
        const auto& response = std::get<OwnedResponse>(initialized);
        auto routing = decode_init_identity_value(response.value);
        if (!routing || response.status != server::ResponseStatus::ok || response.request_id != init_id ||
            response.worker_count == 0 || response.worker_count > 256 || response.routing_epoch == 0) {
            connection.reset();
            return fail(ErrorCode::corrupted_data, "server INIT response is not valid protocol v2 metadata");
        }
        const Metadata metadata{response.worker_count, response.routing_epoch, *routing};
        if (expected &&
            (metadata.worker_count != expected->worker_count ||
             metadata.routing_epoch != expected->routing_epoch || metadata.routing != expected->routing)) {
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
        auto bound = exchange(connection, *bind, config_);
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
        auto connected = bootstrap(connection, Metadata{worker_count_, routing_epoch_, routing_});
        if (!connected) {
            return unexpected(connected.error());
        }
        return {};
    }

    [[nodiscard]] auto read(const server::RequestOpcode opcode, const std::span<const std::byte> key,
                            const std::span<const std::byte> value, const RequestOptions options)
        -> Result<std::vector<std::byte>> {
        const auto operation = operation_name(opcode);
        if (!healthy_.load(std::memory_order_acquire)) {
            return unexpected(
                enrich_error({ErrorCode::unavailable, "client is closed or routing metadata changed"},
                             operation, std::nullopt, std::nullopt, std::nullopt));
        }
        auto deadline = resolve_deadline(options);
        if (!deadline) {
            return unexpected(
                enrich_error(deadline.error(), operation, std::nullopt, std::nullopt, std::nullopt));
        }
        const auto worker = opcode == server::RequestOpcode::ping || opcode == server::RequestOpcode::backup
                                ? 0U
                                : worker_for(key);
        auto& connection = *connections_[worker];
        const std::lock_guard lock{connection.mutex};
        if (!healthy_.load(std::memory_order_acquire)) {
            return unexpected(enrich_error({ErrorCode::unavailable, "client closed before read admission"},
                                           operation, std::nullopt, worker, routing_epoch_));
        }
        Error last_error = enrich_error({ErrorCode::unavailable, "request was not attempted"}, operation,
                                        std::nullopt, worker, routing_epoch_);
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (auto connected = ensure_connected(connection); !connected) {
                last_error = enrich_error(connected.error(), operation, std::nullopt, worker, routing_epoch_);
                continue;
            }
            const auto request_id = next_request_id();
            auto encoded = server::encode_request(
                {.opcode = opcode, .request_id = request_id, .key = key, .value = value});
            if (!encoded) {
                return unexpected(
                    enrich_error(encoded.error(), operation, request_id, worker, routing_epoch_));
            }
            if (encoded->size() > config_.maximum_frame_bytes) {
                return unexpected(
                    enrich_error({ErrorCode::record_too_large, "request exceeds the configured frame limit"},
                                 operation, request_id, worker, routing_epoch_));
            }
            auto result = exchange(connection, *encoded, config_, *deadline);
            if (auto* failure = std::get_if<ExchangeFailure>(&result)) {
                last_error = enrich_error(failure->error, operation, request_id, worker, routing_epoch_,
                                          failure->request_bytes_sent);
                connection.reset();
                continue;
            }
            auto& response = std::get<OwnedResponse>(result);
            if (auto valid = validate_response(response, request_id, worker); !valid) {
                connection.reset();
                return unexpected(enrich_error(valid.error(), operation, request_id, worker, routing_epoch_,
                                               encoded->size()));
            }
            if (response.status != server::ResponseStatus::ok) {
                if (response.status == server::ResponseStatus::wrong_owner ||
                    response.status == server::ResponseStatus::not_bound) {
                    healthy_.store(false, std::memory_order_release);
                }
                return unexpected(enrich_error(response_error(response.status), operation, request_id, worker,
                                               routing_epoch_, encoded->size(),
                                               static_cast<std::uint16_t>(response.status)));
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
        return static_cast<std::uint32_t>(hash_key_routing(key, routing_) % worker_count_);
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
    std::shared_ptr<server::TlsContext> tls_context_{};
    std::string tls_server_name_{};
    std::vector<std::unique_ptr<WorkerConnection>> connections_;
    std::uint32_t worker_count_{};
    std::uint64_t routing_epoch_{};
    WorkerRoutingState routing_{};
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

auto Client::get(const std::span<const std::byte> key, const RequestOptions options)
    -> Result<std::vector<std::byte>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->get(key, options);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    }
}

auto Client::get(const std::string_view key, const RequestOptions options) -> Result<std::vector<std::byte>> {
    return get(as_bytes(key), options);
}

auto Client::ping(const std::span<const std::byte> payload, const RequestOptions options)
    -> Result<std::vector<std::byte>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->ping(payload, options);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    }
}

auto Client::backup(const std::string_view destination, const RequestOptions options)
    -> Result<std::vector<std::byte>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->backup(destination, options);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "client allocation failed");
    }
}

auto Client::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                 const PutOptions put_options, const RequestOptions options) -> MutationResult {
    try {
        if (!implementation_) {
            return {.outcome = MutationOutcome::rejected,
                    .error = Error{ErrorCode::unavailable, "client was moved from"}};
        }
        return implementation_->mutate(server::RequestOpcode::put, key, value, put_options.expire_at_ns,
                                       options);
    } catch (const std::bad_alloc&) {
        return {.outcome = MutationOutcome::rejected,
                .error = Error{ErrorCode::resource_exhausted, "client allocation failed"}};
    }
}

auto Client::put(const std::string_view key, const std::string_view value, const PutOptions put_options,
                 const RequestOptions options) -> MutationResult {
    return put(as_bytes(key), as_bytes(value), put_options, options);
}

auto Client::erase(const std::span<const std::byte> key, const RequestOptions options) -> MutationResult {
    try {
        if (!implementation_) {
            return {.outcome = MutationOutcome::rejected,
                    .error = Error{ErrorCode::unavailable, "client was moved from"}};
        }
        return implementation_->mutate(server::RequestOpcode::erase, key, {}, 0, options);
    } catch (const std::bad_alloc&) {
        return {.outcome = MutationOutcome::rejected,
                .error = Error{ErrorCode::resource_exhausted, "client allocation failed"}};
    }
}

auto Client::erase(const std::string_view key, const RequestOptions options) -> MutationResult {
    return erase(as_bytes(key), options);
}

auto Client::execute_pipeline(const std::span<const PipelineRequest> requests, const RequestOptions options)
    -> Result<std::vector<PipelineResponse>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->execute_pipeline(requests, options);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "pipeline allocation failed before completion");
    }
}

auto Client::execute_batch(const std::span<const PipelineRequest> requests, const RequestOptions options)
    -> Result<std::vector<PipelineResponse>> {
    try {
        if (!implementation_) {
            return fail(ErrorCode::unavailable, "client was moved from");
        }
        return implementation_->execute_batch(requests, options);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "batch allocation failed before completion");
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
