#include "glyphastore/server/socket.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto system_error(const char* operation) -> Unexpected {
    return fail(ErrorCode::io_error, std::string{operation} + ": " + std::strerror(errno));
}

[[nodiscard]] auto configure_client_socket(const int descriptor) -> Status {
    if (auto nonblocking = set_nonblocking(descriptor); !nonblocking) {
        return nonblocking;
    }
    if (auto close_on_exec = set_close_on_exec(descriptor); !close_on_exec) {
        return close_on_exec;
    }
    const int enabled = 1;
    if (::setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) != 0) {
        return system_error("setsockopt(TCP_NODELAY)");
    }
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    if (::setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) != 0) {
        return system_error("setsockopt(SO_NOSIGPIPE)");
    }
#endif
    return {};
}

} // namespace

SocketHandle::~SocketHandle() {
    reset();
}

SocketHandle::SocketHandle(SocketHandle&& other) noexcept : descriptor_(other.release()) {}

auto SocketHandle::operator=(SocketHandle&& other) noexcept -> SocketHandle& {
    if (this != &other) {
        reset(other.release());
    }
    return *this;
}

auto SocketHandle::release() noexcept -> int {
    const auto descriptor = descriptor_;
    descriptor_ = -1;
    return descriptor;
}

void SocketHandle::reset(const int descriptor) noexcept {
    if (descriptor_ >= 0) {
        static_cast<void>(::close(descriptor_));
    }
    descriptor_ = descriptor;
}

auto set_nonblocking(const int descriptor) -> Status {
    const auto flags = ::fcntl(descriptor, F_GETFL, 0);
    if (flags < 0 || ::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0) {
        return system_error("fcntl(O_NONBLOCK)");
    }
    return {};
}

auto set_close_on_exec(const int descriptor) -> Status {
    const auto flags = ::fcntl(descriptor, F_GETFD, 0);
    if (flags < 0 || ::fcntl(descriptor, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return system_error("fcntl(FD_CLOEXEC)");
    }
    return {};
}

auto TcpListener::bind(const std::string_view address, const std::uint16_t port, const int backlog,
                       const bool reuse_port) -> Result<TcpListener> {
    SocketHandle socket{::socket(AF_INET, SOCK_STREAM, 0)};
    if (!socket.valid()) {
        return system_error("socket");
    }
    if (auto nonblocking = set_nonblocking(socket.descriptor()); !nonblocking) {
        return unexpected(nonblocking.error());
    }
    if (auto close_on_exec = set_close_on_exec(socket.descriptor()); !close_on_exec) {
        return unexpected(close_on_exec.error());
    }
    const int enabled = 1;
    if (::setsockopt(socket.descriptor(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
        return system_error("setsockopt(SO_REUSEADDR)");
    }
    if (reuse_port &&
        ::setsockopt(socket.descriptor(), SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)) != 0) {
        return system_error("setsockopt(SO_REUSEPORT)");
    }

    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    const std::string address_text{address};
    if (::inet_pton(AF_INET, address_text.c_str(), &endpoint.sin_addr) != 1) {
        return fail(ErrorCode::invalid_argument, "TCP bind address must be an IPv4 literal");
    }
    if (::bind(socket.descriptor(), reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        return system_error("bind");
    }
    if (::listen(socket.descriptor(), backlog) != 0) {
        return system_error("listen");
    }

    sockaddr_in bound{};
    socklen_t bound_size = sizeof(bound);
    if (::getsockname(socket.descriptor(), reinterpret_cast<sockaddr*>(&bound), &bound_size) != 0) {
        return system_error("getsockname");
    }
    return TcpListener{std::move(socket), ntohs(bound.sin_port)};
}

auto TcpListener::accept() const -> Result<std::optional<SocketHandle>> {
    sockaddr_in remote{};
    socklen_t remote_size = sizeof(remote);
#if defined(__linux__)
    const auto accepted_descriptor = ::accept4(descriptor(), reinterpret_cast<sockaddr*>(&remote),
                                               &remote_size, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const auto accepted_descriptor =
        ::accept(descriptor(), reinterpret_cast<sockaddr*>(&remote), &remote_size);
#endif
    if (accepted_descriptor < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return std::optional<SocketHandle>{};
        }
        if (errno == EINTR) {
            return std::optional<SocketHandle>{};
        }
        return system_error("accept");
    }
    SocketHandle client{accepted_descriptor};
    if (auto configured = configure_client_socket(client.descriptor()); !configured) {
        return unexpected(configured.error());
    }
    return std::optional<SocketHandle>{std::move(client)};
}

} // namespace glyphastore::server
