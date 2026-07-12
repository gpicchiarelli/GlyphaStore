#pragma once

#include "glyphastore/core/error.hpp"

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace glyphastore::server {

class SocketHandle final {
  public:
    SocketHandle() = default;
    explicit SocketHandle(int descriptor) noexcept : descriptor_(descriptor) {}
    ~SocketHandle();

    SocketHandle(const SocketHandle&) = delete;
    auto operator=(const SocketHandle&) -> SocketHandle& = delete;
    SocketHandle(SocketHandle&& other) noexcept;
    auto operator=(SocketHandle&& other) noexcept -> SocketHandle&;

    [[nodiscard]] auto descriptor() const noexcept -> int {
        return descriptor_;
    }
    [[nodiscard]] auto valid() const noexcept -> bool {
        return descriptor_ >= 0;
    }
    [[nodiscard]] auto release() noexcept -> int;
    void reset(int descriptor = -1) noexcept;

  private:
    int descriptor_{-1};
};

class TcpListener final {
  public:
    TcpListener() = default;

    [[nodiscard]] static auto bind(std::string_view address, std::uint16_t port, int backlog = 512)
        -> Result<TcpListener>;
    [[nodiscard]] auto accept() const -> Result<std::optional<SocketHandle>>;
    [[nodiscard]] auto descriptor() const noexcept -> int {
        return socket_.descriptor();
    }
    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return port_;
    }

  private:
    TcpListener(SocketHandle socket, std::uint16_t port) noexcept : socket_(std::move(socket)), port_(port) {}

    SocketHandle socket_;
    std::uint16_t port_{};
};

[[nodiscard]] auto set_nonblocking(int descriptor) -> Status;
[[nodiscard]] auto set_close_on_exec(int descriptor) -> Status;

} // namespace glyphastore::server
