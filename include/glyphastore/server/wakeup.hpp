#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/socket.hpp"

namespace glyphastore::server {

class Wakeup final {
  public:
    [[nodiscard]] static auto create() -> Result<Wakeup>;

    [[nodiscard]] auto descriptor() const noexcept -> int {
        return reader_.descriptor();
    }
    [[nodiscard]] auto notify() const -> Status;
    [[nodiscard]] auto drain() const -> Status;

  private:
    Wakeup(SocketHandle reader, SocketHandle writer) noexcept
        : reader_(std::move(reader)), writer_(std::move(writer)) {}

    SocketHandle reader_;
    SocketHandle writer_;
};

} // namespace glyphastore::server
