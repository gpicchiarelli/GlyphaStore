#pragma once

#include <cstdint>

namespace glyphastore::server {

struct ConnectionToken {
    std::uint32_t slot{};
    std::uint32_t generation{};

    [[nodiscard]] auto encode() const noexcept -> std::uint64_t {
        return (static_cast<std::uint64_t>(generation) << 32U) | slot;
    }
    [[nodiscard]] static auto decode(const std::uint64_t encoded) noexcept -> ConnectionToken {
        return {.slot = static_cast<std::uint32_t>(encoded & 0xFFFF'FFFFULL),
                .generation = static_cast<std::uint32_t>(encoded >> 32U)};
    }
};

} // namespace glyphastore::server
