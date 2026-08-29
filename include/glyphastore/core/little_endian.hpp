#pragma once

// Explicit little-endian positional codecs for on-disk and wire layouts.
// Callers must already validate that `at + width` is within `bytes`.

#include <cstddef>
#include <cstdint>
#include <span>

namespace glyphastore::le {

[[nodiscard]] constexpr auto to_u8(const std::byte value) noexcept -> std::uint8_t {
    return std::to_integer<std::uint8_t>(value);
}

inline void put_u16(const std::span<std::byte> out, const std::size_t at,
                    const std::uint16_t value) noexcept {
    out[at] = static_cast<std::byte>(value & 0xFFU);
    out[at + 1U] = static_cast<std::byte>((value >> 8U) & 0xFFU);
}

inline void put_u32(const std::span<std::byte> out, const std::size_t at,
                    const std::uint32_t value) noexcept {
    for (std::size_t index = 0; index < 4; ++index) {
        out[at + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

inline void put_u64(const std::span<std::byte> out, const std::size_t at,
                    const std::uint64_t value) noexcept {
    for (std::size_t index = 0; index < 8; ++index) {
        out[at + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

[[nodiscard]] inline auto get_u16(const std::span<const std::byte> in, const std::size_t at) noexcept
    -> std::uint16_t {
    return static_cast<std::uint16_t>(to_u8(in[at])) |
           static_cast<std::uint16_t>(static_cast<std::uint16_t>(to_u8(in[at + 1U])) << 8U);
}

[[nodiscard]] inline auto get_u32(const std::span<const std::byte> in, const std::size_t at) noexcept
    -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t index = 0; index < 4; ++index) {
        value |= static_cast<std::uint32_t>(to_u8(in[at + index])) << (index * 8U);
    }
    return value;
}

[[nodiscard]] inline auto get_u64(const std::span<const std::byte> in, const std::size_t at) noexcept
    -> std::uint64_t {
    std::uint64_t value{};
    for (std::size_t index = 0; index < 8; ++index) {
        value |= static_cast<std::uint64_t>(to_u8(in[at + index])) << (index * 8U);
    }
    return value;
}

} // namespace glyphastore::le
