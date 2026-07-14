#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore {

struct OwnedValue {
    std::vector<std::byte> bytes;
    std::uint64_t sequence{};
    std::uint64_t expire_at_ns{};

    [[nodiscard]] auto view() const noexcept -> std::span<const std::byte> {
        return bytes;
    }
};

} // namespace glyphastore
