#pragma once

#include "glyphastore/core/error.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace glyphastore {

// Bump allocator for SwissTable heap keys. Individual frees are not supported; rehash compacts live keys.
class KeyArena final {
  public:
    [[nodiscard]] auto allocate(std::size_t size) -> Result<std::uint32_t>;
    [[nodiscard]] auto data(std::uint32_t offset, std::size_t size) const -> std::span<const std::byte>;
    [[nodiscard]] auto allocated_bytes() const noexcept -> std::size_t;
    void clear() noexcept;
    void reserve(std::size_t bytes);

  private:
    std::vector<std::byte> storage_;
    std::size_t bump_{};
};

} // namespace glyphastore
