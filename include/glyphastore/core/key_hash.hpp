#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace glyphastore {

// Deterministic 64-bit FNV-1a used for Worker routing. See docs/adr/0006-key-routing-hash.md.
inline constexpr std::uint64_t kFnv1a64Offset = 14695981039346656037ULL;
inline constexpr std::uint64_t kFnv1a64Prime = 1099511628211ULL;

[[nodiscard]] inline auto hash_key(std::span<const std::byte> key) noexcept -> std::uint64_t {
    std::uint64_t hash = kFnv1a64Offset;
    for (const auto byte : key) {
        hash ^= static_cast<std::uint64_t>(std::to_integer<unsigned char>(byte));
        hash *= kFnv1a64Prime;
    }
    return hash;
}

[[nodiscard]] inline auto hash_key(std::string_view key) noexcept -> std::uint64_t {
    return hash_key({reinterpret_cast<const std::byte*>(key.data()), key.size()});
}

[[nodiscard]] inline auto route_worker(std::uint64_t key_hash, std::size_t worker_count) noexcept
    -> std::size_t {
    return static_cast<std::size_t>(key_hash % worker_count);
}

[[nodiscard]] inline auto route_worker(std::string_view key, std::size_t worker_count) noexcept
    -> std::size_t {
    return route_worker(hash_key(key), worker_count);
}

} // namespace glyphastore
