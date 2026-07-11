#pragma once

#include "glyphastore/index/swiss_table.hpp"

#include <bit>
#include <cstdint>
#include <cstring>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace glyphastore::detail {

[[nodiscard]] inline auto load_control_group64(const std::uint8_t* control) noexcept -> std::uint64_t {
    std::uint64_t word = 0;
    std::memcpy(&word, control, kSwissGroupSize);
    return word;
}

[[nodiscard]] inline auto control_byte_at(const std::uint64_t control, const std::size_t offset) noexcept
    -> std::uint8_t {
    return static_cast<std::uint8_t>((control >> (offset * 8U)) & 0xFFU);
}

[[nodiscard]] inline auto equal_byte_mask(const std::uint8_t* control, const std::uint8_t byte) noexcept
    -> std::uint64_t {
#if defined(__SSE2__)
    std::uint64_t word = 0;
    std::memcpy(&word, control, kSwissGroupSize);
    const __m128i group = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&word));
    const __m128i needle = _mm_set1_epi8(static_cast<char>(byte));
    return static_cast<std::uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, needle))) &
           ((1ULL << kSwissGroupSize) - 1ULL);
#elif defined(__ARM_NEON) || defined(__aarch64__)
    const uint8x8_t compared = vceq_u8(vld1_u8(control), vdup_n_u8(byte));
    std::uint64_t mask = 0;
    if (vget_lane_u8(compared, 0) != 0) {
        mask |= 1ULL << 0;
    }
    if (vget_lane_u8(compared, 1) != 0) {
        mask |= 1ULL << 1;
    }
    if (vget_lane_u8(compared, 2) != 0) {
        mask |= 1ULL << 2;
    }
    if (vget_lane_u8(compared, 3) != 0) {
        mask |= 1ULL << 3;
    }
    if (vget_lane_u8(compared, 4) != 0) {
        mask |= 1ULL << 4;
    }
    if (vget_lane_u8(compared, 5) != 0) {
        mask |= 1ULL << 5;
    }
    if (vget_lane_u8(compared, 6) != 0) {
        mask |= 1ULL << 6;
    }
    if (vget_lane_u8(compared, 7) != 0) {
        mask |= 1ULL << 7;
    }
    return mask;
#else
    const auto word = load_control_group64(control);
    std::uint64_t mask = 0;
    for (std::size_t index = 0; index < kSwissGroupSize; ++index) {
        if (control_byte_at(word, index) == byte) {
            mask |= 1ULL << index;
        }
    }
    return mask;
#endif
}

[[nodiscard]] inline auto first_set_bit(const std::uint64_t mask) noexcept -> std::size_t {
    return static_cast<std::size_t>(std::countr_zero(mask));
}

[[nodiscard]] inline auto clear_lowest_set_bit(const std::uint64_t mask) noexcept -> std::uint64_t {
    return mask & (mask - 1ULL);
}

} // namespace glyphastore::detail
