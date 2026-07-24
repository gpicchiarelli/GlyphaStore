#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>

namespace glyphastore::detail {

struct HotRecordReservePlan {
    std::size_t target{};
    bool overflow{};
};

// Geometric reserve planner for the durable hot-cache unordered_map. The map uses
// max_load_factor(0.5) and transparent string hashing; identity is always a full key
// compare (hash alone is not authoritative). Per-entry accounting charges the map
// node (~value_type + 4 pointers), the owned key bytes, and heap value bytes only
// when the value exceeds the 32-byte inline buffer.
[[nodiscard]] constexpr auto plan_hot_record_reserve(const std::size_t current_size,
                                                     const std::size_t additional_records,
                                                     const std::size_t current_capacity) noexcept
    -> HotRecordReservePlan {
    if (additional_records > std::numeric_limits<std::size_t>::max() - current_size) {
        return {.overflow = true};
    }
    const auto required = current_size + additional_records;
    if (additional_records == 0 || required <= current_capacity) {
        return {};
    }

    constexpr std::size_t kMinimumReserve = 64;
    const auto geometric = current_size > std::numeric_limits<std::size_t>::max() / 2U
                               ? std::numeric_limits<std::size_t>::max()
                               : current_size * 2U;
    return {.target = std::max({required, geometric, kMinimumReserve})};
}

} // namespace glyphastore::detail
