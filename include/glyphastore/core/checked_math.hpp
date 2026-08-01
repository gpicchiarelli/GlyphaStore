#pragma once

#include "glyphastore/core/error.hpp"

#include <concepts>
#include <limits>
#include <type_traits>

namespace glyphastore {

template <std::unsigned_integral T> [[nodiscard]] constexpr auto checked_add(T lhs, T rhs) -> Result<T> {
    if (rhs > std::numeric_limits<T>::max() - lhs) {
        return fail(ErrorCode::arithmetic_overflow, "unsigned addition overflow");
    }
    return static_cast<T>(lhs + rhs);
}

template <std::unsigned_integral T>
[[nodiscard]] constexpr auto align_up_checked(T value, T alignment) -> Result<T> {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        return fail(ErrorCode::invalid_argument, "alignment must be a power of two");
    }
    auto with_padding = checked_add(value, static_cast<T>(alignment - 1));
    if (!with_padding) {
        return unexpected(with_padding.error());
    }
    return static_cast<T>(*with_padding & static_cast<T>(~(alignment - 1)));
}

template <std::unsigned_integral T>
[[nodiscard]] constexpr auto range_contains(T capacity, T offset, T length) -> bool {
    return offset <= capacity && length <= static_cast<T>(capacity - offset);
}

} // namespace glyphastore
