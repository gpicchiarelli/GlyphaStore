#pragma once

#include "glyphastore/core/error.hpp"

#include <cerrno>
#include <string>
#include <string_view>
#include <system_error>

namespace glyphastore {

[[nodiscard]] inline auto persistence_system_error(const std::string_view operation,
                                                   const int error_number = errno) -> Unexpected {
    return fail(ErrorCode::io_error, std::string{operation} + ": " +
                                         std::error_code{error_number, std::system_category()}.message());
}

} // namespace glyphastore
