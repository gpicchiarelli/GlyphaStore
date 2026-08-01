#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/protocol.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <sys/socket.h>

namespace glyphastore::server::reactor_detail {

// sendmsg avoids a value copy, but its fixed syscall/iovec setup cost is larger
// than copying a cache-resident small value into the existing output buffer.
// Keep the common small-response path contiguous and reserve the owning lease
// for payloads large enough to amortize scatter/gather.
inline constexpr std::size_t kMinimumScatterValueBytes = 4U * 1024U;

[[nodiscard]] inline auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] inline auto key_text(const std::span<const std::byte> value) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

[[nodiscard]] inline auto response_status(const Error& error) noexcept -> ResponseStatus {
    switch (error.code) {
    case ErrorCode::not_found:
        return ResponseStatus::not_found;
    case ErrorCode::invalid_argument:
    case ErrorCode::record_too_large:
        return ResponseStatus::invalid_request;
    case ErrorCode::resource_exhausted:
    case ErrorCode::storage_exhausted:
    case ErrorCode::file_too_large:
    case ErrorCode::descriptor_exhausted:
    case ErrorCode::read_only_filesystem:
    case ErrorCode::unavailable:
    case ErrorCode::sequence_conflict:
        return ResponseStatus::overloaded;
    default:
        return ResponseStatus::internal_error;
    }
}

[[nodiscard]] inline auto current_time_ns() noexcept -> std::uint64_t {
    const auto elapsed = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
}

[[nodiscard]] inline auto send_flags() noexcept -> int {
#if defined(__linux__)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}



} // namespace glyphastore::server::reactor_detail
