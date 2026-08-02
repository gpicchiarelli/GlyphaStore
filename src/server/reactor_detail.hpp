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
    case ErrorCode::sequence_conflict:
    case ErrorCode::segment_full:
    case ErrorCode::segment_sealed:
    case ErrorCode::arithmetic_overflow:
        // Capacity / conflict / segment fit — mutation did not newly commit on this
        // attempt. Must not map to INTERNAL_ERROR (reconcile_first / indeterminate).
        return ResponseStatus::overloaded;
    case ErrorCode::unavailable:
        // Store/Writer use unavailable for fail-closed and post-commit sticky paths where
        // the mutation may already have linearized. Wire OVERLOADED means known-not-committed
        // (client semantics); INTERNAL_ERROR forces reconcile_first instead.
        return ResponseStatus::internal_error;
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

// Upper bound for probe_server_backup's OK report (status=ok + destination=path +
// fixed metric lines with ≤20 decimal digits each). Used to refuse BACKUP before the
// fenced copy when the response cannot fit — avoids OVERLOADED after a committed backup.
[[nodiscard]] inline auto backup_ok_report_max_bytes(const std::size_t destination_bytes) noexcept
    -> std::size_t {
    constexpr std::size_t kMaxUint64Digits = 20;
    constexpr std::size_t kFixed =
        10 +  // status=ok\n
        13 +  // destination=\n (path added below)
        13 + kMaxUint64Digits + 1 +  // files_copied=
        13 + kMaxUint64Digits + 1 +  // bytes_copied=
        19 + kMaxUint64Digits + 1 +  // admission_fence_ns=
        16 + kMaxUint64Digits + 1 +  // catalog_copy_ns=
        22 + kMaxUint64Digits + 1 +  // destination_verify_ns=
        21 + kMaxUint64Digits + 1 +  // segment_copy_workers=
        21 +                         // source_crc_scanned=0|1\n
        26 +                         // destination_crc_scanned=0|1\n
        16 + kMaxUint64Digits + 1 +  // source_segments=
        21 + kMaxUint64Digits + 1;   // destination_segments=
    return kFixed + destination_bytes;
}

} // namespace glyphastore::server::reactor_detail
