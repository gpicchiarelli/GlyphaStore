#pragma once

#include <expected>
#include <string>

namespace glyphastore {

enum class ErrorCode {
    invalid_argument,
    arithmetic_overflow,
    record_too_large,
    segment_full,
    segment_sealed,
    invalid_record,
    checksum_mismatch,
    invalid_reference,
    sequence_conflict,
    corrupted_data,
    not_found,
    io_error,
};

struct Error {
    ErrorCode code;
    std::string message;
};

template <typename T> using Result = std::expected<T, Error>;

using Status = Result<void>;

inline auto fail(ErrorCode code, std::string message) -> std::unexpected<Error> {
    return std::unexpected(Error{code, std::move(message)});
}

} // namespace glyphastore
