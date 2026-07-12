#pragma once

#include "glyphastore/core/error.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore::server {

inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kRequestHeaderBytes = 32;
inline constexpr std::size_t kResponseHeaderBytes = 24;
inline constexpr std::size_t kMaxFrameBytes = 2U * 1024U * 1024U;

enum class RequestOpcode : std::uint8_t { hello = 1, ping = 2, get = 3, put = 4, erase = 5 };
enum class ResponseStatus : std::uint16_t {
    ok = 0,
    invalid_request = 1,
    unsupported = 2,
    internal_error = 3,
    not_found = 4,
    overloaded = 5
};

struct RequestView {
    RequestOpcode opcode{RequestOpcode::hello};
    std::uint8_t flags{};
    std::uint64_t request_id{};
    std::uint64_t expire_at_ns{};
    std::span<const std::byte> key;
    std::span<const std::byte> value;
};

struct ResponseView {
    ResponseStatus status{ResponseStatus::ok};
    std::uint64_t request_id{};
    std::span<const std::byte> value;
};

template <typename View> struct DecodedFrame {
    bool complete{};
    std::size_t consumed{};
    View frame{};
};

[[nodiscard]] auto decode_request(std::span<const std::byte> input,
                                  std::size_t maximum_frame_bytes = kMaxFrameBytes)
    -> Result<DecodedFrame<RequestView>>;
[[nodiscard]] auto decode_response(std::span<const std::byte> input,
                                   std::size_t maximum_frame_bytes = kMaxFrameBytes)
    -> Result<DecodedFrame<ResponseView>>;

[[nodiscard]] auto encode_request(const RequestView& request) -> Result<std::vector<std::byte>>;
[[nodiscard]] auto encoded_response_size(const ResponseView& response) -> Result<std::size_t>;
[[nodiscard]] auto encode_response(std::span<std::byte> output, const ResponseView& response)
    -> Result<std::size_t>;
[[nodiscard]] auto encode_response(const ResponseView& response) -> Result<std::vector<std::byte>>;

} // namespace glyphastore::server
