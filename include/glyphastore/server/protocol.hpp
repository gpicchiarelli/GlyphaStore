#pragma once

#include "glyphastore/core/error.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore::server {

inline constexpr std::uint16_t kProtocolVersion = 2;
inline constexpr std::size_t kRequestHeaderBytes = 40;
inline constexpr std::size_t kResponseHeaderBytes = 40;
inline constexpr std::size_t kMaxFrameBytes = 2U * 1024U * 1024U;
inline constexpr std::uint32_t kNoWorker = 0xFFFF'FFFFU;

enum class RequestOpcode : std::uint8_t {
    init = 1,
    ping = 2,
    get = 3,
    put = 4,
    erase = 5,
    bind_worker = 6,
    health = 7,
    ready = 8,
    stats = 9,
};
enum class ResponseStatus : std::uint16_t {
    ok = 0,
    invalid_request = 1,
    unsupported = 2,
    internal_error = 3,
    not_found = 4,
    overloaded = 5,
    wrong_owner = 6,
    not_bound = 7,
    permission_denied = 8
};

struct RequestView {
    RequestOpcode opcode{RequestOpcode::init};
    std::uint8_t flags{};
    std::uint64_t request_id{};
    std::uint64_t expire_at_ns{};
    std::uint32_t target_worker{kNoWorker};
    std::span<const std::byte> key;
    std::span<const std::byte> value;
};

struct ResponseView {
    ResponseStatus status{ResponseStatus::ok};
    std::uint64_t request_id{};
    std::uint32_t owner_worker{kNoWorker};
    std::uint32_t worker_count{};
    std::uint64_t routing_epoch{};
    std::span<const std::byte> value;
};

template <typename View> struct DecodedFrame {
    bool complete{};
    std::size_t consumed{};
    View frame{};
};

[[nodiscard]] auto request_opcode_name(RequestOpcode opcode) noexcept -> std::string_view;

[[nodiscard]] auto decode_request(std::span<const std::byte> input,
                                  std::size_t maximum_frame_bytes = kMaxFrameBytes)
    -> Result<DecodedFrame<RequestView>>;
[[nodiscard]] auto decode_response(std::span<const std::byte> input,
                                   std::size_t maximum_frame_bytes = kMaxFrameBytes)
    -> Result<DecodedFrame<ResponseView>>;

[[nodiscard]] auto encoded_request_size(const RequestView& request) -> Result<std::size_t>;
[[nodiscard]] auto encode_request(std::span<std::byte> output, const RequestView& request)
    -> Result<std::size_t>;
[[nodiscard]] auto encode_request(const RequestView& request) -> Result<std::vector<std::byte>>;
[[nodiscard]] auto encoded_response_size(const ResponseView& response) -> Result<std::size_t>;
[[nodiscard]] auto encode_response(std::span<std::byte> output, const ResponseView& response)
    -> Result<std::size_t>;
[[nodiscard]] auto encode_response(const ResponseView& response) -> Result<std::vector<std::byte>>;

} // namespace glyphastore::server
