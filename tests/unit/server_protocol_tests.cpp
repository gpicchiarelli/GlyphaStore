#include "glyphastore/server/protocol.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto text(const std::span<const std::byte> value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("server protocol request round trips and handles partial frames") {
    const glyphastore::server::RequestView request{
        .opcode = glyphastore::server::RequestOpcode::put,
        .flags = 3,
        .request_id = 42,
        .expire_at_ns = 900,
        .target_worker = 7,
        .key = bytes("key"),
        .value = bytes("value"),
    };
    const auto encoded = glyphastore::server::encode_request(request);
    GLYPHA_REQUIRE(encoded.has_value());

    const auto partial = glyphastore::server::decode_request(
        std::span<const std::byte>{encoded->data(), glyphastore::server::kRequestHeaderBytes - 1U});
    GLYPHA_REQUIRE(partial.has_value());
    GLYPHA_REQUIRE(!partial->complete);

    const auto decoded = glyphastore::server::decode_request(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->complete);
    GLYPHA_REQUIRE(decoded->consumed == encoded->size());
    GLYPHA_REQUIRE(decoded->frame.opcode == glyphastore::server::RequestOpcode::put);
    GLYPHA_REQUIRE(decoded->frame.flags == 3);
    GLYPHA_REQUIRE(decoded->frame.request_id == 42);
    GLYPHA_REQUIRE(decoded->frame.expire_at_ns == 900);
    GLYPHA_REQUIRE(decoded->frame.target_worker == 7);
    GLYPHA_REQUIRE(text(decoded->frame.key) == "key");
    GLYPHA_REQUIRE(text(decoded->frame.value) == "value");
}

GLYPHA_TEST("server protocol rejects inconsistent and oversized request frames") {
    const auto encoded = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 1,
        .value = bytes("ping"),
    });
    GLYPHA_REQUIRE(encoded.has_value());

    auto inconsistent = *encoded;
    inconsistent[20] = std::byte{0x7F};
    GLYPHA_REQUIRE(!glyphastore::server::decode_request(inconsistent).has_value());

    auto oversized = *encoded;
    const auto declared = static_cast<std::uint32_t>(glyphastore::server::kMaxFrameBytes + 1U);
    for (std::size_t byte = 0; byte < 4; ++byte) {
        oversized[byte] = static_cast<std::byte>((declared >> (byte * 8U)) & 0xFFU);
    }
    GLYPHA_REQUIRE(!glyphastore::server::decode_request(oversized).has_value());
}

GLYPHA_TEST("server protocol response round trips") {
    const auto encoded = glyphastore::server::encode_response({
        .status = glyphastore::server::ResponseStatus::ok,
        .request_id = 77,
        .owner_worker = 2,
        .worker_count = 4,
        .routing_epoch = 9,
        .value = bytes("pong"),
    });
    GLYPHA_REQUIRE(encoded.has_value());
    const auto decoded = glyphastore::server::decode_response(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->complete);
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(decoded->frame.request_id == 77);
    GLYPHA_REQUIRE(decoded->frame.owner_worker == 2);
    GLYPHA_REQUIRE(decoded->frame.worker_count == 4);
    GLYPHA_REQUIRE(decoded->frame.routing_epoch == 9);
    GLYPHA_REQUIRE(text(decoded->frame.value) == "pong");
}
