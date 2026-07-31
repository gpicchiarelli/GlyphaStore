#include "glyphastore/server/protocol.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string_view>
#include <vector>

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
        .request_id = 42,
        .expire_at_ns = 900,
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
    GLYPHA_REQUIRE(decoded->frame.flags == 0);
    GLYPHA_REQUIRE(decoded->frame.request_id == 42);
    GLYPHA_REQUIRE(decoded->frame.expire_at_ns == 900);
    GLYPHA_REQUIRE(decoded->frame.target_worker == glyphastore::server::kNoWorker);
    GLYPHA_REQUIRE(text(decoded->frame.key) == "key");
    GLYPHA_REQUIRE(text(decoded->frame.value) == "value");
}

GLYPHA_TEST("server protocol encodes requests into caller-owned storage") {
    const glyphastore::server::RequestView request{
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 42,
        .key = bytes("key"),
        .value = bytes("value"),
    };
    const auto required = glyphastore::server::encoded_request_size(request);
    GLYPHA_REQUIRE(required.has_value());
    std::vector<std::byte> storage(*required);
    const auto written = glyphastore::server::encode_request(storage, request);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(*written == storage.size());

    const auto owned = glyphastore::server::encode_request(request);
    GLYPHA_REQUIRE(owned.has_value());
    GLYPHA_REQUIRE(storage == *owned);

    storage.pop_back();
    GLYPHA_REQUIRE(!glyphastore::server::encode_request(storage, request).has_value());
}

GLYPHA_TEST("server protocol rejects noncanonical flags and reserved fields") {
    GLYPHA_REQUIRE(
        !glyphastore::server::encode_request({
                                                 .opcode = glyphastore::server::RequestOpcode::ping,
                                                 .flags = 1,
                                                 .request_id = 1,
                                             })
             .has_value());

    auto request = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 2,
    });
    GLYPHA_REQUIRE(request.has_value());
    (*request)[36] = std::byte{1};
    GLYPHA_REQUIRE(!glyphastore::server::decode_request(*request).has_value());

    auto response = glyphastore::server::encode_response({
        .status = glyphastore::server::ResponseStatus::ok,
        .request_id = 2,
    });
    GLYPHA_REQUIRE(response.has_value());
    (*response)[28] = std::byte{1};
    GLYPHA_REQUIRE(!glyphastore::server::decode_response(*response).has_value());
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

GLYPHA_TEST("server protocol scatter header matches contiguous response encoding") {
    const glyphastore::server::ResponseView response{.status = glyphastore::server::ResponseStatus::ok,
                                                     .request_id = 91,
                                                     .owner_worker = 0,
                                                     .worker_count = 1,
                                                     .routing_epoch = 3,
                                                     .value = bytes("scatter-value")};
    std::array<std::byte, glyphastore::server::kResponseHeaderBytes> header{};
    const auto declared = glyphastore::server::encode_response_header(header, response);
    GLYPHA_REQUIRE(declared.has_value());
    GLYPHA_REQUIRE(*declared == header.size() + response.value.size());
    std::vector<std::byte> gathered;
    gathered.insert(gathered.end(), header.begin(), header.end());
    gathered.insert(gathered.end(), response.value.begin(), response.value.end());
    const auto contiguous = glyphastore::server::encode_response(response);
    GLYPHA_REQUIRE(contiguous.has_value());
    GLYPHA_REQUIRE(gathered == *contiguous);
}

GLYPHA_TEST("server protocol rejects noncanonical opcode-specific fields") {
    GLYPHA_REQUIRE(!glyphastore::server::encode_request({
                                                            .opcode = glyphastore::server::RequestOpcode::get,
                                                            .request_id = 1,
                                                            .key = bytes("k"),
                                                            .value = bytes("x"),
                                                        })
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::encode_request({
                                                            .opcode = glyphastore::server::RequestOpcode::put,
                                                            .request_id = 1,
                                                            .target_worker = 1,
                                                            .key = bytes("k"),
                                                            .value = bytes("v"),
                                                        })
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::encode_request({
                                                 .opcode = glyphastore::server::RequestOpcode::health,
                                                 .request_id = 1,
                                                 .key = bytes("k"),
                                             })
             .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::encode_request({
                                                 .opcode = glyphastore::server::RequestOpcode::bind_worker,
                                                 .request_id = 1,
                                             })
             .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::encode_request({
                                                            .opcode = glyphastore::server::RequestOpcode::get,
                                                            .request_id = 1,
                                                        })
                        .has_value());

    auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 9,
        .value = bytes("ok"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    // Force a non-canonical target_worker while keeping payload sizes intact.
    (*ping)[32] = std::byte{1};
    (*ping)[33] = std::byte{0};
    (*ping)[34] = std::byte{0};
    (*ping)[35] = std::byte{0};
    GLYPHA_REQUIRE(!glyphastore::server::decode_request(*ping).has_value());
}

GLYPHA_TEST("wire protocol v2 matches independent canonical request fixtures") {
    const auto corpus = glyphastore::test::read_hex_fixture(std::filesystem::path{GLYPHASTORE_SOURCE_DIR} /
                                                            "tests/fixtures/wire_requests_v2.hex");
    std::size_t offset{};
    std::uint8_t expected_opcode{1};
    while (offset < corpus.size()) {
        const auto remaining = std::span<const std::byte>{corpus}.subspan(offset);
        const auto decoded = glyphastore::server::decode_request(remaining);
        GLYPHA_REQUIRE(decoded.has_value());
        GLYPHA_REQUIRE(decoded->complete);
        GLYPHA_REQUIRE(static_cast<std::uint8_t>(decoded->frame.opcode) == expected_opcode);
        const auto reencoded = glyphastore::server::encode_request(decoded->frame);
        GLYPHA_REQUIRE(reencoded.has_value());
        GLYPHA_REQUIRE(reencoded->size() == decoded->consumed);
        GLYPHA_REQUIRE(std::equal(reencoded->begin(), reencoded->end(), remaining.begin()));
        offset += decoded->consumed;
        ++expected_opcode;
    }
    GLYPHA_REQUIRE(expected_opcode == 11);
}

GLYPHA_TEST("wire protocol v2 matches independent canonical response fixtures") {
    const auto corpus = glyphastore::test::read_hex_fixture(std::filesystem::path{GLYPHASTORE_SOURCE_DIR} /
                                                            "tests/fixtures/wire_responses_v2.hex");
    std::size_t offset{};
    std::uint16_t expected_status{};
    while (offset < corpus.size()) {
        const auto remaining = std::span<const std::byte>{corpus}.subspan(offset);
        const auto decoded = glyphastore::server::decode_response(remaining);
        GLYPHA_REQUIRE(decoded.has_value());
        GLYPHA_REQUIRE(decoded->complete);
        GLYPHA_REQUIRE(static_cast<std::uint16_t>(decoded->frame.status) == expected_status);
        const auto reencoded = glyphastore::server::encode_response(decoded->frame);
        GLYPHA_REQUIRE(reencoded.has_value());
        GLYPHA_REQUIRE(reencoded->size() == decoded->consumed);
        GLYPHA_REQUIRE(std::equal(reencoded->begin(), reencoded->end(), remaining.begin()));
        offset += decoded->consumed;
        ++expected_status;
    }
    GLYPHA_REQUIRE(expected_status == 9);
}
