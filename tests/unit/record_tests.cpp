#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/segment/record.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <filesystem>
#include <string_view>
#include <vector>

namespace {

auto as_bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

void put_u32(std::span<std::byte> out, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        out[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

} // namespace

GLYPHA_TEST("record v1 matches its golden binary-key fixture") {
    static constexpr std::array key{std::byte{0x00}, std::byte{0x6B}, std::byte{0xFF}};
    static constexpr std::array value{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40}};
    const auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{0x0102030405060708ULL},
        .flags = 0x11223344U,
        .key_hash = 0x1112131415161718ULL,
        .expire_at_ns = 0x2122232425262728ULL,
        .key = key,
        .value = value,
    });
    GLYPHA_REQUIRE(encoded.has_value());
    const auto fixture = glyphastore::test::read_hex_fixture(std::filesystem::path{GLYPHASTORE_SOURCE_DIR} /
                                                             "tests/fixtures/record_v1.hex");
    GLYPHA_REQUIRE(*encoded == fixture);

    const auto decoded = glyphastore::decode_record(fixture);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->sequence.value == 0x0102030405060708ULL);
    GLYPHA_REQUIRE(decoded->flags == 0x11223344U);
    GLYPHA_REQUIRE(decoded->key_hash == 0x1112131415161718ULL);
    GLYPHA_REQUIRE(decoded->expire_at_ns == 0x2122232425262728ULL);
    GLYPHA_REQUIRE(std::equal(decoded->key.begin(), decoded->key.end(), key.begin(), key.end()));
    GLYPHA_REQUIRE(std::equal(decoded->value.begin(), decoded->value.end(), value.begin(), value.end()));
    GLYPHA_REQUIRE(fixture.back() == std::byte{0});

    auto future = fixture;
    future[4] = std::byte{2};
    put_u32(future, 20, glyphastore::crc32c_with_zeroed_checksum_field(future));
    const auto unknown_version = glyphastore::decode_record(future);
    GLYPHA_REQUIRE(!unknown_version.has_value());
    GLYPHA_REQUIRE(unknown_version.error().code == glyphastore::ErrorCode::invalid_record);

    auto nonzero_padding = fixture;
    nonzero_padding.back() = std::byte{1};
    put_u32(nonzero_padding, 20, glyphastore::crc32c_with_zeroed_checksum_field(nonzero_padding));
    const auto noncanonical_padding = glyphastore::decode_record(nonzero_padding);
    GLYPHA_REQUIRE(!noncanonical_padding.has_value());
    GLYPHA_REQUIRE(noncanonical_padding.error().code == glyphastore::ErrorCode::invalid_record);

    auto oversized_extent = fixture;
    oversized_extent.resize(fixture.size() + glyphastore::kRecordAlignment, std::byte{0});
    put_u32(oversized_extent, 8, static_cast<std::uint32_t>(oversized_extent.size()));
    put_u32(oversized_extent, 20, glyphastore::crc32c_with_zeroed_checksum_field(oversized_extent));
    const auto noncanonical_extent = glyphastore::decode_record(oversized_extent);
    GLYPHA_REQUIRE(!noncanonical_extent.has_value());
    GLYPHA_REQUIRE(noncanonical_extent.error().code == glyphastore::ErrorCode::invalid_record);
}

GLYPHA_TEST("record codec round-trips explicit little-endian fields") {
    const auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{42},
        .key_hash = 1234,
        .expire_at_ns = 9999,
        .key = as_bytes("alpha"),
        .value = as_bytes("value"),
    });
    GLYPHA_REQUIRE(encoded.has_value());
    const auto decoded = glyphastore::decode_record(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->sequence.value == 42);
    GLYPHA_REQUIRE(decoded->key_string() == "alpha");
    GLYPHA_REQUIRE(decoded->value.size() == 5);
    GLYPHA_REQUIRE(decoded->encoded_size % glyphastore::kRecordAlignment == 0);
}

GLYPHA_TEST("record codec rejects truncation and checksum corruption") {
    auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{1},
        .key = as_bytes("k"),
        .value = as_bytes("v"),
    });
    GLYPHA_REQUIRE(encoded.has_value());
    GLYPHA_REQUIRE(!glyphastore::decode_record(std::span<const std::byte>{*encoded}.first(10)).has_value());
    (*encoded)[glyphastore::kEncodedRecordHeaderSize] ^= std::byte{1};
    const auto corrupted = glyphastore::decode_record(*encoded);
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("record codec rejects total size smaller than its header") {
    auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{1},
        .key = as_bytes("k"),
        .value = as_bytes("v"),
    });
    GLYPHA_REQUIRE(encoded.has_value());
    for (std::size_t index = 8; index < 12; ++index) {
        (*encoded)[index] = std::byte{0};
    }
    for (std::size_t index = 20; index < 24; ++index) {
        (*encoded)[index] = std::byte{0};
    }
    const auto decoded = glyphastore::decode_record(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::invalid_record);
}

GLYPHA_TEST("record codec rejects unknown opcode and value type on encode") {
    const auto opcode = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{1},
        .opcode = static_cast<glyphastore::Opcode>(999),
    });
    GLYPHA_REQUIRE(!opcode.has_value());
    const auto type = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{1},
        .type = static_cast<glyphastore::ValueType>(999),
    });
    GLYPHA_REQUIRE(!type.has_value());
}

GLYPHA_TEST("record codec supports empty key and value without unsafe extents") {
    const auto encoded = glyphastore::encode_record({.sequence = glyphastore::SequenceNumber{7}});
    GLYPHA_REQUIRE(encoded.has_value());
    const auto decoded = glyphastore::decode_record(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->key.empty());
    GLYPHA_REQUIRE(decoded->value.empty());
}

GLYPHA_TEST("record encode with precomputed size matches encoded_record_size path") {
    const glyphastore::RecordInput input{
        .sequence = glyphastore::SequenceNumber{9},
        .key = as_bytes("pre"),
        .value = as_bytes("size"),
    };
    const auto size = glyphastore::encoded_record_size(input);
    GLYPHA_REQUIRE(size.has_value());

    std::vector<std::byte> via_size(*size, std::byte{0});
    std::vector<std::byte> via_compute(*size, std::byte{0});
    GLYPHA_REQUIRE(glyphastore::encode_record(via_size, input, *size).has_value());
    GLYPHA_REQUIRE(glyphastore::encode_record(via_compute, input).has_value());
    GLYPHA_REQUIRE(via_size == via_compute);

    const auto decoded = glyphastore::decode_record(via_size);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->key_string() == "pre");
}

GLYPHA_TEST("crc32c matches the standard Castagnoli test vector") {
    static constexpr std::array<char, 9> payload{'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    const auto bytes =
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(payload.data()), payload.size()};
    GLYPHA_REQUIRE(glyphastore::crc32c(bytes) == 0xE3069283U);
}

GLYPHA_TEST("record encode zeroes alignment padding in destination buffers") {
    const auto size = glyphastore::encoded_record_size({
        .sequence = glyphastore::SequenceNumber{1},
        .key = as_bytes("k"),
        .value = as_bytes("v"),
    });
    GLYPHA_REQUIRE(size.has_value());

    std::vector<std::byte> buffer(*size, std::byte{0xA5});
    GLYPHA_REQUIRE(glyphastore::encode_record(buffer,
                                              {
                                                  .sequence = glyphastore::SequenceNumber{1},
                                                  .key = as_bytes("k"),
                                                  .value = as_bytes("v"),
                                              })
                       .has_value());

    const auto payload_end = glyphastore::kEncodedRecordHeaderSize + 1U + 1U;
    for (std::size_t index = payload_end; index < *size; ++index) {
        GLYPHA_REQUIRE(buffer[index] == std::byte{0});
    }

    const auto decoded = glyphastore::decode_record(buffer);
    GLYPHA_REQUIRE(decoded.has_value());
}
