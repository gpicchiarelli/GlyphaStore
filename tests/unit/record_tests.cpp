#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <array>
#include <cstddef>
#include <string_view>

namespace {

auto as_bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

} // namespace

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
