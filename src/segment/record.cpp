#include "glyphastore/segment/record.hpp"

#include "glyphastore/core/checked_math.hpp"
#include "glyphastore/segment/crc32c.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace glyphastore {
namespace {

constexpr auto to_u8(std::byte value) noexcept -> std::uint8_t {
    return std::to_integer<std::uint8_t>(value);
}

void put_u16(std::span<std::byte> out, std::size_t at, std::uint16_t value) {
    out[at] = static_cast<std::byte>(value & 0xFFU);
    out[at + 1] = static_cast<std::byte>((value >> 8U) & 0xFFU);
}

void put_u32(std::span<std::byte> out, std::size_t at, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        out[at + i] = static_cast<std::byte>((value >> (i * 8U)) & 0xFFU);
    }
}

void put_u64(std::span<std::byte> out, std::size_t at, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        out[at + i] = static_cast<std::byte>((value >> (i * 8U)) & 0xFFU);
    }
}

auto get_u16(std::span<const std::byte> in, std::size_t at) -> std::uint16_t {
    return static_cast<std::uint16_t>(to_u8(in[at])) |
           static_cast<std::uint16_t>(static_cast<std::uint16_t>(to_u8(in[at + 1])) << 8U);
}

auto get_u32(std::span<const std::byte> in, std::size_t at) -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t i = 0; i < 4; ++i) {
        value |= static_cast<std::uint32_t>(to_u8(in[at + i])) << (i * 8U);
    }
    return value;
}

auto get_u64(std::span<const std::byte> in, std::size_t at) -> std::uint64_t {
    std::uint64_t value{};
    for (std::size_t i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(to_u8(in[at + i])) << (i * 8U);
    }
    return value;
}

[[nodiscard]] auto validate_record_input(const RecordInput& input) -> Status {
    const auto opcode_raw = static_cast<std::uint16_t>(input.opcode);
    if (opcode_raw != static_cast<std::uint16_t>(Opcode::put) &&
        opcode_raw != static_cast<std::uint16_t>(Opcode::erase)) {
        return fail(ErrorCode::invalid_argument, "unknown record opcode");
    }
    const auto type_raw = static_cast<std::uint16_t>(input.type);
    if (type_raw < static_cast<std::uint16_t>(ValueType::bytes) ||
        type_raw > static_cast<std::uint16_t>(ValueType::lease)) {
        return fail(ErrorCode::invalid_argument, "unknown record value type");
    }
    if (input.key.size() > std::numeric_limits<std::uint32_t>::max() ||
        input.value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::record_too_large, "key or value exceeds record format limits");
    }
    return {};
}

[[nodiscard]] auto compute_encoded_size(const RecordInput& input) -> Result<std::size_t> {
    if (auto valid = validate_record_input(input); !valid) {
        return unexpected(valid.error());
    }
    auto raw = checked_add<std::size_t>(kEncodedRecordHeaderSize, input.key.size());
    if (!raw) {
        return unexpected(raw.error());
    }
    raw = checked_add<std::size_t>(*raw, input.value.size());
    if (!raw) {
        return unexpected(raw.error());
    }
    auto aligned = align_up_checked<std::size_t>(*raw, kRecordAlignment);
    if (!aligned || *aligned > kMaxNormalRecordSize || *aligned > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::record_too_large, "encoded record exceeds the normal record limit");
    }
    return *aligned;
}

} // namespace

auto RecordView::key_string() const -> std::string_view {
    return {reinterpret_cast<const char*>(key.data()), key.size()};
}

auto RecordView::expired(std::uint64_t now_ns) const noexcept -> bool {
    return expire_at_ns != 0 && now_ns != 0 && expire_at_ns <= now_ns;
}

auto encoded_record_size(const RecordInput& input) -> Result<std::size_t> {
    return compute_encoded_size(input);
}

auto encode_record(const std::span<std::byte> out, const RecordInput& input,
                   const std::size_t encoded_size) -> Status {
    if (auto valid = validate_record_input(input); !valid) {
        return valid;
    }
    const auto minimum = kEncodedRecordHeaderSize + input.key.size() + input.value.size();
    if (encoded_size < minimum || encoded_size % kRecordAlignment != 0 || out.size() < encoded_size ||
        encoded_size > kMaxNormalRecordSize) {
        return fail(ErrorCode::invalid_argument, "encode buffer size is inconsistent with the record");
    }
#ifndef NDEBUG
    const auto checked = compute_encoded_size(input);
    if (!checked) {
        return unexpected(checked.error());
    }
    if (*checked != encoded_size) {
        return fail(ErrorCode::invalid_argument,
                    "encode_record encoded_size does not match encoded_record_size(input)");
    }
#endif

    const auto opcode_raw = static_cast<std::uint16_t>(input.opcode);
    const auto type_raw = static_cast<std::uint16_t>(input.type);
    const auto encoded = out.first(encoded_size);
    put_u32(encoded, 0, kRecordMagic);
    put_u16(encoded, 4, kRecordFormatVersion);
    put_u16(encoded, 6, kEncodedRecordHeaderSize);
    put_u32(encoded, 8, static_cast<std::uint32_t>(encoded_size));
    put_u32(encoded, 12, static_cast<std::uint32_t>(input.key.size()));
    put_u32(encoded, 16, static_cast<std::uint32_t>(input.value.size()));
    put_u32(encoded, 20, 0);
    put_u64(encoded, 24, input.sequence.value);
    put_u64(encoded, 32, input.key_hash);
    put_u64(encoded, 40, input.expire_at_ns);
    put_u16(encoded, 48, opcode_raw);
    put_u16(encoded, 50, type_raw);
    put_u32(encoded, 52, input.flags);
    if (!input.key.empty()) {
        std::memcpy(encoded.data() + kEncodedRecordHeaderSize, input.key.data(), input.key.size());
    }
    if (!input.value.empty()) {
        std::memcpy(encoded.data() + kEncodedRecordHeaderSize + input.key.size(), input.value.data(),
                    input.value.size());
    }
    const auto payload_end = kEncodedRecordHeaderSize + input.key.size() + input.value.size();
    if (payload_end < encoded_size) {
        std::memset(encoded.data() + payload_end, 0, encoded_size - payload_end);
    }
    put_u32(encoded, 20, crc32c_with_zeroed_checksum_field(encoded));
    return {};
}

auto encode_record(const std::span<std::byte> out, const RecordInput& input) -> Status {
    const auto size = compute_encoded_size(input);
    if (!size) {
        return unexpected(size.error());
    }
    return encode_record(out, input, *size);
}

auto encode_record(const RecordInput& input) -> Result<std::vector<std::byte>> {
    const auto size = compute_encoded_size(input);
    if (!size) {
        return unexpected(size.error());
    }
    std::vector<std::byte> bytes(*size, std::byte{0});
    if (auto encoded = encode_record(bytes, input, *size); !encoded) {
        return unexpected(encoded.error());
    }
    return bytes;
}

auto decode_record(const std::span<const std::byte> bytes, const bool verify_checksum) -> Result<RecordView> {
    if (bytes.size() < kEncodedRecordHeaderSize) {
        return fail(ErrorCode::invalid_record, "record is shorter than the encoded header");
    }
    if (get_u32(bytes, 0) != kRecordMagic || get_u16(bytes, 4) != kRecordFormatVersion ||
        get_u16(bytes, 6) != kEncodedRecordHeaderSize) {
        return fail(ErrorCode::invalid_record, "record magic, version, or header size is invalid");
    }
    const auto total_size = get_u32(bytes, 8);
    const auto key_size = get_u32(bytes, 12);
    const auto value_size = get_u32(bytes, 16);
    if (total_size < kEncodedRecordHeaderSize || total_size > bytes.size() ||
        total_size > kMaxNormalRecordSize || total_size % kRecordAlignment != 0) {
        return fail(ErrorCode::invalid_record, "record total size is invalid");
    }
    auto payload_size = checked_add<std::size_t>(key_size, value_size);
    if (!payload_size) {
        return fail(ErrorCode::invalid_record, "record payload extent is invalid");
    }
    auto raw_size = checked_add<std::size_t>(kEncodedRecordHeaderSize, *payload_size);
    if (!raw_size) {
        return fail(ErrorCode::invalid_record, "record payload extent overflows its encoded size");
    }
    auto expected_size = align_up_checked<std::size_t>(*raw_size, kRecordAlignment);
    if (!expected_size || *expected_size != total_size) {
        return fail(ErrorCode::invalid_record, "record total size is not its canonical aligned extent");
    }
    const auto encoded = bytes.first(total_size);
    if (verify_checksum && get_u32(bytes, 20) != crc32c_with_zeroed_checksum_field(encoded)) {
        return fail(ErrorCode::checksum_mismatch, "record checksum mismatch");
    }
    const auto opcode_raw = get_u16(bytes, 48);
    if (opcode_raw != static_cast<std::uint16_t>(Opcode::put) &&
        opcode_raw != static_cast<std::uint16_t>(Opcode::erase)) {
        return fail(ErrorCode::invalid_record, "unknown record opcode");
    }
    const auto type_raw = get_u16(bytes, 50);
    if (type_raw < static_cast<std::uint16_t>(ValueType::bytes) ||
        type_raw > static_cast<std::uint16_t>(ValueType::lease)) {
        return fail(ErrorCode::invalid_record, "unknown record value type");
    }
    const auto key_begin = static_cast<std::size_t>(kEncodedRecordHeaderSize);
    const auto payload_end = key_begin + *payload_size;
    if (!std::ranges::all_of(encoded.subspan(payload_end),
                             [](std::byte value) { return value == std::byte{0}; })) {
        return fail(ErrorCode::invalid_record, "record alignment padding is not zero");
    }
    return RecordView{
        .sequence = SequenceNumber{get_u64(bytes, 24)},
        .opcode = static_cast<Opcode>(opcode_raw),
        .type = static_cast<ValueType>(type_raw),
        .flags = get_u32(bytes, 52),
        .key_hash = get_u64(bytes, 32),
        .expire_at_ns = get_u64(bytes, 40),
        .key = encoded.subspan(key_begin, key_size),
        .value = encoded.subspan(key_begin + key_size, value_size),
        .encoded_size = total_size,
    };
}

} // namespace glyphastore
