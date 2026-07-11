#include "glyphastore/segment/record.hpp"

#include "glyphastore/core/checked_math.hpp"

#include <array>
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

auto checksum_with_zeroed_field(std::span<const std::byte> bytes) noexcept -> std::uint32_t {
    std::uint32_t crc = 0xFFFFFFFFU;
    auto update = [&crc](std::uint8_t byte) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            const auto mask = static_cast<std::uint32_t>(0U - (crc & 1U));
            crc = (crc >> 1U) ^ (0x82F63B78U & mask);
        }
    };
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        update(i >= 20 && i < 24 ? 0U : to_u8(bytes[i]));
    }
    return ~crc;
}

} // namespace

auto RecordView::key_string() const -> std::string_view {
    return {reinterpret_cast<const char*>(key.data()), key.size()};
}

auto RecordView::expired(std::uint64_t now_ns) const noexcept -> bool {
    return expire_at_ns != 0 && now_ns != 0 && expire_at_ns <= now_ns;
}

auto crc32c(std::span<const std::byte> bytes) noexcept -> std::uint32_t {
    std::uint32_t crc = 0xFFFFFFFFU;
    for (const auto value : bytes) {
        crc ^= to_u8(value);
        for (int bit = 0; bit < 8; ++bit) {
            const auto mask = static_cast<std::uint32_t>(0U - (crc & 1U));
            crc = (crc >> 1U) ^ (0x82F63B78U & mask);
        }
    }
    return ~crc;
}

auto encode_record(const RecordInput& input) -> Result<std::vector<std::byte>> {
    if (input.key.size() > std::numeric_limits<std::uint32_t>::max() ||
        input.value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::record_too_large, "key or value exceeds record format limits");
    }
    auto raw = checked_add<std::size_t>(kEncodedRecordHeaderSize, input.key.size());
    if (!raw) {
        return std::unexpected(raw.error());
    }
    raw = checked_add<std::size_t>(*raw, input.value.size());
    if (!raw) {
        return std::unexpected(raw.error());
    }
    auto aligned = align_up_checked<std::size_t>(*raw, kRecordAlignment);
    if (!aligned || *aligned > kMaxNormalRecordSize || *aligned > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::record_too_large, "encoded record exceeds the normal record limit");
    }

    std::vector<std::byte> bytes(*aligned, std::byte{0});
    auto out = std::span<std::byte>{bytes};
    put_u32(out, 0, kRecordMagic);
    put_u16(out, 4, kRecordFormatVersion);
    put_u16(out, 6, kEncodedRecordHeaderSize);
    put_u32(out, 8, static_cast<std::uint32_t>(*aligned));
    put_u32(out, 12, static_cast<std::uint32_t>(input.key.size()));
    put_u32(out, 16, static_cast<std::uint32_t>(input.value.size()));
    put_u64(out, 24, input.sequence.value);
    put_u64(out, 32, input.key_hash);
    put_u64(out, 40, input.expire_at_ns);
    put_u16(out, 48, static_cast<std::uint16_t>(input.opcode));
    put_u16(out, 50, static_cast<std::uint16_t>(input.type));
    put_u32(out, 52, input.flags);
    std::memcpy(bytes.data() + kEncodedRecordHeaderSize, input.key.data(), input.key.size());
    std::memcpy(bytes.data() + kEncodedRecordHeaderSize + input.key.size(), input.value.data(),
                input.value.size());
    put_u32(out, 20, checksum_with_zeroed_field(bytes));
    return bytes;
}

auto decode_record(std::span<const std::byte> bytes) -> Result<RecordView> {
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
    if (total_size > bytes.size() || total_size > kMaxNormalRecordSize ||
        total_size % kRecordAlignment != 0) {
        return fail(ErrorCode::invalid_record, "record total size is invalid");
    }
    auto payload_size = checked_add<std::size_t>(key_size, value_size);
    if (!payload_size || *payload_size > total_size - kEncodedRecordHeaderSize) {
        return fail(ErrorCode::invalid_record, "record payload extent is invalid");
    }
    const auto encoded = bytes.first(total_size);
    if (get_u32(bytes, 20) != checksum_with_zeroed_field(encoded)) {
        return fail(ErrorCode::checksum_mismatch, "record checksum mismatch");
    }
    const auto opcode_raw = get_u16(bytes, 48);
    if (opcode_raw != static_cast<std::uint16_t>(Opcode::put) &&
        opcode_raw != static_cast<std::uint16_t>(Opcode::erase)) {
        return fail(ErrorCode::invalid_record, "unknown record opcode");
    }
    const auto key_begin = static_cast<std::size_t>(kEncodedRecordHeaderSize);
    return RecordView{
        .sequence = SequenceNumber{get_u64(bytes, 24)},
        .opcode = static_cast<Opcode>(opcode_raw),
        .type = static_cast<ValueType>(get_u16(bytes, 50)),
        .flags = get_u32(bytes, 52),
        .key_hash = get_u64(bytes, 32),
        .expire_at_ns = get_u64(bytes, 40),
        .key = encoded.subspan(key_begin, key_size),
        .value = encoded.subspan(key_begin + key_size, value_size),
        .encoded_size = total_size,
    };
}

} // namespace glyphastore
