#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace glyphastore {

enum class Opcode : std::uint16_t { put = 1, erase = 2 };
enum class ValueType : std::uint16_t { bytes = 1, integer = 2, map = 3, lease = 4 };

struct RecordInput {
    SequenceNumber sequence;
    Opcode opcode{Opcode::put};
    ValueType type{ValueType::bytes};
    std::uint32_t flags{};
    std::uint64_t key_hash{};
    std::uint64_t expire_at_ns{};
    std::span<const std::byte> key{};
    std::span<const std::byte> value{};
};

struct RecordView {
    SequenceNumber sequence;
    Opcode opcode;
    ValueType type;
    std::uint32_t flags;
    std::uint64_t key_hash;
    std::uint64_t expire_at_ns;
    // key and value span encoded bytes inside a Segment buffer. The spans remain
    // valid only while the owning Store or Segment that produced this view lives.
    std::span<const std::byte> key;
    std::span<const std::byte> value;
    std::uint32_t encoded_size;

    [[nodiscard]] auto key_string() const -> std::string_view;
    [[nodiscard]] auto expired(std::uint64_t now_ns) const noexcept -> bool;
};

inline constexpr std::uint32_t kRecordMagic = 0x52594C47U;
inline constexpr std::uint16_t kRecordFormatVersion = 1;
inline constexpr std::uint16_t kEncodedRecordHeaderSize = 56;

[[nodiscard]] auto encoded_record_size(const RecordInput& input) -> Result<std::size_t>;
[[nodiscard]] auto encode_record(std::span<std::byte> out, const RecordInput& input) -> Status;
[[nodiscard]] auto encode_record(const RecordInput& input) -> Result<std::vector<std::byte>>;
[[nodiscard]] auto decode_record(std::span<const std::byte> bytes, bool verify_checksum = true)
    -> Result<RecordView>;

} // namespace glyphastore
