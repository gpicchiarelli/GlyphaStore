#include "glyphastore/segment/segment.hpp"
#include "test.hpp"

#include <cstddef>
#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

void flip_record_payload_byte(glyphastore::Segment& segment, const glyphastore::RecordRef& ref) {
    segment.mutable_base()[ref.offset.value + 32U] ^= std::byte{0xFF};
}
} // namespace

GLYPHA_TEST("active resident segment is not trusted") {
    glyphastore::Segment segment{glyphastore::SegmentId{1}};
    GLYPHA_REQUIRE(segment.residency() == glyphastore::ResidencyState::resident);
    GLYPHA_REQUIRE(segment.state() == glyphastore::SegmentState::active);
    GLYPHA_REQUIRE(!segment.is_trusted());
}

GLYPHA_TEST("sealed resident segment is trusted for read_trusted gate") {
    glyphastore::Segment segment{glyphastore::SegmentId{2}};
    const auto ref = segment.append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes("key"),
        .value = bytes("value"),
    });
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(segment.seal().has_value());
    GLYPHA_REQUIRE(segment.is_trusted());
}

GLYPHA_TEST("verified read detects corruption read_trusted skips checksum on sealed segment") {
    glyphastore::Segment segment{glyphastore::SegmentId{3}};
    const auto ref = segment.append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes("probe"),
        .value = bytes("payload"),
    });
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(segment.seal().has_value());
    flip_record_payload_byte(segment, *ref);

    const auto verified = segment.read(*ref);
    GLYPHA_REQUIRE(!verified.has_value());
    GLYPHA_REQUIRE(verified.error().code == glyphastore::ErrorCode::checksum_mismatch);

    const auto trusted = segment.read_trusted(*ref);
    GLYPHA_REQUIRE(trusted.has_value());
    GLYPHA_REQUIRE(trusted->key_string() == "probe");
}
