#include "glyphastore/segment/segment.hpp"
#include "test.hpp"

#include <cstddef>
#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}
} // namespace

GLYPHA_TEST("segment is exactly 64 MiB and provides positional record access") {
    glyphastore::Segment segment{glyphastore::SegmentId{1}};
    GLYPHA_REQUIRE(segment.capacity() == 67'108'864);
    const auto ref = segment.append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes("key"),
        .value = bytes("value"),
    });
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(ref->offset.value >= glyphastore::kSegmentHeaderReservedBytes);
    const auto view = segment.read(*ref);
    GLYPHA_REQUIRE(view.has_value());
    GLYPHA_REQUIRE(view->key_string() == "key");
    GLYPHA_REQUIRE(segment.base() + ref->offset.value != nullptr);
}

GLYPHA_TEST("segment seal forbids future append") {
    glyphastore::Segment segment{glyphastore::SegmentId{2}};
    GLYPHA_REQUIRE(segment.seal().has_value());
    const auto result = segment.append({.sequence = glyphastore::SequenceNumber{1}});
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::segment_sealed);
}

GLYPHA_TEST("segment generation protects against stale RecordRef") {
    glyphastore::Segment segment{glyphastore::SegmentId{3}, {}, glyphastore::GenerationId{9}};
    const auto ref = segment.append({.sequence = glyphastore::SequenceNumber{1}, .key = bytes("k")});
    GLYPHA_REQUIRE(ref.has_value());
    auto stale = *ref;
    stale.generation = glyphastore::GenerationId{8};
    GLYPHA_REQUIRE(!segment.read(stale).has_value());
}

GLYPHA_TEST("segment liveness permits retirement only after all live records die") {
    glyphastore::Segment segment{glyphastore::SegmentId{4}};
    const auto ref = segment.append({.sequence = glyphastore::SequenceNumber{1}, .key = bytes("k")});
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(segment.mark_live(*ref).has_value());
    GLYPHA_REQUIRE(segment.seal().has_value());
    GLYPHA_REQUIRE(!segment.retire().has_value());
    GLYPHA_REQUIRE(segment.mark_dead(*ref).has_value());
    GLYPHA_REQUIRE(segment.retire().has_value());
}
