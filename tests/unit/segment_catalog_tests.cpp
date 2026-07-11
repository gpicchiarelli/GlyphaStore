#include "glyphastore/segment/global_manager.hpp"
#include "test.hpp"

#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}
} // namespace

GLYPHA_TEST("segment catalog dense lookup survives rotation") {
    glyphastore::GlobalSegmentManager manager;
    const auto first = manager.allocate_active(glyphastore::WorkerId{1});
    GLYPHA_REQUIRE(first != nullptr);
    const auto rotated = manager.rotate_active(first, glyphastore::WorkerId{1});
    GLYPHA_REQUIRE(rotated.has_value());
    GLYPHA_REQUIRE((*rotated)->state() == glyphastore::SegmentState::active);
    GLYPHA_REQUIRE(first->state() == glyphastore::SegmentState::sealed);
    GLYPHA_REQUIRE(manager.find(first->id()) == first.get());
    GLYPHA_REQUIRE(manager.find((*rotated)->id()) == rotated->get());
    GLYPHA_REQUIRE(manager.segments().size() == 2);
}

GLYPHA_TEST("segment catalog clears retired slot but keeps segment history") {
    glyphastore::GlobalSegmentManager manager;
    const auto segment = manager.allocate_active(glyphastore::WorkerId{2});
    GLYPHA_REQUIRE(segment != nullptr);
    const auto ref = segment->append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes("k"),
        .value = bytes("v"),
    });
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(segment->mark_live(*ref).has_value());
    GLYPHA_REQUIRE(segment->seal().has_value());
    GLYPHA_REQUIRE(segment->mark_dead(*ref).has_value());
    GLYPHA_REQUIRE(manager.try_retire(segment->id()).has_value());
    GLYPHA_REQUIRE(manager.find(segment->id()) == nullptr);
    GLYPHA_REQUIRE(manager.segments().size() == 1);
    GLYPHA_REQUIRE(!manager.retired_pool().empty());
}
