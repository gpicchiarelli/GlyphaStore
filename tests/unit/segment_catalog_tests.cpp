#include "glyphastore/segment/global_manager.hpp"
#include "test.hpp"

#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}
} // namespace

GLYPHA_TEST("segment catalog lookup and snapshot order survive rotation") {
    glyphastore::GlobalSegmentManager manager;
    const auto first = manager.allocate_active(glyphastore::WorkerId{1});
    GLYPHA_REQUIRE(first != nullptr);
    const auto before_rotation = manager.segments();
    const auto rotated = manager.prepare_rotation(first, glyphastore::WorkerId{1});
    GLYPHA_REQUIRE(rotated.has_value());
    GLYPHA_REQUIRE(first->state() == glyphastore::SegmentState::active);
    GLYPHA_REQUIRE(manager.find((*rotated)->id()) == nullptr);
    GLYPHA_REQUIRE(manager.commit_rotation(first, *rotated).has_value());
    GLYPHA_REQUIRE((*rotated)->state() == glyphastore::SegmentState::active);
    GLYPHA_REQUIRE(first->state() == glyphastore::SegmentState::sealed);
    GLYPHA_REQUIRE(manager.find(first->id()) == first);
    GLYPHA_REQUIRE(manager.find((*rotated)->id()) == *rotated);
    GLYPHA_REQUIRE(before_rotation.size() == 1);
    const auto after_rotation = manager.segments();
    GLYPHA_REQUIRE(after_rotation.size() == 2);
    GLYPHA_REQUIRE(after_rotation[0]->id() == first->id());
    GLYPHA_REQUIRE(after_rotation[1]->id() == (*rotated)->id());
}

GLYPHA_TEST("segment catalog releases ownership of retired segments") {
    glyphastore::GlobalSegmentManager manager;
    auto segment = manager.allocate_active(glyphastore::WorkerId{2});
    GLYPHA_REQUIRE(segment != nullptr);
    const std::weak_ptr<glyphastore::Segment> lifetime = segment;
    const auto ref = segment->append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes("k"),
        .value = bytes("v"),
    });
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(segment->mark_live(*ref).has_value());
    GLYPHA_REQUIRE(segment->seal().has_value());
    GLYPHA_REQUIRE(segment->mark_dead(*ref).has_value());
    const auto before_retirement = manager.retired_count();
    GLYPHA_REQUIRE(manager.try_retire(segment->id()).has_value());
    GLYPHA_REQUIRE(manager.find(segment->id()) == nullptr);
    GLYPHA_REQUIRE(manager.segments().empty());
    GLYPHA_REQUIRE(before_retirement == 0);
    GLYPHA_REQUIRE(manager.retired_count() == 1);
    GLYPHA_REQUIRE(!lifetime.expired());
    segment.reset();
    GLYPHA_REQUIRE(lifetime.expired());
}
