#include "glyphastore/index/index.hpp"
#include "test.hpp"

GLYPHA_TEST("index inserts replaces finds erases and iterates") {
    glyphastore::Index index;
    const glyphastore::RecordRef first{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                       glyphastore::RecordSize{20}, glyphastore::SequenceNumber{1},
                                       glyphastore::GenerationId{1}};
    const auto inserted = index.insert_or_assign("key", first);
    GLYPHA_REQUIRE(inserted.inserted);
    GLYPHA_REQUIRE(index.find("key") == first);

    auto second = first;
    second.sequence = glyphastore::SequenceNumber{2};
    const auto replaced = index.insert_or_assign("key", second);
    GLYPHA_REQUIRE(!replaced.inserted);
    GLYPHA_REQUIRE(replaced.previous == first);
    GLYPHA_REQUIRE(index.entries().size() == 1);
    GLYPHA_REQUIRE(index.erase("key").previous == second);
    GLYPHA_REQUIRE(!index.find("key").has_value());
}
