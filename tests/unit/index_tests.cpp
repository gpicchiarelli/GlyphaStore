#include "glyphastore/index/index.hpp"
#include "test.hpp"

#include <cstdint>
#include <limits>
#include <string>

GLYPHA_TEST("index inserts replaces finds erases and iterates") {
    glyphastore::Index index;
    const glyphastore::RecordRef first{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                       glyphastore::RecordSize{20}, glyphastore::SequenceNumber{1},
                                       glyphastore::GenerationId{1}};
    const auto inserted = index.insert_or_assign("key", first);
    GLYPHA_REQUIRE(inserted.has_value());
    GLYPHA_REQUIRE(inserted->inserted);
    GLYPHA_REQUIRE(index.find("key") == first);

    auto second = first;
    second.sequence = glyphastore::SequenceNumber{2};
    const auto replaced = index.insert_or_assign("key", second);
    GLYPHA_REQUIRE(replaced.has_value());
    GLYPHA_REQUIRE(!replaced->inserted);
    GLYPHA_REQUIRE(replaced->previous == first);
    GLYPHA_REQUIRE(index.entries().size() == 1);
    GLYPHA_REQUIRE(index.erase("key").previous == second);
    GLYPHA_REQUIRE(!index.find("key").has_value());
}

GLYPHA_TEST("index preserves keys larger than 16-bit lengths") {
    glyphastore::Index index;
    const std::string key(70'000, 'x');
    const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                     glyphastore::RecordSize{20}, glyphastore::SequenceNumber{1},
                                     glyphastore::GenerationId{1}};
    const auto inserted = index.insert_or_assign(key, ref);
    GLYPHA_REQUIRE(inserted.has_value());
    GLYPHA_REQUIRE(inserted->inserted);
    GLYPHA_REQUIRE(index.find(key) == ref);
    GLYPHA_REQUIRE(index.entries().front().key == key);
}

GLYPHA_TEST("index grows and rejects impossible reserve sizes") {
    glyphastore::Index index;
    for (std::uint64_t value = 0; value < 1'000; ++value) {
        const auto key = std::to_string(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20}, glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    GLYPHA_REQUIRE(index.stats().size == 1'000);
    GLYPHA_REQUIRE(index.stats().slot_bytes == 64);
    GLYPHA_REQUIRE(index.stats().table_allocated_bytes ==
                   index.stats().bucket_count * (index.stats().slot_bytes + sizeof(std::uint8_t)));
    GLYPHA_REQUIRE(!index.reserve(std::numeric_limits<std::size_t>::max()).has_value());
    GLYPHA_REQUIRE(!index.prepare_batch_insert(std::numeric_limits<std::size_t>::max(), 0).has_value());
}

GLYPHA_TEST("index resolves complete-hash collisions by full key bytes") {
    glyphastore::Index index;
    constexpr std::uint64_t forced_hash = 0xDEADBEEF12345678ULL;
    const glyphastore::HashedKey first{"collision-a", forced_hash};
    const glyphastore::HashedKey second{"collision-b", forced_hash};
    const glyphastore::RecordRef first_ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                           glyphastore::RecordSize{20}, glyphastore::SequenceNumber{11},
                                           glyphastore::GenerationId{1}};
    auto second_ref = first_ref;
    second_ref.sequence = glyphastore::SequenceNumber{12};
    GLYPHA_REQUIRE(index.insert_or_assign(first, first_ref).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign(second, second_ref).has_value());
    GLYPHA_REQUIRE(index.find(first) == first_ref);
    GLYPHA_REQUIRE(index.find(second) == second_ref);
}

GLYPHA_TEST("index preflights long-key publication before a durable commit") {
    glyphastore::Index index;
    const std::string key(80'000, 'k');
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key(key)};
    GLYPHA_REQUIRE(index.prepare_insert(hashed).has_value());
    const glyphastore::RecordRef ref{glyphastore::SegmentId{3}, glyphastore::RecordOffset{4096},
                                     glyphastore::RecordSize{80'056}, glyphastore::SequenceNumber{7},
                                     glyphastore::GenerationId{1}};
    const auto inserted = index.insert_or_assign(hashed, ref);
    GLYPHA_REQUIRE(inserted.has_value());
    GLYPHA_REQUIRE(inserted->inserted);
    GLYPHA_REQUIRE(index.find(hashed) == ref);
    GLYPHA_REQUIRE(index.erase(hashed).previous == ref);
}
