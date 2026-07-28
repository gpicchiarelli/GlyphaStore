#include "glyphastore/index/index.hpp"
#include "glyphastore/index/swiss_control_group.hpp"
#include "test.hpp"

#include <array>
#include <cstdint>
#include <limits>
#include <string>

namespace {

auto test_ref(const std::uint64_t sequence) -> glyphastore::RecordRef {
    return {glyphastore::SegmentId{1}, glyphastore::RecordOffset{4096}, glyphastore::RecordSize{64},
            glyphastore::SequenceNumber{sequence}, glyphastore::GenerationId{1}};
}

auto mixed_hash(const std::uint64_t value) -> std::uint64_t {
    auto hash = value ^ 0x243F6A8885A308D3ULL;
    hash *= 0x9E3779B97F4A7C15ULL;
    hash ^= hash >> 33U;
    hash *= 0x9E3779B97F4A7C15ULL;
    hash ^= hash >> 29U;
    return hash;
}

} // namespace

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

GLYPHA_TEST("index tracks and reuses deleted slots without changing capacity") {
    glyphastore::Index index;
    constexpr std::uint64_t forced_hash = 0xD311E7EDULL;
    const glyphastore::HashedKey first{"deleted-a", forced_hash};
    const glyphastore::HashedKey second{"deleted-b", forced_hash};
    const glyphastore::HashedKey replacement{"deleted-c", forced_hash};
    GLYPHA_REQUIRE(index.insert_or_assign(first, test_ref(1)).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign(second, test_ref(2)).has_value());
    const auto capacity = index.stats().bucket_count;
    GLYPHA_REQUIRE(index.erase_no_compact(first).previous == test_ref(1));
    GLYPHA_REQUIRE(index.stats().deleted_count == 1);
    GLYPHA_REQUIRE(index.stats().size + index.stats().deleted_count <= capacity);

    GLYPHA_REQUIRE(index.insert_or_assign(replacement, test_ref(3)).has_value());
    const auto reused = index.stats();
    GLYPHA_REQUIRE(reused.bucket_count == capacity);
    GLYPHA_REQUIRE(reused.deleted_count == 0);
    GLYPHA_REQUIRE(index.find(second) == test_ref(2));
    GLYPHA_REQUIRE(index.find(replacement) == test_ref(3));
}

GLYPHA_TEST("index rebuilds tombstones transactionally at the same capacity") {
    glyphastore::Index index;
    GLYPHA_REQUIRE(index.reserve(100).has_value());
    for (std::uint64_t value = 0; value < 100; ++value) {
        const auto key = std::string{"collision-"} + std::to_string(value);
        GLYPHA_REQUIRE(index.insert_or_assign(key, test_ref(value + 1)).has_value());
    }
    const auto capacity = index.stats().bucket_count;
    for (std::uint64_t value = 0; value < 80; ++value) {
        const auto key = std::string{"collision-"} + std::to_string(value);
        const glyphastore::HashedKey hashed{key, glyphastore::hash_key(key)};
        GLYPHA_REQUIRE(index.erase_no_compact(hashed).previous.has_value());
    }
    const auto churned = index.stats();
    GLYPHA_REQUIRE(churned.size == 20);
    GLYPHA_REQUIRE(churned.deleted_count == 80);
    GLYPHA_REQUIRE(churned.effective_load_factor > churned.load_factor);

    const std::string next_key{"collision-next"};
    const glyphastore::HashedKey next{next_key, glyphastore::hash_key(next_key)};
    GLYPHA_REQUIRE(index.prepare_insert(next).has_value());
    const auto rebuilt = index.stats();
    GLYPHA_REQUIRE(rebuilt.bucket_count == capacity);
    GLYPHA_REQUIRE(rebuilt.deleted_count == 0);
    GLYPHA_REQUIRE(rebuilt.tombstone_rebuild_count == 1);
    GLYPHA_REQUIRE(rebuilt.rehash_count >= 1);
    GLYPHA_REQUIRE(index.insert_or_assign(next, test_ref(101)).has_value());
    for (std::uint64_t value = 80; value < 100; ++value) {
        const auto key = std::string{"collision-"} + std::to_string(value);
        GLYPHA_REQUIRE(index.find(key) == test_ref(value + 1));
    }
}

GLYPHA_TEST("index bounds full collision probing across table wraparound") {
    glyphastore::Index index;
    GLYPHA_REQUIRE(index.reserve(32).has_value());
    const auto capacity = index.stats().bucket_count;
    std::uint64_t forced_hash{};
    while (((mixed_hash(forced_hash) >> 7U) & ((capacity / glyphastore::kSwissGroupSize) - 1U)) !=
           (capacity / glyphastore::kSwissGroupSize) - 1U) {
        ++forced_hash;
    }
    for (std::uint64_t value = 0; value < 16; ++value) {
        const auto key = std::string{"wrap-"} + std::to_string(value);
        GLYPHA_REQUIRE(index.insert_or_assign(glyphastore::HashedKey{key, forced_hash}, test_ref(value + 1))
                           .has_value());
    }
    for (std::uint64_t value = 0; value < 16; ++value) {
        const auto key = std::string{"wrap-"} + std::to_string(value);
        GLYPHA_REQUIRE(index.find(glyphastore::HashedKey{key, forced_hash}) == test_ref(value + 1));
    }
    const glyphastore::HashedKey missing{"wrap-missing", forced_hash};
    GLYPHA_REQUIRE(!index.find(missing).has_value());
    const auto stats = index.stats();
    GLYPHA_REQUIRE(stats.maximum_probe_groups >= 3);
    GLYPHA_REQUIRE(stats.maximum_probe_groups <= stats.bucket_count / glyphastore::kSwissGroupSize);
}

GLYPHA_TEST("index grows only after reaching seven eighths live occupancy") {
    glyphastore::Index index;
    for (std::uint64_t value = 0; value < 7; ++value) {
        GLYPHA_REQUIRE(index.insert_or_assign(std::to_string(value), test_ref(value + 1)).has_value());
    }
    GLYPHA_REQUIRE(index.stats().bucket_count == 8);
    GLYPHA_REQUIRE(index.stats().size == 7);
    GLYPHA_REQUIRE(index.stats().effective_load_factor == glyphastore::kSwissMaxLoadFactor);
    GLYPHA_REQUIRE(index.insert_or_assign("growth", test_ref(8)).has_value());
    GLYPHA_REQUIRE(index.stats().bucket_count == 16);
    GLYPHA_REQUIRE(index.stats().deleted_count == 0);
}

GLYPHA_TEST("selected Swiss control matcher equals scalar masks") {
    std::array<std::uint8_t, glyphastore::kSwissGroupSize> control{};
    for (std::uint32_t pattern = 0; pattern < 256; ++pattern) {
        for (std::size_t index = 0; index < control.size(); ++index) {
            control[index] = static_cast<std::uint8_t>((pattern * 37U + index * 53U) & 0xFFU);
        }
        for (std::uint32_t byte = 0; byte < 256; ++byte) {
            GLYPHA_REQUIRE(
                glyphastore::detail::equal_byte_mask(control.data(), static_cast<std::uint8_t>(byte)) ==
                glyphastore::detail::equal_byte_mask_scalar(control.data(), static_cast<std::uint8_t>(byte)));
        }
    }
}

GLYPHA_TEST("index keeps inline and heap keys stable through prolonged churn") {
    glyphastore::Index index;
    constexpr std::size_t rounds = 32;
    constexpr std::size_t entries_per_round = 96;
    GLYPHA_REQUIRE(index.reserve(entries_per_round).has_value());

    for (std::size_t round = 0; round < rounds; ++round) {
        for (std::size_t value = 0; value < entries_per_round; ++value) {
            const auto suffix = std::to_string(round * entries_per_round + value);
            const auto key = value % 2U == 0 ? std::string{"inline-"} + suffix
                                             : std::string(96, static_cast<char>('a' + round % 26U)) + suffix;
            GLYPHA_REQUIRE(index.insert_or_assign(key, test_ref(value + 1)).has_value());
        }
        for (std::size_t value = 0; value + 1 < entries_per_round; ++value) {
            const auto suffix = std::to_string(round * entries_per_round + value);
            const auto key = value % 2U == 0 ? std::string{"inline-"} + suffix
                                             : std::string(96, static_cast<char>('a' + round % 26U)) + suffix;
            GLYPHA_REQUIRE(index.erase_no_compact(glyphastore::HashedKey::compute(key)).previous ==
                           test_ref(value + 1));
        }

        const auto survivor_suffix = std::to_string((round + 1U) * entries_per_round - 1U);
        const auto survivor = std::string(96, static_cast<char>('a' + round % 26U)) + survivor_suffix;
        GLYPHA_REQUIRE(index.find(survivor) == test_ref(entries_per_round));
        GLYPHA_REQUIRE(!index.find("absent-after-heavy-churn").has_value());
        GLYPHA_REQUIRE(index.stats().size + index.stats().deleted_count <= index.stats().bucket_count);
        GLYPHA_REQUIRE(index.erase_no_compact(glyphastore::HashedKey::compute(survivor)).previous ==
                       test_ref(entries_per_round));
    }

    GLYPHA_REQUIRE(index.stats().size == 0);
    GLYPHA_REQUIRE(index.prepare_insert(glyphastore::HashedKey::compute("final-inline")).has_value());
    GLYPHA_REQUIRE(index.stats().deleted_count == 0);
    GLYPHA_REQUIRE(index.insert_or_assign("final-inline", test_ref(1)).has_value());
    GLYPHA_REQUIRE(index.find("final-inline") == test_ref(1));
}
