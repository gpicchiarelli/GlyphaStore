#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto heap_key(std::uint64_t suffix) -> std::string {
    return std::string(32, 'h') + std::to_string(suffix);
}
} // namespace

GLYPHA_TEST("index heap key arena survives erase churn and rehash") {
    glyphastore::Index index;
    constexpr std::uint64_t initial = 2'000;
    for (std::uint64_t value = 0; value < initial; ++value) {
        const auto key = heap_key(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1},
                                         glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20},
                                         glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    for (std::uint64_t value = 0; value < initial / 2; ++value) {
        GLYPHA_REQUIRE(index.erase(heap_key(value)).previous.has_value());
    }
    for (std::uint64_t value = initial; value < initial + (initial / 2); ++value) {
        const auto key = heap_key(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1},
                                         glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20},
                                         glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    for (std::uint64_t value = initial / 2; value < initial + (initial / 2); ++value) {
        GLYPHA_REQUIRE(index.find(heap_key(value)).has_value());
    }
    GLYPHA_REQUIRE(index.stats().size == initial);
}

GLYPHA_TEST("index swiss table probe path matches under mixed inline and heap keys") {
    glyphastore::Index index;
    for (std::uint64_t value = 0; value < 4'096; ++value) {
        const auto key = (value % 3 == 0) ? heap_key(value) : ("inline-" + std::to_string(value));
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1},
                                         glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20},
                                         glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    std::size_t expected = 4'096;
    for (std::uint64_t value = 0; value < 4'096; value += 7) {
        const auto key = (value % 3 == 0) ? heap_key(value) : ("inline-" + std::to_string(value));
        GLYPHA_REQUIRE(index.find(key).has_value());
        GLYPHA_REQUIRE(index.erase(key).previous.has_value());
        --expected;
    }
    GLYPHA_REQUIRE(index.stats().size == expected);
}

GLYPHA_TEST("hashed key path preserves single hash lookup") {
    glyphastore::Index index;
    const auto hashed = glyphastore::HashedKey::compute("hashed-route");
    const glyphastore::RecordRef ref{glyphastore::SegmentId{9},
                                     glyphastore::RecordOffset{10},
                                     glyphastore::RecordSize{20},
                                     glyphastore::SequenceNumber{1},
                                     glyphastore::GenerationId{1}};
    GLYPHA_REQUIRE(index.insert_or_assign(hashed, ref).has_value());
    GLYPHA_REQUIRE(index.find(hashed) == ref);
}

GLYPHA_TEST("store get verifies checksum after mutable segment corruption") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("integrity", bytes("payload")).has_value());
    const auto route = glyphastore::route_worker("integrity", store.worker_count());
    const auto ref = store.worker(route).index().find("integrity");
    GLYPHA_REQUIRE(ref.has_value());
    GLYPHA_REQUIRE(!store.segments().empty());
    auto& segment = *store.segments().front();
    segment.mutable_base()[ref->offset.value + 32U] ^= std::byte{0xFF};

    const auto corrupted = store.get("integrity");
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
}
