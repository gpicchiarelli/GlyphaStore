#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/store/store.hpp"
#include "persistence/hot_record_capacity.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto heap_key(std::uint64_t suffix) -> std::string {
    return std::string(32, 'h') + std::to_string(suffix);
}

auto fixed_heap_key(std::uint64_t suffix) -> std::string {
    std::string key(64, 'h');
    for (std::size_t index = 0; index < sizeof(suffix); ++index) {
        key[key.size() - 1U - index] = static_cast<char>(suffix & 0xFFU);
        suffix >>= 8U;
    }
    return key;
}
} // namespace

GLYPHA_TEST("hot record capacity grows geometrically instead of rehashing every insert") {
    const auto initial = glyphastore::detail::plan_hot_record_reserve(0, 1, 0);
    GLYPHA_REQUIRE(!initial.overflow);
    GLYPHA_REQUIRE(initial.target == 64);

    // Flat table load 0.75: 64 slots hold 48 live entries.
    const auto within_capacity = glyphastore::detail::plan_hot_record_reserve(47, 1, 64);
    GLYPHA_REQUIRE(!within_capacity.overflow);
    GLYPHA_REQUIRE(within_capacity.target == 0);

    const auto next_growth = glyphastore::detail::plan_hot_record_reserve(48, 1, 64);
    GLYPHA_REQUIRE(!next_growth.overflow);
    GLYPHA_REQUIRE(next_growth.target == 128);

    const auto overflow = glyphastore::detail::plan_hot_record_reserve(
        std::numeric_limits<std::size_t>::max(), 1, std::numeric_limits<std::size_t>::max());
    GLYPHA_REQUIRE(overflow.overflow);
}

GLYPHA_TEST("index heap key arena survives erase churn and rehash") {
    glyphastore::Index index;
    constexpr std::uint64_t initial = 2'000;
    for (std::uint64_t value = 0; value < initial; ++value) {
        const auto key = heap_key(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20}, glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    GLYPHA_REQUIRE(index.stats().arena_live_bytes >= initial * 32U);
    for (std::uint64_t value = 0; value < initial / 2; ++value) {
        GLYPHA_REQUIRE(index.erase(heap_key(value)).previous.has_value());
    }
    for (std::uint64_t value = initial; value < initial + (initial / 2); ++value) {
        const auto key = heap_key(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20}, glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }
    for (std::uint64_t value = initial / 2; value < initial + (initial / 2); ++value) {
        GLYPHA_REQUIRE(index.find(heap_key(value)).has_value());
    }
    GLYPHA_REQUIRE(index.stats().size == initial);
    GLYPHA_REQUIRE(index.stats().arena_live_bytes <= index.stats().size * 64U);
    GLYPHA_REQUIRE(index.stats().arena_allocated_bytes <= index.stats().arena_live_bytes + 65'536U);
}

GLYPHA_TEST("index heap arena reclaims memory after insert erase churn") {
    glyphastore::Index index;
    const std::string key(256, 'k');
    const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                     glyphastore::RecordSize{20}, glyphastore::SequenceNumber{1},
                                     glyphastore::GenerationId{1}};
    for (std::uint64_t cycle = 0; cycle < 10'000; ++cycle) {
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
        GLYPHA_REQUIRE(index.erase(key).previous.has_value());
    }
    GLYPHA_REQUIRE(index.stats().size == 0);
    GLYPHA_REQUIRE(index.stats().arena_live_bytes == 0);
    GLYPHA_REQUIRE(index.stats().arena_allocated_bytes <= key.size() + 64U);
}

GLYPHA_TEST("index heap arena waits for geometric fragmentation before reclaim") {
    glyphastore::Index index;
    constexpr std::uint64_t count = 8'192;
    constexpr std::uint64_t first_erase_count = 2'048;
    for (std::uint64_t value = 0; value < count; ++value) {
        const auto key = fixed_heap_key(value);
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20}, glyphastore::SequenceNumber{value},
                                         glyphastore::GenerationId{1}};
        GLYPHA_REQUIRE(index.insert_or_assign(key, ref).has_value());
    }

    for (std::uint64_t value = 0; value < first_erase_count; ++value) {
        GLYPHA_REQUIRE(index.erase(fixed_heap_key(value)).previous.has_value());
    }
    const auto before_geometric_trigger = index.stats();
    GLYPHA_REQUIRE(before_geometric_trigger.arena_allocated_bytes >
                   before_geometric_trigger.arena_live_bytes + 65'536U);

    for (std::uint64_t value = first_erase_count; value < (count / 2U) + 1U; ++value) {
        GLYPHA_REQUIRE(index.erase(fixed_heap_key(value)).previous.has_value());
    }
    const auto after_geometric_trigger = index.stats();
    GLYPHA_REQUIRE(after_geometric_trigger.arena_allocated_bytes <=
                   after_geometric_trigger.arena_live_bytes + 256U);
    for (std::uint64_t value = (count / 2U) + 1U; value < count; ++value) {
        GLYPHA_REQUIRE(index.find(fixed_heap_key(value)).has_value());
    }
}

GLYPHA_TEST("index swiss table probe path matches under mixed inline and heap keys") {
    glyphastore::Index index;
    for (std::uint64_t value = 0; value < 4'096; ++value) {
        const auto key = (value % 3 == 0) ? heap_key(value) : ("inline-" + std::to_string(value));
        const glyphastore::RecordRef ref{glyphastore::SegmentId{1}, glyphastore::RecordOffset{10},
                                         glyphastore::RecordSize{20}, glyphastore::SequenceNumber{value},
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
    const glyphastore::RecordRef ref{glyphastore::SegmentId{9}, glyphastore::RecordOffset{10},
                                     glyphastore::RecordSize{20}, glyphastore::SequenceNumber{1},
                                     glyphastore::GenerationId{1}};
    GLYPHA_REQUIRE(index.insert_or_assign(hashed, ref).has_value());
    GLYPHA_REQUIRE(index.find(hashed) == ref);
}

GLYPHA_TEST("store get verifies checksum after mutable segment corruption") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::legacy_mutex});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("integrity", bytes("payload")).has_value());
    const auto route = glyphastore::route_worker("integrity", store.worker_count());
    const auto ref = glyphastore::detail::StoreAccess::worker(store, route).index().find("integrity");
    GLYPHA_REQUIRE(ref.has_value());
    const auto segments = glyphastore::detail::StoreAccess::segments(store);
    GLYPHA_REQUIRE(!segments.empty());
    auto& segment = *segments.front();
    segment.mutable_base()[ref->offset.value + 32U] ^= std::byte{0xFF};

    const auto corrupted = store.get("integrity");
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("worker local segment catalog resolves records across rotation") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::legacy_mutex});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string value(900U * 1024U, 'v');

    for (std::size_t index = 0; index < 80; ++index) {
        const auto key = "rotation-key-" + std::to_string(index);
        GLYPHA_REQUIRE(store.put(key, bytes(value)).has_value());
    }

    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::worker(store, 0).owned_segments().size() >= 2);
    const auto first = store.get("rotation-key-0");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(first->bytes.size() == value.size());
    GLYPHA_REQUIRE(first->bytes.front() == std::byte{'v'});
}

GLYPHA_TEST("volatile overwrite churn releases fully dead segment storage") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::legacy_mutex});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string value(900U * 1024U, 'r');
    auto initial_segments = glyphastore::detail::StoreAccess::segments(store);
    GLYPHA_REQUIRE(initial_segments.size() == 1);
    const std::weak_ptr<glyphastore::Segment> initial_lifetime = initial_segments.front();
    initial_segments.clear();

    for (std::size_t iteration = 0; iteration < 160; ++iteration) {
        GLYPHA_REQUIRE(store.put("stable-live-key", bytes(value)).has_value());
    }

    const auto& worker = glyphastore::detail::StoreAccess::worker(store, 0);
    GLYPHA_REQUIRE(worker.index().stats().size == 1);
    GLYPHA_REQUIRE(worker.owned_segments().size() == 1);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::segments(store).size() == 1);
    GLYPHA_REQUIRE(initial_lifetime.expired());
    GLYPHA_REQUIRE(store.verify_index().has_value());

    const auto visible = store.get("stable-live-key");
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(visible->bytes.size() == value.size());
}

GLYPHA_TEST("volatile vacuum consolidates sparse sealed segments and preserves visibility") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::legacy_mutex});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string churn_value(900U * 1024U, 'c');

    for (std::size_t generation = 0; generation < 4; ++generation) {
        const auto stable_key = "vacuum-stable-" + std::to_string(generation);
        const auto stable_value = "value-" + std::to_string(generation);
        GLYPHA_REQUIRE(store.put(stable_key, bytes(stable_value)).has_value());
        const auto target_segments = generation + 2;
        std::size_t attempts{};
        while (glyphastore::detail::StoreAccess::worker(store, 0).owned_segments().size() < target_segments) {
            GLYPHA_REQUIRE(store.put("vacuum-churn", bytes(churn_value)).has_value());
            GLYPHA_REQUIRE(++attempts < 100);
        }
    }

    auto before = glyphastore::detail::StoreAccess::segments(store);
    GLYPHA_REQUIRE(before.size() == 5);
    auto retained_snapshot = before.front();
    const std::weak_ptr<glyphastore::Segment> retired_lifetime = retained_snapshot;
    before.clear();

    const auto compacted = store.compact();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->compacted);
    GLYPHA_REQUIRE(compacted->worker_index == 0);
    GLYPHA_REQUIRE(compacted->source_records_verified == 4);
    GLYPHA_REQUIRE(compacted->records_copied == 4);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::segments(store).size() == 2);
    GLYPHA_REQUIRE(!retired_lifetime.expired());
    GLYPHA_REQUIRE(retained_snapshot->state() == glyphastore::SegmentState::retired);
    retained_snapshot.reset();
    GLYPHA_REQUIRE(retired_lifetime.expired());
    GLYPHA_REQUIRE(store.verify_index().has_value());

    for (std::size_t generation = 0; generation < 4; ++generation) {
        const auto stable_key = "vacuum-stable-" + std::to_string(generation);
        const auto stable_value = "value-" + std::to_string(generation);
        const auto visible = store.get(stable_key);
        GLYPHA_REQUIRE(visible.has_value());
        GLYPHA_REQUIRE(std::string(reinterpret_cast<const char*>(visible->bytes.data()),
                                   visible->bytes.size()) == stable_value);
    }
    const auto churn = store.get("vacuum-churn");
    GLYPHA_REQUIRE(churn.has_value());
    GLYPHA_REQUIRE(churn->bytes.size() == churn_value.size());

    const auto stable_catalog = glyphastore::detail::StoreAccess::segments(store);
    const auto no_gain = store.compact();
    GLYPHA_REQUIRE(no_gain.has_value());
    GLYPHA_REQUIRE(!no_gain->compacted);
    const auto unchanged_catalog = glyphastore::detail::StoreAccess::segments(store);
    GLYPHA_REQUIRE(unchanged_catalog.size() == stable_catalog.size());
    for (std::size_t index = 0; index < stable_catalog.size(); ++index) {
        GLYPHA_REQUIRE(unchanged_catalog[index] == stable_catalog[index]);
    }
}
