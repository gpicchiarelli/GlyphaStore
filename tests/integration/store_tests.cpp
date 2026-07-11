#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto value_string(const glyphastore::RecordView& record) -> std::string_view {
    return {reinterpret_cast<const char*>(record.value.data()), record.value.size()};
}
} // namespace

GLYPHA_TEST("key routing is deterministic and stable across worker counts") {
    GLYPHA_REQUIRE(glyphastore::route_worker("alpha", 4) == glyphastore::route_worker("alpha", 4));
    GLYPHA_REQUIRE(glyphastore::route_worker("alpha", 4) != glyphastore::route_worker("beta", 4) ||
                   glyphastore::hash_key("alpha") % 4 == glyphastore::hash_key("beta") % 4);
}

GLYPHA_TEST("store put get round trip preserves value") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("hello", bytes("world")).has_value());
    const auto record = store.get("hello");
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(record->key_string() == "hello");
    GLYPHA_REQUIRE(value_string(*record) == "world");
}

GLYPHA_TEST("store replace updates visible value and sequence") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("key", bytes("old")).has_value());
    const auto first = store.get("key");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(store.put("key", bytes("new")).has_value());
    const auto second = store.get("key");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(second->sequence.value > first->sequence.value);
    GLYPHA_REQUIRE(value_string(*second) == "new");
}

GLYPHA_TEST("store erase removes key and rejects subsequent reads") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("gone", bytes("v")).has_value());
    GLYPHA_REQUIRE(store.erase("gone").has_value());
    const auto missing = store.get("gone");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("store get hides expired keys") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("expired", bytes("v"), 100).has_value());
    const auto visible = store.get("expired", 99);
    GLYPHA_REQUIRE(visible.has_value());
    const auto hidden = store.get("expired", 100);
    GLYPHA_REQUIRE(!hidden.has_value());
    GLYPHA_REQUIRE(hidden.error().code == glyphastore::ErrorCode::not_found);
    const auto route = glyphastore::route_worker("expired", store.worker_count());
    GLYPHA_REQUIRE(!store.worker(route).index().find("expired").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
}

GLYPHA_TEST("store keeps partitioned keys on routed workers only") {
    constexpr std::size_t worker_total = 8;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = worker_total}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::string key_a;
    std::string key_b;
    std::size_t route_a{};
    std::size_t route_b{};
    for (std::uint64_t seed = 0;; ++seed) {
        key_a = "route-key-" + std::to_string(seed);
        key_b = "route-key-" + std::to_string(seed + 100000U);
        route_a = glyphastore::route_worker(key_a, worker_total);
        route_b = glyphastore::route_worker(key_b, worker_total);
        if (route_a != route_b) {
            break;
        }
    }

    GLYPHA_REQUIRE(store.put(key_a, bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put(key_b, bytes("b")).has_value());
    GLYPHA_REQUIRE(store.worker(route_a).index().find(key_a).has_value());
    GLYPHA_REQUIRE(!store.worker(route_a).index().find(key_b).has_value());
    GLYPHA_REQUIRE(store.worker(route_b).index().find(key_b).has_value());
    GLYPHA_REQUIRE(!store.worker(route_b).index().find(key_a).has_value());
}

GLYPHA_TEST("store routes keys to distinct worker partitions") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 4}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("worker-key-a", bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put("worker-key-b", bytes("b")).has_value());
    const auto route_a = glyphastore::route_worker("worker-key-a", store.worker_count());
    const auto route_b = glyphastore::route_worker("worker-key-b", store.worker_count());
    GLYPHA_REQUIRE(store.get("worker-key-a").has_value());
    GLYPHA_REQUIRE(store.get("worker-key-b").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    GLYPHA_REQUIRE(route_a < store.worker_count());
    GLYPHA_REQUIRE(route_b < store.worker_count());
}

GLYPHA_TEST("store verify index matches segment scan rebuild") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 3}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("one", bytes("1")).has_value());
    GLYPHA_REQUIRE(store.put("two", bytes("2")).has_value());
    GLYPHA_REQUIRE(store.put("three", bytes("3")).has_value());
    GLYPHA_REQUIRE(store.put("two", bytes("22")).has_value());
    GLYPHA_REQUIRE(store.erase("one").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    const auto rebuilt = glyphastore::rebuild_index_from_segments(store.segments());
    GLYPHA_REQUIRE(rebuilt.has_value());
    GLYPHA_REQUIRE(rebuilt->index.find("one") == std::nullopt);
    GLYPHA_REQUIRE(rebuilt->index.find("two").has_value());
    GLYPHA_REQUIRE(rebuilt->index.find("three").has_value());
}

GLYPHA_TEST("store round trips a key larger than 16-bit lengths") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key(70'000, 'k');
    GLYPHA_REQUIRE(store.put(key, bytes("value")).has_value());
    const auto record = store.get(key);
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(record->key_string() == key);
    GLYPHA_REQUIRE(store.verify_index().has_value());
}
