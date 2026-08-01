#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto value_string(const glyphastore::OwnedValue& value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

struct ObservedState {
    std::vector<std::pair<std::string, std::string>> present{};
    std::vector<std::string> missing{};
};

auto observe(glyphastore::Store& store, const std::vector<std::string>& keys) -> ObservedState {
    ObservedState out;
    for (const auto& key : keys) {
        const auto record = store.get(key);
        if (record.has_value()) {
            out.present.emplace_back(key, std::string{value_string(*record)});
        } else {
            out.missing.push_back(key);
        }
    }
    return out;
}

auto run_sequential_script(glyphastore::StoreConcurrencyMode mode) -> ObservedState {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}, .concurrency = mode});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("a", bytes("1")).has_value());
    GLYPHA_REQUIRE(store.put("a", bytes("2")).has_value());
    GLYPHA_REQUIRE(store.put("b", bytes("b-value")).has_value());
    GLYPHA_REQUIRE(store.erase("b").has_value());
    GLYPHA_REQUIRE(store.put("c", bytes("ttl"), /*expire_at_ns=*/1).has_value());
    // expire_at_ns=1 is in the past → must be absent on subsequent get.
    return observe(store, {"a", "b", "c", "missing"});
}

auto run_concurrent_distinct(glyphastore::StoreConcurrencyMode mode) -> ObservedState {
    constexpr std::size_t thread_total = 4;
    constexpr std::size_t keys_per_thread = 40;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 4}, .concurrency = mode});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(thread_total);
    for (std::size_t thread_id = 0; thread_id < thread_total; ++thread_id) {
        threads.emplace_back([&, thread_id]() {
            for (std::size_t offset = 0; offset < keys_per_thread; ++offset) {
                const auto key = "eq-" + std::to_string(thread_id) + "-" + std::to_string(offset);
                const auto value = "v-" + std::to_string(offset);
                if (!store.put(key, bytes(value)).has_value()) {
                    failed.store(true);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    GLYPHA_REQUIRE(!failed.load());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    std::vector<std::string> keys;
    keys.reserve(thread_total * keys_per_thread);
    for (std::size_t thread_id = 0; thread_id < thread_total; ++thread_id) {
        for (std::size_t offset = 0; offset < keys_per_thread; ++offset) {
            keys.push_back("eq-" + std::to_string(thread_id) + "-" + std::to_string(offset));
        }
    }
    return observe(store, keys);
}

auto run_close_drain(glyphastore::StoreConcurrencyMode mode) -> ObservedState {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}, .concurrency = mode});
    GLYPHA_REQUIRE(opened.has_value());
    auto store = std::move(*opened);
    GLYPHA_REQUIRE(store->put("close-key", bytes("close-value")).has_value());
    const auto before = observe(*store, {"close-key"});
    store.reset();
    return before;
}

} // namespace

GLYPHA_TEST("paired and legacy_mutex agree on sequential volatile outcomes") {
    const auto paired = run_sequential_script(glyphastore::StoreConcurrencyMode::paired);
    const auto legacy = run_sequential_script(glyphastore::StoreConcurrencyMode::legacy_mutex);
    GLYPHA_REQUIRE(paired.present == legacy.present);
    GLYPHA_REQUIRE(paired.missing == legacy.missing);
    GLYPHA_REQUIRE(paired.present.size() == 1);
    GLYPHA_REQUIRE(paired.present[0].first == "a");
    GLYPHA_REQUIRE(paired.present[0].second == "2");
    GLYPHA_REQUIRE(paired.missing.size() == 3);
}

GLYPHA_TEST("paired and legacy_mutex agree on concurrent distinct-key finals") {
    const auto paired = run_concurrent_distinct(glyphastore::StoreConcurrencyMode::paired);
    const auto legacy = run_concurrent_distinct(glyphastore::StoreConcurrencyMode::legacy_mutex);
    GLYPHA_REQUIRE(paired.present == legacy.present);
    GLYPHA_REQUIRE(paired.missing.empty());
    GLYPHA_REQUIRE(legacy.missing.empty());
    GLYPHA_REQUIRE(paired.present.size() == 160);
}

GLYPHA_TEST("paired and legacy_mutex agree on close-drain acknowledged values") {
    const auto paired = run_close_drain(glyphastore::StoreConcurrencyMode::paired);
    const auto legacy = run_close_drain(glyphastore::StoreConcurrencyMode::legacy_mutex);
    GLYPHA_REQUIRE(paired.present == legacy.present);
    GLYPHA_REQUIRE(paired.present.size() == 1);
    GLYPHA_REQUIRE(paired.present[0].second == "close-value");
}
