#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("paired Store read-after-write and close drain") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("alpha", bytes("one")).has_value());
    const auto first = store.get("alpha");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(first->bytes.data()), first->bytes.size()) == "one");
    GLYPHA_REQUIRE(store.put("alpha", bytes("two")).has_value());
    const auto second = store.get("alpha");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(second->bytes.data()), second->bytes.size()) == "two");
    GLYPHA_REQUIRE(store.erase("alpha").has_value());
    GLYPHA_REQUIRE(!store.get("alpha").has_value());
    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(!store.put("alpha", bytes("late")).has_value());
}

GLYPHA_TEST("paired Store concurrent GET and PUT on one key stay linearized") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("shared", bytes("0")).has_value());

    std::atomic_bool failed{false};
    std::atomic_uint64_t writes{0};
    std::thread writer([&] {
        for (std::uint64_t value = 1; value <= 200; ++value) {
            if (!store.put("shared", bytes(std::to_string(value))).has_value()) {
                failed.store(true);
                return;
            }
            writes.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::thread reader([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (std::chrono::steady_clock::now() < deadline) {
            auto value = store.get("shared");
            if (!value.has_value()) {
                failed.store(true);
                return;
            }
            std::this_thread::yield();
        }
    });
    writer.join();
    reader.join();
    GLYPHA_REQUIRE(!failed.load());
    GLYPHA_REQUIRE(writes.load() == 200);
    const auto final_value = store.get("shared");
    GLYPHA_REQUIRE(final_value.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(final_value->bytes.data()),
                                    final_value->bytes.size()) == "200");
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store concurrent read-after-write keeps adopted generations alive") {
    constexpr std::size_t kThreadCount = 4;
    constexpr std::size_t kWritesPerThread = 1'024;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 4}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::atomic_bool failed{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (std::size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&, thread] {
            for (std::size_t write = 0; write < kWritesPerThread; ++write) {
                const auto key = "lease-" + std::to_string(thread) + '-' + std::to_string(write);
                const auto value = "value-" + std::to_string(write);
                if (!store.put(key, bytes(value)).has_value() || !store.get(key).has_value()) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store concurrent GET observes live generation under overwrite storm") {
    // ADR 0036 V2/V3 baseline under production shared_ptr + ReadLease (not slot-pool).
    // Slot-pool landing must keep this class of race green (see ADR 0036 verification matrix).
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key = "overwrite-storm";
    GLYPHA_REQUIRE(store.put(key, bytes("seed")).has_value());

    std::atomic_bool stop{false};
    std::atomic_bool failed{false};
    std::thread reader{[&] {
        while (!stop.load(std::memory_order_acquire)) {
            const auto got = store.get(key);
            if (!got.has_value() || got->bytes.empty()) {
                failed.store(true, std::memory_order_relaxed);
                return;
            }
        }
    }};
    for (std::size_t write = 0; write < 8'192; ++write) {
        const auto value = "v-" + std::to_string(write);
        if (!store.put(key, bytes(value)).has_value()) {
            failed.store(true, std::memory_order_relaxed);
            break;
        }
    }
    stop.store(true, std::memory_order_release);
    reader.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store put_batch publishes once per shard and keeps RAW") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(64);
    values.reserve(64);
    std::vector<glyphastore::Store::PutItem> items;
    items.reserve(64);
    for (int index = 0; index < 64; ++index) {
        keys.push_back("batch-key-" + std::to_string(index));
        values.push_back("batch-value-" + std::to_string(index));
        items.push_back(glyphastore::Store::PutItem{
            .key = keys.back(),
            .value = bytes(values.back()),
        });
    }

    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == items.size());
    for (const auto& status : statuses) {
        GLYPHA_REQUIRE(status.has_value());
    }
    for (std::size_t index = 0; index < items.size(); ++index) {
        const auto got = store.get(keys[index]);
        GLYPHA_REQUIRE(got.has_value());
        GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                       values[index]);
    }
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store put_batch preserves same-key FIFO within one batch") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key = "same-key";
    const std::string first = "first";
    const std::string second = "second";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes(first)},
        {.key = key, .value = bytes(second)},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 2);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(statuses[1].has_value());
    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   second);
    GLYPHA_REQUIRE(store.close().has_value());
}
