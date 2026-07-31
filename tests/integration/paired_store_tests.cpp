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
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(first->bytes.data()),
                                    first->bytes.size()) == "one");
    GLYPHA_REQUIRE(store.put("alpha", bytes("two")).has_value());
    const auto second = store.get("alpha");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(second->bytes.data()),
                                    second->bytes.size()) == "two");
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
