#include "glyphastore/store/paired/shard_combining_executor.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

using glyphastore::store::paired::execution_token_executing;
using glyphastore::store::paired::execution_token_idle;
using glyphastore::store::paired::release_execution_token;
using glyphastore::store::paired::try_acquire_execution_token;
using glyphastore::store::paired::try_reacquire_execution_token_if_pending;

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("execution token CAS grants sole ownership") {
    std::atomic<std::uint32_t> token{execution_token_idle()};
    GLYPHA_REQUIRE(try_acquire_execution_token(token));
    GLYPHA_REQUIRE(token.load() == execution_token_executing());
    GLYPHA_REQUIRE(!try_acquire_execution_token(token));
    release_execution_token(token);
    GLYPHA_REQUIRE(token.load() == execution_token_idle());
    GLYPHA_REQUIRE(try_acquire_execution_token(token));
    release_execution_token(token);
}

GLYPHA_TEST("execution token lost-wakeup reacquire when pending") {
    std::atomic<std::uint32_t> token{execution_token_idle()};
    GLYPHA_REQUIRE(try_acquire_execution_token(token));
    release_execution_token(token);
    GLYPHA_REQUIRE(!try_reacquire_execution_token_if_pending(token, false));
    GLYPHA_REQUIRE(try_reacquire_execution_token_if_pending(token, true));
    release_execution_token(token);
}

GLYPHA_TEST("execution token serializes concurrent acquirers") {
    std::atomic<std::uint32_t> token{execution_token_idle()};
    std::atomic_int holders{0};
    std::atomic_int max_holders{0};
    constexpr int kThreads = 8;
    constexpr int kIters = 200;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kIters; ++i) {
                while (!try_acquire_execution_token(token)) {
                }
                const int now = holders.fetch_add(1) + 1;
                int observed = max_holders.load();
                while (now > observed && !max_holders.compare_exchange_weak(observed, now)) {
                }
                holders.fetch_sub(1);
                release_execution_token(token);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    GLYPHA_REQUIRE(max_holders.load() == 1);
}

GLYPHA_TEST("embedded volatile combining omits dedicated Writer threads") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(**opened);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(runtime->combining_enabled());
    GLYPHA_REQUIRE(!runtime->dedicated_writer_required());
    GLYPHA_REQUIRE((**opened).put("k", bytes("v")).has_value());
    const auto got = (**opened).get("k");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "v");
}

GLYPHA_TEST("embedded volatile combining preserves same-key FIFO under contention") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    constexpr int kThreads = 4;
    constexpr int kIters = 50;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    std::atomic_bool failed{false};
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kIters; ++i) {
                const auto value = std::to_string(t * kIters + i);
                if (!store.put("shared", bytes(value)).has_value()) {
                    failed.store(true);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    GLYPHA_REQUIRE(!failed.load());
    const auto got = store.get("shared");
    GLYPHA_REQUIRE(got.has_value());
    const auto text = std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size());
    GLYPHA_REQUIRE(!text.empty());
}

GLYPHA_TEST("embedded durable_sync combining omits dedicated Writer and keeps RAW") {
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-combine-durable-sync-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path store_path{writable.data()};
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(**opened);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(runtime->combining_enabled());
    GLYPHA_REQUIRE(!runtime->dedicated_writer_required());
    GLYPHA_REQUIRE((**opened).put("alpha", bytes("one")).has_value());
    const auto first = (**opened).get("alpha");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(first->bytes.data()), first->bytes.size()) == "one");
    GLYPHA_REQUIRE((**opened).close().has_value());
    std::error_code ec;
    std::filesystem::remove_all(store_path, ec);
}
