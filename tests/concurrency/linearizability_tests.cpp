#include "concurrency/linearizability.hpp"
#include "glyphastore/core/error.hpp"
#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto value_string(const glyphastore::OwnedValue& value) -> std::string {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

using glyphastore::test::lin::HistoryRecorder;
using glyphastore::test::lin::OpKind;
using glyphastore::test::lin::Operation;
using glyphastore::test::lin::OutcomeKind;
using glyphastore::test::lin::check_and_minimize;
using glyphastore::test::lin::check_history;

auto map_get_outcome(const glyphastore::Result<glyphastore::OwnedValue>& result)
    -> std::pair<OutcomeKind, std::string> {
    if (result.has_value()) return {OutcomeKind::ok_value, value_string(*result)};
    if (result.error().code == glyphastore::ErrorCode::not_found) return {OutcomeKind::not_found, {}};
    if (result.error().code == glyphastore::ErrorCode::unavailable) return {OutcomeKind::unavailable, {}};
    return {OutcomeKind::other_error, {}};
}

auto map_status(const glyphastore::Status& status) -> OutcomeKind {
    if (status.has_value()) return OutcomeKind::ok_void;
    if (status.error().code == glyphastore::ErrorCode::unavailable) return OutcomeKind::unavailable;
    return OutcomeKind::other_error;
}

void run_seeded_history(const std::uint64_t seed, const std::size_t thread_count,
                        const std::size_t ops_per_thread) {
    glyphastore::fault::configure(seed ^ 0x9E3779B97F4A7C15ULL, 35, 40);

    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    HistoryRecorder recorder;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (std::size_t t = 0; t < thread_count; ++t) {
        threads.emplace_back([&, t, seed] {
            std::mt19937_64 rng{seed + 0xA5A5A5A5ULL * (t + 1)};
            for (std::size_t i = 0; i < ops_per_thread; ++i) {
                const auto key = "k" + std::to_string(rng() % 4U);
                const auto roll = rng() % 100U;
                if (roll < 40U) {
                    const auto value = "v" + std::to_string(rng() % 12U);
                    const auto id = recorder.begin(OpKind::put, key, value);
                    recorder.complete(id, map_status(store.put(key, bytes(value))));
                } else if (roll < 65U) {
                    const auto id = recorder.begin(OpKind::get, key);
                    const auto [outcome, result] = map_get_outcome(store.get(key));
                    recorder.complete(id, outcome, result);
                } else if (roll < 80U) {
                    const auto id = recorder.begin(OpKind::erase, key);
                    recorder.complete(id, map_status(store.erase(key)));
                } else if (roll < 88U) {
                    const auto value = "t" + std::to_string(rng() % 8U);
                    const auto id = recorder.begin(OpKind::put_ttl, key, value, 0, 0);
                    recorder.complete(id, map_status(store.put(key, bytes(value), 0)));
                } else if (roll < 93U) {
                    const auto raw_key = std::string{"\x01"} + key;
                    const auto value = "r" + std::to_string(rng() % 6U);
                    const auto id = recorder.begin(OpKind::raw_put, raw_key, value);
                    recorder.complete(id, map_status(store.put(bytes(raw_key), bytes(value))));
                } else if (roll < 96U) {
                    const auto raw_key = std::string{"\x01"} + key;
                    const auto id = recorder.begin(OpKind::raw_get, raw_key);
                    const auto [outcome, result] = map_get_outcome(store.get(bytes(raw_key)));
                    recorder.complete(id, outcome, result);
                } else if (roll < 98U) {
                    const auto raw_key = std::string{"\x01"} + key;
                    const auto id = recorder.begin(OpKind::raw_erase, raw_key);
                    recorder.complete(id, map_status(store.erase(bytes(raw_key))));
                } else {
                    const auto id = recorder.begin(OpKind::compact);
                    const auto compacted = store.compact();
                    if (compacted.has_value()) recorder.complete(id, OutcomeKind::ok_void);
                    else if (compacted.error().code == glyphastore::ErrorCode::unavailable)
                        recorder.complete(id, OutcomeKind::unavailable);
                    else recorder.complete(id, OutcomeKind::other_error);
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    const auto close_id = recorder.begin(OpKind::close);
    recorder.complete(close_id, map_status(store.close()));

    const auto result = check_and_minimize(recorder.snapshot());
    if (!result.linearizable) {
        throw std::runtime_error("seed=" + std::to_string(seed) + " " + result.message);
    }
}

auto make_op(const std::size_t id, const OpKind kind, std::string key, std::string value,
             const OutcomeKind outcome, const std::uint64_t call_tick, const std::uint64_t return_tick,
             std::string result_value = {}, const std::uint64_t expire_at_ns = 0,
             const std::uint64_t now_ns = 0) -> Operation {
    Operation op;
    op.id = id; op.kind = kind; op.key = std::move(key); op.value = std::move(value);
    op.expire_at_ns = expire_at_ns; op.now_ns = now_ns; op.outcome = outcome;
    op.result_value = std::move(result_value); op.call_tick = call_tick; op.return_tick = return_tick;
    op.completed = true;
    return op;
}

} // namespace

GLYPHA_TEST("linearizability checker accepts a sequential put-get history") {
    const std::vector<Operation> history{
        make_op(0, OpKind::put, "a", "1", OutcomeKind::ok_void, 0, 1),
        make_op(1, OpKind::get, "a", {}, OutcomeKind::ok_value, 2, 3, "1"),
    };
    GLYPHA_REQUIRE(check_history(history).linearizable);
}

GLYPHA_TEST("linearizability checker rejects a non-linearizable read") {
    const std::vector<Operation> history{
        make_op(0, OpKind::put, "a", "1", OutcomeKind::ok_void, 0, 3),
        make_op(1, OpKind::get, "a", {}, OutcomeKind::ok_value, 1, 2, "2"),
    };
    const auto result = check_and_minimize(history);
    GLYPHA_REQUIRE(!result.linearizable);
    GLYPHA_REQUIRE(result.minimal_trace.size() <= 2);
}

GLYPHA_TEST("linearizability checker models TTL expiry on get") {
    const std::vector<Operation> history{
        make_op(0, OpKind::put_ttl, "a", "1", OutcomeKind::ok_void, 0, 1, {}, 100, 50),
        make_op(1, OpKind::get, "a", {}, OutcomeKind::not_found, 2, 3, {}, 0, 100),
    };
    GLYPHA_REQUIRE(check_history(history).linearizable);
}

GLYPHA_TEST("paired Store concurrent histories stay linearizable (seeded)") {
    // GS-CONCUR-LIN-001
    constexpr std::uint64_t seeds[] = {1, 2, 7, 42, 99};
    for (const auto seed : seeds) run_seeded_history(seed, 3, 12);
}

GLYPHA_TEST("paired Store TTL put-get stays linearizable under fixed clock") {
    class FixedClock final : public glyphastore::StoreClock {
      public:
        explicit FixedClock(const std::uint64_t now) : now_(now) {}
        [[nodiscard]] auto now_ns() const noexcept -> std::uint64_t override { return now_; }
      private:
        std::uint64_t now_;
    };
    const auto clock = std::make_shared<FixedClock>(50);
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}, .clock = clock});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    HistoryRecorder recorder;
    const auto put_id = recorder.begin(OpKind::put_ttl, "ttl", "live", 100, 50);
    recorder.complete(put_id, map_status(store.put("ttl", bytes("live"), 100)));
    const auto get_id = recorder.begin(OpKind::get, "ttl", {}, 0, 50);
    const auto [outcome, value] = map_get_outcome(store.get("ttl"));
    recorder.complete(get_id, outcome, value);
    GLYPHA_REQUIRE(outcome == OutcomeKind::ok_value);
    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(check_history(recorder.snapshot()).linearizable);
}

GLYPHA_TEST("paired Store close_drain_deadline_ms is honored when idle") {
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .close_drain_deadline_ms = 5'000,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("d", bytes("1")).has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store close linearizes against late mutations") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("x", bytes("1")).has_value());
    HistoryRecorder recorder;
    std::atomic_bool start{false};
    std::thread closer([&] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        const auto id = recorder.begin(OpKind::close);
        recorder.complete(id, map_status(store.close()));
    });
    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (int i = 0; i < 32; ++i) {
            const auto id = recorder.begin(OpKind::put, "x", std::to_string(i));
            recorder.complete(id, map_status(store.put("x", bytes(std::to_string(i)))));
        }
    });
    start.store(true, std::memory_order_release);
    closer.join();
    writer.join();
    GLYPHA_REQUIRE(check_and_minimize(recorder.snapshot()).linearizable);
}
