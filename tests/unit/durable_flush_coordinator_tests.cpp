#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "persistence/durable_flush_coordinator_internal.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

GLYPHA_TEST("durable flush coordinator executes an exact requested deadline") {
    using clock = std::chrono::steady_clock;
    std::mutex mutex;
    std::condition_variable completed;
    bool fired{};
    clock::time_point fired_at{};
    glyphastore::DurableFlushCoordinator coordinator{60'000, 60'000, false, true,
                                                     [&](const bool force_all) -> glyphastore::Status {
                                                         GLYPHA_REQUIRE(!force_all);
                                                         {
                                                             const std::lock_guard lock{mutex};
                                                             fired = true;
                                                             fired_at = clock::now();
                                                         }
                                                         completed.notify_all();
                                                         return {};
                                                     }};

    const auto started = clock::now();
    coordinator.request_flush_at(started + std::chrono::milliseconds{25});
    {
        std::unique_lock lock{mutex};
        GLYPHA_REQUIRE(completed.wait_for(lock, std::chrono::seconds{2}, [&] { return fired; }));
    }
    coordinator.stop();
    const auto elapsed = fired_at - started;
    GLYPHA_REQUIRE(elapsed >= std::chrono::milliseconds{20});
    GLYPHA_REQUIRE(elapsed < std::chrono::milliseconds{500});
}

GLYPHA_TEST("blocking durable flush runs on the coordinator and propagates failure") {
    const auto caller = std::this_thread::get_id();
    std::thread::id callback_thread;
    glyphastore::DurableFlushCoordinator coordinator{
        60'000, 60'000, false, true, [&](const bool force_all) -> glyphastore::Status {
            GLYPHA_REQUIRE(force_all);
            callback_thread = std::this_thread::get_id();
            return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected flush failure");
        }};

    const auto flushed = coordinator.flush_all_blocking();
    coordinator.stop();
    GLYPHA_REQUIRE(!flushed.has_value());
    GLYPHA_REQUIRE(flushed.error().code == glyphastore::ErrorCode::io_error);
    GLYPHA_REQUIRE(callback_thread != std::thread::id{});
    GLYPHA_REQUIRE(callback_thread != caller);
}

GLYPHA_TEST("durable flush coordinator translates callback exceptions and stops") {
    glyphastore::DurableFlushCoordinator coordinator{
        60'000, 60'000, false, true,
        [](const bool) -> glyphastore::Status { throw std::runtime_error("injected callback exception"); }};

    const auto first = coordinator.flush_all_blocking();
    GLYPHA_REQUIRE(!first.has_value());
    GLYPHA_REQUIRE(first.error().code == glyphastore::ErrorCode::internal_error);
    const auto repeated = coordinator.flush_all_blocking();
    GLYPHA_REQUIRE(!repeated.has_value());
    GLYPHA_REQUIRE(repeated.error().code == glyphastore::ErrorCode::internal_error);
    coordinator.stop();
}

GLYPHA_TEST("durable flush coordinator rejects exhausted flush generations without invoking callback") {
    std::atomic calls{0};
    glyphastore::DurableFlushCoordinator coordinator{60'000, 60'000, false, true,
                                                     [&](const bool) -> glyphastore::Status {
                                                         calls.fetch_add(1, std::memory_order_relaxed);
                                                         return {};
                                                     }};
    glyphastore::detail::DurableFlushCoordinatorAccess::set_flush_all_generation(
        coordinator, std::numeric_limits<std::uint64_t>::max());

    const auto exhausted = coordinator.flush_all_blocking();
    GLYPHA_REQUIRE(!exhausted.has_value());
    GLYPHA_REQUIRE(exhausted.error().code == glyphastore::ErrorCode::arithmetic_overflow);
    GLYPHA_REQUIRE(calls.load(std::memory_order_relaxed) == 0);
    coordinator.stop();
}

GLYPHA_TEST("concurrent coordinator stop releases a blocking flush without deadlock") {
    std::mutex mutex;
    std::condition_variable changed;
    bool callback_entered{};
    bool release_callback{};
    bool flush_completed{};
    glyphastore::Status flush_result;
    glyphastore::DurableFlushCoordinator coordinator{60'000, 60'000, false, true,
                                                     [&](const bool force_all) -> glyphastore::Status {
                                                         GLYPHA_REQUIRE(force_all);
                                                         std::unique_lock lock{mutex};
                                                         callback_entered = true;
                                                         changed.notify_all();
                                                         changed.wait(lock, [&] { return release_callback; });
                                                         return {};
                                                     }};

    std::thread flusher{[&] {
        auto result = coordinator.flush_all_blocking();
        {
            const std::lock_guard lock{mutex};
            flush_result = std::move(result);
            flush_completed = true;
        }
        changed.notify_all();
    }};
    {
        std::unique_lock lock{mutex};
        GLYPHA_REQUIRE(changed.wait_for(lock, std::chrono::seconds{2}, [&] { return callback_entered; }));
    }
    std::thread stopper{[&] { coordinator.stop(); }};
    {
        std::unique_lock lock{mutex};
        GLYPHA_REQUIRE(changed.wait_for(lock, std::chrono::seconds{2}, [&] { return flush_completed; }));
        release_callback = true;
    }
    changed.notify_all();
    flusher.join();
    stopper.join();

    GLYPHA_REQUIRE(!flush_result.has_value());
    GLYPHA_REQUIRE(flush_result.error().code == glyphastore::ErrorCode::unavailable);
}
