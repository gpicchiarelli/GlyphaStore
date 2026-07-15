#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "test.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
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
