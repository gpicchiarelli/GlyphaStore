#include "glyphastore/server/abuse_limits.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

GLYPHA_TEST("abuse controller admits within accept and request windows") {
    using glyphastore::server::AbuseController;
    using glyphastore::server::AbuseLimits;

    AbuseController controller{AbuseLimits{
        .max_accepts_per_sec = 2,
        .connection_max_requests_per_sec = 2,
        .principal_max_requests_per_sec = 2,
        .principal_max_bytes_per_sec = 32,
    }};
    const auto now = std::chrono::steady_clock::now();
    GLYPHA_REQUIRE(controller.try_admit_accept(now));
    GLYPHA_REQUIRE(controller.try_admit_accept(now));
    GLYPHA_REQUIRE(!controller.try_admit_accept(now));
    GLYPHA_REQUIRE(controller.stats().accepts_rejected == 1);

    std::uint64_t window_start = 0;
    std::uint32_t used = 0;
    GLYPHA_REQUIRE(controller.try_admit_connection_request(window_start, used, now));
    GLYPHA_REQUIRE(controller.try_admit_connection_request(window_start, used, now));
    GLYPHA_REQUIRE(!controller.try_admit_connection_request(window_start, used, now));
    GLYPHA_REQUIRE(controller.stats().connection_rate_rejected == 1);

    GLYPHA_REQUIRE(controller.try_admit_principal("alice", 8, now));
    GLYPHA_REQUIRE(controller.try_admit_principal("alice", 8, now));
    GLYPHA_REQUIRE(!controller.try_admit_principal("alice", 8, now));
    GLYPHA_REQUIRE(controller.stats().principal_request_rejected == 1);

    AbuseController bandwidth{AbuseLimits{.principal_max_bytes_per_sec = 16}};
    GLYPHA_REQUIRE(bandwidth.try_admit_principal("bob", 16, now));
    GLYPHA_REQUIRE(!bandwidth.try_admit_principal("bob", 1, now));
    GLYPHA_REQUIRE(bandwidth.stats().principal_bandwidth_rejected == 1);
    // Anonymous / empty principal skips identity quotas.
    GLYPHA_REQUIRE(bandwidth.try_admit_principal("", 1'000'000, now));
}

GLYPHA_TEST("abuse controller windows reset after one second") {
    using glyphastore::server::AbuseController;
    using glyphastore::server::AbuseLimits;

    AbuseController controller{AbuseLimits{.max_accepts_per_sec = 1}};
    auto now = std::chrono::steady_clock::now();
    GLYPHA_REQUIRE(controller.try_admit_accept(now));
    GLYPHA_REQUIRE(!controller.try_admit_accept(now));
    now += std::chrono::seconds{1};
    GLYPHA_REQUIRE(controller.try_admit_accept(now));
}

GLYPHA_TEST("secure profile abuse defaults are non-zero") {
    const auto defaults = glyphastore::server::secure_profile_abuse_defaults();
    GLYPHA_REQUIRE(defaults.any_enabled());
    GLYPHA_REQUIRE(defaults.max_accepts_per_sec > 0);
    GLYPHA_REQUIRE(defaults.idle_timeout_ms > 0);
    GLYPHA_REQUIRE(defaults.request_timeout_ms > 0);
    GLYPHA_REQUIRE(defaults.connection_max_requests_per_sec > 0);
    GLYPHA_REQUIRE(defaults.principal_max_requests_per_sec > 0);
    GLYPHA_REQUIRE(defaults.principal_max_bytes_per_sec > 0);
}

GLYPHA_TEST("abuse controller principal quotas are process-wide under concurrency") {
    using glyphastore::server::AbuseController;
    using glyphastore::server::AbuseLimits;

    AbuseController controller{AbuseLimits{
        .principal_max_requests_per_sec = 32,
        .principal_max_bytes_per_sec = 256,
    }};
    const auto now = std::chrono::steady_clock::now();
    constexpr int kThreads = 8;
    constexpr int kAttempts = 64;
    std::atomic<std::uint64_t> admitted{0};
    std::atomic<std::uint64_t> rejected{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int attempt = 0; attempt < kAttempts; ++attempt) {
                if (controller.try_admit_principal("shared", 8, now)) {
                    admitted.fetch_add(1U, std::memory_order_relaxed);
                } else {
                    rejected.fetch_add(1U, std::memory_order_relaxed);
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    const auto admitted_count = admitted.load(std::memory_order_relaxed);
    const auto rejected_count = rejected.load(std::memory_order_relaxed);
    GLYPHA_REQUIRE(admitted_count <= 32);
    GLYPHA_REQUIRE(admitted_count > 0);
    GLYPHA_REQUIRE(rejected_count > 0);
    const auto stats = controller.stats();
    GLYPHA_REQUIRE(stats.principal_request_rejected + stats.principal_bandwidth_rejected == rejected_count);
}
