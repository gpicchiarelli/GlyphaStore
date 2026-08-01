#include "glyphastore/server/abuse_limits.hpp"
#include "test.hpp"

#include <chrono>

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
