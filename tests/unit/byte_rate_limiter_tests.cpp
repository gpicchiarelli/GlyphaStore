#include "glyphastore/core/byte_rate_limiter.hpp"
#include "test.hpp"

#include <cstdint>
#include <limits>

GLYPHA_TEST("bounded byte rate limiter derives a ten millisecond capped burst") {
    using glyphastore::BoundedByteRateLimiter;
    GLYPHA_REQUIRE(BoundedByteRateLimiter::recommended_burst_bytes(0) == 0);
    GLYPHA_REQUIRE(BoundedByteRateLimiter::recommended_burst_bytes(1) == 1);
    GLYPHA_REQUIRE(BoundedByteRateLimiter::recommended_burst_bytes(100) == 1);
    GLYPHA_REQUIRE(BoundedByteRateLimiter::recommended_burst_bytes(64U * 1024U * 1024U) == 671'089U);
    GLYPHA_REQUIRE(BoundedByteRateLimiter::recommended_burst_bytes(
                       std::numeric_limits<std::uint64_t>::max()) == 1U * 1024U * 1024U);
}

GLYPHA_TEST("bounded byte rate limiter spaces requests after one immediate burst") {
    glyphastore::BoundedByteRateLimiter limiter{100};
    GLYPHA_REQUIRE(limiter.burst_bytes() == 1);

    const auto first = limiter.request(3, 1'000);
    GLYPHA_REQUIRE(first.granted_bytes == 1);
    GLYPHA_REQUIRE(first.sleep_ns == 0);

    const auto second = limiter.request(2, 1'000);
    GLYPHA_REQUIRE(second.granted_bytes == 1);
    GLYPHA_REQUIRE(second.sleep_ns == 10'000'000U);

    const auto after_idle = limiter.request(1, 30'000'000U);
    GLYPHA_REQUIRE(after_idle.granted_bytes == 1);
    GLYPHA_REQUIRE(after_idle.sleep_ns == 0);
}

GLYPHA_TEST("disabled byte rate limiter grants the complete request without debt") {
    glyphastore::BoundedByteRateLimiter limiter{0};
    const auto first = limiter.request(std::numeric_limits<std::uint64_t>::max(), 10);
    const auto second = limiter.request(17, 10);
    GLYPHA_REQUIRE(first.granted_bytes == std::numeric_limits<std::uint64_t>::max());
    GLYPHA_REQUIRE(first.sleep_ns == 0);
    GLYPHA_REQUIRE(second.granted_bytes == 17);
    GLYPHA_REQUIRE(second.sleep_ns == 0);
}
