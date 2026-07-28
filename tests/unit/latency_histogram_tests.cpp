#include "glyphastore/server/latency_histogram.hpp"
#include "test.hpp"

#include <string>

GLYPHA_TEST("latency histogram buckets and approximate percentiles") {
    glyphastore::server::LatencyHistogram histogram{};
    histogram.observe(500);          // <= 1 us
    histogram.observe(5'000);        // <= 10 us
    histogram.observe(50'000);       // <= 100 us
    histogram.observe(5'000'000);    // <= 10 ms
    histogram.observe(50'000'000);   // <= 100 ms

    GLYPHA_REQUIRE(histogram.observations == 5);
    GLYPHA_REQUIRE(histogram.sum_ns == 55'055'500);
    const auto cumulative = histogram.cumulative();
    GLYPHA_REQUIRE(cumulative[0] == 1);
    GLYPHA_REQUIRE(cumulative[1] == 2);
    GLYPHA_REQUIRE(cumulative[2] == 3);
    GLYPHA_REQUIRE(cumulative[4] == 4);
    GLYPHA_REQUIRE(cumulative[5] == 5);
    GLYPHA_REQUIRE(cumulative.back() == 5);

    const auto p50 = histogram.approximate_percentile_ns(0.50);
    const auto p99 = histogram.approximate_percentile_ns(0.99);
    GLYPHA_REQUIRE(p50 >= 10'000);
    GLYPHA_REQUIRE(p50 <= 100'000);
    GLYPHA_REQUIRE(p99 >= p50);
    GLYPHA_REQUIRE(p99 <= 100'000'000);

    std::string exported;
    glyphastore::server::append_latency_histogram(exported, "lane[0].service_ns", histogram);
    GLYPHA_REQUIRE(exported.find("lane[0].service_ns.count=5\n") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("lane[0].service_ns.le_1000=1\n") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("lane[0].service_ns.le_inf=5\n") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("lane[0].service_ns.p50=") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("lane[0].service_ns.p99=") != std::string::npos);
}

GLYPHA_TEST("empty latency histogram exports zeros") {
    glyphastore::server::LatencyHistogram histogram{};
    GLYPHA_REQUIRE(histogram.approximate_percentile_ns(0.50) == 0);
    GLYPHA_REQUIRE(histogram.approximate_percentile_ns(0.99) == 0);
    std::string exported;
    glyphastore::server::append_latency_histogram(exported, "empty", histogram);
    GLYPHA_REQUIRE(exported.find("empty.count=0\n") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("empty.p50=0\n") != std::string::npos);
    GLYPHA_REQUIRE(exported.find("empty.p99=0\n") != std::string::npos);
}
