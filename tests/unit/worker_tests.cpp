#include "glyphastore/worker/topology.hpp"
#include "test.hpp"

GLYPHA_TEST("worker sizing respects physical cores reservation and memory") {
    const glyphastore::WorkerTopology topology{
        .logical_cpus = 16,
        .physical_cores = 8,
        .available_cpus = 16,
        .available_memory_bytes = 4 * glyphastore::kSegmentSizeBytes,
    };
    const auto chosen = glyphastore::WorkerCountPolicy::choose(topology, {.reserved_cores = 1});
    GLYPHA_REQUIRE(chosen == 4);
}

GLYPHA_TEST("worker sizing always returns at least one and honors explicit override") {
    const glyphastore::WorkerTopology topology{};
    GLYPHA_REQUIRE(glyphastore::WorkerCountPolicy::choose(topology, {.reserved_cores = 99}) == 1);
    GLYPHA_REQUIRE(
        glyphastore::WorkerCountPolicy::choose(topology, {.explicit_count = 12, .maximum_workers = 8}) == 8);
}
