#include "glyphastore/server/mutation_window.hpp"
#include "test.hpp"

using glyphastore::server::MutationVisibilityBarrier;
using glyphastore::server::kMaximumMutationWindow;

GLYPHA_TEST("mutation window visibility barrier enforces RAW epoch") {
    MutationVisibilityBarrier barrier{};
    GLYPHA_REQUIRE(!barrier.armed());
    GLYPHA_REQUIRE(barrier.allows(0));
    barrier.raise_to(7);
    GLYPHA_REQUIRE(barrier.armed());
    GLYPHA_REQUIRE(!barrier.allows(6));
    GLYPHA_REQUIRE(barrier.allows(7));
    GLYPHA_REQUIRE(barrier.allows(9));
    barrier.clear();
    GLYPHA_REQUIRE(!barrier.armed());
    GLYPHA_REQUIRE(kMaximumMutationWindow == 32);
}
