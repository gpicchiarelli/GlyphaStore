#include "persistence/adaptive_batch_sizer.hpp"
#include "test.hpp"

GLYPHA_TEST("adaptive batch target contracts to deadline occupancy") {
    glyphastore::detail::AdaptiveBatchSizer sizer;
    sizer.reset(1, 32);
    GLYPHA_REQUIRE(sizer.target() == 32);

    sizer.observe_deadline(4);
    GLYPHA_REQUIRE(sizer.target() == 4);
    sizer.observe_target_reached(4, 4);
    GLYPHA_REQUIRE(sizer.target() == 4);
}

GLYPHA_TEST("adaptive batch target grows to admitted burst within bounds") {
    glyphastore::detail::AdaptiveBatchSizer sizer;
    sizer.reset(4, 32);
    sizer.observe_deadline(1);
    GLYPHA_REQUIRE(sizer.target() == 4);

    sizer.observe_target_reached(4, 16);
    GLYPHA_REQUIRE(sizer.target() == 16);
    sizer.observe_target_reached(16, 64);
    GLYPHA_REQUIRE(sizer.target() == 32);
}
