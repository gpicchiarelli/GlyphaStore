#include "glyphastore/server/connection_lifecycle.hpp"
#include "test.hpp"

using glyphastore::server::ConnectionAction;
using glyphastore::server::ConnectionDrainSnapshot;
using glyphastore::server::ConnectionLifecycle;
using glyphastore::server::DecidedOutput;
using glyphastore::server::InputLifecycle;
using glyphastore::server::connection_lifecycle_of;
using glyphastore::server::decide_connection_action;
using glyphastore::server::input_lifecycle_of;

GLYPHA_TEST("connection_lifecycle decide close_now only when drained") {
    ConnectionDrainSnapshot open{};
    GLYPHA_REQUIRE(decide_connection_action(open) == ConnectionAction::none);
    GLYPHA_REQUIRE(connection_lifecycle_of(open) == ConnectionLifecycle::open);

    ConnectionDrainSnapshot half{.peer_read_closed = true, .residual_input = true};
    GLYPHA_REQUIRE(decide_connection_action(half) == ConnectionAction::drain_then_close);
    GLYPHA_REQUIRE(connection_lifecycle_of(half) == ConnectionLifecycle::draining_decided_output);

    ConnectionDrainSnapshot half_drained{.peer_read_closed = true};
    GLYPHA_REQUIRE(decide_connection_action(half_drained) == ConnectionAction::close_now);
    GLYPHA_REQUIRE(connection_lifecycle_of(half_drained) == ConnectionLifecycle::peer_half_closed);

    ConnectionDrainSnapshot soft{.close_after_flush = true, .has_pending_output = true};
    GLYPHA_REQUIRE(decide_connection_action(soft) == ConnectionAction::refuse_new_frames);
    GLYPHA_REQUIRE(input_lifecycle_of(soft, false) == InputLifecycle::stopped);

    ConnectionDrainSnapshot soft_drained{.close_after_flush = true};
    GLYPHA_REQUIRE(decide_connection_action(soft_drained) == ConnectionAction::close_now);

    DecidedOutput decided{.contiguous_bytes = 4, .lease_header_remaining = 1};
    GLYPHA_REQUIRE(!decided.empty());
    GLYPHA_REQUIRE(decided.total_bytes() == 5);
}
