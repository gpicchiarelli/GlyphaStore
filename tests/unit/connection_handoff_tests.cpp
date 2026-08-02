#include "glyphastore/server/connection_handoff.hpp"
#include "test.hpp"

#include <utility>

GLYPHA_TEST("connection handoff mesh is bounded and preserves binding state") {
    glyphastore::server::ConnectionHandoffMesh mesh{2, 2};
    glyphastore::server::ConnectionHandoff first{.bound_worker = 1};
    glyphastore::server::ConnectionHandoff second{.bound_worker = 1};
    glyphastore::server::ConnectionHandoff rejected{.bound_worker = 1, .initialized = true};

    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(first)));
    GLYPHA_REQUIRE(mesh.has_pending(1));
    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(second)));
    GLYPHA_REQUIRE(!mesh.try_handoff(1, std::move(rejected)));
    // Full-queue rejection must not consume the handoff: the producer still owns it.
    GLYPHA_REQUIRE(rejected.bound_worker == 1);
    GLYPHA_REQUIRE(rejected.initialized);

    const auto first_received = mesh.try_pop(1);
    const auto second_received = mesh.try_pop(1);
    GLYPHA_REQUIRE(first_received.has_value());
    GLYPHA_REQUIRE(second_received.has_value());
    GLYPHA_REQUIRE(first_received->bound_worker == 1);
    GLYPHA_REQUIRE(second_received->bound_worker == 1);
    GLYPHA_REQUIRE(!mesh.try_pop(1).has_value());
    GLYPHA_REQUIRE(!mesh.has_pending(1));
}

GLYPHA_TEST("connection handoff mesh stop_accepting refuses new transfers") {
    glyphastore::server::ConnectionHandoffMesh mesh{2, 2};
    glyphastore::server::ConnectionHandoff queued{.bound_worker = 1, .initialized = true};
    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(queued)));
    GLYPHA_REQUIRE(mesh.has_pending(1));

    mesh.stop_accepting();
    GLYPHA_REQUIRE(!mesh.accepting());
    glyphastore::server::ConnectionHandoff late{.bound_worker = 1, .initialized = true};
    GLYPHA_REQUIRE(!mesh.try_handoff(1, std::move(late)));
    GLYPHA_REQUIRE(late.bound_worker == 1);
    GLYPHA_REQUIRE(late.initialized);

    // Already-queued cells remain poppable for drain / reject_orphaned.
    const auto received = mesh.try_pop(1);
    GLYPHA_REQUIRE(received.has_value());
    GLYPHA_REQUIRE(received->bound_worker == 1);
    GLYPHA_REQUIRE(!mesh.has_pending(1));
}
