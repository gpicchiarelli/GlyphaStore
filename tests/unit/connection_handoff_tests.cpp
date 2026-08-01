#include "glyphastore/server/connection_handoff.hpp"
#include "test.hpp"

#include <utility>

GLYPHA_TEST("connection handoff mesh is bounded and preserves binding state") {
    glyphastore::server::ConnectionHandoffMesh mesh{2, 2};
    glyphastore::server::ConnectionHandoff first{.bound_worker = 1};
    glyphastore::server::ConnectionHandoff second{.bound_worker = 1};
    glyphastore::server::ConnectionHandoff rejected{.bound_worker = 1};

    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(first)));
    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(second)));
    GLYPHA_REQUIRE(!mesh.try_handoff(1, std::move(rejected)));

    const auto first_received = mesh.try_pop(1);
    const auto second_received = mesh.try_pop(1);
    GLYPHA_REQUIRE(first_received.has_value());
    GLYPHA_REQUIRE(second_received.has_value());
    GLYPHA_REQUIRE(first_received->bound_worker == 1);
    GLYPHA_REQUIRE(second_received->bound_worker == 1);
    GLYPHA_REQUIRE(!mesh.try_pop(1).has_value());
}
