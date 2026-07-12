#include "glyphastore/server/dispatcher.hpp"
#include "test.hpp"

#include <cstddef>
#include <utility>

GLYPHA_TEST("dispatch mesh bounds Worker inboxes and completion queues") {
    glyphastore::server::DispatchMesh mesh{2, 2, 2};

    glyphastore::server::DispatchTask first_task{.origin_executor = 0, .request_id = 1};
    glyphastore::server::DispatchTask second_task{.origin_executor = 0, .request_id = 2};
    glyphastore::server::DispatchTask rejected_task{.origin_executor = 0, .request_id = 3};
    GLYPHA_REQUIRE(mesh.try_submit(1, std::move(first_task)));
    GLYPHA_REQUIRE(mesh.try_submit(1, std::move(second_task)));
    GLYPHA_REQUIRE(!mesh.try_submit(1, std::move(rejected_task)));
    const auto first_received = mesh.try_pop_task(1);
    const auto second_received = mesh.try_pop_task(1);
    GLYPHA_REQUIRE(first_received.has_value());
    GLYPHA_REQUIRE(second_received.has_value());
    GLYPHA_REQUIRE(first_received->request_id == 1);
    GLYPHA_REQUIRE(second_received->request_id == 2);

    glyphastore::server::DispatchCompletion first_completion{.request_id = 10};
    glyphastore::server::DispatchCompletion second_completion{.request_id = 11};
    glyphastore::server::DispatchCompletion rejected_completion{.request_id = 12};
    GLYPHA_REQUIRE(mesh.try_complete(0, std::move(first_completion)));
    GLYPHA_REQUIRE(mesh.try_complete(0, std::move(second_completion)));
    GLYPHA_REQUIRE(!mesh.try_complete(0, std::move(rejected_completion)));
    const auto first_completed = mesh.try_pop_completion(0);
    const auto second_completed = mesh.try_pop_completion(0);
    GLYPHA_REQUIRE(first_completed.has_value());
    GLYPHA_REQUIRE(second_completed.has_value());
    GLYPHA_REQUIRE(first_completed->request_id == 10);
    GLYPHA_REQUIRE(second_completed->request_id == 11);
}
