#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <chrono>
#include <thread>

GLYPHA_TEST("validate maintenance config rejects inverted intervals") {
    glyphastore::MaintenanceConfig config{};
    config.min_eval_interval_ms = 5'000;
    config.max_eval_interval_ms = 1'000;
    const auto status = glyphastore::validate_maintenance_config(config);
    GLYPHA_REQUIRE(!status.has_value());
    GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("cooperative store starts no maintenance thread") {
    auto store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::cooperative},
    });
    GLYPHA_REQUIRE(store.has_value());
    const auto snapshot = (**store).maintenance_snapshot();
    GLYPHA_REQUIRE(snapshot.mode == glyphastore::MaintenanceMode::cooperative);
    GLYPHA_REQUIRE(!snapshot.thread_running);
    GLYPHA_REQUIRE(snapshot.state == glyphastore::MaintenanceState::stopped);
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("disabled store starts no maintenance thread") {
    auto store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    GLYPHA_REQUIRE(store.has_value());
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().thread_running);
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("background store phase1 auto-compacts and reports no_gain on empty volatile") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 20;
    config.maintenance.max_eval_interval_ms = 20;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    GLYPHA_REQUIRE((**store).maintenance_snapshot().thread_running);

    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    glyphastore::MaintenanceSnapshot snapshot{};
    while (std::chrono::steady_clock::now() < deadline) {
        snapshot = (**store).maintenance_snapshot();
        if (snapshot.compact_attempts > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(snapshot.evaluation_cycles > 0);
    GLYPHA_REQUIRE(snapshot.compact_attempts > 0);
    GLYPHA_REQUIRE(snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::no_gain ||
                   snapshot.compact_completed > 0);
    GLYPHA_REQUIRE(!snapshot.last_observation.durable);

    const auto compacted = (**store).compact();
    GLYPHA_REQUIRE(compacted.has_value());

    GLYPHA_REQUIRE((**store).close().has_value());
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().thread_running);
}

GLYPHA_TEST("background auto-compact can be disabled for observation-only eval") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->set_auto_compact_enabled(false);
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    glyphastore::MaintenanceSnapshot snapshot{};
    while (std::chrono::steady_clock::now() < deadline) {
        snapshot = (**store).maintenance_snapshot();
        if (snapshot.evaluation_cycles > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(snapshot.evaluation_cycles > 0);
    GLYPHA_REQUIRE(snapshot.compact_attempts == 0);
    GLYPHA_REQUIRE(snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::policy_deferred);
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("manual compact races background controller without queuing") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 1;
    config.maintenance.max_eval_interval_ms = 1;
    config.maintenance.max_no_gain_attempts = 1'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);

    bool saw_conflict = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < deadline) {
        controller->request_evaluate();
        const auto result = (**store).compact();
        if (!result.has_value() && result.error().code == glyphastore::ErrorCode::sequence_conflict) {
            saw_conflict = true;
            break;
        }
    }
    GLYPHA_REQUIRE(saw_conflict);
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("normal policy respects max_no_gain budget backoff") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 5;
    config.maintenance.max_eval_interval_ms = 5;
    config.maintenance.max_no_gain_attempts = 2;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    bool saw_budget = false;
    while (std::chrono::steady_clock::now() < deadline) {
        controller->request_evaluate();
        const auto snapshot = (**store).maintenance_snapshot();
        if (snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::budget ||
            snapshot.state == glyphastore::MaintenanceState::suspended) {
            saw_budget = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(saw_budget);
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("close joins maintenance after draining admitted operations") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 1;
    config.maintenance.max_eval_interval_ms = 1;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->request_evaluate();
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    GLYPHA_REQUIRE((**store).close().has_value());
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().thread_running);
    GLYPHA_REQUIRE((**store).maintenance_snapshot().state == glyphastore::MaintenanceState::stopped);
}
