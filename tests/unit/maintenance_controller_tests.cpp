#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/store.hpp"
#include "glyphastore/core/types.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <chrono>
#include <span>
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

GLYPHA_TEST("classify_maintenance_pressure detects segment and free-space watermarks") {
    glyphastore::MaintenanceConfig config{};
    config.segment_count_pressure_pct = 80;
    config.free_bytes_pressure_margin = glyphastore::kSegmentSizeBytes + 1'000ULL;

    glyphastore::MaintenanceObservation ok{
        .durable = true,
        .segment_count = 10,
        .sealed_segment_count = 2,
        .max_segment_count = 100,
        .reserved_free_bytes = 256,
        .available_free_bytes = glyphastore::kSegmentSizeBytes + 10'000ULL,
    };
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(ok, config) ==
                   glyphastore::MaintenancePressureLevel::normal);

    glyphastore::MaintenanceObservation segments = ok;
    segments.segment_count = 80;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(segments, config) ==
                   glyphastore::MaintenancePressureLevel::pressure);

    glyphastore::MaintenanceObservation free_space = ok;
    // Pressure watermark without emergency: above reserved+Segment, at/under reserved+margin.
    free_space.available_free_bytes = free_space.reserved_free_bytes + glyphastore::kSegmentSizeBytes;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(free_space, config) ==
                   glyphastore::MaintenancePressureLevel::pressure);

    glyphastore::MaintenanceObservation emergency_segments = ok;
    emergency_segments.segment_count = 100;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(emergency_segments, config) ==
                   glyphastore::MaintenancePressureLevel::emergency);

    glyphastore::MaintenanceObservation emergency_free = ok;
    emergency_free.available_free_bytes =
        emergency_free.reserved_free_bytes + glyphastore::kSegmentSizeBytes - 1ULL;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(emergency_free, config) ==
                   glyphastore::MaintenancePressureLevel::emergency);
}

GLYPHA_TEST("pressure policy continues compacting despite no-gain budget") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;
    config.maintenance.max_no_gain_attempts = 1;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);

    // First eval: volatile no-gain increments streak to 1 (at budget limit).
    controller->request_evaluate();
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (std::chrono::steady_clock::now() < deadline) {
            if ((**store).maintenance_snapshot().compact_attempts > 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
        }
    }
    GLYPHA_REQUIRE((**store).maintenance_snapshot().compact_attempts >= 1);

    // Inject durable pressure (not emergency) with sealed history so budget would suspend under
    // normal, but pressure must still attempt compact.
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 90,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 100,
            .available_free_bytes = 100ULL + glyphastore::kSegmentSizeBytes,
        };
    });
    const auto attempts_before = (**store).maintenance_snapshot().compact_attempts;
    controller->request_evaluate();
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (std::chrono::steady_clock::now() < deadline) {
            const auto snap = (**store).maintenance_snapshot();
            if (snap.compact_attempts > attempts_before) {
                GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::pressure);
                GLYPHA_REQUIRE(!snap.mutations_rejected);
                GLYPHA_REQUIRE(snap.last_activation_reason ==
                                   glyphastore::MaintenanceActivationReason::segment_pressure ||
                               snap.last_activation_reason ==
                                   glyphastore::MaintenanceActivationReason::free_space_pressure);
                GLYPHA_REQUIRE(snap.last_eval_duration_ns > 0);
                GLYPHA_REQUIRE((**store).close().has_value());
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
        }
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("pressure observes no_candidate when durable sealed set is empty") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 90,
            .sealed_segment_count = 0,
            .max_segment_count = 100,
            .reserved_free_bytes = 100,
            .available_free_bytes = 100ULL + glyphastore::kSegmentSizeBytes,
        };
    });
    controller->request_evaluate();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.last_skip_reason == glyphastore::MaintenanceSkipReason::no_candidate) {
            GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::pressure);
            GLYPHA_REQUIRE(!snap.mutations_rejected);
            GLYPHA_REQUIRE(snap.compact_attempts == 0 || snap.last_activation_reason ==
                                                             glyphastore::MaintenanceActivationReason::no_candidate);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("emergency rejects put and erase with storage_exhausted") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    GLYPHA_REQUIRE((**store).put("alive", std::as_bytes(std::span{"v", 1})).has_value());

    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if ((**store).maintenance_snapshot().mutations_rejected) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto snap = (**store).maintenance_snapshot();
    GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::emergency);
    GLYPHA_REQUIRE(snap.mutations_rejected);
    GLYPHA_REQUIRE(snap.last_activation_reason ==
                   glyphastore::MaintenanceActivationReason::emergency_capacity);

    const auto put = (**store).put("blocked", std::as_bytes(std::span{"x", 1}));
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);
    GLYPHA_REQUIRE(put.error().message == glyphastore::kMaintenanceEmergencyMutationMessage);

    const auto erased = (**store).erase("alive");
    GLYPHA_REQUIRE(!erased.has_value());
    GLYPHA_REQUIRE(erased.error().code == glyphastore::ErrorCode::storage_exhausted);

    // Reads and compact remain available under emergency.
    const auto got = (**store).get("alive");
    GLYPHA_REQUIRE(got.has_value());
    const auto compacted = (**store).compact();
    GLYPHA_REQUIRE(compacted.has_value() ||
                   compacted.error().code == glyphastore::ErrorCode::sequence_conflict);

    // Recovery: observation clears emergency → mutations resume.
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 1,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller->request_evaluate();
    const auto recover_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < recover_deadline) {
        if (!(**store).maintenance_snapshot().mutations_rejected) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().mutations_rejected);
    GLYPHA_REQUIRE((**store).put("recovered", std::as_bytes(std::span{"y", 1})).has_value());
    GLYPHA_REQUIRE((**store).close().has_value());
}

GLYPHA_TEST("emergency rejects mutations even when auto-compact is disabled") {
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
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 4,
            .sealed_segment_count = 0,
            .max_segment_count = 8,
            .reserved_free_bytes = 100,
            .available_free_bytes = 50,
        };
    });
    controller->request_evaluate();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.mutations_rejected) {
            GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::emergency);
            GLYPHA_REQUIRE(snap.compact_attempts == 0);
            GLYPHA_REQUIRE(snap.last_skip_reason == glyphastore::MaintenanceSkipReason::policy_deferred);
            const auto put = (**store).put("x", std::as_bytes(std::span{"z", 1}));
            GLYPHA_REQUIRE(!put.has_value());
            GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("emergency gate survives compact fault") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);

    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->bind_compact([]() -> glyphastore::Result<glyphastore::CompactionResult> {
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected compact fault");
    });
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.mutations_rejected && snap.last_error.has_value() &&
            snap.last_error->code == glyphastore::ErrorCode::io_error) {
            GLYPHA_REQUIRE(snap.state == glyphastore::MaintenanceState::faulted ||
                           snap.compact_attempts > 0);
            GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::emergency);
            const auto put = (**store).put("still-blocked", std::as_bytes(std::span{"z", 1}));
            GLYPHA_REQUIRE(!put.has_value());
            GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);

            controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
                return glyphastore::MaintenanceObservation{
                    .durable = true,
                    .segment_count = 10,
                    .sealed_segment_count = 1,
                    .max_segment_count = 100,
                    .reserved_free_bytes = 1'024,
                    .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
                };
            });
            controller->request_evaluate();
            const auto recover_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
            while (std::chrono::steady_clock::now() < recover_deadline) {
                if (!(**store).maintenance_snapshot().mutations_rejected) {
                    GLYPHA_REQUIRE((**store).put("after-fault", std::as_bytes(std::span{"y", 1})).has_value());
                    GLYPHA_REQUIRE((**store).close().has_value());
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds{5});
            }
            GLYPHA_REQUIRE(false);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("close under emergency clears mutations_rejected and stops thread") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 1,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->request_evaluate();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if ((**store).maintenance_snapshot().mutations_rejected) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE((**store).maintenance_snapshot().mutations_rejected);
    GLYPHA_REQUIRE((**store).close().has_value());
    const auto snap = (**store).maintenance_snapshot();
    GLYPHA_REQUIRE(!snap.thread_running);
    GLYPHA_REQUIRE(!snap.mutations_rejected);
    GLYPHA_REQUIRE(snap.state == glyphastore::MaintenanceState::stopped);
}

GLYPHA_TEST("flush succeeds while emergency rejects put") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->bind_observe([]() -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 0,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->request_evaluate();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if ((**store).maintenance_snapshot().mutations_rejected) {
            GLYPHA_REQUIRE((**store).flush().has_value());
            const auto put = (**store).put("nope", std::as_bytes(std::span{"n", 1}));
            GLYPHA_REQUIRE(!put.has_value());
            GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}
