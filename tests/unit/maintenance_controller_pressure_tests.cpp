#include "glyphastore/core/types.hpp"
#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/store.hpp"
#include "maintenance_controller_test_support.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <thread>

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

    glyphastore::MaintenanceObservation rotate_headroom = ok;
    rotate_headroom.rotate_additional_bytes = glyphastore::kSegmentSizeBytes + 4'096ULL;
    rotate_headroom.available_free_bytes =
        rotate_headroom.reserved_free_bytes + glyphastore::kSegmentSizeBytes;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(rotate_headroom, config) ==
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
            GLYPHA_REQUIRE(snap.compact_attempts == 0 ||
                           snap.last_activation_reason ==
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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

    const auto initial = wait_for_initial_idle(**store);
    controller->set_auto_compact_enabled(false);
    const auto baseline_cycles = initial.evaluation_cycles;
    const auto baseline_attempts = initial.compact_attempts;
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.evaluation_cycles > baseline_cycles && snap.mutations_rejected) {
            GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::emergency);
            GLYPHA_REQUIRE(snap.compact_attempts == baseline_attempts);
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

    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->bind_compact(
        [](std::optional<std::size_t>, std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected compact fault");
        });
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.mutations_rejected && snap.last_error.has_value() &&
            snap.last_error->code == glyphastore::ErrorCode::io_error) {
            GLYPHA_REQUIRE(snap.state == glyphastore::MaintenanceState::faulted || snap.compact_attempts > 0);
            GLYPHA_REQUIRE(snap.pressure == glyphastore::MaintenancePressureLevel::emergency);
            const auto put = (**store).put("still-blocked", std::as_bytes(std::span{"z", 1}));
            GLYPHA_REQUIRE(!put.has_value());
            GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);

            controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                         -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
                    GLYPHA_REQUIRE(
                        (**store).put("after-fault", std::as_bytes(std::span{"y", 1})).has_value());
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
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

GLYPHA_TEST("emergency compact fault keeps reclaim attempts while gate is armed") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    static_cast<void>(wait_for_initial_idle(**store));

    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });
    controller->bind_compact(
        [compact_calls](std::optional<std::size_t>,
                        std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::fail(glyphastore::ErrorCode::storage_exhausted, "injected reclaim fault");
        });

    controller->request_evaluate();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snapshot = (**store).maintenance_snapshot();
        if (compact_calls->load(std::memory_order_relaxed) >= 1 && snapshot.mutations_rejected &&
            snapshot.state == glyphastore::MaintenanceState::faulted) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto first_fault = (**store).maintenance_snapshot();
    GLYPHA_REQUIRE(first_fault.mutations_rejected);
    GLYPHA_REQUIRE(first_fault.state == glyphastore::MaintenanceState::faulted);
    const auto attempts_after_first = first_fault.compact_attempts;
    GLYPHA_REQUIRE(attempts_after_first >= 1);

    controller->request_evaluate();
    const auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < retry_deadline) {
        if ((**store).maintenance_snapshot().compact_attempts > attempts_after_first) {
            GLYPHA_REQUIRE((**store).maintenance_snapshot().mutations_rejected);
            const auto put = (**store).put("still-gated", std::as_bytes(std::span{"z", 1}));
            GLYPHA_REQUIRE(!put.has_value());
            GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::storage_exhausted);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("background start evaluates promptly without request_evaluate") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if ((**store).maintenance_snapshot().evaluation_cycles > 0) {
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("join after stop leaves mutations_rejected cleared") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 1;
    config.maintenance.max_eval_interval_ms = 1;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 100,
            .sealed_segment_count = 1,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 2'048,
        };
    });

    for (int i = 0; i < 20; ++i) {
        controller->request_evaluate();
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE((**store).close().has_value());
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().mutations_rejected);
    GLYPHA_REQUIRE(!(**store).maintenance_snapshot().thread_running);
}

GLYPHA_TEST("maintenance snapshot records expired_records_dropped from compact") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    const auto initial = wait_for_initial_idle(**store);

    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller->bind_compact(
        [](std::optional<std::size_t>, std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            return glyphastore::CompactionResult{
                .compacted = true,
                .worker_index = 0,
                .source_records_verified = 3,
                .records_copied = 2,
                .bytes_copied = 4'096,
                .expired_records_dropped = 7,
            };
        });
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.useful_compactions > initial.useful_compactions) {
            GLYPHA_REQUIRE(snap.last_expired_records_dropped == 7);
            GLYPHA_REQUIRE(snap.total_expired_records_dropped == initial.total_expired_records_dropped + 7);
            GLYPHA_REQUIRE(snap.last_records_copied == 2);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("maintenance snapshot records no-gain planning scan counters") {
    glyphastore::StoreConfig config{};
    config.worker_config.explicit_count = 1;
    config.maintenance.mode = glyphastore::MaintenanceMode::background;
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    auto* controller = glyphastore::detail::StoreAccess::maintenance_controller(**store);
    GLYPHA_REQUIRE(controller != nullptr);
    const auto initial = wait_for_initial_idle(**store);

    controller->bind_observe([](glyphastore::MaintenanceObserveRequest)
                                 -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller->bind_compact(
        [](std::optional<std::size_t>, std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            return glyphastore::CompactionResult{
                .compacted = false,
                .worker_index = 0,
                .source_records_verified = 11,
                .source_bytes_verified = 22'016,
                .expired_records_dropped = 3,
            };
        });
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snap = (**store).maintenance_snapshot();
        if (snap.last_skip_reason == glyphastore::MaintenanceSkipReason::no_gain &&
            snap.consecutive_no_gain > initial.consecutive_no_gain) {
            GLYPHA_REQUIRE(snap.last_no_gain_source_records_verified == 11);
            GLYPHA_REQUIRE(snap.last_no_gain_source_bytes_verified == 22'016);
            GLYPHA_REQUIRE(snap.last_no_gain_expired_records_dropped == 3);
            GLYPHA_REQUIRE(snap.total_no_gain_source_records_verified ==
                           initial.total_no_gain_source_records_verified + 11);
            GLYPHA_REQUIRE(snap.total_no_gain_source_bytes_verified ==
                           initial.total_no_gain_source_bytes_verified + 22'016);
            GLYPHA_REQUIRE(snap.total_no_gain_expired_records_dropped ==
                           initial.total_no_gain_expired_records_dropped + 3);
            GLYPHA_REQUIRE((**store).close().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("pressure evaluation requests unread TTL probe when enabled") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 10'000;
    config.unread_ttl_pressure_probe = true;

    glyphastore::MaintenanceController controller{config};
    auto probe_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([probe_calls](glyphastore::MaintenanceObserveRequest request)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        if (request.probe_unread_expired_ttl) {
            probe_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::MaintenanceObservation{
                .durable = true,
                .segment_count = 90,
                .sealed_segment_count = 2,
                .compaction_candidate_worker = 0,
                .candidate_sealed_record_bytes = 2'000,
                .candidate_live_record_bytes = 1'000,
                .candidate_dead_record_bytes = 1'000,
                .candidate_dead_byte_ratio_bp = 5'000,
                .unread_ttl_probe_performed = true,
                .candidate_unread_expired_sealed_record_count = 2,
                .candidate_unread_expired_sealed_record_bytes = 128,
                .max_segment_count = 100,
                .reserved_free_bytes = 1'024,
                .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
            };
        }
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 90,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 1'000,
            .candidate_dead_record_bytes = 1'000,
            .candidate_dead_byte_ratio_bp = 5'000,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact([](const std::optional<std::size_t>,
                               const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
        return glyphastore::CompactionResult{};
    });
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snapshot = controller.snapshot();
        if (snapshot.last_observation.unread_ttl_probe_performed &&
            snapshot.last_observation.candidate_unread_expired_sealed_record_count == 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(probe_calls->load(std::memory_order_relaxed) >= 1);
    const auto snapshot = controller.snapshot();
    GLYPHA_REQUIRE(snapshot.last_observation.unread_ttl_probe_performed);
    GLYPHA_REQUIRE(snapshot.last_observation.candidate_unread_expired_sealed_record_count == 2);
    GLYPHA_REQUIRE(snapshot.last_observation.candidate_unread_expired_sealed_record_bytes == 128);
    controller.stop();
}

GLYPHA_TEST("normal evaluation skips unread TTL probe") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.unread_ttl_pressure_probe = true;

    glyphastore::MaintenanceController controller{config};
    auto probe_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([probe_calls](glyphastore::MaintenanceObserveRequest request)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        if (request.probe_unread_expired_ttl) {
            probe_calls->fetch_add(1, std::memory_order_relaxed);
        }
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 1'000,
            .candidate_dead_record_bytes = 1'000,
            .candidate_dead_byte_ratio_bp = 5'000,
            .unread_ttl_probe_performed = request.probe_unread_expired_ttl,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact([](const std::optional<std::size_t>,
                               const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
        return glyphastore::CompactionResult{.compacted = true, .bytes_copied = 1};
    });
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if (controller.snapshot().compact_completed > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(probe_calls->load(std::memory_order_relaxed) == 0);
    GLYPHA_REQUIRE(!controller.snapshot().last_observation.unread_ttl_probe_performed);
    controller.stop();
}

GLYPHA_TEST("normal unread TTL scheduling probes and lowers reclaim threshold") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 5'000;
    config.unread_ttl_normal_scheduling = true;

    glyphastore::MaintenanceController controller{config};
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest request)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        glyphastore::MaintenanceObservation observation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 10'000,
            .candidate_live_record_bytes = 6'500,
            .candidate_dead_record_bytes = 3'500,
            .candidate_dead_byte_ratio_bp = 3'500,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
        if (request.probe_unread_expired_ttl) {
            observation.unread_ttl_probe_performed = true;
            observation.candidate_unread_expired_sealed_record_count = 1;
            observation.candidate_unread_expired_sealed_record_bytes = 2'000;
        }
        observation.candidate_scheduling_dead_byte_ratio_bp =
            glyphastore::scheduling_dead_byte_ratio_bp(observation);
        return observation;
    });
    controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{.compacted = true, .bytes_copied = 1};
        });
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snapshot = controller.snapshot();
        if (snapshot.compact_completed > 0) {
            GLYPHA_REQUIRE(snapshot.last_observation.unread_ttl_probe_performed);
            GLYPHA_REQUIRE(snapshot.last_observation.candidate_scheduling_dead_byte_ratio_bp == 5'500);
            GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) >= 1);
            controller.stop();
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("normal unread TTL scheduling disabled keeps conservative threshold") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 5'000;
    config.unread_ttl_normal_scheduling = false;

    glyphastore::MaintenanceController controller{config};
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest request)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        if (request.probe_unread_expired_ttl) {
            return glyphastore::fail(glyphastore::ErrorCode::internal_error, "unexpected probe");
        }
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 10'000,
            .candidate_live_record_bytes = 6'500,
            .candidate_dead_record_bytes = 3'500,
            .candidate_dead_byte_ratio_bp = 3'500,
            .candidate_scheduling_dead_byte_ratio_bp = 3'500,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{.compacted = true, .bytes_copied = 1};
        });
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snapshot = controller.snapshot();
        if (snapshot.skips > 0 &&
            snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::reclaim_threshold) {
            GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 0);
            controller.stop();
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("max_copy_bytes_per_sec budget refreshes after the one-second window") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.max_copy_bytes_per_cycle = 0;
    config.max_copy_bytes_per_sec = 1'000;

    glyphastore::MaintenanceController controller{config};
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 10,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 1'000,
            .candidate_dead_record_bytes = 1'000,
            .candidate_dead_byte_ratio_bp = 5'000,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{.compacted = true, .bytes_copied = 1'000};
        });
    controller.start();

    const auto first_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < first_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);

    controller.request_evaluate();
    const auto skip_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < skip_deadline &&
           controller.snapshot().last_skip_reason != glyphastore::MaintenanceSkipReason::rate_budget) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().last_skip_reason == glyphastore::MaintenanceSkipReason::rate_budget);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);

    std::this_thread::sleep_for(std::chrono::milliseconds{1'100});
    controller.request_evaluate();
    const auto second_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < second_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 2);
    controller.stop();
}
