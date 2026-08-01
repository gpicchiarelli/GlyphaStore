#include "glyphastore/core/integer_math.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/store.hpp"
#include "maintenance_controller_test_support.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <thread>

GLYPHA_TEST("validate maintenance config rejects inverted intervals") {
    glyphastore::MaintenanceConfig config{};
    config.min_eval_interval_ms = 5'000;
    config.max_eval_interval_ms = 1'000;
    const auto status = glyphastore::validate_maintenance_config(config);
    GLYPHA_REQUIRE(!status.has_value());
    GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::invalid_argument);

    config.min_eval_interval_ms = 1'000;
    config.suspend_on_p99_min_samples = 0;
    const auto samples = glyphastore::validate_maintenance_config(config);
    GLYPHA_REQUIRE(!samples.has_value());
    GLYPHA_REQUIRE(samples.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("maintenance integer ratios and pressure thresholds do not overflow") {
    constexpr auto maximum_u64 = std::numeric_limits<std::uint64_t>::max();
    constexpr auto maximum_size = std::numeric_limits<std::size_t>::max();
    GLYPHA_REQUIRE(glyphastore::basis_points(maximum_u64 / 2U, maximum_u64) == 4'999);
    GLYPHA_REQUIRE(glyphastore::basis_points(maximum_u64 - 1U, maximum_u64) == 9'999);
    GLYPHA_REQUIRE(glyphastore::basis_points(maximum_u64, maximum_u64) == 10'000);
    for (std::uint64_t denominator = 1; denominator <= 1'000; ++denominator) {
        for (std::uint64_t numerator = 0; numerator <= denominator; ++numerator) {
            GLYPHA_REQUIRE(glyphastore::basis_points(numerator, denominator) ==
                           numerator * 10'000U / denominator);
        }
    }

    glyphastore::MaintenanceConfig config{};
    config.segment_count_pressure_pct = 100;
    auto observation = glyphastore::MaintenanceObservation{
        .durable = true,
        .segment_count = maximum_size - 1U,
        .max_segment_count = maximum_size,
    };
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(observation, config) ==
                   glyphastore::MaintenancePressureLevel::normal);

    observation.segment_count = 1;
    observation.reserved_free_bytes = maximum_u64 - 100U;
    observation.rotate_additional_bytes = 50;
    observation.available_free_bytes = maximum_u64 - 25U;
    config.free_bytes_pressure_margin = 200;
    GLYPHA_REQUIRE(glyphastore::classify_maintenance_pressure(observation, config) ==
                   glyphastore::MaintenancePressureLevel::pressure);

    observation.candidate_dead_byte_ratio_bp = 9'999;
    observation.candidate_sealed_record_bytes = maximum_u64;
    observation.candidate_dead_record_bytes = maximum_u64 - 100U;
    observation.candidate_unread_expired_sealed_record_bytes = 200;
    GLYPHA_REQUIRE(glyphastore::scheduling_dead_byte_ratio_bp(observation) == 10'000);
}

GLYPHA_TEST("normal dead-byte threshold is inclusive and pressure bypasses it") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 5'000;

    glyphastore::MaintenanceController controller{config};
    auto ratio = std::make_shared<std::atomic<std::uint32_t>>(4'999);
    auto segment_count = std::make_shared<std::atomic<std::size_t>>(10);
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto selected_worker =
        std::make_shared<std::atomic<std::size_t>>(std::numeric_limits<std::size_t>::max());
    controller.bind_observe([ratio, segment_count](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = segment_count->load(std::memory_order_relaxed),
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 3,
            .candidate_sealed_record_bytes = 10'000,
            .candidate_live_record_bytes = 10'000 - ratio->load(std::memory_order_relaxed),
            .candidate_dead_record_bytes = ratio->load(std::memory_order_relaxed),
            .candidate_dead_byte_ratio_bp = ratio->load(std::memory_order_relaxed),
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact([compact_calls, selected_worker](
                                const std::optional<std::size_t> worker,
                                const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
        compact_calls->fetch_add(1, std::memory_order_relaxed);
        selected_worker->store(worker.value_or(std::numeric_limits<std::size_t>::max()),
                               std::memory_order_relaxed);
        return glyphastore::CompactionResult{};
    });
    controller.start();

    const auto below_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < below_deadline) {
        const auto snapshot = controller.snapshot();
        if (snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::reclaim_threshold) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().last_skip_reason ==
                   glyphastore::MaintenanceSkipReason::reclaim_threshold);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 0);

    ratio->store(5'000, std::memory_order_relaxed);
    controller.request_evaluate();
    const auto threshold_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < threshold_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(selected_worker->load(std::memory_order_relaxed) == 3);

    ratio->store(0, std::memory_order_relaxed);
    segment_count->store(90, std::memory_order_relaxed);
    controller.request_evaluate();
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto pressure_snapshot = controller.snapshot();
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 2);
    GLYPHA_REQUIRE(pressure_snapshot.pressure == glyphastore::MaintenancePressureLevel::pressure);
    controller.stop();
}

GLYPHA_TEST("normal copy budget preflights one candidate and pressure bypasses it") {
    glyphastore::MaintenanceConfig config{};
    GLYPHA_REQUIRE(config.max_copy_bytes_per_cycle == glyphastore::kDefaultMaintenanceMaxCopyBytesPerCycle);
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.max_copy_bytes_per_cycle = 1'000;

    glyphastore::MaintenanceController controller{config};
    auto candidate_live_bytes = std::make_shared<std::atomic<std::uint64_t>>(1'001);
    auto segment_count = std::make_shared<std::atomic<std::size_t>>(10);
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto observed_copy_limit = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([candidate_live_bytes, segment_count](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = segment_count->load(std::memory_order_relaxed),
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = candidate_live_bytes->load(std::memory_order_relaxed),
            .candidate_dead_record_bytes = 999,
            .candidate_dead_byte_ratio_bp = 5'000,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact([compact_calls, observed_copy_limit](const std::optional<std::size_t>,
                                                                 const std::uint64_t max_copy_bytes)
                                -> glyphastore::Result<glyphastore::CompactionResult> {
        compact_calls->fetch_add(1, std::memory_order_relaxed);
        observed_copy_limit->store(max_copy_bytes, std::memory_order_relaxed);
        return glyphastore::CompactionResult{};
    });
    controller.start();

    const auto over_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < over_deadline &&
           controller.snapshot().last_skip_reason != glyphastore::MaintenanceSkipReason::copy_budget) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().last_skip_reason == glyphastore::MaintenanceSkipReason::copy_budget);
    GLYPHA_REQUIRE(controller.snapshot().last_activation_reason ==
                   glyphastore::MaintenanceActivationReason::copy_budget);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 0);

    candidate_live_bytes->store(1'000, std::memory_order_relaxed);
    controller.request_evaluate();
    const auto equal_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < equal_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(observed_copy_limit->load(std::memory_order_relaxed) == 1'000);

    candidate_live_bytes->store(1'001, std::memory_order_relaxed);
    segment_count->store(90, std::memory_order_relaxed);
    controller.request_evaluate();
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 2);
    GLYPHA_REQUIRE(observed_copy_limit->load(std::memory_order_relaxed) == 0);
    GLYPHA_REQUIRE(controller.snapshot().pressure == glyphastore::MaintenancePressureLevel::pressure);
    controller.stop();

    config.max_copy_bytes_per_cycle = 0;
    segment_count->store(10, std::memory_order_relaxed);
    compact_calls->store(0, std::memory_order_relaxed);
    glyphastore::MaintenanceController unlimited_controller{config};
    unlimited_controller.bind_observe(
        [candidate_live_bytes, segment_count](glyphastore::MaintenanceObserveRequest)
            -> glyphastore::Result<glyphastore::MaintenanceObservation> {
            return glyphastore::MaintenanceObservation{
                .durable = true,
                .segment_count = segment_count->load(std::memory_order_relaxed),
                .sealed_segment_count = 2,
                .compaction_candidate_worker = 0,
                .candidate_sealed_record_bytes = 2'000,
                .candidate_live_record_bytes = candidate_live_bytes->load(std::memory_order_relaxed),
                .candidate_dead_record_bytes = 999,
                .candidate_dead_byte_ratio_bp = 5'000,
                .max_segment_count = 100,
                .reserved_free_bytes = 1'024,
                .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
            };
        });
    unlimited_controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{};
        });
    unlimited_controller.start();
    const auto unlimited_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < unlimited_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    unlimited_controller.stop();
}

GLYPHA_TEST("normal rate and cpu budgets suspend and pressure bypasses them") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.max_copy_bytes_per_cycle = 0;
    config.max_copy_bytes_per_sec = 500;
    config.max_cpu_ms_per_window = 0;

    glyphastore::MaintenanceController controller{config};
    auto segment_count = std::make_shared<std::atomic<std::size_t>>(10);
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto observed_copy_limit = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([segment_count](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = segment_count->load(std::memory_order_relaxed),
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 400,
            .candidate_dead_record_bytes = 1'600,
            .candidate_dead_byte_ratio_bp = 8'000,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    controller.bind_compact([compact_calls, observed_copy_limit](const std::optional<std::size_t>,
                                                                 const std::uint64_t max_copy_bytes)
                                -> glyphastore::Result<glyphastore::CompactionResult> {
        compact_calls->fetch_add(1, std::memory_order_relaxed);
        observed_copy_limit->store(max_copy_bytes, std::memory_order_relaxed);
        return glyphastore::CompactionResult{
            .compacted = true,
            .records_copied = 1,
            .bytes_copied = 400,
        };
    });
    controller.start();

    const auto first_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < first_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(observed_copy_limit->load(std::memory_order_relaxed) == 500);
    GLYPHA_REQUIRE(controller.snapshot().rate_window_bytes_copied == 400);

    controller.request_evaluate();
    const auto rate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < rate_deadline &&
           controller.snapshot().last_skip_reason != glyphastore::MaintenanceSkipReason::rate_budget) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().last_skip_reason == glyphastore::MaintenanceSkipReason::rate_budget);
    GLYPHA_REQUIRE(controller.snapshot().last_activation_reason ==
                   glyphastore::MaintenanceActivationReason::rate_budget);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);

    segment_count->store(90, std::memory_order_relaxed);
    controller.request_evaluate();
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 2);
    GLYPHA_REQUIRE(observed_copy_limit->load(std::memory_order_relaxed) == 0);
    GLYPHA_REQUIRE(controller.snapshot().pressure == glyphastore::MaintenancePressureLevel::pressure);
    controller.stop();

    config.max_copy_bytes_per_sec = 0;
    config.max_cpu_ms_per_window = 1;
    segment_count->store(10, std::memory_order_relaxed);
    compact_calls->store(0, std::memory_order_relaxed);
    glyphastore::MaintenanceController cpu_controller{config};
    cpu_controller.bind_observe([segment_count](glyphastore::MaintenanceObserveRequest)
                                    -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = segment_count->load(std::memory_order_relaxed),
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 400,
            .candidate_dead_record_bytes = 1'600,
            .candidate_dead_byte_ratio_bp = 8'000,
            .max_segment_count = 100,
            .reserved_free_bytes = 1'024,
            .available_free_bytes = 1'024ULL + glyphastore::kSegmentSizeBytes + 4'096ULL,
        };
    });
    cpu_controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{5};
            while (std::chrono::steady_clock::now() < deadline) {
            }
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{
                .compacted = true,
                .records_copied = 1,
                .bytes_copied = 10,
            };
        });
    cpu_controller.start();
    const auto cpu_first = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < cpu_first &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    const auto after_cpu = cpu_controller.snapshot();
    GLYPHA_REQUIRE(after_cpu.last_compact_duration_ns >= 1'000'000ULL);
    GLYPHA_REQUIRE(after_cpu.rate_window_cpu_ns >= 1'000'000ULL);
    cpu_controller.request_evaluate();
    const auto cpu_budget_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < cpu_budget_deadline &&
           cpu_controller.snapshot().last_skip_reason != glyphastore::MaintenanceSkipReason::rate_budget) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(cpu_controller.snapshot().last_skip_reason ==
                   glyphastore::MaintenanceSkipReason::rate_budget);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    cpu_controller.stop();
}

GLYPHA_TEST("foreground p99 suspends normal compaction and pressure bypasses latency fairness") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.suspend_on_p99_latency_ms = 25;
    config.suspend_on_p99_min_samples = 1;

    glyphastore::MaintenanceController controller{config};
    auto segment_count = std::make_shared<std::atomic<std::size_t>>(10);
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([segment_count](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = segment_count->load(std::memory_order_relaxed),
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
            return glyphastore::CompactionResult{};
        });

    controller.report_foreground_latency(40'000'000ULL);
    controller.start();
    const auto suspended_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < suspended_deadline &&
           controller.snapshot().last_skip_reason != glyphastore::MaintenanceSkipReason::latency_budget) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto suspended = controller.snapshot();
    GLYPHA_REQUIRE(suspended.last_skip_reason == glyphastore::MaintenanceSkipReason::latency_budget);
    GLYPHA_REQUIRE(suspended.last_activation_reason ==
                   glyphastore::MaintenanceActivationReason::latency_budget);
    GLYPHA_REQUIRE(suspended.foreground_latency_samples == 1);
    GLYPHA_REQUIRE(suspended.last_foreground_p99_ns == 50'000'000ULL);
    GLYPHA_REQUIRE(suspended.latency_suspends == 1);
    GLYPHA_REQUIRE(suspended.latency_guard_active);
    GLYPHA_REQUIRE(suspended.latency_deferral_age_ns > 0);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 0);

    segment_count->store(90, std::memory_order_relaxed);
    controller.report_foreground_latency(40'000'000ULL);
    controller.request_evaluate();
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           compact_calls->load(std::memory_order_relaxed) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto pressure = controller.snapshot();
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(pressure.pressure == glyphastore::MaintenancePressureLevel::pressure);
    GLYPHA_REQUIRE(pressure.foreground_latency_samples == 1);
    GLYPHA_REQUIRE(pressure.latency_suspends == 1);
    GLYPHA_REQUIRE(!pressure.latency_guard_active);
    controller.stop();
}

GLYPHA_TEST("foreground latency guard requires a representative sample window") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.suspend_on_p99_latency_ms = 25;
    config.suspend_on_p99_min_samples = 2;

    glyphastore::MaintenanceController controller{config};
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 3,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 1'000,
            .candidate_dead_record_bytes = 1'000,
            .candidate_dead_byte_ratio_bp = 5'000,
            .max_segment_count = 100,
        };
    });
    controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{};
        });
    controller.report_foreground_latency(40'000'000ULL);
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline &&
           compact_calls->load(std::memory_order_relaxed) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto snapshot = controller.snapshot();
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(snapshot.foreground_latency_samples == 1);
    GLYPHA_REQUIRE(!snapshot.latency_guard_active);
    GLYPHA_REQUIRE(snapshot.latency_suspends == 0);
    controller.stop();
}

GLYPHA_TEST("foreground latency guard uses hysteresis and bounded reclaim debt") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;
    config.dead_byte_ratio_bp_normal = 0;
    config.suspend_on_p99_latency_ms = 25;
    config.suspend_on_p99_min_samples = 1;
    // Headroom above CI scheduling jitter so hysteresis checks are not raced by debt override.
    config.max_latency_deferral_ms = 500;

    glyphastore::MaintenanceController controller{config};
    auto compact_calls = std::make_shared<std::atomic<std::uint64_t>>(0);
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 3,
            .sealed_segment_count = 2,
            .compaction_candidate_worker = 0,
            .candidate_sealed_record_bytes = 2'000,
            .candidate_live_record_bytes = 1'000,
            .candidate_dead_record_bytes = 1'000,
            .candidate_dead_byte_ratio_bp = 5'000,
            .max_segment_count = 100,
        };
    });
    controller.bind_compact(
        [compact_calls](const std::optional<std::size_t>,
                        const std::uint64_t) -> glyphastore::Result<glyphastore::CompactionResult> {
            compact_calls->fetch_add(1, std::memory_order_relaxed);
            return glyphastore::CompactionResult{};
        });

    controller.report_foreground_latency(40'000'000ULL);
    controller.start();
    const auto first_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < first_deadline && controller.snapshot().latency_suspends < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().latency_guard_active);

    // A conservative 25 ms bucket stays latched until p99 falls below the
    // derived 20 ms resume boundary.
    controller.report_foreground_latency(25'000'000ULL);
    controller.request_evaluate();
    const auto second_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < second_deadline && controller.snapshot().latency_suspends < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().latency_guard_active);
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 0);

    controller.report_foreground_latency(10'000'000ULL);
    controller.request_evaluate();
    const auto resume_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < resume_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(!controller.snapshot().latency_guard_active);

    controller.report_foreground_latency(40'000'000ULL);
    controller.request_evaluate();
    const auto rearm_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < rearm_deadline && controller.snapshot().latency_suspends < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(controller.snapshot().latency_guard_active);

    std::this_thread::sleep_for(std::chrono::milliseconds{520});
    controller.report_foreground_latency(40'000'000ULL);
    controller.request_evaluate();
    const auto debt_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < debt_deadline &&
           compact_calls->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto debt = controller.snapshot();
    GLYPHA_REQUIRE(compact_calls->load(std::memory_order_relaxed) == 2);
    GLYPHA_REQUIRE(debt.latency_debt_overrides == 1);
    GLYPHA_REQUIRE(!debt.latency_guard_active);
    GLYPHA_REQUIRE(debt.last_activation_reason ==
                   glyphastore::MaintenanceActivationReason::latency_debt_override);
    controller.stop();
}

GLYPHA_TEST("maintenance telemetry counts sequence conflicts") {
    glyphastore::MaintenanceConfig config{};
    config.mode = glyphastore::MaintenanceMode::background;
    config.min_eval_interval_ms = 60'000;
    config.max_eval_interval_ms = 60'000;

    glyphastore::MaintenanceController controller{config};
    controller.bind_observe([](glyphastore::MaintenanceObserveRequest)
                                -> glyphastore::Result<glyphastore::MaintenanceObservation> {
        return glyphastore::MaintenanceObservation{
            .durable = true,
            .segment_count = 3,
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
        return glyphastore::fail(glyphastore::ErrorCode::sequence_conflict, "injected maintenance conflict");
    });
    controller.start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline && controller.snapshot().sequence_conflicts == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    const auto snapshot = controller.snapshot();
    GLYPHA_REQUIRE(snapshot.compact_attempts == 1);
    GLYPHA_REQUIRE(snapshot.sequence_conflicts == 1);
    GLYPHA_REQUIRE(snapshot.skips == 1);
    GLYPHA_REQUIRE(snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::sequence_conflict);
    controller.stop();
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
    config.maintenance.min_eval_interval_ms = 60'000;
    config.maintenance.max_eval_interval_ms = 60'000;

    auto store = glyphastore::Store::open(config);
    GLYPHA_REQUIRE(store.has_value());
    GLYPHA_REQUIRE((**store).maintenance_snapshot().thread_running);

    const auto snapshot = wait_for_initial_idle(**store);
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

    const auto initial = wait_for_initial_idle(**store);
    controller->set_auto_compact_enabled(false);
    const auto baseline_cycles = initial.evaluation_cycles;
    const auto baseline_attempts = initial.compact_attempts;
    controller->request_evaluate();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    glyphastore::MaintenanceSnapshot snapshot{};
    while (std::chrono::steady_clock::now() < deadline) {
        snapshot = (**store).maintenance_snapshot();
        if (snapshot.evaluation_cycles > baseline_cycles &&
            snapshot.last_skip_reason == glyphastore::MaintenanceSkipReason::policy_deferred) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(snapshot.evaluation_cycles > baseline_cycles);
    GLYPHA_REQUIRE(snapshot.compact_attempts == baseline_attempts);
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

