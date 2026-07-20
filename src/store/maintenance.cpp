#include "glyphastore/store/maintenance.hpp"

#include <algorithm>
#include <utility>

namespace glyphastore {
namespace {

[[nodiscard]] auto clamp_interval_ms(const MaintenanceConfig& config, const bool use_max) -> std::uint32_t {
    const auto min_ms = std::max<std::uint32_t>(config.min_eval_interval_ms, 1U);
    const auto max_ms = std::max(config.max_eval_interval_ms, min_ms);
    if (use_max) {
        return max_ms;
    }
    return min_ms + (max_ms - min_ms) / 2U;
}

} // namespace

auto validate_maintenance_config(const MaintenanceConfig& config) -> Status {
    if (config.mode != MaintenanceMode::cooperative && config.mode != MaintenanceMode::background &&
        config.mode != MaintenanceMode::disabled) {
        return fail(ErrorCode::invalid_argument, "maintenance mode is unsupported");
    }
    if (config.min_eval_interval_ms == 0 || config.max_eval_interval_ms == 0) {
        return fail(ErrorCode::invalid_argument, "maintenance eval intervals must be greater than zero");
    }
    if (config.min_eval_interval_ms > config.max_eval_interval_ms) {
        return fail(ErrorCode::invalid_argument, "maintenance min_eval_interval_ms exceeds max");
    }
    if (config.max_segments_per_cycle == 0) {
        return fail(ErrorCode::invalid_argument, "maintenance max_segments_per_cycle must be greater than zero");
    }
    if (config.segment_count_pressure_pct == 0 || config.segment_count_pressure_pct > 100) {
        return fail(ErrorCode::invalid_argument, "maintenance segment_count_pressure_pct must be in 1..100");
    }
    if (config.dead_byte_ratio_bp_normal > 10'000) {
        return fail(ErrorCode::invalid_argument, "maintenance dead_byte_ratio_bp_normal must be <= 10000");
    }
    return {};
}

MaintenanceController::MaintenanceController(MaintenanceConfig config) : config_(std::move(config)) {}

MaintenanceController::~MaintenanceController() {
    stop();
}

void MaintenanceController::bind_compact(CompactCallback compact) {
    const std::lock_guard lock{mutex_};
    compact_ = std::move(compact);
}

void MaintenanceController::bind_observe(ObserveCallback observe) {
    const std::lock_guard lock{mutex_};
    observe_ = std::move(observe);
}

void MaintenanceController::set_auto_compact_enabled(const bool enabled) noexcept {
    const std::lock_guard lock{mutex_};
    auto_compact_enabled_ = enabled;
}

void MaintenanceController::start() {
    if (config_.mode != MaintenanceMode::background) {
        const std::lock_guard lock{mutex_};
        state_ = MaintenanceState::stopped;
        return;
    }
    {
        const std::lock_guard lock{mutex_};
        if (worker_.joinable()) {
            return;
        }
        stop_requested_ = false;
        state_ = MaintenanceState::idle;
    }
    auto thread = std::jthread([this](const std::stop_token stop_token) { run(stop_token); });
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_) {
            thread.request_stop();
            thread.join();
            state_ = MaintenanceState::stopped;
            return;
        }
        worker_ = std::move(thread);
    }
}

void MaintenanceController::request_stop() noexcept {
    {
        const std::lock_guard lock{mutex_};
        stop_requested_ = true;
        if (state_ == MaintenanceState::compacting) {
            state_ = MaintenanceState::draining;
        } else if (state_ != MaintenanceState::faulted && state_ != MaintenanceState::draining) {
            state_ = MaintenanceState::stopped;
        }
        // Drop callbacks so a late evaluate cannot touch a closing Store.
        compact_ = {};
        observe_ = {};
    }
    wake_.notify_all();
    if (worker_.joinable()) {
        worker_.request_stop();
    }
}

void MaintenanceController::join() {
    std::jthread local;
    {
        const std::lock_guard lock{mutex_};
        local = std::move(worker_);
    }
    if (local.joinable()) {
        local.join();
    }
    {
        const std::lock_guard lock{mutex_};
        if (state_ != MaintenanceState::faulted) {
            state_ = MaintenanceState::stopped;
        }
    }
}

void MaintenanceController::stop() {
    request_stop();
    join();
}

void MaintenanceController::request_evaluate() {
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_ || config_.mode != MaintenanceMode::background) {
            return;
        }
        evaluate_requested_ = true;
    }
    wake_.notify_one();
}

auto MaintenanceController::snapshot() const -> MaintenanceSnapshot {
    const std::lock_guard lock{mutex_};
    return MaintenanceSnapshot{
        .state = state_,
        .thread_running = worker_.joinable(),
        .mode = config_.mode,
        .evaluation_cycles = evaluation_cycles_,
        .compact_attempts = compact_attempts_,
        .compact_completed = compact_completed_,
        .skips = skips_,
        .consecutive_no_gain = consecutive_no_gain_,
        .bytes_copied_window = bytes_copied_window_,
        .last_skip_reason = last_skip_reason_,
        .last_observation = last_observation_,
        .last_error = last_error_,
    };
}

auto MaintenanceController::thread_running() const noexcept -> bool {
    const std::lock_guard lock{mutex_};
    return worker_.joinable();
}

auto MaintenanceController::eval_interval() const -> std::chrono::milliseconds {
    const bool backoff = consecutive_no_gain_ >= config_.max_no_gain_attempts && config_.max_no_gain_attempts > 0;
    return std::chrono::milliseconds{clamp_interval_ms(config_, backoff)};
}

void MaintenanceController::record_skip(const MaintenanceSkipReason reason, const MaintenanceState next) {
    ++skips_;
    last_skip_reason_ = reason;
    state_ = stop_requested_ ? MaintenanceState::stopped : next;
}

void MaintenanceController::evaluate_once() {
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_) {
            return;
        }
        state_ = MaintenanceState::evaluating;
        ++evaluation_cycles_;
    }

    ObserveCallback observe;
    CompactCallback compact;
    bool auto_compact = false;
    MaintenanceConfig config{};
    std::uint64_t consecutive_no_gain = 0;
    std::uint64_t bytes_copied_window = 0;
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_) {
            return;
        }
        observe = observe_;
        compact = compact_;
        auto_compact = auto_compact_enabled_;
        config = config_;
        consecutive_no_gain = consecutive_no_gain_;
        bytes_copied_window = bytes_copied_window_;
    }

    if (!auto_compact || !compact) {
        const std::lock_guard lock{mutex_};
        record_skip(MaintenanceSkipReason::policy_deferred, MaintenanceState::idle);
        return;
    }

    MaintenanceObservation observation{};
    if (observe) {
        auto observed = observe();
        if (!observed) {
            const std::lock_guard lock{mutex_};
            last_error_ = observed.error();
            if (observed.error().code == ErrorCode::unavailable) {
                record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle);
                return;
            }
            // Observation faults disable automatic maintenance without poisoning the Store.
            state_ = MaintenanceState::faulted;
            auto_compact_enabled_ = false;
            return;
        }
        observation = *observed;
        const std::lock_guard lock{mutex_};
        last_observation_ = observation;
        if (stop_requested_) {
            return;
        }
    }

    // Normal policy: skip when durable catalog has no sealed history (nothing to compact).
    if (observation.durable && observation.sealed_segment_count == 0) {
        const std::lock_guard lock{mutex_};
        consecutive_no_gain_ = 0;
        record_skip(MaintenanceSkipReason::no_candidate, MaintenanceState::idle);
        return;
    }

    // After waiting the max interval (eval_interval backoff), clear the streak and skip this
    // cycle so the next evaluation may compact again under the normal interval.
    if (config.max_no_gain_attempts > 0 && consecutive_no_gain >= config.max_no_gain_attempts) {
        const std::lock_guard lock{mutex_};
        consecutive_no_gain_ = 0;
        record_skip(MaintenanceSkipReason::budget, MaintenanceState::suspended);
        return;
    }

    if (config.max_copy_bytes_per_cycle > 0 && bytes_copied_window >= config.max_copy_bytes_per_cycle) {
        const std::lock_guard lock{mutex_};
        bytes_copied_window_ = 0;
        record_skip(MaintenanceSkipReason::budget, MaintenanceState::suspended);
        return;
    }

    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_ || !compact_) {
            return;
        }
        compact = compact_;
        state_ = MaintenanceState::compacting;
        ++compact_attempts_;
    }

    auto result = compact();
    {
        const std::lock_guard lock{mutex_};
        if (!result) {
            last_error_ = result.error();
            if (result.error().code == ErrorCode::unavailable) {
                record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle);
            } else if (result.error().code == ErrorCode::sequence_conflict) {
                record_skip(MaintenanceSkipReason::sequence_conflict, MaintenanceState::idle);
            } else {
                // Keep the Store usable; disable further automatic compact attempts.
                state_ = MaintenanceState::faulted;
                auto_compact_enabled_ = false;
            }
            return;
        }
        if (!result->compacted) {
            ++consecutive_no_gain_;
            record_skip(MaintenanceSkipReason::no_gain, MaintenanceState::idle);
            return;
        }
        ++compact_completed_;
        consecutive_no_gain_ = 0;
        bytes_copied_window_ += result->bytes_copied;
        last_skip_reason_ = MaintenanceSkipReason::none;
        last_error_.reset();
        state_ = stop_requested_ ? MaintenanceState::stopped : MaintenanceState::idle;
    }
}

void MaintenanceController::run(const std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        {
            std::unique_lock lock{mutex_};
            if (stop_requested_) {
                break;
            }
            if (state_ != MaintenanceState::suspended) {
                state_ = MaintenanceState::idle;
            }
            const auto deadline = std::chrono::steady_clock::now() + eval_interval();
            wake_.wait_until(lock, deadline, [&] {
                return stop_requested_ || evaluate_requested_ || stop_token.stop_requested();
            });
            evaluate_requested_ = false;
            if (stop_requested_ || stop_token.stop_requested()) {
                break;
            }
        }
        evaluate_once();
    }
    {
        const std::lock_guard lock{mutex_};
        if (state_ != MaintenanceState::faulted) {
            state_ = MaintenanceState::stopped;
        }
    }
}

} // namespace glyphastore
