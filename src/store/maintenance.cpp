#include "glyphastore/store/maintenance.hpp"

#include "glyphastore/core/types.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace glyphastore {
namespace {

[[nodiscard]] auto clamp_interval_ms(const MaintenanceConfig& config, const bool use_min,
                                     const bool use_max) -> std::uint32_t {
    const auto min_ms = std::max<std::uint32_t>(config.min_eval_interval_ms, 1U);
    const auto max_ms = std::max(config.max_eval_interval_ms, min_ms);
    if (use_min) {
        return min_ms;
    }
    if (use_max) {
        return max_ms;
    }
    return min_ms + (max_ms - min_ms) / 2U;
}

[[nodiscard]] auto elapsed_ns(const std::chrono::steady_clock::time_point start) -> std::uint64_t {
    const auto delta = std::chrono::steady_clock::now() - start;
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(delta).count());
}

[[nodiscard]] auto aggressive_pressure(const MaintenancePressureLevel level) noexcept -> bool {
    return level == MaintenancePressureLevel::pressure || level == MaintenancePressureLevel::emergency;
}

[[nodiscard]] auto rotate_additional_bytes_for(const MaintenanceObservation& observation) noexcept
    -> std::uint64_t {
    if (observation.rotate_additional_bytes != 0) {
        return observation.rotate_additional_bytes;
    }
    return static_cast<std::uint64_t>(kSegmentSizeBytes);
}

[[nodiscard]] auto free_space_blocks_rotation(const MaintenanceObservation& observation) noexcept -> bool {
    if (!observation.available_free_bytes.has_value()) {
        return false;
    }
    const auto available = *observation.available_free_bytes;
    const auto reserved = observation.reserved_free_bytes;
    const auto additional = rotate_additional_bytes_for(observation);
    if (additional > std::numeric_limits<std::uint64_t>::max() - reserved) {
        return true;
    }
    return available < reserved + additional;
}

} // namespace

auto classify_maintenance_pressure(const MaintenanceObservation& observation,
                                   const MaintenanceConfig& config) noexcept -> MaintenancePressureLevel {
    if (!observation.durable) {
        return MaintenancePressureLevel::normal;
    }

    // Emergency: cannot create/rotate a Segment while preserving reserved free space.
    if (observation.max_segment_count > 0 && observation.segment_count >= observation.max_segment_count) {
        return MaintenancePressureLevel::emergency;
    }
    if (free_space_blocks_rotation(observation)) {
        return MaintenancePressureLevel::emergency;
    }

    bool pressure = false;
    if (observation.max_segment_count > 0) {
        const auto threshold =
            (observation.max_segment_count * static_cast<std::size_t>(config.segment_count_pressure_pct) + 99U) /
            100U;
        if (observation.segment_count >= threshold) {
            pressure = true;
        }
    }
    if (observation.available_free_bytes.has_value()) {
        const auto watermark = observation.reserved_free_bytes + config.free_bytes_pressure_margin;
        if (*observation.available_free_bytes <= watermark) {
            pressure = true;
        }
    }
    return pressure ? MaintenancePressureLevel::pressure : MaintenancePressureLevel::normal;
}

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
        evaluate_requested_ = true; // first observation must not wait a mid-interval
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
    wake_.notify_one();
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
        compact_ = {};
        observe_ = {};
        publish_mutations_rejected_locked(false);
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
        publish_mutations_rejected_locked(false);
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
    std::uint64_t since_useful = 0;
    if (last_useful_at_) {
        since_useful = elapsed_ns(*last_useful_at_);
    }
    return MaintenanceSnapshot{
        .state = state_,
        .thread_running = worker_.joinable(),
        .mode = config_.mode,
        .pressure = pressure_,
        .last_activation_reason = last_activation_reason_,
        .mutations_rejected = mutations_rejected_.load(std::memory_order_relaxed),
        .evaluation_cycles = evaluation_cycles_,
        .compact_attempts = compact_attempts_,
        .compact_completed = compact_completed_,
        .useful_compactions = useful_compactions_,
        .skips = skips_,
        .suspend_count = suspend_count_,
        .consecutive_no_gain = consecutive_no_gain_,
        .bytes_copied_window = bytes_copied_window_,
        .total_bytes_copied = total_bytes_copied_,
        .last_bytes_copied = last_bytes_copied_,
        .last_records_copied = last_records_copied_,
        .last_eval_duration_ns = last_eval_duration_ns_,
        .last_compact_duration_ns = last_compact_duration_ns_,
        .ns_since_last_useful_compaction = since_useful,
        .last_skip_reason = last_skip_reason_,
        .last_observation = last_observation_,
        .last_error = last_error_,
    };
}

auto MaintenanceController::thread_running() const noexcept -> bool {
    const std::lock_guard lock{mutex_};
    return worker_.joinable();
}

auto MaintenanceController::mutations_rejected() const noexcept -> bool {
    return mutations_rejected_.load(std::memory_order_acquire);
}

void MaintenanceController::publish_mutations_rejected_locked(const bool rejected) noexcept {
    // Once stop is requested, never re-arm the gate (in-flight eval must not undo request_stop).
    if (stop_requested_) {
        mutations_rejected_.store(false, std::memory_order_release);
        return;
    }
    mutations_rejected_.store(rejected, std::memory_order_release);
}

auto MaintenanceController::eval_interval_locked() const -> std::chrono::milliseconds {
    const bool under_pressure = aggressive_pressure(pressure_);
    const bool backoff =
        !under_pressure && consecutive_no_gain_ >= config_.max_no_gain_attempts && config_.max_no_gain_attempts > 0;
    return std::chrono::milliseconds{clamp_interval_ms(config_, under_pressure, backoff)};
}

void MaintenanceController::record_skip(const MaintenanceSkipReason reason, const MaintenanceState next,
                                        const MaintenanceActivationReason activation) {
    ++skips_;
    last_skip_reason_ = reason;
    last_activation_reason_ = activation;
    if (next == MaintenanceState::suspended) {
        ++suspend_count_;
    }
    state_ = stop_requested_ ? MaintenanceState::stopped : next;
}

void MaintenanceController::evaluate_once() {
    const auto eval_started = std::chrono::steady_clock::now();
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

    MaintenanceObservation observation{};
    if (!observe) {
        const std::lock_guard lock{mutex_};
        pressure_ = MaintenancePressureLevel::normal;
        publish_mutations_rejected_locked(false);
        if (!auto_compact || !compact) {
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            record_skip(MaintenanceSkipReason::policy_deferred, MaintenanceState::idle,
                        MaintenanceActivationReason::policy_deferred);
            return;
        }
    } else {
        auto observed = observe();
        if (!observed) {
            const std::lock_guard lock{mutex_};
            last_error_ = observed.error();
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            if (observed.error().code == ErrorCode::unavailable) {
                publish_mutations_rejected_locked(false);
                record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle,
                            MaintenanceActivationReason::none);
                return;
            }
            // Keep an already-published emergency gate. Do not latch auto-compact off while the
            // gate is armed: reclaim must keep retrying under emergency.
            state_ = MaintenanceState::faulted;
            if (!mutations_rejected_.load(std::memory_order_relaxed)) {
                auto_compact_enabled_ = false;
            }
            return;
        }
        observation = *observed;
        {
            const std::lock_guard lock{mutex_};
            last_observation_ = observation;
            pressure_ = classify_maintenance_pressure(observation, config_);
            publish_mutations_rejected_locked(pressure_ == MaintenancePressureLevel::emergency);
            if (stop_requested_) {
                return;
            }
        }
        if (!auto_compact || !compact) {
            const std::lock_guard lock{mutex_};
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            record_skip(MaintenanceSkipReason::policy_deferred, MaintenanceState::idle,
                        pressure_ == MaintenancePressureLevel::emergency
                            ? MaintenanceActivationReason::emergency_capacity
                            : MaintenanceActivationReason::policy_deferred);
            return;
        }
    }

    const auto pressure = [&] {
        const std::lock_guard lock{mutex_};
        return pressure_;
    }();
    const bool under_pressure = aggressive_pressure(pressure);

    if (observation.durable && observation.sealed_segment_count == 0) {
        const std::lock_guard lock{mutex_};
        consecutive_no_gain_ = 0;
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        record_skip(MaintenanceSkipReason::no_candidate, MaintenanceState::idle,
                    pressure == MaintenancePressureLevel::emergency
                        ? MaintenanceActivationReason::emergency_capacity
                        : MaintenanceActivationReason::no_candidate);
        return;
    }

    // Normal-only backoff. Under pressure/emergency, reclaim attempts continue despite no-gain streak.
    if (!under_pressure && config.max_no_gain_attempts > 0 && consecutive_no_gain >= config.max_no_gain_attempts) {
        const std::lock_guard lock{mutex_};
        consecutive_no_gain_ = 0;
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        record_skip(MaintenanceSkipReason::budget, MaintenanceState::suspended,
                    MaintenanceActivationReason::budget_backoff);
        return;
    }

    if (!under_pressure && config.max_copy_bytes_per_cycle > 0 &&
        bytes_copied_window >= config.max_copy_bytes_per_cycle) {
        const std::lock_guard lock{mutex_};
        bytes_copied_window_ = 0;
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        record_skip(MaintenanceSkipReason::budget, MaintenanceState::suspended,
                    MaintenanceActivationReason::budget_backoff);
        return;
    }

    MaintenanceActivationReason activation = MaintenanceActivationReason::scheduled;
    if (pressure == MaintenancePressureLevel::emergency) {
        activation = MaintenanceActivationReason::emergency_capacity;
    } else if (pressure == MaintenancePressureLevel::pressure) {
        if (observation.available_free_bytes.has_value() &&
            *observation.available_free_bytes <=
                observation.reserved_free_bytes + config.free_bytes_pressure_margin) {
            activation = MaintenanceActivationReason::free_space_pressure;
        } else {
            activation = MaintenanceActivationReason::segment_pressure;
        }
    } else if (observation.durable && observation.sealed_segment_count > 0) {
        activation = MaintenanceActivationReason::sealed_history;
    }

    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_ || !compact_) {
            return;
        }
        compact = compact_;
        state_ = MaintenanceState::compacting;
        last_activation_reason_ = activation;
        ++compact_attempts_;
    }

    const auto compact_started = std::chrono::steady_clock::now();
    auto result = compact();
    const auto compact_ns = elapsed_ns(compact_started);
    {
        const std::lock_guard lock{mutex_};
        last_compact_duration_ns_ = compact_ns;
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        if (!result) {
            last_error_ = result.error();
            if (result.error().code == ErrorCode::unavailable) {
                publish_mutations_rejected_locked(false);
                record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle,
                            MaintenanceActivationReason::none);
            } else if (result.error().code == ErrorCode::sequence_conflict) {
                record_skip(MaintenanceSkipReason::sequence_conflict, MaintenanceState::idle, activation);
            } else {
                // Preserve emergency rejection. While the gate is armed, keep auto-compact enabled
                // so budgeted reclaim retries continue (critical: do not wedge the Store).
                state_ = MaintenanceState::faulted;
                const bool gate_armed = mutations_rejected_.load(std::memory_order_relaxed) ||
                                        pressure_ == MaintenancePressureLevel::emergency;
                if (!gate_armed) {
                    auto_compact_enabled_ = false;
                }
            }
            return;
        }
        last_bytes_copied_ = result->bytes_copied;
        last_records_copied_ = result->records_copied;
        if (!result->compacted) {
            ++consecutive_no_gain_;
            record_skip(MaintenanceSkipReason::no_gain, MaintenanceState::idle, activation);
            return;
        }
        ++compact_completed_;
        ++useful_compactions_;
        consecutive_no_gain_ = 0;
        bytes_copied_window_ += result->bytes_copied;
        total_bytes_copied_ += result->bytes_copied;
        last_useful_at_ = std::chrono::steady_clock::now();
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
            if (state_ != MaintenanceState::suspended && state_ != MaintenanceState::faulted) {
                state_ = MaintenanceState::idle;
            }
            const auto deadline = std::chrono::steady_clock::now() + eval_interval_locked();
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
        publish_mutations_rejected_locked(false);
        if (state_ != MaintenanceState::faulted) {
            state_ = MaintenanceState::stopped;
        }
    }
}

} // namespace glyphastore
