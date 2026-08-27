#include "glyphastore/store/maintenance.hpp"

#include "glyphastore/core/integer_math.hpp"
#include "glyphastore/core/types.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace glyphastore {
namespace {

[[nodiscard]] auto clamp_interval_ms(const MaintenanceConfig& config, const bool use_min, const bool use_max)
    -> std::uint32_t {
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

[[nodiscard]] auto ceil_percentage(const std::size_t total, const std::uint32_t percentage) noexcept
    -> std::size_t {
    const auto whole = total / 100U;
    const auto remainder = total % 100U;
    return whole * percentage + (remainder * percentage + 99U) / 100U;
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
            ceil_percentage(observation.max_segment_count, config.segment_count_pressure_pct);
        if (observation.segment_count >= threshold) {
            pressure = true;
        }
    }
    if (observation.available_free_bytes.has_value()) {
        const auto watermark =
            saturating_add(observation.reserved_free_bytes, config.free_bytes_pressure_margin);
        if (*observation.available_free_bytes <= watermark) {
            pressure = true;
        }
    }
    return pressure ? MaintenancePressureLevel::pressure : MaintenancePressureLevel::normal;
}

auto scheduling_dead_byte_ratio_bp(const MaintenanceObservation& observation) noexcept
    -> std::optional<std::uint32_t> {
    if (!observation.candidate_dead_byte_ratio_bp || observation.candidate_sealed_record_bytes == 0) {
        return observation.candidate_dead_byte_ratio_bp;
    }
    const auto effective_dead = saturating_add(observation.candidate_dead_record_bytes,
                                               observation.candidate_unread_expired_sealed_record_bytes);
    const auto capped = std::min(effective_dead, observation.candidate_sealed_record_bytes);
    return basis_points(capped, observation.candidate_sealed_record_bytes);
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
    if (config.max_segments_per_cycle != 1) {
        return fail(ErrorCode::invalid_argument,
                    "persistence v1 requires maintenance max_segments_per_cycle to equal one");
    }
    if (config.segment_count_pressure_pct == 0 || config.segment_count_pressure_pct > 100) {
        return fail(ErrorCode::invalid_argument, "maintenance segment_count_pressure_pct must be in 1..100");
    }
    if (config.dead_byte_ratio_bp_normal > 10'000) {
        return fail(ErrorCode::invalid_argument, "maintenance dead_byte_ratio_bp_normal must be <= 10000");
    }
    if (config.suspend_on_p99_min_samples == 0) {
        return fail(ErrorCode::invalid_argument,
                    "maintenance suspend_on_p99_min_samples must be greater than zero");
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
    auto thread = std::thread([this] { run(); });
    bool stopped_during_start = false;
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_) {
            state_ = MaintenanceState::stopped;
            stopped_during_start = true;
        } else {
            worker_ = std::move(thread);
        }
    }
    if (stopped_during_start) {
        wake_.notify_all();
        thread.join();
        return;
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
        latency_guard_active_ = false;
        latency_deferral_started_.reset();
        clear_no_gain_backoff_locked();
        publish_mutations_rejected_locked(false);
    }
    wake_.notify_all();
}

void MaintenanceController::join() {
    std::thread local;
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

void MaintenanceController::report_foreground_latency(const std::uint64_t latency_ns) noexcept {
    if (config_.suspend_on_p99_latency_ms == 0) {
        return;
    }
    for (std::size_t index = 0; index < kForegroundLatencyBoundsNs.size(); ++index) {
        if (latency_ns <= kForegroundLatencyBoundsNs[index]) {
            foreground_latency_buckets_[index].fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
}

auto MaintenanceController::consume_foreground_latency_window_locked() noexcept
    -> std::pair<std::uint64_t, std::uint64_t> {
    std::array<std::uint64_t, kForegroundLatencyBoundsNs.size()> counts{};
    std::uint64_t samples{};
    for (std::size_t index = 0; index < counts.size(); ++index) {
        counts[index] = foreground_latency_buckets_[index].exchange(0, std::memory_order_acq_rel);
        samples = counts[index] > std::numeric_limits<std::uint64_t>::max() - samples
                      ? std::numeric_limits<std::uint64_t>::max()
                      : samples + counts[index];
    }
    if (samples == 0) {
        return {0, 0};
    }
    const auto rank = samples - samples / 100U; // ceil(0.99 * samples), without overflow
    std::uint64_t cumulative{};
    for (std::size_t index = 0; index < counts.size(); ++index) {
        cumulative = counts[index] > std::numeric_limits<std::uint64_t>::max() - cumulative
                         ? std::numeric_limits<std::uint64_t>::max()
                         : cumulative + counts[index];
        if (cumulative >= rank) {
            return {samples, kForegroundLatencyBoundsNs[index]};
        }
    }
    return {samples, std::numeric_limits<std::uint64_t>::max()};
}

auto MaintenanceController::snapshot() const -> MaintenanceSnapshot {
    const std::lock_guard lock{mutex_};
    std::uint64_t since_useful = 0;
    if (last_useful_at_) {
        since_useful = elapsed_ns(*last_useful_at_);
    }
    const auto latency_deferral_age = latency_deferral_started_ ? elapsed_ns(*latency_deferral_started_) : 0;
    const auto snapshot_now = std::chrono::steady_clock::now();
    const auto no_gain_retry_after =
        no_gain_retry_at_ && snapshot_now < *no_gain_retry_at_
            ? static_cast<std::uint64_t>(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(*no_gain_retry_at_ - snapshot_now)
                      .count())
            : 0;
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
        .sequence_conflicts = sequence_conflicts_,
        .skips = skips_,
        .suspend_count = suspend_count_,
        .consecutive_no_gain = consecutive_no_gain_,
        .no_gain_scans_suppressed = no_gain_scans_suppressed_,
        .no_gain_retry_after_ns = no_gain_retry_after,
        .bytes_copied_window = bytes_copied_window_,
        .total_bytes_copied = total_bytes_copied_,
        .last_bytes_copied = last_bytes_copied_,
        .last_records_copied = last_records_copied_,
        .last_expired_records_dropped = last_expired_records_dropped_,
        .total_expired_records_dropped = total_expired_records_dropped_,
        .last_no_gain_source_records_verified = last_no_gain_source_records_verified_,
        .last_no_gain_source_bytes_verified = last_no_gain_source_bytes_verified_,
        .last_no_gain_expired_records_dropped = last_no_gain_expired_records_dropped_,
        .total_no_gain_source_records_verified = total_no_gain_source_records_verified_,
        .total_no_gain_source_bytes_verified = total_no_gain_source_bytes_verified_,
        .total_no_gain_expired_records_dropped = total_no_gain_expired_records_dropped_,
        .last_eval_duration_ns = last_eval_duration_ns_,
        .last_compact_duration_ns = last_compact_duration_ns_,
        .last_compaction_pacing_delay_ns = last_compaction_pacing_delay_ns_,
        .total_compaction_pacing_delay_ns = total_compaction_pacing_delay_ns_,
        .last_compaction_pacing_sleep_count = last_compaction_pacing_sleep_count_,
        .last_compaction_pacing_burst_bytes = last_compaction_pacing_burst_bytes_,
        .ns_since_last_useful_compaction = since_useful,
        .rate_window_bytes_copied = rate_window_bytes_copied_,
        .rate_window_cpu_ns = rate_window_cpu_ns_,
        .foreground_latency_samples = foreground_latency_samples_,
        .last_foreground_p99_ns = last_foreground_p99_ns_,
        .latency_suspends = latency_suspends_,
        .latency_guard_active = latency_guard_active_,
        .latency_deferral_age_ns = latency_deferral_age,
        .latency_debt_overrides = latency_debt_overrides_,
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

auto MaintenanceController::compaction_copy_rate_limit() const noexcept -> std::uint64_t {
    const std::lock_guard lock{mutex_};
    return aggressive_pressure(pressure_) ? 0U : config_.max_copy_bytes_per_sec;
}

void MaintenanceController::publish_mutations_rejected(const bool rejected) noexcept {
    std::lock_guard lock{mutex_};
    publish_mutations_rejected_locked(rejected);
}

void MaintenanceController::publish_mutations_rejected_locked(const bool rejected) noexcept {
    // Once stop is requested, never re-arm the gate (in-flight eval must not undo request_stop).
    if (stop_requested_) {
        mutations_rejected_.store(false, std::memory_order_release);
        return;
    }
    mutations_rejected_.store(rejected, std::memory_order_release);
}

void MaintenanceController::refresh_rate_window_locked(
    const std::chrono::steady_clock::time_point now) noexcept {
    constexpr auto kWindow = std::chrono::seconds{1};
    if (rate_window_start_.time_since_epoch().count() == 0 || now - rate_window_start_ >= kWindow) {
        rate_window_start_ = now;
        rate_window_bytes_copied_ = 0;
        rate_window_cpu_ns_ = 0;
    }
}

auto MaintenanceController::remaining_copy_bytes_locked(const MaintenanceConfig& config) const noexcept
    -> std::optional<std::uint64_t> {
    if (config.max_copy_bytes_per_sec == 0) {
        return std::nullopt;
    }
    if (rate_window_bytes_copied_ >= config.max_copy_bytes_per_sec) {
        return 0;
    }
    return config.max_copy_bytes_per_sec - rate_window_bytes_copied_;
}

auto MaintenanceController::cpu_budget_exhausted_locked(const MaintenanceConfig& config) const noexcept
    -> bool {
    if (config.max_cpu_ms_per_window == 0) {
        return false;
    }
    const auto budget_ns = static_cast<std::uint64_t>(config.max_cpu_ms_per_window) * 1'000'000ULL;
    return rate_window_cpu_ns_ >= budget_ns;
}

auto MaintenanceController::same_no_gain_candidate(const MaintenanceObservation& left,
                                                   const MaintenanceObservation& right) noexcept -> bool {
    return left.durable == right.durable && left.sealed_segment_count == right.sealed_segment_count &&
           left.compaction_candidate_worker == right.compaction_candidate_worker &&
           left.candidate_sealed_record_bytes == right.candidate_sealed_record_bytes &&
           left.candidate_live_record_bytes == right.candidate_live_record_bytes &&
           left.candidate_dead_record_bytes == right.candidate_dead_record_bytes &&
           left.candidate_dead_byte_ratio_bp == right.candidate_dead_byte_ratio_bp &&
           left.candidate_scheduling_dead_byte_ratio_bp == right.candidate_scheduling_dead_byte_ratio_bp &&
           left.candidate_unread_expired_sealed_record_count ==
               right.candidate_unread_expired_sealed_record_count &&
           left.candidate_unread_expired_sealed_record_bytes ==
               right.candidate_unread_expired_sealed_record_bytes;
}

void MaintenanceController::clear_no_gain_backoff_locked() noexcept {
    consecutive_no_gain_ = 0;
    no_gain_candidate_.reset();
    no_gain_retry_at_.reset();
}

auto MaintenanceController::eval_interval_locked() const -> std::chrono::milliseconds {
    const bool under_pressure = aggressive_pressure(pressure_);
    const bool backoff = !under_pressure && consecutive_no_gain_ >= config_.max_no_gain_attempts &&
                         config_.max_no_gain_attempts > 0;
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
    {
        const std::lock_guard lock{mutex_};
        if (stop_requested_) {
            return;
        }
        observe = observe_;
        compact = compact_;
        auto_compact = auto_compact_enabled_;
        config = config_;
        bytes_copied_window_ = 0;
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
        auto observed = observe({});
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
        const bool probe_unread_ttl =
            observe && observation.durable && observation.compaction_candidate_worker.has_value() &&
            ((config.unread_ttl_pressure_probe && aggressive_pressure(pressure_)) ||
             (config.unread_ttl_normal_scheduling && pressure_ == MaintenancePressureLevel::normal));
        if (probe_unread_ttl) {
            if (auto probed = observe(MaintenanceObserveRequest{.probe_unread_expired_ttl = true})) {
                observation.unread_ttl_probe_performed = probed->unread_ttl_probe_performed;
                observation.candidate_unread_expired_sealed_record_count =
                    probed->candidate_unread_expired_sealed_record_count;
                observation.candidate_unread_expired_sealed_record_bytes =
                    probed->candidate_unread_expired_sealed_record_bytes;
                const std::lock_guard lock{mutex_};
                last_observation_ = observation;
            } else {
                const std::lock_guard lock{mutex_};
                last_error_ = probed.error();
                last_eval_duration_ns_ = elapsed_ns(eval_started);
                if (probed.error().code == ErrorCode::unavailable) {
                    publish_mutations_rejected_locked(false);
                    record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle,
                                MaintenanceActivationReason::none);
                    return;
                }
                state_ = MaintenanceState::faulted;
                const bool gate_armed = mutations_rejected_.load(std::memory_order_relaxed) ||
                                        pressure_ == MaintenancePressureLevel::emergency;
                if (!gate_armed) {
                    auto_compact_enabled_ = false;
                }
                return;
            }
        }
        observation.candidate_scheduling_dead_byte_ratio_bp = scheduling_dead_byte_ratio_bp(observation);
        {
            const std::lock_guard lock{mutex_};
            last_observation_.candidate_scheduling_dead_byte_ratio_bp =
                observation.candidate_scheduling_dead_byte_ratio_bp;
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
    std::uint64_t foreground_samples{};
    std::uint64_t foreground_p99_ns{};
    {
        const std::lock_guard lock{mutex_};
        const auto window = consume_foreground_latency_window_locked();
        foreground_samples = window.first;
        foreground_p99_ns = window.second;
        foreground_latency_samples_ = foreground_samples;
        last_foreground_p99_ns_ = foreground_p99_ns;
    }

    if (observation.durable && observation.sealed_segment_count == 0) {
        const std::lock_guard lock{mutex_};
        latency_guard_active_ = false;
        latency_deferral_started_.reset();
        clear_no_gain_backoff_locked();
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        record_skip(MaintenanceSkipReason::no_candidate, MaintenanceState::idle,
                    pressure == MaintenancePressureLevel::emergency
                        ? MaintenanceActivationReason::emergency_capacity
                        : MaintenanceActivationReason::no_candidate);
        return;
    }

    if (!under_pressure && observation.durable) {
        const auto scheduling_bp = observation.candidate_scheduling_dead_byte_ratio_bp
                                       ? *observation.candidate_scheduling_dead_byte_ratio_bp
                                       : observation.candidate_dead_byte_ratio_bp;
        if (scheduling_bp && *scheduling_bp < config.dead_byte_ratio_bp_normal) {
            const std::lock_guard lock{mutex_};
            latency_guard_active_ = false;
            latency_deferral_started_.reset();
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            record_skip(MaintenanceSkipReason::reclaim_threshold, MaintenanceState::idle,
                        MaintenanceActivationReason::reclaim_threshold);
            return;
        }
    }

    bool latency_debt_override{};
    {
        const auto now = std::chrono::steady_clock::now();
        const std::lock_guard lock{mutex_};
        const bool enough_samples = foreground_samples >= config.suspend_on_p99_min_samples;
        if (under_pressure || config.suspend_on_p99_latency_ms == 0 || !enough_samples) {
            latency_guard_active_ = false;
            latency_deferral_started_.reset();
        } else {
            const auto suspend_ns =
                static_cast<std::uint64_t>(config.suspend_on_p99_latency_ms) * 1'000'000ULL;
            const auto resume_ns = std::max<std::uint64_t>(1'000'000ULL, suspend_ns - suspend_ns / 5U);
            if (!latency_guard_active_ && foreground_p99_ns >= suspend_ns) {
                latency_guard_active_ = true;
                latency_deferral_started_ = now;
            } else if (latency_guard_active_ && foreground_p99_ns < resume_ns) {
                latency_guard_active_ = false;
                latency_deferral_started_.reset();
            }

            if (latency_guard_active_) {
                const auto maximum_deferral_ns =
                    static_cast<std::uint64_t>(config.max_latency_deferral_ms) * 1'000'000ULL;
                const bool debt_due = maximum_deferral_ns != 0 && latency_deferral_started_ &&
                                      elapsed_ns(*latency_deferral_started_) >= maximum_deferral_ns;
                if (debt_due) {
                    latency_debt_override = true;
                    ++latency_debt_overrides_;
                    latency_guard_active_ = false;
                    latency_deferral_started_.reset();
                } else {
                    ++latency_suspends_;
                    last_eval_duration_ns_ = elapsed_ns(eval_started);
                    record_skip(MaintenanceSkipReason::latency_budget, MaintenanceState::suspended,
                                MaintenanceActivationReason::latency_budget);
                    return;
                }
            }
        }
    }

    // An exact no-gain result is deterministic while the selected Worker's
    // physical candidate is unchanged. Do not repeatedly rescan it. A changed
    // candidate invalidates immediately; the maximum evaluation interval
    // provides a bounded TTL/time-based retry. Capacity pressure always bypasses.
    {
        const auto now = std::chrono::steady_clock::now();
        const std::lock_guard lock{mutex_};
        if (no_gain_candidate_ && !same_no_gain_candidate(*no_gain_candidate_, observation)) {
            clear_no_gain_backoff_locked();
        }
        if (no_gain_candidate_ && no_gain_retry_at_ && now >= *no_gain_retry_at_) {
            clear_no_gain_backoff_locked();
        }
        if (!under_pressure && config.max_no_gain_attempts > 0 && no_gain_candidate_ && no_gain_retry_at_ &&
            now < *no_gain_retry_at_ && consecutive_no_gain_ >= config.max_no_gain_attempts) {
            ++no_gain_scans_suppressed_;
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            record_skip(MaintenanceSkipReason::budget, MaintenanceState::suspended,
                        MaintenanceActivationReason::budget_backoff);
            return;
        }
    }

    if (!under_pressure && observation.durable && config.max_copy_bytes_per_cycle > 0 &&
        observation.candidate_live_record_bytes > config.max_copy_bytes_per_cycle) {
        const std::lock_guard lock{mutex_};
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        record_skip(MaintenanceSkipReason::copy_budget, MaintenanceState::suspended,
                    MaintenanceActivationReason::copy_budget);
        return;
    }

    std::uint64_t max_copy_bytes = under_pressure ? 0 : config.max_copy_bytes_per_cycle;
    if (!under_pressure) {
        const std::lock_guard lock{mutex_};
        refresh_rate_window_locked(std::chrono::steady_clock::now());
        if (cpu_budget_exhausted_locked(config)) {
            last_eval_duration_ns_ = elapsed_ns(eval_started);
            record_skip(MaintenanceSkipReason::rate_budget, MaintenanceState::suspended,
                        MaintenanceActivationReason::rate_budget);
            return;
        }
        if (const auto remaining = remaining_copy_bytes_locked(config)) {
            if (*remaining == 0) {
                last_eval_duration_ns_ = elapsed_ns(eval_started);
                record_skip(MaintenanceSkipReason::rate_budget, MaintenanceState::suspended,
                            MaintenanceActivationReason::rate_budget);
                return;
            }
            // The rate is enforced continuously by bounded pre-intent write
            // grants. Do not reinterpret the current-window remainder as a
            // whole-transaction copy cap: that would permanently starve every
            // candidate larger than one second of configured bandwidth.
            // A later candidate in an already-consumed window still waits for
            // refresh, preventing adjacent compactions from each taking a new
            // initial burst.
            if (rate_window_bytes_copied_ != 0U && observation.durable &&
                observation.candidate_live_record_bytes > *remaining) {
                last_eval_duration_ns_ = elapsed_ns(eval_started);
                record_skip(MaintenanceSkipReason::rate_budget, MaintenanceState::suspended,
                            MaintenanceActivationReason::rate_budget);
                return;
            }
        }
    }

    MaintenanceActivationReason activation = MaintenanceActivationReason::scheduled;
    if (pressure == MaintenancePressureLevel::emergency) {
        activation = MaintenanceActivationReason::emergency_capacity;
    } else if (pressure == MaintenancePressureLevel::pressure) {
        if (observation.available_free_bytes.has_value() &&
            *observation.available_free_bytes <=
                saturating_add(observation.reserved_free_bytes, config.free_bytes_pressure_margin)) {
            activation = MaintenanceActivationReason::free_space_pressure;
        } else {
            activation = MaintenanceActivationReason::segment_pressure;
        }
    } else if (latency_debt_override) {
        activation = MaintenanceActivationReason::latency_debt_override;
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
    auto result = compact(observation.compaction_candidate_worker, max_copy_bytes);
    const auto compact_ns = elapsed_ns(compact_started);
    {
        const std::lock_guard lock{mutex_};
        last_compact_duration_ns_ = compact_ns;
        last_eval_duration_ns_ = elapsed_ns(eval_started);
        refresh_rate_window_locked(std::chrono::steady_clock::now());
        if (!under_pressure) {
            rate_window_cpu_ns_ = compact_ns > std::numeric_limits<std::uint64_t>::max() - rate_window_cpu_ns_
                                      ? std::numeric_limits<std::uint64_t>::max()
                                      : rate_window_cpu_ns_ + compact_ns;
        }
        if (!result) {
            last_error_ = result.error();
            if (result.error().code == ErrorCode::unavailable) {
                publish_mutations_rejected_locked(false);
                record_skip(MaintenanceSkipReason::store_closed, MaintenanceState::idle,
                            MaintenanceActivationReason::none);
            } else if (result.error().code == ErrorCode::sequence_conflict) {
                ++sequence_conflicts_;
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
        last_expired_records_dropped_ = result->expired_records_dropped;
        last_compaction_pacing_delay_ns_ = result->pacing_delay_ns;
        last_compaction_pacing_sleep_count_ = result->pacing_sleep_count;
        last_compaction_pacing_burst_bytes_ = result->pacing_burst_bytes;
        total_compaction_pacing_delay_ns_ =
            result->pacing_delay_ns >
                    std::numeric_limits<std::uint64_t>::max() - total_compaction_pacing_delay_ns_
                ? std::numeric_limits<std::uint64_t>::max()
                : total_compaction_pacing_delay_ns_ + result->pacing_delay_ns;
        if (!under_pressure) {
            rate_window_bytes_copied_ =
                result->bytes_copied > std::numeric_limits<std::uint64_t>::max() - rate_window_bytes_copied_
                    ? std::numeric_limits<std::uint64_t>::max()
                    : rate_window_bytes_copied_ + result->bytes_copied;
        }
        if (!result->compacted) {
            ++consecutive_no_gain_;
            last_no_gain_source_records_verified_ = result->source_records_verified;
            last_no_gain_source_bytes_verified_ = result->source_bytes_verified;
            last_no_gain_expired_records_dropped_ = result->expired_records_dropped;
            total_no_gain_source_records_verified_ += result->source_records_verified;
            total_no_gain_source_bytes_verified_ += result->source_bytes_verified;
            total_no_gain_expired_records_dropped_ += result->expired_records_dropped;
            if (config.max_no_gain_attempts > 0 && consecutive_no_gain_ >= config.max_no_gain_attempts) {
                no_gain_candidate_ = observation;
                no_gain_retry_at_ =
                    std::chrono::steady_clock::now() + std::chrono::milliseconds{config.max_eval_interval_ms};
            }
            record_skip(MaintenanceSkipReason::no_gain, MaintenanceState::idle, activation);
            return;
        }
        ++compact_completed_;
        ++useful_compactions_;
        clear_no_gain_backoff_locked();
        bytes_copied_window_ = result->bytes_copied;
        total_bytes_copied_ += result->bytes_copied;
        total_expired_records_dropped_ += result->expired_records_dropped;
        last_useful_at_ = std::chrono::steady_clock::now();
        last_skip_reason_ = MaintenanceSkipReason::none;
        last_error_.reset();
        state_ = stop_requested_ ? MaintenanceState::stopped : MaintenanceState::idle;
    }
}

void MaintenanceController::run() {
    for (;;) {
        {
            std::unique_lock lock{mutex_};
            if (stop_requested_) {
                break;
            }
            if (state_ != MaintenanceState::suspended && state_ != MaintenanceState::faulted) {
                state_ = MaintenanceState::idle;
            }
            const auto deadline = std::chrono::steady_clock::now() + eval_interval_locked();
            wake_.wait_until(lock, deadline, [&] { return stop_requested_ || evaluate_requested_; });
            evaluate_requested_ = false;
            if (stop_requested_) {
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
