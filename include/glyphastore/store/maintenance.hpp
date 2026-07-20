#pragma once

#include "glyphastore/store/maintenance_types.hpp"
#include "glyphastore/store/store.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>

namespace glyphastore {

// Store-owned optional scheduler. Phase 2: normal + pressure budgets; emergency deferred.
class MaintenanceController final {
  public:
    using CompactCallback = std::function<Result<CompactionResult>()>;
    using ObserveCallback = std::function<Result<MaintenanceObservation>()>;

    explicit MaintenanceController(MaintenanceConfig config);
    ~MaintenanceController();

    MaintenanceController(const MaintenanceController&) = delete;
    auto operator=(const MaintenanceController&) -> MaintenanceController& = delete;

    void bind_compact(CompactCallback compact);
    void bind_observe(ObserveCallback observe);
    void set_auto_compact_enabled(bool enabled) noexcept;
    void start();
    void request_stop() noexcept;
    void join();
    void stop();
    void request_evaluate();

    [[nodiscard]] auto snapshot() const -> MaintenanceSnapshot;
    [[nodiscard]] auto thread_running() const noexcept -> bool;

  private:
    void run(std::stop_token stop_token);
    [[nodiscard]] auto eval_interval_locked() const -> std::chrono::milliseconds;
    void evaluate_once();
    void record_skip(MaintenanceSkipReason reason, MaintenanceState next,
                     MaintenanceActivationReason activation);

    MaintenanceConfig config_;
    CompactCallback compact_;
    ObserveCallback observe_;
    bool auto_compact_enabled_{true};
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    bool stop_requested_{};
    bool evaluate_requested_{};
    MaintenanceState state_{MaintenanceState::stopped};
    MaintenancePressureLevel pressure_{MaintenancePressureLevel::none};
    MaintenanceActivationReason last_activation_reason_{MaintenanceActivationReason::none};
    std::uint64_t evaluation_cycles_{};
    std::uint64_t compact_attempts_{};
    std::uint64_t compact_completed_{};
    std::uint64_t useful_compactions_{};
    std::uint64_t skips_{};
    std::uint64_t suspend_count_{};
    std::uint64_t consecutive_no_gain_{};
    std::uint64_t bytes_copied_window_{};
    std::uint64_t total_bytes_copied_{};
    std::uint64_t last_bytes_copied_{};
    std::uint64_t last_records_copied_{};
    std::uint64_t last_eval_duration_ns_{};
    std::uint64_t last_compact_duration_ns_{};
    std::optional<std::chrono::steady_clock::time_point> last_useful_at_{};
    MaintenanceSkipReason last_skip_reason_{MaintenanceSkipReason::none};
    MaintenanceObservation last_observation_{};
    std::optional<Error> last_error_{};
    std::jthread worker_;
};

} // namespace glyphastore
