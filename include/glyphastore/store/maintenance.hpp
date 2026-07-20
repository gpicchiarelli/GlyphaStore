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

// Store-owned optional scheduler. Phase 1 background mode may invoke Store::compact()
// under normal-policy budgets (no pressure/emergency yet).
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
    // White-box: force-disable automatic compact while keeping the eval thread (tests).
    void set_auto_compact_enabled(bool enabled) noexcept;
    void start();
    // Stop admission to the controller without joining (Store::close waits for ops first).
    void request_stop() noexcept;
    void join();
    void stop();
    void request_evaluate();

    [[nodiscard]] auto snapshot() const -> MaintenanceSnapshot;
    [[nodiscard]] auto thread_running() const noexcept -> bool;

  private:
    void run(std::stop_token stop_token);
    [[nodiscard]] auto eval_interval() const -> std::chrono::milliseconds;
    void evaluate_once();
    void record_skip(MaintenanceSkipReason reason, MaintenanceState next);

    MaintenanceConfig config_;
    CompactCallback compact_;
    ObserveCallback observe_;
    bool auto_compact_enabled_{true};
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    bool stop_requested_{};
    bool evaluate_requested_{};
    MaintenanceState state_{MaintenanceState::stopped};
    std::uint64_t evaluation_cycles_{};
    std::uint64_t compact_attempts_{};
    std::uint64_t compact_completed_{};
    std::uint64_t skips_{};
    std::uint64_t consecutive_no_gain_{};
    std::uint64_t bytes_copied_window_{};
    MaintenanceSkipReason last_skip_reason_{MaintenanceSkipReason::none};
    MaintenanceObservation last_observation_{};
    std::optional<Error> last_error_{};
    std::jthread worker_;
};

} // namespace glyphastore
