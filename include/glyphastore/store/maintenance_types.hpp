#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/store/config.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace glyphastore {

enum class MaintenanceState : std::uint8_t {
    stopped,
    idle,
    evaluating,
    compacting,
    suspended,
    draining,
    faulted,
};

enum class MaintenanceSkipReason : std::uint8_t {
    none,
    mode_disabled,
    mode_cooperative,
    no_gain,
    no_candidate,
    budget,
    store_closed,
    sequence_conflict,
    policy_deferred,
    reclaim_threshold,
    copy_budget,
    rate_budget,
};

enum class MaintenancePressureLevel : std::uint8_t {
    none,
    normal,
    pressure,
    emergency,
};

enum class MaintenanceActivationReason : std::uint8_t {
    none,
    scheduled,
    sealed_history,
    segment_pressure,
    free_space_pressure,
    emergency_capacity,
    budget_backoff,
    policy_deferred,
    no_candidate,
    reclaim_threshold,
    copy_budget,
    rate_budget,
};

struct MaintenanceObserveRequest {
    // When true, the durable runtime may read sealed source Records for the
    // round-robin candidate to count Index-resident puts expired at Store time
    // that have not yet been visited by GET or compaction.
    bool probe_unread_expired_ttl{};
};

// Read-only physical observation for scheduling. Candidate byte counters are
// exact published Record extents. Unread TTL probes are optional and run only
// when explicitly requested (pressure/emergency or opt-in normal scheduling).
struct MaintenanceObservation {
    bool durable{};
    std::size_t segment_count{};
    std::size_t sealed_segment_count{};
    std::optional<std::size_t> compaction_candidate_worker{};
    std::uint64_t candidate_sealed_record_bytes{};
    std::uint64_t candidate_live_record_bytes{};
    std::uint64_t candidate_dead_record_bytes{};
    std::optional<std::uint32_t> candidate_dead_byte_ratio_bp{};
    // Inclusive dead-byte ratio used for normal reclaim_threshold decisions.
    // Equals candidate_dead_byte_ratio_bp unless a probe counted unread expired
    // sealed puts and unread_ttl_normal_scheduling is enabled.
    std::optional<std::uint32_t> candidate_scheduling_dead_byte_ratio_bp{};
    bool unread_ttl_probe_performed{};
    std::uint64_t candidate_unread_expired_sealed_record_count{};
    std::uint64_t candidate_unread_expired_sealed_record_bytes{};
    std::size_t max_segment_count{};
    std::uint64_t reserved_free_bytes{};
    // Bytes required in addition to reserved_free_bytes to create/rotate one Segment
    // (typically kSegmentSizeBytes + next manifest size). Zero means "use kSegmentSizeBytes".
    std::uint64_t rotate_additional_bytes{};
    std::optional<std::uint64_t> available_free_bytes{};
};

// Runtime-local rotation telemetry. Publication wait includes time acquiring
// the Manifest serializer and any condition wait for an active compaction
// lease; execution begins only after that authority is available.
struct DurableRotationStats {
    std::uint64_t attempts{};
    std::uint64_t committed{};
    std::uint64_t compaction_waits{};
    std::uint64_t final_record_commit_attempts{};
    std::uint64_t final_record_commits{};
    std::uint64_t last_publication_wait_duration_ns{};
    std::uint64_t total_publication_wait_duration_ns{};
    std::uint64_t maximum_publication_wait_duration_ns{};
    std::uint64_t last_seal_duration_ns{};
    std::uint64_t total_seal_duration_ns{};
    std::uint64_t maximum_seal_duration_ns{};
    std::uint64_t last_create_duration_ns{};
    std::uint64_t total_create_duration_ns{};
    std::uint64_t maximum_create_duration_ns{};
    std::uint64_t last_manifest_publication_duration_ns{};
    std::uint64_t total_manifest_publication_duration_ns{};
    std::uint64_t maximum_manifest_publication_duration_ns{};
    std::uint64_t last_execution_duration_ns{};
    std::uint64_t total_execution_duration_ns{};
    std::uint64_t maximum_execution_duration_ns{};
    std::uint64_t last_total_duration_ns{};
    std::uint64_t total_duration_ns{};
    std::uint64_t maximum_total_duration_ns{};
    std::uint64_t last_final_record_commit_duration_ns{};
    std::uint64_t total_final_record_commit_duration_ns{};
    std::uint64_t maximum_final_record_commit_duration_ns{};
};

struct MaintenanceSnapshot {
    MaintenanceState state{MaintenanceState::stopped};
    bool thread_running{};
    MaintenanceMode mode{MaintenanceMode::cooperative};
    MaintenancePressureLevel pressure{MaintenancePressureLevel::none};
    MaintenanceActivationReason last_activation_reason{MaintenanceActivationReason::none};
    bool mutations_rejected{};
    std::uint64_t evaluation_cycles{};
    std::uint64_t compact_attempts{};
    std::uint64_t compact_completed{};
    std::uint64_t useful_compactions{};
    std::uint64_t sequence_conflicts{};
    std::uint64_t skips{};
    std::uint64_t suspend_count{};
    std::uint64_t consecutive_no_gain{};
    // Bytes copied by the current/most recently completed evaluation cycle.
    std::uint64_t bytes_copied_window{};
    std::uint64_t total_bytes_copied{};
    std::uint64_t last_bytes_copied{};
    std::uint64_t last_records_copied{};
    std::uint64_t last_expired_records_dropped{};
    std::uint64_t total_expired_records_dropped{};
    // Exact Index-referenced sealed Records examined by the most recent no-gain
    // planning decision (DurableCompactionOutcome::not_beneficial). Cheap policy
    // skips such as reclaim_threshold or copy_budget do not update these fields.
    std::uint64_t last_no_gain_source_records_verified{};
    std::uint64_t last_no_gain_source_bytes_verified{};
    std::uint64_t last_no_gain_expired_records_dropped{};
    std::uint64_t total_no_gain_source_records_verified{};
    std::uint64_t total_no_gain_source_bytes_verified{};
    std::uint64_t total_no_gain_expired_records_dropped{};
    std::uint64_t last_eval_duration_ns{};
    std::uint64_t last_compact_duration_ns{};
    std::uint64_t ns_since_last_useful_compaction{};
    // Current one-second rate-limit window consumption (zero when unlimited).
    std::uint64_t rate_window_bytes_copied{};
    std::uint64_t rate_window_cpu_ns{};
    DurableRotationStats rotation{};
    MaintenanceSkipReason last_skip_reason{MaintenanceSkipReason::none};
    MaintenanceObservation last_observation{};
    std::optional<Error> last_error{};
};

[[nodiscard]] auto validate_maintenance_config(const MaintenanceConfig& config) -> Status;

[[nodiscard]] auto classify_maintenance_pressure(const MaintenanceObservation& observation,
                                                 const MaintenanceConfig& config) noexcept
    -> MaintenancePressureLevel;

[[nodiscard]] auto scheduling_dead_byte_ratio_bp(const MaintenanceObservation& observation) noexcept
    -> std::optional<std::uint32_t>;

// Shared diagnostic text for Store::put/erase rejected under emergency.
inline constexpr std::string_view kMaintenanceEmergencyMutationMessage =
    "maintenance emergency: insufficient capacity to create or rotate a Segment";

} // namespace glyphastore
