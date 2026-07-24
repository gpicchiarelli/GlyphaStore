#include "server/server_stats.hpp"

#include "glyphastore/server/latency_histogram.hpp"

#include <string>
#include <utility>

namespace glyphastore::server {

auto ServerStatsReporter::maintenance_state_name(const MaintenanceState state) noexcept
    -> std::string_view {
    switch (state) {
    case MaintenanceState::stopped:
        return "stopped";
    case MaintenanceState::idle:
        return "idle";
    case MaintenanceState::evaluating:
        return "evaluating";
    case MaintenanceState::compacting:
        return "compacting";
    case MaintenanceState::suspended:
        return "suspended";
    case MaintenanceState::draining:
        return "draining";
    case MaintenanceState::faulted:
        return "faulted";
    }
    return "unknown";
}

auto ServerStatsReporter::maintenance_pressure_name(const MaintenancePressureLevel level) noexcept
    -> std::string_view {
    switch (level) {
    case MaintenancePressureLevel::none:
        return "none";
    case MaintenancePressureLevel::normal:
        return "normal";
    case MaintenancePressureLevel::pressure:
        return "pressure";
    case MaintenancePressureLevel::emergency:
        return "emergency";
    }
    return "unknown";
}

auto ServerStatsReporter::maintenance_skip_reason_name(const MaintenanceSkipReason reason) noexcept
    -> std::string_view {
    switch (reason) {
    case MaintenanceSkipReason::none:
        return "none";
    case MaintenanceSkipReason::mode_disabled:
        return "mode_disabled";
    case MaintenanceSkipReason::mode_cooperative:
        return "mode_cooperative";
    case MaintenanceSkipReason::no_gain:
        return "no_gain";
    case MaintenanceSkipReason::no_candidate:
        return "no_candidate";
    case MaintenanceSkipReason::budget:
        return "budget";
    case MaintenanceSkipReason::store_closed:
        return "store_closed";
    case MaintenanceSkipReason::sequence_conflict:
        return "sequence_conflict";
    case MaintenanceSkipReason::policy_deferred:
        return "policy_deferred";
    case MaintenanceSkipReason::reclaim_threshold:
        return "reclaim_threshold";
    case MaintenanceSkipReason::copy_budget:
        return "copy_budget";
    case MaintenanceSkipReason::rate_budget:
        return "rate_budget";
    }
    return "unknown";
}

auto ServerStatsReporter::render(const ServerStatsSnapshot& snapshot, const std::size_t maximum_bytes)
    -> Result<std::string> {
    try {
        std::string out;
        out.reserve(4096);
        out += "GlyphaStore/stats\n";
#ifndef GLYPHASTORE_VERSION
#define GLYPHASTORE_VERSION "dev"
#endif
        out += "version=";
        out += GLYPHASTORE_VERSION;
        out += '\n';
        out += snapshot.live ? "live=1\n" : "live=0\n";
        out += snapshot.ready ? "ready=1\n" : "ready=0\n";
        out += "executors=";
        out += std::to_string(snapshot.executors.size());
        out += '\n';

        std::uint64_t connections_active{};
        std::uint64_t connections_adopted{};
        for (const auto& executor : snapshot.executors) {
            connections_active += executor.active_connections;
            connections_adopted += executor.adopted_connections;
        }
        out += "connections_active=";
        out += std::to_string(connections_active);
        out += '\n';
        out += "connections_adopted=";
        out += std::to_string(connections_adopted);
        out += '\n';

        const auto& maintenance = snapshot.maintenance;
        out += "maintenance_state=";
        out += maintenance_state_name(maintenance.state);
        out += '\n';
        out += "maintenance_pressure=";
        out += maintenance_pressure_name(maintenance.pressure);
        out += '\n';
        out += "mutations_rejected=";
        out += maintenance.mutations_rejected ? "1\n" : "0\n";
        out += "compact_attempts=";
        out += std::to_string(maintenance.compact_attempts);
        out += '\n';
        out += "compact_completed=";
        out += std::to_string(maintenance.compact_completed);
        out += '\n';
        out += "useful_compactions=";
        out += std::to_string(maintenance.useful_compactions);
        out += '\n';
        out += "maintenance_skips=";
        out += std::to_string(maintenance.skips);
        out += '\n';
        out += "maintenance_consecutive_no_gain=";
        out += std::to_string(maintenance.consecutive_no_gain);
        out += '\n';
        out += "maintenance_last_skip_reason=";
        out += maintenance_skip_reason_name(maintenance.last_skip_reason);
        out += '\n';
        out += "maintenance_last_no_gain_source_records_verified=";
        out += std::to_string(maintenance.last_no_gain_source_records_verified);
        out += '\n';
        out += "maintenance_last_no_gain_source_bytes_verified=";
        out += std::to_string(maintenance.last_no_gain_source_bytes_verified);
        out += '\n';
        out += "maintenance_last_no_gain_expired_records_dropped=";
        out += std::to_string(maintenance.last_no_gain_expired_records_dropped);
        out += '\n';
        out += "maintenance_total_no_gain_source_records_verified=";
        out += std::to_string(maintenance.total_no_gain_source_records_verified);
        out += '\n';
        out += "maintenance_total_no_gain_source_bytes_verified=";
        out += std::to_string(maintenance.total_no_gain_source_bytes_verified);
        out += '\n';
        out += "maintenance_total_no_gain_expired_records_dropped=";
        out += std::to_string(maintenance.total_no_gain_expired_records_dropped);
        out += '\n';
        out += "maintenance_sequence_conflicts=";
        out += std::to_string(maintenance.sequence_conflicts);
        out += '\n';
        out += "durable_rotation_attempts=";
        out += std::to_string(maintenance.rotation.attempts);
        out += "\ndurable_rotations_committed=";
        out += std::to_string(maintenance.rotation.committed);
        out += "\ndurable_rotation_compaction_waits=";
        out += std::to_string(maintenance.rotation.compaction_waits);
        out += "\ndurable_rotation_final_record_commit_attempts=";
        out += std::to_string(maintenance.rotation.final_record_commit_attempts);
        out += "\ndurable_rotation_final_record_commits=";
        out += std::to_string(maintenance.rotation.final_record_commits);
        out += "\ndurable_rotation_last_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.last_publication_wait_duration_ns);
        out += "\ndurable_rotation_total_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.total_publication_wait_duration_ns);
        out += "\ndurable_rotation_maximum_publication_wait_ns=";
        out += std::to_string(maintenance.rotation.maximum_publication_wait_duration_ns);
        out += "\ndurable_rotation_last_seal_ns=";
        out += std::to_string(maintenance.rotation.last_seal_duration_ns);
        out += "\ndurable_rotation_total_seal_ns=";
        out += std::to_string(maintenance.rotation.total_seal_duration_ns);
        out += "\ndurable_rotation_maximum_seal_ns=";
        out += std::to_string(maintenance.rotation.maximum_seal_duration_ns);
        out += "\ndurable_rotation_last_create_ns=";
        out += std::to_string(maintenance.rotation.last_create_duration_ns);
        out += "\ndurable_rotation_total_create_ns=";
        out += std::to_string(maintenance.rotation.total_create_duration_ns);
        out += "\ndurable_rotation_maximum_create_ns=";
        out += std::to_string(maintenance.rotation.maximum_create_duration_ns);
        out += "\ndurable_rotation_last_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.last_manifest_publication_duration_ns);
        out += "\ndurable_rotation_total_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.total_manifest_publication_duration_ns);
        out += "\ndurable_rotation_maximum_manifest_publication_ns=";
        out += std::to_string(maintenance.rotation.maximum_manifest_publication_duration_ns);
        out += "\ndurable_rotation_last_execution_ns=";
        out += std::to_string(maintenance.rotation.last_execution_duration_ns);
        out += "\ndurable_rotation_total_execution_ns=";
        out += std::to_string(maintenance.rotation.total_execution_duration_ns);
        out += "\ndurable_rotation_maximum_execution_ns=";
        out += std::to_string(maintenance.rotation.maximum_execution_duration_ns);
        out += "\ndurable_rotation_last_total_ns=";
        out += std::to_string(maintenance.rotation.last_total_duration_ns);
        out += "\ndurable_rotation_total_ns=";
        out += std::to_string(maintenance.rotation.total_duration_ns);
        out += "\ndurable_rotation_maximum_total_ns=";
        out += std::to_string(maintenance.rotation.maximum_total_duration_ns);
        out += "\ndurable_rotation_last_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.last_final_record_commit_duration_ns);
        out += "\ndurable_rotation_total_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.total_final_record_commit_duration_ns);
        out += "\ndurable_rotation_maximum_final_record_commit_ns=";
        out += std::to_string(maintenance.rotation.maximum_final_record_commit_duration_ns);
        out += '\n';
        out += "maintenance_candidate_worker=";
        if (maintenance.last_observation.compaction_candidate_worker) {
            out += std::to_string(*maintenance.last_observation.compaction_candidate_worker);
        } else {
            out += "none";
        }
        out += "\nmaintenance_candidate_sealed_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_sealed_record_bytes);
        out += "\nmaintenance_candidate_live_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_live_record_bytes);
        out += "\nmaintenance_candidate_dead_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_dead_record_bytes);
        out += "\nmaintenance_candidate_dead_byte_ratio_bp=";
        if (maintenance.last_observation.candidate_dead_byte_ratio_bp) {
            out += std::to_string(*maintenance.last_observation.candidate_dead_byte_ratio_bp);
        } else {
            out += "none";
        }
        out += "\nmaintenance_candidate_scheduling_dead_byte_ratio_bp=";
        if (maintenance.last_observation.candidate_scheduling_dead_byte_ratio_bp) {
            out += std::to_string(*maintenance.last_observation.candidate_scheduling_dead_byte_ratio_bp);
        } else {
            out += "none";
        }
        out += "\nmaintenance_unread_ttl_probe_performed=";
        out += maintenance.last_observation.unread_ttl_probe_performed ? "1" : "0";
        out += "\nmaintenance_candidate_unread_expired_sealed_record_count=";
        out += std::to_string(maintenance.last_observation.candidate_unread_expired_sealed_record_count);
        out += "\nmaintenance_candidate_unread_expired_sealed_record_bytes=";
        out += std::to_string(maintenance.last_observation.candidate_unread_expired_sealed_record_bytes);
        out += "\nmaintenance_rate_window_bytes_copied=";
        out += std::to_string(maintenance.rate_window_bytes_copied);
        out += "\nmaintenance_rate_window_cpu_ns=";
        out += std::to_string(maintenance.rate_window_cpu_ns);
        out += '\n';

        for (const auto& lane : snapshot.mutations) {
            out += "lane[";
            out += std::to_string(lane.worker_index);
            out += "].queue_depth=";
            out += std::to_string(lane.queue_depth);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].admitted=";
            out += std::to_string(lane.admitted);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].rejected=";
            out += std::to_string(lane.rejected);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].expired_before_store=";
            out += std::to_string(lane.expired_before_store);
            out += "\nlane[";
            out += std::to_string(lane.worker_index);
            out += "].completed=";
            out += std::to_string(lane.completed);
            out += '\n';
            append_latency_histogram(out, "lane[" + std::to_string(lane.worker_index) + "].queue_wait_ns",
                                     lane.queue_wait_histogram);
            append_latency_histogram(out, "lane[" + std::to_string(lane.worker_index) + "].service_ns",
                                     lane.service_histogram);
            if (out.size() > maximum_bytes) {
                return fail(ErrorCode::resource_exhausted, "stats report exceeds the bounded size budget");
            }
        }
        for (const auto& batch : snapshot.batches) {
            out += "batch[";
            out += std::to_string(batch.worker_id.value);
            out += "].enabled=";
            out += batch.enabled ? "1" : "0";
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].pending_records=";
            out += std::to_string(batch.pending_records);
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].committed_batches=";
            out += std::to_string(batch.committed_batches);
            out += "\nbatch[";
            out += std::to_string(batch.worker_id.value);
            out += "].failed_batches=";
            out += std::to_string(batch.failed_batches);
            out += '\n';
            if (out.size() > maximum_bytes) {
                return fail(ErrorCode::resource_exhausted, "stats report exceeds the bounded size budget");
            }
        }
        return out;
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, {});
    }
}

} // namespace glyphastore::server
