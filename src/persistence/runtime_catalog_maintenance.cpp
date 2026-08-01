#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/integer_math.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "persistence/adaptive_batch_sizer.hpp"
#include "persistence/hot_record_table.hpp"
#include "persistence/runtime_catalog_detail.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>

namespace glyphastore {
using namespace glyphastore::runtime_catalog_detail;

auto DurableRuntimeCatalog::healthy() const noexcept -> bool {
    return !closed_.load(std::memory_order_acquire) && healthy_.load(std::memory_order_acquire) &&
           directory_.healthy();
}

void DurableRuntimeCatalog::mark_fail_closed() noexcept {
    abandon_pending_batches();
}

auto DurableRuntimeCatalog::worker_count() const noexcept -> std::size_t {
    return workers_.size();
}

auto DurableRuntimeCatalog::manifest() const -> Manifest {
    const std::shared_lock lock{catalog_mutex_};
    return manifest_;
}

auto DurableRuntimeCatalog::namespace_audit() const -> NamespaceAuditReport {
    const std::shared_lock lock{catalog_mutex_};
    return namespace_audit_;
}

auto DurableRuntimeCatalog::recovery_stats() const noexcept -> const DurableRecoveryStats& {
    return recovery_stats_;
}

auto DurableRuntimeCatalog::hot_cache_stats() const -> std::vector<DurableHotCacheWorkerStats> {
    std::vector<DurableHotCacheWorkerStats> result;
    result.reserve(workers_.size());
    for (std::size_t index = 0; index < workers_.size(); ++index) {
        auto& worker = *workers_[index];
        const auto& metrics = worker.get_path_metrics;
        std::size_t resident_entries{};
        std::uint64_t resident_bytes{};
        std::size_t staged_entries{};
        std::uint64_t staged_bytes{};
        std::uint64_t table_bytes{};
        std::uint64_t total_accounted{};
        {
            const std::lock_guard lock{worker.mutex};
            resident_entries = worker.hot_records.size();
            resident_bytes = worker.hot_record_resident_bytes;
            staged_entries = worker.hot_record_staged_entries;
            staged_bytes = worker.hot_record_staged_bytes;
            table_bytes = hot_cache_table_bytes(worker.hot_records.capacity());
            total_accounted = worker.hot_cache_total_bytes();
        }
        const auto hits = metrics.hot_hits.load(std::memory_order_relaxed);
        const auto misses = metrics.hot_misses.load(std::memory_order_relaxed);
        result.push_back({
            .worker_id = worker.worker_id,
            .resident_entries = resident_entries,
            .resident_bytes = resident_bytes,
            .staged_entries = staged_entries,
            .staged_bytes = staged_bytes,
            .bucket_bytes = table_bytes,
            .total_accounted_bytes = total_accounted,
            .byte_budget = hot_cache_worker_budget(index, workers_.size(), options_.limits),
            .staging_byte_budget = options_.limits.max_hot_cache_staging_bytes_per_worker,
            .entry_budget = options_.limits.max_hot_cache_entries_per_worker,
            .hits = hits,
            .misses = misses,
            .stale_hits = metrics.hot_stale_hits.load(std::memory_order_relaxed),
            .evictions = metrics.hot_evictions.load(std::memory_order_relaxed),
            .admission_bypasses = metrics.admission_bypasses.load(std::memory_order_relaxed),
            .size_rejected = metrics.size_rejected.load(std::memory_order_relaxed),
            .expired_gets = metrics.expired_ttl_gets.load(std::memory_order_relaxed),
            .hit_rate_bp = (hits + misses) == 0 ? 0 : (hits * 10'000ULL) / (hits + misses),
            .enabled = options_.limits.hot_cache_enabled &&
                       hot_cache_worker_budget(index, workers_.size(), options_.limits) > 0 &&
                       options_.limits.max_hot_cache_entries_per_worker > 0 &&
                       options_.limits.max_hot_cache_value_bytes > 0,
            .max_value_bytes = options_.limits.max_hot_cache_value_bytes,
        });
    }
    return result;
}

auto DurableRuntimeCatalog::get_path_stats() const -> std::vector<DurableGetPathWorkerStats> {
    std::vector<DurableGetPathWorkerStats> result;
    result.reserve(workers_.size());
    for (const auto& worker : workers_) {
        const auto& metrics = worker->get_path_metrics;
        std::size_t resident_entries{};
        std::uint64_t resident_bytes{};
        {
            const std::lock_guard lock{worker->mutex};
            resident_entries = worker->hot_records.size();
            resident_bytes = worker->hot_record_resident_bytes;
        }
        result.push_back({
            .worker_id = worker->worker_id,
            .prepare_calls = metrics.prepare_calls.load(std::memory_order_relaxed),
            .complete_calls = metrics.complete_calls.load(std::memory_order_relaxed),
            .mutex_wait_ns = metrics.mutex_wait_ns.load(std::memory_order_relaxed),
            .prepare_hold_ns = metrics.prepare_hold_ns.load(std::memory_order_relaxed),
            .complete_revalidate_hold_ns =
                metrics.complete_revalidate_hold_ns.load(std::memory_order_relaxed),
            .index_lookup_ns = metrics.index_lookup_ns.load(std::memory_order_relaxed),
            .hot_cache_lookup_ns = metrics.hot_cache_lookup_ns.load(std::memory_order_relaxed),
            .generation_pin_lookup_ns = metrics.generation_pin_lookup_ns.load(std::memory_order_relaxed),
            .cold_read_ns = metrics.cold_read_ns.load(std::memory_order_relaxed),
            .crc_value_copy_ns = metrics.crc_value_copy_ns.load(std::memory_order_relaxed),
            .relinearization_retries = metrics.relinearization_retries.load(std::memory_order_relaxed),
            .hot_hits = metrics.hot_hits.load(std::memory_order_relaxed),
            .hot_misses = metrics.hot_misses.load(std::memory_order_relaxed),
            .hot_stale = metrics.hot_stale_hits.load(std::memory_order_relaxed),
            .hot_evictions = metrics.hot_evictions.load(std::memory_order_relaxed),
            .expired_ttl_gets = metrics.expired_ttl_gets.load(std::memory_order_relaxed),
            .hot_resident_entries = resident_entries,
            .hot_resident_bytes = resident_bytes,
        });
    }
    return result;
}

auto DurableRuntimeCatalog::batch_stats() const -> std::vector<DurableBatchWorkerStats> {
    std::vector<DurableBatchWorkerStats> result;
    result.reserve(workers_.size());
    for (const auto& worker : workers_) {
        const auto& metrics = worker->batch_metrics;
        result.push_back({
            .worker_id = worker->worker_id,
            .enabled = options_.batch.has_value(),
            .pending_records = metrics.pending_records.load(std::memory_order_relaxed),
            .pending_bytes = metrics.pending_bytes.load(std::memory_order_relaxed),
            .current_record_target = metrics.current_record_target.load(std::memory_order_relaxed),
            .flush_attempts = metrics.flush_attempts.load(std::memory_order_relaxed),
            .committed_batches = metrics.committed_batches.load(std::memory_order_relaxed),
            .failed_batches = metrics.failed_batches.load(std::memory_order_relaxed),
            .committed_records = metrics.committed_records.load(std::memory_order_relaxed),
            .committed_bytes = metrics.committed_bytes.load(std::memory_order_relaxed),
            .maximum_batch_records = metrics.maximum_batch_records.load(std::memory_order_relaxed),
            .maximum_batch_bytes = metrics.maximum_batch_bytes.load(std::memory_order_relaxed),
            .total_commit_duration_ns = metrics.total_commit_duration_ns.load(std::memory_order_relaxed),
            .maximum_commit_duration_ns = metrics.maximum_commit_duration_ns.load(std::memory_order_relaxed),
            .record_limit_closes = metrics.record_limit_closes.load(std::memory_order_relaxed),
            .byte_limit_closes = metrics.byte_limit_closes.load(std::memory_order_relaxed),
            .adaptive_target_closes = metrics.adaptive_target_closes.load(std::memory_order_relaxed),
            .deadline_closes = metrics.deadline_closes.load(std::memory_order_relaxed),
        });
    }
    return result;
}

void DurableRuntimeCatalog::record_rotation_final_commit(const std::uint64_t duration_ns,
                                                         const bool committed) noexcept {
    const auto previous_version = begin_atomic_stats_publication(rotation_stats_version_);
    rotation_final_record_commit_attempts_.fetch_add(1U, std::memory_order_relaxed);
    if (committed) {
        rotation_final_record_commits_.fetch_add(1U, std::memory_order_relaxed);
    }
    last_rotation_final_record_commit_ns_.store(duration_ns, std::memory_order_relaxed);
    atomic_saturating_add(total_rotation_final_record_commit_ns_, duration_ns);
    atomic_observe_max(maximum_rotation_final_record_commit_ns_, duration_ns);
    end_atomic_stats_publication(rotation_stats_version_, previous_version);
}

auto DurableRuntimeCatalog::rotation_stats() const noexcept -> DurableRotationStats {
    while (true) {
        const auto before = rotation_stats_version_.load(std::memory_order_acquire);
        if ((before & 1U) != 0) {
            continue;
        }
        const DurableRotationStats result{
            .attempts = rotation_attempts_.load(std::memory_order_relaxed),
            .committed = rotations_committed_.load(std::memory_order_relaxed),
            .compaction_waits = rotation_compaction_waits_.load(std::memory_order_relaxed),
            .final_record_commit_attempts =
                rotation_final_record_commit_attempts_.load(std::memory_order_relaxed),
            .final_record_commits = rotation_final_record_commits_.load(std::memory_order_relaxed),
            .last_publication_wait_duration_ns =
                last_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .total_publication_wait_duration_ns =
                total_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .maximum_publication_wait_duration_ns =
                maximum_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .last_seal_duration_ns = last_rotation_seal_ns_.load(std::memory_order_relaxed),
            .total_seal_duration_ns = total_rotation_seal_ns_.load(std::memory_order_relaxed),
            .maximum_seal_duration_ns = maximum_rotation_seal_ns_.load(std::memory_order_relaxed),
            .last_create_duration_ns = last_rotation_create_ns_.load(std::memory_order_relaxed),
            .total_create_duration_ns = total_rotation_create_ns_.load(std::memory_order_relaxed),
            .maximum_create_duration_ns = maximum_rotation_create_ns_.load(std::memory_order_relaxed),
            .last_manifest_publication_duration_ns =
                last_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .total_manifest_publication_duration_ns =
                total_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .maximum_manifest_publication_duration_ns =
                maximum_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .last_execution_duration_ns = last_rotation_execution_ns_.load(std::memory_order_relaxed),
            .total_execution_duration_ns = total_rotation_execution_ns_.load(std::memory_order_relaxed),
            .maximum_execution_duration_ns = maximum_rotation_execution_ns_.load(std::memory_order_relaxed),
            .last_total_duration_ns = last_rotation_total_ns_.load(std::memory_order_relaxed),
            .total_duration_ns = total_rotation_ns_.load(std::memory_order_relaxed),
            .maximum_total_duration_ns = maximum_rotation_total_ns_.load(std::memory_order_relaxed),
            .last_final_record_commit_duration_ns =
                last_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
            .total_final_record_commit_duration_ns =
                total_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
            .maximum_final_record_commit_duration_ns =
                maximum_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
        };
        if (before == rotation_stats_version_.load(std::memory_order_acquire)) {
            return result;
        }
    }
}

auto DurableRuntimeCatalog::next_sequence(const std::size_t worker_index) const -> Result<SequenceNumber> {
    if (worker_index >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "Worker index is outside the durable runtime");
    }
    const std::lock_guard lock{workers_[worker_index]->mutex};
    return workers_[worker_index]->next_sequence;
}

auto DurableRuntimeCatalog::active_segment(const std::size_t worker_index) const -> Result<SegmentId> {
    if (worker_index >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "Worker index is outside the durable runtime");
    }
    const std::lock_guard lock{workers_[worker_index]->mutex};
    return workers_[worker_index]->active_segment;
}

auto DurableRuntimeCatalog::next_compaction_worker(const std::size_t start_worker) const
    -> Result<std::optional<std::size_t>> {
    if (start_worker >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "compaction cursor is outside the durable runtime");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    std::optional<std::size_t> candidate;
    auto best_distance = workers_.size();
    for (const auto& entry : manifest_.segments) {
        if (entry.role != ManifestSegmentRole::sealed) {
            continue;
        }
        const auto owner = static_cast<std::size_t>(entry.owner_worker.value);
        if (owner >= workers_.size()) {
            return fail(ErrorCode::corrupted_data, "sealed Segment owner is outside the durable runtime");
        }
        const auto distance =
            owner >= start_worker ? owner - start_worker : workers_.size() - start_worker + owner;
        if (distance < best_distance) {
            candidate = owner;
            best_distance = distance;
            if (distance == 0) {
                break;
            }
        }
    }
    return candidate;
}

auto DurableRuntimeCatalog::maintenance_observation(const std::size_t start_worker,
                                                    const std::uint64_t now_ns,
                                                    const bool probe_unread_expired_ttl)
    -> Result<MaintenanceObservation> {
    if (start_worker >= workers_.size()) {
        return fail(ErrorCode::invalid_argument,
                    "maintenance compaction cursor is outside the durable runtime");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    MaintenanceObservation observation{
        .durable = true,
        .segment_count = manifest_.segments.size(),
        .max_segment_count = options_.limits.max_segment_count,
        .reserved_free_bytes = options_.limits.reserved_free_bytes,
    };
    if (segments_.size() != manifest_.segments.size()) {
        return fail(ErrorCode::corrupted_data, "maintenance observation catalog metadata is not aligned");
    }
    auto best_distance = workers_.size();
    for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
        const auto& entry = manifest_.segments[index];
        if (entry.role == ManifestSegmentRole::sealed) {
            ++observation.sealed_segment_count;
            const auto owner = static_cast<std::size_t>(entry.owner_worker.value);
            if (owner >= workers_.size()) {
                return fail(ErrorCode::corrupted_data, "sealed Segment owner is outside the durable runtime");
            }
            const auto distance =
                owner >= start_worker ? owner - start_worker : workers_.size() - start_worker + owner;
            if (distance < best_distance) {
                observation.compaction_candidate_worker = owner;
                best_distance = distance;
            }
        }
    }
    if (observation.compaction_candidate_worker) {
        const auto candidate = *observation.compaction_candidate_worker;
        for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
            const auto& entry = manifest_.segments[index];
            if (entry.role != ManifestSegmentRole::sealed || entry.owner_worker.value != candidate) {
                continue;
            }
            const auto committed_end = segments_[index].selected.commit.committed_end;
            if (committed_end < kSegmentHeaderReservedBytes || committed_end > kSegmentSizeBytes) {
                return fail(ErrorCode::corrupted_data,
                            "sealed Segment committed extent is outside v1 bounds");
            }
            const auto record_bytes = static_cast<std::uint64_t>(committed_end - kSegmentHeaderReservedBytes);
            if (record_bytes >
                std::numeric_limits<std::uint64_t>::max() - observation.candidate_sealed_record_bytes) {
                return fail(ErrorCode::arithmetic_overflow,
                            "maintenance sealed Record byte count overflows uint64_t");
            }
            observation.candidate_sealed_record_bytes += record_bytes;
        }
        observation.candidate_live_record_bytes =
            workers_[candidate]->sealed_live_record_bytes.load(std::memory_order_acquire);
        if (observation.candidate_live_record_bytes > observation.candidate_sealed_record_bytes) {
            return fail(ErrorCode::corrupted_data,
                        "maintenance live Record bytes exceed sealed committed bytes");
        }
        observation.candidate_dead_record_bytes =
            observation.candidate_sealed_record_bytes - observation.candidate_live_record_bytes;
        observation.candidate_dead_byte_ratio_bp =
            observation.candidate_sealed_record_bytes == 0
                ? 10'000U
                : basis_points(observation.candidate_dead_record_bytes,
                               observation.candidate_sealed_record_bytes);
    }
    // Match rotate_active: require_durable_available_space(kSegmentSizeBytes + next_manifest_bytes).
    const auto next_count = observation.segment_count + 1U;
    if (auto next_manifest = durable_manifest_bytes(next_count); next_manifest) {
        if (*next_manifest <= std::numeric_limits<std::uint64_t>::max() - kSegmentSizeBytes) {
            observation.rotate_additional_bytes = kSegmentSizeBytes + *next_manifest;
        } else {
            observation.rotate_additional_bytes = std::numeric_limits<std::uint64_t>::max();
        }
    } else {
        observation.rotate_additional_bytes = std::numeric_limits<std::uint64_t>::max();
    }
    if (auto free = directory_.available_space_bytes(); free) {
        observation.available_free_bytes = *free;
    } else if (free.error().code != ErrorCode::unavailable && free.error().code != ErrorCode::io_error) {
        return unexpected(free.error());
    }

    if (probe_unread_expired_ttl && now_ns != 0 && observation.compaction_candidate_worker) {
        struct ProbeContext {
            std::uint64_t now_ns{};
            std::uint64_t key_hash{};
            bool expired{};
        };
        const RecordVisitor probe_visitor = [](void* opaque, const RecordView& record) -> Status {
            auto& context = *static_cast<ProbeContext*>(opaque);
            if (record.opcode != Opcode::put || record.key_hash != context.key_hash) {
                return fail(ErrorCode::corrupted_data,
                            "unread TTL probe Index entry disagrees with its source Record");
            }
            context.expired = record.expired(context.now_ns);
            return {};
        };

        const auto candidate = *observation.compaction_candidate_worker;
        auto& worker = *workers_[candidate];
        std::lock_guard worker_lock{worker.mutex};
        const std::shared_lock probe_catalog_lock{catalog_mutex_};
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }
        if (generation_pins_.size() != manifest_.segments.size()) {
            return fail(ErrorCode::corrupted_data, "unread TTL probe catalog metadata is not aligned");
        }

        for (const auto& entry : worker.index.entries()) {
            if (entry.record.segment_id == worker.active_segment) {
                continue;
            }
            const auto found = std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(),
                                                entry.record.segment_id,
                                                [](const ManifestSegmentEntry& segment, const SegmentId id) {
                                                    return segment.segment_id.value < id.value;
                                                });
            if (found == manifest_.segments.end() || found->segment_id != entry.record.segment_id ||
                found->role != ManifestSegmentRole::sealed || found->owner_worker.value != candidate) {
                continue;
            }
            const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
            const auto& pin = generation_pins_[catalog_index];
            if (!pin || pin->identity.segment_id != entry.record.segment_id ||
                pin->identity.generation != entry.record.generation ||
                pin->identity.owner_worker != worker.worker_id) {
                return fail(ErrorCode::corrupted_data,
                            "unread TTL probe generation pin disagrees with the Index");
            }
            ProbeContext context{.now_ns = now_ns, .key_hash = hash_key_routing(entry.key, worker_routing_)};
            if (auto visited = pin->file.visit_runtime_record(entry.record, &context, probe_visitor);
                !visited) {
                return unexpected(visited.error());
            }
            if (!context.expired) {
                continue;
            }
            ++observation.candidate_unread_expired_sealed_record_count;
            if (entry.record.size.value > std::numeric_limits<std::uint64_t>::max() -
                                              observation.candidate_unread_expired_sealed_record_bytes) {
                return fail(ErrorCode::arithmetic_overflow,
                            "unread TTL probe sealed Record byte count overflows uint64_t");
            }
            observation.candidate_unread_expired_sealed_record_bytes += entry.record.size.value;
        }
        observation.unread_ttl_probe_performed = true;
    }
    return observation;
}

auto DurableRuntimeCatalog::compact_worker(const std::size_t worker_index, const std::uint64_t now_ns,
                                           const std::uint64_t max_copy_bytes) -> DurableCompactionResult {
    bool recovery_required{};
    DurableCompactionCopyStats stats{};
    const auto notify_fail_closed = [&] {
        healthy_.store(false, std::memory_order_release);
        for (const auto& worker : workers_) {
            worker->batch_closed.notify_all();
        }
    };
    const auto failure = [&](Error error) {
        if (recovery_required) {
            notify_fail_closed();
        }
        return DurableCompactionResult{
            .outcome = recovery_required ? DurableCompactionOutcome::recovery_required
                                         : DurableCompactionOutcome::not_compacted,
            .stats = stats,
            .error = std::move(error),
        };
    };

    try {
        if (worker_index >= workers_.size()) {
            return failure(Error{ErrorCode::invalid_argument, "Worker index is outside the durable runtime"});
        }
        if (!healthy()) {
            return failure(Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }

        auto& worker = *workers_[worker_index];
        Manifest snapshot;
        SequenceNumber snapshot_next_sequence{};
        std::vector<IndexEntry> snapshot_entries;
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> source_pins;

        // Phase A: capture only owning state. No file operation is allowed in
        // this scope. The complete Index enumeration is currently necessary
        // because the replacement Index must retain active-Segment references.
        {
            std::unique_lock worker_lock{worker.mutex};
            if (worker.mutation_io_active) {
                return failure(Error{ErrorCode::sequence_conflict,
                                     "durable compaction conflicts with active mutation I/O"});
            }
            if (dedicated_commit_executor_) {
                worker.batch_closed.wait(worker_lock, [&] { return !worker.batch_closing || !healthy(); });
            }
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                return failure(Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
            }
            if (!worker.pending_group_mutations.empty() || worker.batch_closing) {
                return failure(Error{ErrorCode::sequence_conflict,
                                     "durable compaction cannot snapshot a pending group publication"});
            }
            const auto sealed_live_record_bytes =
                worker.sealed_live_record_bytes.load(std::memory_order_relaxed);
            if (max_copy_bytes > 0 && sealed_live_record_bytes > max_copy_bytes) {
                return failure(Error{ErrorCode::sequence_conflict,
                                     "durable compaction candidate exceeds its maintenance copy budget"});
            }
            snapshot = manifest_;
            snapshot_next_sequence = worker.next_sequence;
            const auto index_size = worker.index.stats().size;
            snapshot_entries = worker.index.entries();
            if (snapshot_entries.size() != index_size || generation_pins_.size() != segments_.size() ||
                segments_.size() != snapshot.segments.size()) {
                return failure(
                    Error{ErrorCode::corrupted_data, "durable compaction snapshot is not catalog-aligned"});
            }
            for (std::size_t index = 0; index < snapshot.segments.size(); ++index) {
                const auto& entry = snapshot.segments[index];
                if (entry.owner_worker != worker.worker_id || entry.role != ManifestSegmentRole::sealed) {
                    continue;
                }
                const auto& pin = generation_pins_[index];
                if (!pin || pin->identity.segment_id != entry.segment_id ||
                    pin->identity.generation != entry.generation ||
                    pin->identity.owner_worker != entry.owner_worker) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "durable compaction source has no matching generation pin"});
                }
                source_pins.push_back(pin);
            }
        }

        if (source_pins.empty()) {
            return failure(Error{ErrorCode::not_found, "durable Worker has no sealed Segments to compact"});
        }

        // Phase B1 performs the read-only scan, CRC verification, layout, and
        // replacement Index construction without a global publication lease.
        // The builder invokes this gate immediately before the durable intent;
        // from that boundary through manifest publication, recovery v1 requires
        // the exact old/next authority pair to remain exclusive.
        bool publication_lease_active{};
        ScopeExit publication_lease{[&]() noexcept {
            if (!publication_lease_active) {
                return;
            }
            {
                const std::lock_guard lock{manifest_publication_mutex_};
                compaction_publication_active_ = false;
            }
            manifest_publication_changed_.notify_all();
        }};
        struct IntentGateContext final {
            DurableRuntimeCatalog& runtime;
            const Manifest& snapshot;
            bool& active;
        } intent_gate_context{.runtime = *this, .snapshot = snapshot, .active = publication_lease_active};
        const DurableCompactionIntentGate intent_gate{
            .context = &intent_gate_context,
            .acquire = [](void* opaque) -> Status {
                auto& gate = *static_cast<IntentGateContext*>(opaque);
                std::lock_guard publication_lock{gate.runtime.manifest_publication_mutex_};
                if (gate.runtime.compaction_publication_active_ ||
                    gate.runtime.rotation_publication_active_) {
                    return fail(ErrorCode::sequence_conflict,
                                "another durable manifest publication is active");
                }
                const std::shared_lock catalog_lock{gate.runtime.catalog_mutex_};
                if (!gate.runtime.healthy() || gate.runtime.manifest_ != gate.snapshot) {
                    return fail(ErrorCode::sequence_conflict,
                                "manifest changed before compaction intent publication");
                }
                gate.runtime.compaction_publication_active_ = true;
                gate.active = true;
                return {};
            },
        };
        auto built = build_durable_worker_compaction(directory_, snapshot, worker.worker_id,
                                                     std::move(snapshot_entries), now_ns, options_.limits,
                                                     intent_gate);
        if (!built.succeeded()) {
            if (built.outcome == DurableCompactionBuildOutcome::not_beneficial) {
                return {.outcome = DurableCompactionOutcome::not_beneficial,
                        .stats = built.stats,
                        .error = std::move(built.error)};
            }
            recovery_required = built.outcome == DurableCompactionBuildOutcome::recovery_required;
            return failure(built.error.value_or(
                Error{ErrorCode::io_error, "durable compaction replacement build failed"}));
        }
        recovery_required = true;
        auto prepared = std::move(*built.prepared);
        stats = prepared.stats;
        auto sources = std::move(prepared.plan.sources);
        const auto abort_prepared = [&](Error reason) -> DurableCompactionResult {
            auto rolled_back = rollback_prepared_compaction(directory_, snapshot, prepared.plan.replacements,
                                                            options_.limits);
            if (!rolled_back) {
                return failure(rolled_back.error());
            }
            {
                const std::unique_lock catalog_lock{catalog_mutex_};
                namespace_audit_ = std::move(*rolled_back);
            }
            recovery_required = false;
            return failure(std::move(reason));
        };
        if (prepared.replacement_commits.size() != prepared.plan.replacements.size()) {
            return abort_prepared(
                Error{ErrorCode::internal_error, "compaction replacement commit catalog is incomplete"});
        }
        std::vector<RecoveredSegmentState> next_segments;
        next_segments.reserve(prepared.plan.next_manifest.segments.size());
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> replacement_pins;
        replacement_pins.reserve(prepared.plan.replacements.size());
        for (std::size_t index = 0; index < prepared.plan.replacements.size(); ++index) {
            const auto& entry = prepared.plan.replacements[index];
            const SegmentHeaderIdentity identity{.store_id = snapshot.store_id,
                                                 .segment_id = entry.segment_id,
                                                 .generation = entry.generation,
                                                 .owner_worker = entry.owner_worker};
            auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_only);
            if (!opened) {
                return abort_prepared(opened.error());
            }
            if (opened->selected_commit() != prepared.replacement_commits[index]) {
                return abort_prepared(
                    Error{ErrorCode::corrupted_data,
                          "compaction replacement changed before generation pin publication"});
            }
            replacement_pins.push_back(
                std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
                    .identity = identity,
                    .selected = prepared.replacement_commits[index],
                    .file = std::move(*opened),
                }));
        }
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> next_generation_pins;
        next_generation_pins.reserve(prepared.plan.next_manifest.segments.size());
        std::vector<std::size_t> retained_old_indices;
        retained_old_indices.reserve(prepared.plan.next_manifest.segments.size());
        auto installed_manifest = prepared.plan.next_manifest;
        auto next_pin_slots = prepare_pin_slot_index(prepared.plan.next_manifest);
        if (!next_pin_slots) {
            return abort_prepared(next_pin_slots.error());
        }

        // Phase C first validates and prepares a non-allocating publication
        // while locked. The durable manifest write happens only after the
        // target Worker is logically quiesced and every physical mutex has
        // been released.
        std::unique_lock worker_lock{worker.mutex, std::try_to_lock};
        if (!worker_lock.owns_lock()) {
            return abort_prepared(
                Error{ErrorCode::sequence_conflict, "Worker changed while compaction was prepared"});
        }
        std::unique_lock catalog_lock{catalog_mutex_};
        bool sources_still_pinned = source_pins.size() == sources.size();
        for (std::size_t source_index = 0; sources_still_pinned && source_index < sources.size();
             ++source_index) {
            const auto found = std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(),
                                                sources[source_index].segment_id,
                                                [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                                    return entry.segment_id.value < id.value;
                                                });
            if (found == manifest_.segments.end() || *found != sources[source_index]) {
                sources_still_pinned = false;
                break;
            }
            const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
            sources_still_pinned = catalog_index < generation_pins_.size() &&
                                   generation_pins_[catalog_index] == source_pins[source_index];
        }
        if (!healthy() || manifest_ != snapshot || segments_.size() != snapshot.segments.size() ||
            worker.next_sequence != snapshot_next_sequence || !worker.pending_group_mutations.empty() ||
            worker.batch_closing || !sources_still_pinned) {
            catalog_lock.unlock();
            worker_lock.unlock();
            return abort_prepared(
                Error{ErrorCode::sequence_conflict, "runtime state changed during durable compaction"});
        }

        {
            std::size_t replacement_index{};
            for (const auto& entry : prepared.plan.next_manifest.segments) {
                if (replacement_index < prepared.plan.replacements.size() &&
                    entry == prepared.plan.replacements[replacement_index]) {
                    next_segments.push_back({.selected = prepared.replacement_commits[replacement_index]});
                    next_generation_pins.push_back(replacement_pins[replacement_index]);
                    retained_old_indices.push_back(std::numeric_limits<std::size_t>::max());
                    ++replacement_index;
                    continue;
                }
                const auto found =
                    std::lower_bound(snapshot.segments.begin(), snapshot.segments.end(), entry.segment_id,
                                     [](const ManifestSegmentEntry& candidate, const SegmentId id) {
                                         return candidate.segment_id.value < id.value;
                                     });
                if (found == snapshot.segments.end() || *found != entry) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "compaction retained entry is absent from the old catalog"});
                }
                const auto old_index = static_cast<std::size_t>(found - snapshot.segments.begin());
                next_segments.push_back(segments_[old_index]);
                if (old_index >= generation_pins_.size() || !generation_pins_[old_index]) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "retained compaction Segment has no generation pin"});
                }
                next_generation_pins.push_back(generation_pins_[old_index]);
                retained_old_indices.push_back(old_index);
            }
            if (replacement_index != prepared.plan.replacements.size() ||
                next_segments.size() != prepared.plan.next_manifest.segments.size() ||
                next_generation_pins.size() != prepared.plan.next_manifest.segments.size() ||
                retained_old_indices.size() != prepared.plan.next_manifest.segments.size()) {
                return failure(
                    Error{ErrorCode::internal_error, "compaction runtime catalog preparation is incomplete"});
            }
        }

        worker.compaction_commit_active.store(true, std::memory_order_release);
        if (options_.exclusive_writer) {
            for (;;) {
                const auto depth = worker.hot_path_depth.load(std::memory_order_acquire);
                if (depth == 0) {
                    break;
                }
                worker.hot_path_depth.wait(depth, std::memory_order_acquire);
            }
        }
        struct WorkerCommitGate final {
            RuntimeWorker& worker;
            bool active{true};

            explicit WorkerCommitGate(RuntimeWorker& owner) noexcept : worker(owner) {}
            ~WorkerCommitGate() {
                if (!active) {
                    return;
                }
                const std::lock_guard lock{worker.mutex};
                worker.compaction_commit_active.store(false, std::memory_order_release);
                worker.compaction_commit_finished.notify_all();
            }
            void clear_locked() noexcept {
                worker.compaction_commit_active.store(false, std::memory_order_release);
                active = false;
                worker.compaction_commit_finished.notify_all();
            }

            WorkerCommitGate(const WorkerCommitGate&) = delete;
            auto operator=(const WorkerCommitGate&) -> WorkerCommitGate& = delete;
        } commit_gate{worker};
        catalog_lock.unlock();
        worker_lock.unlock();

        const auto published =
            directory_.publish_manifest(prepared.plan.next_manifest, options_.limits.max_manifest_bytes);
        if (!published.durable()) {
            return failure(published.error.value_or(
                Error{ErrorCode::io_error, "compaction manifest publication failed"}));
        }

        try {
            worker_lock.lock();
            catalog_lock.lock();
            for (std::size_t next_index = 0; next_index < retained_old_indices.size(); ++next_index) {
                const auto old_index = retained_old_indices[next_index];
                if (old_index == std::numeric_limits<std::size_t>::max()) {
                    continue;
                }
                next_segments[next_index] = segments_[old_index];
                next_generation_pins[next_index] = generation_pins_[old_index];
            }
            manifest_ = std::move(prepared.plan.next_manifest);
            segments_ = std::move(next_segments);
            generation_pins_ = std::move(next_generation_pins);
            pin_slot_by_segment_id_ = std::move(*next_pin_slots);
            worker.index = std::move(prepared.index);
            worker.active_live_record_bytes.store(prepared.active_live_record_bytes,
                                                  std::memory_order_release);
            worker.sealed_live_record_bytes.store(stats.bytes_copied, std::memory_order_release);
            if (!advance_read_catalog_revision(worker)) {
                throw std::overflow_error{"durable read catalog revision exhausted"};
            }
            commit_gate.clear_locked();
        } catch (...) {
            if (worker_lock.owns_lock()) {
                commit_gate.clear_locked();
            }
            if (catalog_lock.owns_lock()) {
                catalog_lock.unlock();
            }
            if (worker_lock.owns_lock()) {
                worker_lock.unlock();
            }
            throw;
        }
        catalog_lock.unlock();
        worker_lock.unlock();

        const auto retired = directory_.retire_compaction_segments(snapshot.store_id, sources);
        if (!retired.durable()) {
            return failure(
                retired.error.value_or(Error{ErrorCode::io_error, "compaction source retirement failed"}));
        }
        const auto removed = directory_.remove_compaction_intent();
        if (!removed.durable()) {
            return failure(
                removed.error.value_or(Error{ErrorCode::io_error, "compaction intent removal failed"}));
        }
        auto audit = audit_data_directory(directory_, installed_manifest);
        if (!audit) {
            return failure(audit.error());
        }
        if (auto safe = validate_namespace_for_recovery(*audit); !safe) {
            return failure(safe.error());
        }
        {
            const std::unique_lock audit_lock{catalog_mutex_};
            namespace_audit_ = std::move(*audit);
        }
        recovery_required = false;
        return {.outcome = DurableCompactionOutcome::compacted, .stats = stats, .error = std::nullopt};
    } catch (const std::bad_alloc&) {
        return failure(Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        return failure(Error{ErrorCode::internal_error, {}});
    }
}

auto DurableRuntimeCatalog::snapshot_live_keys() -> Result<std::vector<std::string>> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    std::vector<std::string> keys;
    for (std::size_t worker_index = 0; worker_index < workers_.size(); ++worker_index) {
        auto& worker = *workers_[worker_index];
        const std::lock_guard lock{worker.mutex};
        const std::shared_lock catalog_lock{catalog_mutex_};
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }
        auto entries = worker.index.entries();
        for (auto& entry : entries) {
            if (route_worker(hash_key_routing(entry.key, worker_routing_), workers_.size()) != worker_index) {
                return fail(ErrorCode::corrupted_data, "durable Index entry is routed to the wrong Worker");
            }
            keys.push_back(std::move(entry.key));
        }
    }
    std::sort(keys.begin(), keys.end());
    return keys;
}

auto DurableRuntimeCatalog::verify_index() -> Status {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    auto keys = snapshot_live_keys();
    if (!keys) {
        return unexpected(keys.error());
    }
    for (const auto& key : *keys) {
        if (auto verified = get(key); !verified) {
            return unexpected(verified.error());
        }
    }
    return {};
}

} // namespace glyphastore
