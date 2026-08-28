#include "glyphastore/store/paired/shard_pair_runtime.hpp"

#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/hot_path_phases.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/paired/completion_policy.hpp"
#include "glyphastore/store/paired/fail_closed_state.hpp"
#include "glyphastore/store/paired/lane_publication.hpp"
#include "glyphastore/store/paired/mutation_batch.hpp"
#include "glyphastore/store/paired/mutation_execution.hpp"
#include "glyphastore/store/paired/mutation_recovery.hpp"
#include "glyphastore/store/paired/mutation_state.hpp"
#include "glyphastore/store/paired/publication_coordinator.hpp"
#include "glyphastore/store/paired/shard_combining_executor.hpp"
#include "glyphastore/store/paired/volatile_sync_chunk.hpp"
#include "store/paired/shard_pair_runtime_impl.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace glyphastore::store::paired {

void ShardPairRuntime::run(const std::size_t shard) noexcept {
    auto& lane = *lanes_[shard];
    struct ExitGuard final {
        ShardPairRuntime& executor;
        ~ExitGuard() {
            executor.note_writer_exit();
        }
    } exit_guard{*this};
    const auto pop_queued = [&lane]() noexcept -> std::optional<runtime_detail::AsyncMutationTask> {
        const std::lock_guard lock{lane.async.queue_consumer_mutex};
        return lane.queue.try_pop();
    };

    const auto batch_config = detail::StoreAccess::durable_writer_batch_config(store_);
    const auto maximum_batch_records =
        std::min(batch_config ? static_cast<std::size_t>(batch_config->max_records)
                              : static_cast<std::size_t>(config_.async_writer_batch_max_records),
                 lane.queue.capacity());
    const auto maximum_batch_bytes = batch_config ? static_cast<std::size_t>(batch_config->max_bytes)
                                                  : config_.async_writer_batch_max_bytes;
    std::vector<runtime_detail::AsyncMutationTask> batch;
    std::vector<std::uint64_t> queue_waits;
    std::vector<std::chrono::steady_clock::time_point> service_started;
    std::vector<bool> expired;
    std::vector<MutationOutcome> completions;
    std::vector<detail::StoreAccess::DurableMutationView> durable_views;
    std::vector<std::size_t> durable_indices;
    std::vector<ReadMutation> read_mutations;
    std::vector<std::size_t> read_mutation_indices;
    std::optional<runtime_detail::AsyncMutationTask> carried_task;
    SyncMutation* carried_sync = nullptr;
    batch.reserve(maximum_batch_records);
    queue_waits.reserve(maximum_batch_records);
    service_started.reserve(maximum_batch_records);
    expired.reserve(maximum_batch_records);
    completions.reserve(maximum_batch_records);
    durable_views.reserve(maximum_batch_records);
    durable_indices.reserve(maximum_batch_records);
    read_mutations.reserve(maximum_batch_records);
    read_mutation_indices.reserve(maximum_batch_records);
    bool merge_retry_blocked{};

    FailClosedState fail_closed{store_, healthy_, expire_remaining_};
    LanePublicationContext publication_ctx{lane.generation, lane.merge, store_, shard,
                                           ShardPairRuntime::kMaximumRetiredReadGenerations};
    const auto publish_fail_closed = [&]() noexcept {
        fail_closed.arm(fail_closed_wakes_, FailClosedScope::pair_and_store);
    };
    const auto sticky_pair_before_durable_mark = [&]() noexcept {
        fail_closed.arm(fail_closed_wakes_, FailClosedScope::pair_only);
    };
    const auto update_delta_stats = [&]() noexcept {
        store_generation_memory_stats(lane.generation, lane.generation.writer_generation->memory_stats());
    };
    const auto start_merge_with_optional_pin = [&]() -> Result<std::unique_ptr<PairReadMerge>> {
        return start_incremental_merge(lane.generation, config_.merge_maximum_post_entries);
    };
    const auto reclaim_quiescent = [&]() noexcept {
        reclaim_quiescent_generations(lane.generation, lane.reclaim, config_.reader_epoch_lease);
    };
    // Drain durable Index authority into the published generation before sticky
    // close (allow_fail_closed: durable may already be unhealthy). Sync single-op
    // and batch catch use this so committed keys are not left unpublished.
    const auto drain_durable_snapshot = [&]() noexcept -> bool {
        return try_drain_durable_snapshot(publication_ctx, true, [&]() {
            merge_retry_blocked = false;
            reclaim_quiescent();
        });
    };
    // Publication produces one retired generation per successful incremental
    // publish. Reclaiming on every single-mutation publish dominated volatile
    // PUT ack time; reclaim proportionally when debt accumulates, and always
    // before hard retire limits / merge pressure checks.
    const auto reclaim_proportional = [&]() noexcept {
        if (lane.merge.read_merge) {
            store_merge_progress(lane.merge);
        }
        constexpr std::size_t kReclaimPublishQuantum = 8;
        if (lane.generation.retired_debt() >= kReclaimPublishQuantum ||
            lane.generation.retired_debt() + 1U >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
            reclaim_quiescent();
        } else {
            lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                           std::memory_order_relaxed);
        }
    };
    const auto process_reclamation = [&]() noexcept {
        if (lane.generation.reclaim_requested.exchange(false, std::memory_order_acq_rel)) {
            reclaim_quiescent();
        }
    };
    const auto process_refresh = [&]() noexcept {
        if (!lane.generation.refresh_requested.exchange(false, std::memory_order_acq_rel)) {
            return;
        }
        reclaim_quiescent();
        if (decide_generation_admission(lane.generation.retired_debt(),
                                        ShardPairRuntime::kMaximumRetiredReadGenerations,
                                        true) != GenerationAdmissionDecision::admitted) {
            lane.metrics.read_refresh_deferrals.fetch_add(1U, std::memory_order_relaxed);
            return;
        }
        lane.metrics.read_refresh_attempts.fetch_add(1U, std::memory_order_relaxed);
        auto snapshot = detail::StoreAccess::snapshot_durable_reads(store_, shard);
        if (!snapshot) {
            lane.metrics.read_refresh_failures.fetch_add(1U, std::memory_order_relaxed);
            if (snapshot.error().code != ErrorCode::resource_exhausted &&
                !(lane.async.stopping.load(std::memory_order_acquire) &&
                  snapshot.error().code == ErrorCode::unavailable)) {
                publish_fail_closed();
            }
            return;
        }
        if (snapshot->catalog_revision ==
            lane.generation.published_catalog_revision.load(std::memory_order_acquire)) {
            return;
        }
        const auto outcome = replace_durable_snapshot_and_publish(publication_ctx, snapshot->records);
        if (outcome.status == DualPathPublishStatus::deferred) {
            lane.metrics.read_refresh_deferrals.fetch_add(1U, std::memory_order_relaxed);
            return;
        }
        if (outcome.status == DualPathPublishStatus::build_failed) {
            lane.metrics.read_refresh_failures.fetch_add(1U, std::memory_order_relaxed);
            if (outcome.build_error != ErrorCode::resource_exhausted) {
                publish_fail_closed();
            }
            return;
        }
        if (outcome.status != DualPathPublishStatus::published) {
            publish_fail_closed();
            return;
        }
        note_catalog_snapshot_installed(publication_ctx, snapshot->catalog_revision);
        merge_retry_blocked = false;
        lane.metrics.read_refresh_successes.fetch_add(1U, std::memory_order_relaxed);
    };
    const auto process_merge = [&](const std::size_t publication_records) noexcept {
        if (!lane.merge.read_merge && !merge_retry_blocked &&
            (lane.generation.writer_generation->delta_entries() >= config_.merge_delta_entries ||
             lane.generation.writer_generation->delta_record_versions() >= config_.merge_delta_entries)) {
            auto started = start_merge_with_optional_pin();
            if (!started) {
                lane.merge.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
                if (started.error().code == ErrorCode::resource_exhausted) {
                    merge_retry_blocked = true;
                } else {
                    publish_fail_closed();
                }
                return;
            }
            lane.merge.read_merge = std::move(*started);
            lane.merge.read_merge_active.store(true, std::memory_order_relaxed);
            lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
            store_merge_progress(lane.merge);
            lane.merge.read_merge_starts.fetch_add(1U, std::memory_order_relaxed);
        }
        if (!lane.merge.read_merge) {
            return;
        }
        if (!PairReadGeneration::merge_ready(*lane.merge.read_merge)) {
            const auto advance_budget = PairReadGeneration::merge_advance_budget(
                *lane.merge.read_merge, publication_records,
                runtime_detail::merge_minimum_advance_slots(config_.merge_quantum_slots, publication_records));
            auto advanced =
                PairReadGeneration::advance_incremental_merge(*lane.merge.read_merge, advance_budget);
            if (!advanced) {
                lane.merge.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
                if (advanced.error().code == ErrorCode::resource_exhausted) {
                    lane.merge.read_merge.reset();
                    lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
                    lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
                    store_merge_progress(lane.merge);
                    merge_retry_blocked = true;
                } else {
                    publish_fail_closed();
                }
                return;
            }
            runtime_detail::note_merge_advance(lane.merge, *advanced);
        }
        if (!PairReadGeneration::merge_ready(*lane.merge.read_merge)) {
            return;
        }
        reclaim_quiescent();
        if (decide_generation_admission(lane.generation.retired_debt(),
                                        ShardPairRuntime::kMaximumRetiredReadGenerations,
                                        true) != GenerationAdmissionDecision::admitted) {
            return;
        }
        const auto outcome = finish_incremental_merge_and_publish(publication_ctx);
        if (outcome.status == DualPathPublishStatus::deferred) {
            return;
        }
        if (outcome.status == DualPathPublishStatus::build_failed) {
            if (outcome.build_error == ErrorCode::resource_exhausted) {
                merge_retry_blocked = true;
            } else {
                publish_fail_closed();
            }
            return;
        }
        if (outcome.status != DualPathPublishStatus::published) {
            publish_fail_closed();
            return;
        }
        lane.merge.read_merge.reset();
        lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
        lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
        store_merge_progress(lane.merge);
        lane.merge.read_merge_completions.fetch_add(1U, std::memory_order_relaxed);
        merge_retry_blocked = false;
        reclaim_quiescent();
    };

    for (;;) {
        // ADR 0037: refresh/merge/sync share the execution token with the combiner.
        if (!try_acquire_execution_token(lane.async.execution_token)) {
            const auto observed = lane.async.signal.load(std::memory_order_acquire);
            lane.async.signal.wait(observed, std::memory_order_acquire);
            continue;
        }
        process_reclamation();
        if (!lane.async.stopping.load(std::memory_order_acquire) &&
            healthy_.load(std::memory_order_acquire)) {
            process_refresh();
        }

        // Drain one bounded synchronous turn before asynchronous lane work. Neither
        // repeated sync admissions nor one large caller batch may keep the dedicated
        // Writer here forever: after at most 32 records, an already-admitted async
        // batch gets a turn under the same execution token.
        bool drained_sync_turn = false;
        for (;;) {
            SyncMutation* rev = nullptr;
            if (carried_sync != nullptr) {
                // Writer-local continuation is already FIFO and is older than
                // every new LIFO admission still attached to sync_head.
                rev = std::exchange(carried_sync, nullptr);
            } else {
                SyncMutation* sync_batch = nullptr;
                {
                    const std::lock_guard lock{lane.sync.sync_mutex};
                    sync_batch = lane.sync_head;
                    lane.sync_head = nullptr;
                }
                if (sync_batch == nullptr) {
                    break;
                }
                // Reverse to preserve FIFO order from LIFO admission.
                while (sync_batch != nullptr) {
                    auto* next = sync_batch->next;
                    sync_batch->next = rev;
                    rev = sync_batch;
                    sync_batch = next;
                }
            }

            // Bound one dedicated-Writer sync turn. The continuation stays
            // Writer-local, keeps caller-owned nodes alive (callers still wait),
            // and cannot be overtaken by later sync_head admissions.
            constexpr std::size_t kMaximumSyncTurnRecords = 32U;
            auto* turn_tail = rev;
            for (std::size_t records = 1U; records < kMaximumSyncTurnRecords && turn_tail->next != nullptr;
                 ++records) {
                turn_tail = turn_tail->next;
            }
            if (turn_tail->next != nullptr) {
                carried_sync = turn_tail->next;
                turn_tail->next = nullptr;
                lane.metrics.sync_turn_splits.fetch_add(1U, std::memory_order_relaxed);
            }
            drained_sync_turn = true;
            lane.metrics.sync_drain_turns.fetch_add(1U, std::memory_order_relaxed);
            GS_FAULT_BLOCK(sync_lane_snapshot);
            if (!healthy_.load(std::memory_order_acquire)) {
                for (auto* node = rev; node != nullptr;) {
                    auto* const next = node->next;
                    // Never Store-entered — known not newly committed (not sticky reconcile).
                    node->status =
                        Status{fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                    node->done.store(true, std::memory_order_release);
                    node->done.notify_one();
                    node = next;
                }
                break;
            }
            // Every sync drain below publishes at least one replacement generation
            // when it Store-commits. Reserve that bounded ownership capacity before
            // entering volatile or durable Store authority.
            reclaim_quiescent();
            auto turn_admission = decide_generation_admission(
                lane.generation.retired_debt(), ShardPairRuntime::kMaximumRetiredReadGenerations, true);
            turn_admission = runtime_detail::wait_generation_admission(
                turn_admission, [&] { reclaim_quiescent(); },
                [&] {
                    return decide_generation_admission(lane.generation.retired_debt(),
                                                       ShardPairRuntime::kMaximumRetiredReadGenerations,
                                                       true);
                });
            if (turn_admission != GenerationAdmissionDecision::admitted) {
                for (auto* node = rev; node != nullptr;) {
                    auto* const next = node->next;
                    lane.generation.generation_admission_backpressure_total.fetch_add(
                        1U, std::memory_order_relaxed);
                    node->status = Status{
                        fail(ErrorCode::resource_exhausted, generation_admission_message(turn_admission))};
                    node->done.store(true, std::memory_order_release);
                    node->done.notify_one();
                    node = next;
                }
                break;
            }
            if (batch_config && detail::StoreAccess::is_durable(store_)) {
                std::vector<SyncMutation*> nodes;
                std::vector<detail::StoreAccess::DurableMutationView> views;
                bool publication_required = false;
                bool durable_mutate_entered = false;
                bool sticky_publication_failure = false;
                bool sibling_snapshot_published = false;
                std::size_t first_unprocessed = 0;
                std::size_t inflight_begin = 0;
                std::size_t inflight_end = 0;
                bool mutate_inflight = false;
                // Only Index-applied committed+error items may ACK-after-visibility.
                // Put-hit alone must not upgrade a later same-key not_committed failure.
                std::vector<SyncMutation*> sticky_committed_nodes;
                sticky_committed_nodes.reserve(8);
                const auto ack_sticky_after_visibility = [&]() noexcept {
                    const auto* published =
                        lane.generation.published_generation.load(std::memory_order_acquire);
                    if (published == nullptr) {
                        return;
                    }
                    for (auto* node : sticky_committed_nodes) {
                        if (node->status) {
                            continue;
                        }
                        const auto view = published->prepare_durable(*node->key);
                        if (node->kind == MutationKind::put) {
                            if (view.has_value()) {
                                node->status = Status{};
                            }
                        } else if (!view.has_value() && view.error().code == ErrorCode::not_found) {
                            node->status = Status{};
                        }
                    }
                };
                // sticky_pair_before_durable_mark (outer): stop later sub-batches without
                // Store mark_fail_closed before sibling snapshot.
                const auto upgrade_placeholder = [&](const std::size_t index,
                                                     const ErrorCode before_mutate) -> ErrorCode {
                    // In-flight sub-batch may have crossed a write boundary → unavailable.
                    // Later never-started placeholders stay known-not-committed.
                    if (mutate_inflight && index >= inflight_begin && index < inflight_end) {
                        return ErrorCode::unavailable;
                    }
                    if (mutate_inflight && index >= inflight_end) {
                        return ErrorCode::resource_exhausted;
                    }
                    if (index >= first_unprocessed) {
                        return durable_mutate_entered || publication_required ? ErrorCode::resource_exhausted
                                                                              : before_mutate;
                    }
                    return ErrorCode::unavailable;
                };
                try {
                    for (auto* node = rev; node != nullptr; node = node->next) {
                        nodes.push_back(node);
                    }
                    // Default Status{} is success — must not survive catch/drain for items
                    // never Store-mutated (would success-ACK invisible keys).
                    for (auto* node : nodes) {
                        node->status = Status{
                            fail(ErrorCode::resource_exhausted, "paired durable batch item not processed")};
                    }
                    views.reserve(nodes.size());
                    std::size_t begin = 0;
                    while (begin < nodes.size()) {
                        if (!healthy_.load(std::memory_order_acquire) ||
                            !detail::StoreAccess::operational(store_)) {
                            sticky_publication_failure = true;
                            sticky_pair_before_durable_mark();
                            for (std::size_t index = begin; index < nodes.size(); ++index) {
                                // Remaining siblings never entered mutate_durable_batch.
                                nodes[index]->status = Status{
                                    fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                            }
                            break;
                        }
                        const std::size_t end = durable_subbatch_end(
                            begin, nodes.size(),
                            [&](const std::size_t index) -> const HashedKey& { return *nodes[index]->key; });
                        views.clear();
                        for (std::size_t index = begin; index < end; ++index) {
                            auto* node = nodes[index];
                            views.push_back({.operation = node->kind == MutationKind::put
                                                              ? detail::StoreAccess::MutationOperation::put
                                                              : detail::StoreAccess::MutationOperation::erase,
                                             .key = *node->key,
                                             .value = node->value,
                                             .expire_at_ns = node->expire_at_ns});
                        }
                        GS_FAULT_SITE(mutate);
                        inflight_begin = begin;
                        inflight_end = end;
                        mutate_inflight = true;
                        durable_mutate_entered = true;
                        auto results = detail::StoreAccess::mutate_durable_batch(store_, shard, views);
                        // Keep mutate_inflight through classification: a throw here must not
                        // stamp Store-entered siblings as never-started resource_exhausted.
                        if (glyphastore::fault::consume_fail(glyphastore::fault::Site::post_mutate)) {
                            throw std::bad_alloc{};
                        }
                        if (results.size() != views.size()) {
                            std::terminate();
                        }
                        for (std::size_t offset = 0; offset < results.size(); ++offset) {
                            auto& result = results[offset].mutation;
                            publication_required = publication_required || result.committed();
                            if (!result.committed() || result.error) {
                                auto error = result.error ? std::move(*result.error)
                                                          : Error{ErrorCode::io_error,
                                                                  "durable mutation failed without an error"};
                                if (result.committed() ||
                                    result.outcome == DurableMutationOutcome::indeterminate) {
                                    // Defer durable mark_fail_closed until after sibling snapshot.
                                    error.code = ErrorCode::unavailable;
                                    sticky_publication_failure = true;
                                    sticky_pair_before_durable_mark();
                                    if (result.committed()) {
                                        sticky_committed_nodes.push_back(nodes[begin + offset]);
                                    }
                                } else {
                                    rewrite_known_not_committed_wire_error(error);
                                }
                                nodes[begin + offset]->status = Status{unexpected(std::move(error))};
                            } else {
                                nodes[begin + offset]->status = Status{};
                            }
                        }
                        first_unprocessed = end;
                        mutate_inflight = false;
                        begin = end;
                    }
                    if (publication_required) {
                        // allow_fail_closed: failing mutate may already have marked durable
                        // unhealthy while earlier siblings remain indexed.
                        auto snapshot = detail::StoreAccess::snapshot_durable_reads(store_, shard, true);
                        if (!snapshot) {
                            publish_fail_closed();
                            for (auto* node : nodes) {
                                if (node->status) {
                                    node->status =
                                        Status{fail(ErrorCode::unavailable,
                                                    "read publication failed after durable Writer batch")};
                                }
                            }
                        } else {
                            const auto outcome =
                                replace_durable_snapshot_and_publish(publication_ctx, snapshot->records);
                            if (outcome.status != DualPathPublishStatus::published) {
                                publish_fail_closed();
                                for (auto* node : nodes) {
                                    if (node->status) {
                                        node->status = Status{
                                            fail(ErrorCode::unavailable,
                                                 "read publication failed after durable Writer batch")};
                                    }
                                }
                            } else {
                                note_catalog_snapshot_installed(publication_ctx, snapshot->catalog_revision);
                                merge_retry_blocked = false;
                                reclaim_quiescent();
                                sibling_snapshot_published = true;
                                // ACK-after-visibility only for sticky committed+error items.
                                ack_sticky_after_visibility();
                                if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                                    throw std::bad_alloc{};
                                }
                                if (sticky_publication_failure) {
                                    publish_fail_closed();
                                }
                            }
                        }
                    } else if (sticky_publication_failure) {
                        publish_fail_closed();
                    }
                } catch (const std::bad_alloc&) {
                    if (publication_required || durable_mutate_entered) {
                        sibling_snapshot_published = drain_durable_snapshot();
                    }
                    if (publication_required || durable_mutate_entered) {
                        publish_fail_closed();
                    }
                    if (sibling_snapshot_published) {
                        // Only sticky Index-committed items — never presence-upgrade
                        // unprocessed attempted puts (pre-existing key would false-ACK).
                        ack_sticky_after_visibility();
                    }
                    for (std::size_t index = 0; index < nodes.size(); ++index) {
                        auto* node = nodes[index];
                        if (sibling_snapshot_published && node->status) {
                            continue;
                        }
                        if (!node->status) {
                            // Keep mid-chunk fail-closed / rewritten not-committed / sticky
                            // unavailable. Only the pre-mutate placeholder may be upgraded.
                            if (node->status.error().message.find(
                                    "paired durable batch item not processed") == std::string::npos) {
                                continue;
                            }
                        }
                        node->status = Status{fail(upgrade_placeholder(index, ErrorCode::resource_exhausted),
                                                   "paired mutation allocation failed")};
                    }
                } catch (...) {
                    if (publication_required || durable_mutate_entered) {
                        sibling_snapshot_published = drain_durable_snapshot();
                    }
                    if (publication_required || durable_mutate_entered) {
                        publish_fail_closed();
                    }
                    if (sibling_snapshot_published) {
                        ack_sticky_after_visibility();
                    }
                    for (std::size_t index = 0; index < nodes.size(); ++index) {
                        auto* node = nodes[index];
                        if (sibling_snapshot_published && node->status) {
                            continue;
                        }
                        if (!node->status) {
                            if (node->status.error().message.find(
                                    "paired durable batch item not processed") == std::string::npos) {
                                continue;
                            }
                        }
                        node->status = Status{fail(upgrade_placeholder(index, ErrorCode::resource_exhausted),
                                                   "paired Writer failure")};
                    }
                }
                // Durable may mark itself fail-closed while swallowing exceptions into
                // Status errors. Couple the pair sticky bit so sync mutate rejects next.
                // Do not rewrite successful sibling ACKs after a successful drain snapshot.
                if (durable_mutate_entered && !detail::StoreAccess::operational(store_)) {
                    publish_fail_closed();
                    if (!sibling_snapshot_published) {
                        for (auto* node = rev; node != nullptr; node = node->next) {
                            if (node->status) {
                                node->status =
                                    Status{fail(ErrorCode::unavailable, "paired runtime is fail-closed")};
                            }
                        }
                    }
                }
                for (auto* node = rev; node != nullptr;) {
                    // Snapshot next before notify: SyncMutation lives on the caller's stack and
                    // may be destroyed as soon as done is observed.
                    auto* const next = node->next;
                    node->done.store(true, std::memory_order_release);
                    node->done.notify_one();
                    node = next;
                }
                break;
            }
            if (!detail::StoreAccess::is_durable(store_)) {
                // Volatile sync: apply FIFO then one publish_incremental per ≤32
                // mutations (mirrors async). ACK only after publication. Stack
                // chunking keeps the N=1 path allocation-free.
                const auto reject_remaining_fail_closed = [&](SyncMutation*& head) noexcept {
                    while (head != nullptr) {
                        auto* const next = head->next;
                        // Later chunks never mutated after an earlier sticky close.
                        head->status =
                            Status{fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                        head->done.store(true, std::memory_order_release);
                        head->done.notify_one();
                        head = next;
                    }
                };
                while (rev != nullptr) {
                    // After publish_fail_closed in an earlier chunk, do not mutate or
                    // publish later chunks from the same sync drain (put_batch >32).
                    if (!healthy_.load(std::memory_order_acquire)) {
                        reject_remaining_fail_closed(rev);
                        break;
                    }
                    std::array<SyncMutation*, kMaximumPublicationBatch> chunk{};
                    std::size_t chunk_size = 0;
                    const auto chunk_cap = sync_publication_chunk_cap(kMaximumPublicationBatch);
                    while (rev != nullptr && chunk_size < chunk_cap) {
                        chunk[chunk_size++] = rev;
                        rev = rev->next;
                    }
                    reclaim_quiescent();
                    process_merge(chunk_size);
                    auto generation_admission = decide_generation_admission(
                        lane.generation.retired_debt(), ShardPairRuntime::kMaximumRetiredReadGenerations,
                        PairReadGeneration::can_publish_incremental(*lane.generation.writer_generation,
                                                                    lane.merge.read_merge.get(), chunk_size));
                    generation_admission = runtime_detail::wait_generation_admission(
                        generation_admission,
                        [&] {
                            reclaim_quiescent();
                            process_merge(chunk_size);
                        },
                        [&] {
                            return decide_generation_admission(
                                lane.generation.retired_debt(),
                                ShardPairRuntime::kMaximumRetiredReadGenerations,
                                PairReadGeneration::can_publish_incremental(
                                    *lane.generation.writer_generation, lane.merge.read_merge.get(),
                                    chunk_size));
                        });
                    if (generation_admission != GenerationAdmissionDecision::admitted) {
                        lane.generation.generation_admission_backpressure_total.fetch_add(
                            chunk_size, std::memory_order_relaxed);
                        if (generation_admission == GenerationAdmissionDecision::incremental_merge_required) {
                            lane.merge.read_merge_backpressure.fetch_add(1U, std::memory_order_relaxed);
                        }
                        for (std::size_t index = 0; index < chunk_size; ++index) {
                            // Never Store-entered — same polarity as async generation pressure.
                            chunk[index]->status =
                                Status{fail(ErrorCode::resource_exhausted,
                                            generation_admission_message(generation_admission))};
                            chunk[index]->done.store(true, std::memory_order_release);
                            chunk[index]->done.notify_one();
                        }
                        continue;
                    }
                    std::optional<GenerationSlotPool::Reservation> slot_reservation;
                    if (lane.generation.uses_slot_pool()) {
                        slot_reservation = try_reserve_publication_slot(lane.generation);
                        if (!slot_reservation) {
                            lane.generation.generation_admission_backpressure_total.fetch_add(
                                chunk_size, std::memory_order_relaxed);
                            for (std::size_t index = 0; index < chunk_size; ++index) {
                                chunk[index]->status = Status{
                                    fail(ErrorCode::resource_exhausted,
                                         generation_admission_message(
                                             GenerationAdmissionDecision::reader_quiescence_required))};
                                chunk[index]->done.store(true, std::memory_order_release);
                                chunk[index]->done.notify_one();
                            }
                            continue;
                        }
                    }
                    std::array<VolatileSyncMutationView, kMaximumPublicationBatch> view_storage{};
                    for (std::size_t index = 0; index < chunk_size; ++index) {
                        const auto* node = chunk[index];
                        view_storage[index] = VolatileSyncMutationView{
                            .kind = node->kind == MutationKind::put ? VolatileSyncMutationView::Kind::put
                                                                    : VolatileSyncMutationView::Kind::erase,
                            .key = node->key,
                            .value = node->value,
                            .expire_at_ns = node->expire_at_ns,
                            .status = node->status,
                        };
                    }
                    apply_volatile_sync_publication_chunk(
                        store_, shard, publication_ctx, std::span{view_storage.data(), chunk_size},
                        slot_reservation, VolatileSyncChunkMode::dedicated_writer, healthy_,
                        publish_fail_closed, reclaim_proportional, [&](const std::size_t published_count) {
                            reclaim_quiescent();
                            process_merge(published_count);
                        });
                    for (std::size_t index = 0; index < chunk_size; ++index) {
                        chunk[index]->status = view_storage[index].status;
                    }
                    for (std::size_t index = 0; index < chunk_size; ++index) {
                        chunk[index]->done.store(true, std::memory_order_release);
                        chunk[index]->done.notify_one();
                    }
                    if (!healthy_.load(std::memory_order_acquire)) {
                        reject_remaining_fail_closed(rev);
                        break;
                    }
                }
                break;
            }
            while (rev != nullptr) {
                if (!healthy_.load(std::memory_order_acquire)) {
                    for (auto* node = rev; node != nullptr;) {
                        auto* const next = node->next;
                        // Never Store-entered — known not newly committed.
                        node->status =
                            Status{fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                        node->done.store(true, std::memory_order_release);
                        node->done.notify_one();
                        node = next;
                    }
                    rev = nullptr;
                    break;
                }
                auto* node = rev;
                rev = rev->next;
                reclaim_quiescent();
                process_merge(1U);
                auto generation_admission = decide_generation_admission(
                    lane.generation.retired_debt(), ShardPairRuntime::kMaximumRetiredReadGenerations,
                    PairReadGeneration::can_publish_incremental(*lane.generation.writer_generation,
                                                                lane.merge.read_merge.get(), 1U));
                generation_admission = runtime_detail::wait_generation_admission(
                    generation_admission,
                    [&] {
                        reclaim_quiescent();
                        process_merge(1U);
                    },
                    [&] {
                        return decide_generation_admission(
                            lane.generation.retired_debt(), ShardPairRuntime::kMaximumRetiredReadGenerations,
                            PairReadGeneration::can_publish_incremental(*lane.generation.writer_generation,
                                                                        lane.merge.read_merge.get(), 1U));
                    });
                if (generation_admission != GenerationAdmissionDecision::admitted) {
                    lane.generation.generation_admission_backpressure_total.fetch_add(
                        1U, std::memory_order_relaxed);
                    if (generation_admission == GenerationAdmissionDecision::incremental_merge_required) {
                        lane.merge.read_merge_backpressure.fetch_add(1U, std::memory_order_relaxed);
                    }
                    node->status = Status{fail(ErrorCode::resource_exhausted,
                                               generation_admission_message(generation_admission))};
                    node->done.store(true, std::memory_order_release);
                    node->done.notify_one();
                    continue;
                }
                Status status{};
                bool durable_committed = false;
                bool durable_mutate_entered = false;
                bool generation_published = false;
                bool status_resolved = false;
                // Behavior-neutral shadow lifecycle (docs/spec/mutation-lifecycle.md).
                // Existing bools remain authoritative for control flow.
                MutationLifecycle life{};
                static_cast<void>(life.admit());
                static_cast<void>(life.stage_for_writer());
                const auto shadow_mark_published = [&](const bool published) noexcept {
                    if (published) {
                        static_cast<void>(life.mark_published());
                        return;
                    }
                    if (life.publication().state == PublicationState::required ||
                        life.publication().state == PublicationState::staged) {
                        static_cast<void>(life.mark_publication_failed());
                    }
                };
                const auto shadow_resolve_status = [&]() noexcept {
                    status_resolved = true;
                    if (life.stage() != MutationStage::completion_decided &&
                        life.stage() != MutationStage::completed) {
                        auto decided = decide_completion(life.durable(), life.publication());
                        if (status) {
                            decided.kind = CompletionDecision::Kind::success;
                        } else if (status.error().code == ErrorCode::unavailable) {
                            decided.kind = CompletionDecision::Kind::indeterminate;
                        } else {
                            decided.kind = CompletionDecision::Kind::known_not_committed;
                        }
                        static_cast<void>(life.decide(decided));
                    }
                    static_cast<void>(life.mark_completed());
                };
                try {
                    const auto& key = *node->key;
                    if (detail::StoreAccess::is_durable(store_)) {
                        durable_mutate_entered = true;
                        static_cast<void>(life.mark_durable_started());
                        auto result = execute_durable_single(store_, shard, node->kind, key, node->value,
                                                             node->expire_at_ns);
                        if (result.committed()) {
                            durable_committed = true;
                        }
                        static_cast<void>(life.apply_durable_result(result));
                        // After drain/publish: success ACK iff published generation matches the
                        // mutation (put hit / erase miss). Index-insert-fail stays error+miss.
                        const auto ack_after_published_visibility = [&]() -> Status {
                            const auto* published =
                                lane.generation.published_generation.load(std::memory_order_acquire);
                            if (published == nullptr) {
                                return Status{fail(ErrorCode::unavailable,
                                                   "paired read generation missing after drain")};
                            }
                            const auto view = published->prepare_durable(key);
                            if (node->kind == MutationKind::put) {
                                return view.has_value()
                                           ? Status{}
                                           : Status{fail(ErrorCode::unavailable,
                                                         "committed put missing from published generation")};
                            }
                            return (!view.has_value() && view.error().code == ErrorCode::not_found)
                                       ? Status{}
                                       : Status{fail(ErrorCode::unavailable,
                                                     "committed erase still visible after drain")};
                        };
                        if (!result.committed() || result.error) {
                            auto error = result.error ? *result.error
                                                      : Error{ErrorCode::io_error, "durable mutation failed"};
                            if (result.committed() ||
                                result.outcome == DurableMutationOutcome::indeterminate) {
                                error.code = ErrorCode::unavailable;
                                generation_published = drain_durable_snapshot();
                                shadow_mark_published(generation_published);
                                publish_fail_closed();
                                if (generation_published && result.committed()) {
                                    status = ack_after_published_visibility();
                                } else {
                                    status = Status{unexpected(std::move(error))};
                                }
                            } else {
                                rewrite_known_not_committed_wire_error(error);
                                status = Status{unexpected(std::move(error))};
                            }
                            shadow_resolve_status();
                        } else {
                            ReadMutation publication{
                                .key = key,
                                .record = RecordRef{.sequence = *result.sequence},
                                .opcode = node->kind == MutationKind::put ? Opcode::put : Opcode::erase};
                            if (node->kind == MutationKind::put) {
                                auto captured = detail::StoreAccess::capture_durable_read(store_, shard, key);
                                if (!captured) {
                                    generation_published = drain_durable_snapshot();
                                    shadow_mark_published(generation_published);
                                    publish_fail_closed();
                                    status = generation_published
                                                 ? ack_after_published_visibility()
                                                 : Status{fail(ErrorCode::unavailable,
                                                               "durable read capture failed")};
                                    shadow_resolve_status();
                                } else {
                                    publication.record = captured->reference();
                                    publication.durable.emplace(std::move(*captured));
                                    static_cast<void>(life.mark_publication_staged());
                                    auto next = PairReadGeneration::publish_incremental(
                                        lane.generation.writer_generation, std::span{&publication, 1},
                                        lane.merge.read_merge.get());
                                    if (!next) {
                                        generation_published = drain_durable_snapshot();
                                        shadow_mark_published(generation_published);
                                        publish_fail_closed();
                                        status = generation_published
                                                     ? ack_after_published_visibility()
                                                     : Status{fail(ErrorCode::unavailable,
                                                                   "read publication failed")};
                                        shadow_resolve_status();
                                    } else {
                                        install_writer_generation(
                                            lane.generation.writer_generation,
                                            lane.generation.retired_generations,
                                            lane.generation.retired_generation_count,
                                            lane.generation.writer_epoch,
                                            ShardPairRuntime::kMaximumRetiredReadGenerations,
                                            std::move(*next));
                                        update_delta_stats();
                                        publish_read_generation(lane.generation.published_generation,
                                                                lane.generation.writer_generation.get());
                                        // Mark published before reclaim so catch cannot invert RAW.
                                        generation_published = true;
                                        shadow_mark_published(true);
                                        if (glyphastore::fault::consume_fail(
                                                glyphastore::fault::Site::publish)) {
                                            throw std::bad_alloc{};
                                        }
                                        reclaim_proportional();
                                        status = Status{};
                                        shadow_resolve_status();
                                    }
                                }
                            } else {
                                static_cast<void>(life.mark_publication_staged());
                                auto next = PairReadGeneration::publish_incremental(
                                    lane.generation.writer_generation, std::span{&publication, 1},
                                    lane.merge.read_merge.get());
                                if (!next) {
                                    generation_published = drain_durable_snapshot();
                                    shadow_mark_published(generation_published);
                                    publish_fail_closed();
                                    status =
                                        generation_published
                                            ? ack_after_published_visibility()
                                            : Status{fail(ErrorCode::unavailable, "read publication failed")};
                                    shadow_resolve_status();
                                } else {
                                    install_writer_generation(
                                        lane.generation.writer_generation,
                                        lane.generation.retired_generations,
                                        lane.generation.retired_generation_count,
                                        lane.generation.writer_epoch,
                                        ShardPairRuntime::kMaximumRetiredReadGenerations, std::move(*next));
                                    update_delta_stats();
                                    publish_read_generation(lane.generation.published_generation,
                                                            lane.generation.writer_generation.get());
                                    generation_published = true;
                                    shadow_mark_published(true);
                                    if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                                        throw std::bad_alloc{};
                                    }
                                    reclaim_proportional();
                                    status = Status{};
                                    shadow_resolve_status();
                                }
                            }
                        }
                    } else {
                        status = Status{fail(ErrorCode::internal_error, "volatile sync path misrouted")};
                        shadow_resolve_status();
                    }
                } catch (const std::bad_alloc&) {
                    const SyncDurableExceptionContext recovery_ctx{
                        .durable_committed = durable_committed,
                        .durable_mutate_entered = durable_mutate_entered,
                        .generation_published = generation_published,
                        .status_resolved = status_resolved,
                    };
                    const auto recovery = plan_sync_durable_exception_recovery(recovery_ctx);
                    if (recovery.mark_exception_lifecycle) {
                        static_cast<void>(life.mark_exception_after_durable_start());
                    }
                    if (recovery.drain_if_unpublished) {
                        generation_published = drain_durable_snapshot() || generation_published;
                        shadow_mark_published(generation_published);
                    }
                    if (recovery.fail_closed) {
                        publish_fail_closed();
                    }
                    const auto status_plan =
                        plan_sync_durable_exception_status({.durable_committed = durable_committed,
                                                            .durable_mutate_entered = durable_mutate_entered,
                                                            .generation_published = generation_published,
                                                            .status_resolved = status_resolved});
                    switch (status_plan.kind) {
                    case SyncDurableExceptionStatusKind::keep_resolved:
                        // Keep definitive polarity — including visibility-failed errors after
                        // drain. Do not promote those to success just because a generation
                        // was published (inverted RAW).
                        break;
                    case SyncDurableExceptionStatusKind::success_after_visibility:
                        // Happy-path: authority published before status assignment
                        // (e.g. Site::publish after publish_read_generation).
                        status = Status{};
                        shadow_resolve_status();
                        break;
                    case SyncDurableExceptionStatusKind::unavailable_store_entered:
                        status = Status{fail(ErrorCode::unavailable, "paired mutation allocation failed")};
                        shadow_resolve_status();
                        break;
                    case SyncDurableExceptionStatusKind::resource_exhausted_never_entered:
                        status =
                            Status{fail(ErrorCode::resource_exhausted, "paired mutation allocation failed")};
                        shadow_resolve_status();
                        break;
                    }
                } catch (...) {
                    const SyncDurableExceptionContext recovery_ctx{
                        .durable_committed = durable_committed,
                        .durable_mutate_entered = durable_mutate_entered,
                        .generation_published = generation_published,
                        .status_resolved = status_resolved,
                    };
                    const auto recovery = plan_sync_durable_exception_recovery(recovery_ctx);
                    if (recovery.mark_exception_lifecycle) {
                        static_cast<void>(life.mark_exception_after_durable_start());
                    }
                    if (recovery.drain_if_unpublished) {
                        generation_published = drain_durable_snapshot() || generation_published;
                        shadow_mark_published(generation_published);
                    }
                    if (recovery.fail_closed) {
                        publish_fail_closed();
                    }
                    const auto status_plan =
                        plan_sync_durable_exception_status({.durable_committed = durable_committed,
                                                            .durable_mutate_entered = durable_mutate_entered,
                                                            .generation_published = generation_published,
                                                            .status_resolved = status_resolved});
                    switch (status_plan.kind) {
                    case SyncDurableExceptionStatusKind::keep_resolved:
                        break;
                    case SyncDurableExceptionStatusKind::success_after_visibility:
                        status = Status{};
                        shadow_resolve_status();
                        break;
                    case SyncDurableExceptionStatusKind::unavailable_store_entered:
                        status = Status{fail(ErrorCode::unavailable, "paired Writer failure")};
                        shadow_resolve_status();
                        break;
                    case SyncDurableExceptionStatusKind::resource_exhausted_never_entered:
                        status = Status{fail(ErrorCode::resource_exhausted, "paired Writer failure")};
                        shadow_resolve_status();
                        break;
                    }
                }
                if (durable_mutate_entered && !detail::StoreAccess::operational(store_)) {
                    if (!generation_published) {
                        generation_published = drain_durable_snapshot();
                        shadow_mark_published(generation_published);
                    }
                    publish_fail_closed();
                    // Match catch / durable_group sync: keep definitive polarity. A
                    // known-not-committed rewrite (resource_exhausted → wire OVERLOADED)
                    // must not be overwritten to unavailable (wire INTERNAL_ERROR /
                    // reconcile_first) just because the catalog went fail-closed.
                    // Only fill when unresolved; only demote unpublished success.
                    if (!generation_published) {
                        if (!status_resolved) {
                            status = Status{fail(ErrorCode::unavailable, "paired runtime is fail-closed")};
                            shadow_resolve_status();
                        } else if (status) {
                            status = Status{fail(ErrorCode::unavailable, "paired runtime is fail-closed")};
                        }
                    }
                }
#ifndef NDEBUG
                if (durable_mutate_entered) {
                    assert(life.durable().mutate_entered);
                    if (durable_committed) {
                        assert(life.durable().committed());
                    }
                    if (generation_published) {
                        assert(life.publication().published() || life.stage() == MutationStage::completed ||
                               life.stage() == MutationStage::completion_decided);
                    }
                    if (status_resolved) {
                        assert(life.stage() == MutationStage::completed ||
                               life.stage() == MutationStage::completion_decided);
                    }
                }
#endif
                node->status = status;
                node->done.store(true, std::memory_order_release);
                node->done.notify_one();
            }
            break;
        }
        if (drained_sync_turn && (carried_task.has_value() || lane.queue.size() != 0U)) {
            lane.metrics.sync_async_fairness_turns.fetch_add(1U, std::memory_order_relaxed);
        }

        auto task = carried_task ? std::exchange(carried_task, std::nullopt) : pop_queued();
        if (!task) {
            if (lane.async.stopping.load(std::memory_order_acquire) && carried_sync == nullptr) {
                release_execution_token(lane.async.execution_token);
                return;
            }
            const auto observed = lane.async.signal.load(std::memory_order_acquire);
            task = pop_queued();
            bool sync_pending = carried_sync != nullptr;
            if (!sync_pending) {
                const std::lock_guard lock{lane.sync.sync_mutex};
                sync_pending = lane.sync_head != nullptr;
            }
            if (!task && !sync_pending && !lane.async.stopping.load(std::memory_order_acquire) &&
                healthy_.load(std::memory_order_acquire)) {
                // Foreground completions are already delivered. Run one
                // bounded maintenance turn before deciding whether this
                // Writer can sleep; a just-reached delta threshold therefore
                // cannot strand a merge until the next client mutation.
                process_merge(0U);
            }
            if (!task && !sync_pending && !lane.async.stopping.load(std::memory_order_acquire) &&
                !lane.generation.refresh_requested.load(std::memory_order_acquire) &&
                !lane.generation.reclaim_requested.load(std::memory_order_acquire) &&
                !lane.merge.read_merge) {
                // Wave 2 wakeup ladder: pause-spin → OS yield → park on signal.
                // Keeps the hot handoff short while avoiding immediate
                // atomic::wait under brief Writer contention.
                bool woke = false;
                for (unsigned spin = 0; spin < 16U; ++spin) {
                    if (lane.async.signal.load(std::memory_order_acquire) != observed) {
                        woke = true;
                        break;
                    }
#if defined(__aarch64__) || defined(_M_ARM64)
                    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(_M_X64)
                    __builtin_ia32_pause();
#else
                    std::this_thread::yield();
#endif
                }
                for (unsigned yield_spin = 0; !woke && yield_spin < 8U; ++yield_spin) {
                    if (lane.async.signal.load(std::memory_order_acquire) != observed) {
                        woke = true;
                        break;
                    }
                    std::this_thread::yield();
                }
                if (!woke) {
                    release_execution_token(lane.async.execution_token);
                    lane.async.signal.wait(observed, std::memory_order_acquire);
                    continue;
                }
            }
            if (!task) {
                release_execution_token(lane.async.execution_token);
                continue;
            }
        }

        batch.clear();
        batch.push_back(std::move(*task));
        WriterAsyncBatchEnv async_env{
            .lane = lane,
            .shard = shard,
            .batch_config = batch_config,
            .maximum_batch_records = maximum_batch_records,
            .maximum_batch_bytes = maximum_batch_bytes,
            .batch = batch,
            .carried_task = carried_task,
            .merge_retry_blocked = merge_retry_blocked,
            .fail_closed = fail_closed,
            .publication_ctx = publication_ctx,
            .queue_waits = queue_waits,
            .service_started = service_started,
            .expired = expired,
            .completions = completions,
            .durable_views = durable_views,
            .durable_indices = durable_indices,
            .read_mutations = read_mutations,
            .read_mutation_indices = read_mutation_indices,
            .hooks = {},
        };
        async_env.hooks.publish_fail_closed = publish_fail_closed;
        async_env.hooks.reclaim_quiescent = reclaim_quiescent;
        async_env.hooks.process_merge = process_merge;
        async_env.hooks.drain_durable_snapshot = drain_durable_snapshot;
        async_env.hooks.update_delta_stats = update_delta_stats;
        run_writer_async_batch(async_env);
    }
}

} // namespace glyphastore::store::paired
