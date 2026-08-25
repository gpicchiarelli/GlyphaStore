#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/server/daemon_log.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "server/reactor_detail.hpp"
#include "server_reactor_test_support.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace glyphastore::test::server_reactor_support;

GLYPHA_TEST("durable daemon retries only a proven non-committed first-attempt sequence conflict") {
    using glyphastore::DurableMutationOutcome;
    using glyphastore::DurableMutationResult;
    using glyphastore::Error;
    using glyphastore::ErrorCode;
    const auto should_retry = [](const DurableMutationResult& result, const unsigned attempt) {
        return glyphastore::detail::StoreAccess::should_retry_durable_mutation(result, attempt);
    };

    const DurableMutationResult retryable{
        .outcome = DurableMutationOutcome::not_committed,
        .error = Error{ErrorCode::sequence_conflict, "stale rotation snapshot"},
    };
    GLYPHA_REQUIRE(should_retry(retryable, 0));
    GLYPHA_REQUIRE(!should_retry(retryable, 1));

    const DurableMutationResult committed{
        .outcome = DurableMutationOutcome::committed,
        .error = Error{ErrorCode::sequence_conflict, "post-commit diagnostic"},
    };
    const DurableMutationResult indeterminate{
        .outcome = DurableMutationOutcome::indeterminate,
        .error = Error{ErrorCode::sequence_conflict, "authority uncertain"},
    };
    const DurableMutationResult other_error{
        .outcome = DurableMutationOutcome::not_committed,
        .error = Error{ErrorCode::io_error, "pre-commit I/O failure"},
    };
    const DurableMutationResult missing_error{
        .outcome = DurableMutationOutcome::not_committed,
    };
    GLYPHA_REQUIRE(!should_retry(committed, 0));
    GLYPHA_REQUIRE(!should_retry(indeterminate, 0));
    GLYPHA_REQUIRE(!should_retry(other_error, 0));
    GLYPHA_REQUIRE(!should_retry(missing_error, 0));
}

GLYPHA_TEST("paired Writer completes incremental read merge in bounded quanta") {
    const glyphastore::server::PairReadMergeConfig merge_config{
        .delta_entries = 4,
        .maximum_post_entries = 8,
        .quantum_slots = 4'096,
    };
    auto opened =
        open_paired_store_for_writer(1, 8, kTestMutationArenaBytes,
                                     {.merge_delta_entries = merge_config.delta_entries,
                                      .merge_maximum_post_entries = merge_config.maximum_post_entries,
                                      .merge_quantum_slots = merge_config.quantum_slots});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0}, merge_config);
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());

    std::array<std::string, 4> keys{"merge-a", "merge-b", "merge-c", "merge-d"};
    for (std::size_t index = 0; index < keys.size(); ++index) {
        GLYPHA_REQUIRE(
            (*executor)
                ->try_submit({
                    .connection = {.slot = static_cast<std::uint32_t>(index + 1U), .generation = 1},
                    .request_id = 800U + index,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(keys[index]),
                    .key_hash = glyphastore::hash_key(keys[index]),
                    .value = bytes("value"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value());
    }

    std::size_t completed{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (completed != keys.size() && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            GLYPHA_REQUIRE(!completion->error.has_value());
            GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
            ++completed;
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completed == keys.size());

    glyphastore::server::PairWriterStats stats;
    const auto merge_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < merge_deadline) {
        static_cast<void>((*executor)->adopt_read_generation(0));
        stats = (*executor)->stats()[0];
        if (stats.read_merge_completions != 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(stats.read_merge_starts == 1);
    GLYPHA_REQUIRE(stats.read_merge_completions == 1);
    GLYPHA_REQUIRE(stats.read_merge_failures == 0);
    GLYPHA_REQUIRE(stats.read_merge_backpressure == 0);
    GLYPHA_REQUIRE(stats.read_merge_slots_processed > 0);
    GLYPHA_REQUIRE(!stats.read_merge_active);
    GLYPHA_REQUIRE(stats.read_merge_post_entries == 0);

    const auto* generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(generation != nullptr);
    GLYPHA_REQUIRE(generation->base_entries() == keys.size());
    GLYPHA_REQUIRE(generation->delta_entries() == 0);
    for (const auto& key : keys) {
        auto value = generation->get({.key = key, .hash = glyphastore::hash_key(key)}, 0);
        GLYPHA_REQUIRE(value.has_value());
        GLYPHA_REQUIRE(text(value->view()) == "value");
    }

    // Four versions of one existing key must start a second merge even though
    // the logical Delta contains only one entry. This bounds overwrite churn
    // in the append-only record arena.
    for (std::size_t index = 0; index < 4; ++index) {
        GLYPHA_REQUIRE(
            (*executor)
                ->try_submit({
                    .connection = {.slot = static_cast<std::uint32_t>(index + 9U), .generation = 1},
                    .request_id = 900U + index,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(keys[0]),
                    .key_hash = glyphastore::hash_key(keys[0]),
                    .value = bytes("new-value"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value());
    }
    completed = 0;
    const auto overwrite_completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (completed != 4 && std::chrono::steady_clock::now() < overwrite_completion_deadline) {
        if (auto completion = completions.try_pop()) {
            GLYPHA_REQUIRE(!completion->error.has_value());
            GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
            ++completed;
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completed == 4);
    const auto overwrite_merge_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < overwrite_merge_deadline) {
        static_cast<void>((*executor)->adopt_read_generation(0));
        stats = (*executor)->stats()[0];
        if (stats.read_merge_completions == 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(stats.read_merge_starts == 2);
    GLYPHA_REQUIRE(stats.read_merge_completions == 2);
    GLYPHA_REQUIRE(stats.delta_entries == 0);
    GLYPHA_REQUIRE(stats.delta_record_versions == 0);
    GLYPHA_REQUIRE(stats.delta_arena_record_bytes == 0);
    generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(generation != nullptr);
    const auto overwritten = generation->get({.key = keys[0], .hash = glyphastore::hash_key(keys[0])}, 0);
    GLYPHA_REQUIRE(overwritten.has_value());
    GLYPHA_REQUIRE(text(overwritten->bytes) == "new-value");

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer validates merge bounds and aligns payload credits with ring capacity") {
    auto opened = open_paired_store_for_writer(1, 3);
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, kTestMutationArenaBytes, std::chrono::milliseconds{0},
                        {.delta_entries = 0, .maximum_post_entries = 1, .quantum_slots = 1})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, kTestMutationArenaBytes, std::chrono::milliseconds{0},
                        {.delta_entries = 1, .maximum_post_entries = 1, .quantum_slots = 0})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, kTestMutationArenaBytes, std::chrono::milliseconds{0},
                        {.delta_entries = 1, .maximum_post_entries = 0, .quantum_slots = 1})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::PairWriterPool::create(
             store, 1, 8, kTestMutationArenaBytes, std::chrono::milliseconds{0},
             {.delta_entries = glyphastore::server::PairReadGeneration::kMaximumIncrementalDeltaEntries,
              .maximum_post_entries = 1,
              .quantum_slots = 1})
             .has_value());

    auto rounded = glyphastore::server::PairWriterPool::create(store, 1, 3, kTestMutationArenaBytes,
                                                               std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(rounded.has_value());
    const auto rounded_stats = (*rounded)->stats();
    GLYPHA_REQUIRE(rounded_stats.size() == 1);
    GLYPHA_REQUIRE(rounded_stats[0].payload_slot_capacity == 4);
    rounded->reset();
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer feeds one bounded maintenance latency window") {
    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 2,
                    .async_lane_payload_bytes = kTestMutationArenaBytes,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .maintenance = {.mode = glyphastore::MaintenanceMode::background,
                         .min_eval_interval_ms = 60'000,
                         .max_eval_interval_ms = 60'000,
                         .suspend_on_p99_latency_ms = std::numeric_limits<std::uint32_t>::max()}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* maintenance = glyphastore::detail::StoreAccess::maintenance_controller(store);
    GLYPHA_REQUIRE(maintenance != nullptr);
    const auto initial_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((store.maintenance_snapshot().evaluation_cycles == 0 ||
            store.maintenance_snapshot().state != glyphastore::MaintenanceState::idle) &&
           std::chrono::steady_clock::now() < initial_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const auto initial_cycles = store.maintenance_snapshot().evaluation_cycles;
    GLYPHA_REQUIRE(initial_cycles > 0);

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{2};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 2, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const std::string key{"latency-feedback"};
    GLYPHA_REQUIRE((*executor)
                       ->try_submit({
                           .connection = {.slot = 1, .generation = 1},
                           .request_id = 601,
                           .worker_index = 0,
                           .kind = glyphastore::server::MutationKind::put,
                           .key = bytes(key),
                           .key_hash = glyphastore::hash_key(key),
                           .value = bytes("value"),
                           .completions = &completions,
                           .wakeup = &*wakeup,
                       })
                       .has_value());
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::optional<glyphastore::server::MutationCompletion> completion;
    while (!completion && std::chrono::steady_clock::now() < completion_deadline) {
        completion = completions.try_pop();
        if (!completion) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completion.has_value());
    GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
    GLYPHA_REQUIRE(!completion->error.has_value());

    auto snapshot = store.maintenance_snapshot();
    const bool feedback_already_consumed =
        snapshot.evaluation_cycles > initial_cycles && snapshot.foreground_latency_samples == 1;
    if (!feedback_already_consumed) {
        const auto cycle_before_request = snapshot.evaluation_cycles;
        maintenance->request_evaluate();
        const auto feedback_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (store.maintenance_snapshot().evaluation_cycles == cycle_before_request &&
               std::chrono::steady_clock::now() < feedback_deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        snapshot = store.maintenance_snapshot();
    }
    GLYPHA_REQUIRE(snapshot.evaluation_cycles > initial_cycles);
    GLYPHA_REQUIRE(snapshot.foreground_latency_samples == 1);
    GLYPHA_REQUIRE(snapshot.last_foreground_p99_ns >= 1'000'000ULL);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer preserves same-shard FIFO while compaction publication is active") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = kTestMutationArenaBytes,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("retry-seed", bytes("v1")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("retry-seed", bytes("v2")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("active-seed", bytes("active")).has_value());

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());

    const auto submit = [&](const std::uint64_t request_id, std::string key, std::string_view value) {
        const auto hash = glyphastore::hash_key(key);
        return (*executor)
            ->try_submit({
                .connection = {.slot = static_cast<std::uint32_t>(request_id), .generation = 1},
                .request_id = request_id,
                .worker_index = 0,
                .kind = glyphastore::server::MutationKind::put,
                .key = bytes(key),
                .key_hash = hash,
                .value = bytes(value),
                .completions = &completions,
                .wakeup = &*wakeup,
            })
            .has_value();
    };

    const auto baseline_rotations = store.maintenance_snapshot().rotation.attempts;
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(submit(501, "retry-after-lease", "first"));
    const auto rotation_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (store.maintenance_snapshot().rotation.attempts == baseline_rotations &&
           std::chrono::steady_clock::now() < rotation_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const bool rotation_waiting = store.maintenance_snapshot().rotation.attempts > baseline_rotations;
    GLYPHA_REQUIRE(submit(502, "progress-during-lease", "second"));

    std::vector<glyphastore::server::MutationCompletion> observed;
    const auto fifo_probe_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (std::chrono::steady_clock::now() < fifo_probe_deadline) {
        if (auto completion = completions.try_pop()) {
            GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
            observed.push_back(std::move(*completion));
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const bool no_completion_before_release = observed.empty();

    blocker.release();
    compactor.join();
    const auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (observed.size() != 2 && std::chrono::steady_clock::now() < retry_deadline) {
        if (auto completion = completions.try_pop()) {
            GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
            observed.push_back(std::move(*completion));
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(rotation_waiting);
    GLYPHA_REQUIRE(no_completion_before_release);
    GLYPHA_REQUIRE(observed.size() == 2);
    GLYPHA_REQUIRE(observed[0].request_id == 501);
    GLYPHA_REQUIRE(observed[1].request_id == 502);
    GLYPHA_REQUIRE(!observed[0].error.has_value());
    GLYPHA_REQUIRE(!observed[1].error.has_value());
    const auto stats = (*executor)->stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE(text(store.get("retry-after-lease")->view()) == "first");
    GLYPHA_REQUIRE(text(store.get("progress-during-lease")->view()) == "second");

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Reader refreshes compacted durable pins and retires the old generation") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = kTestMutationArenaBytes,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("refresh-key", bytes("old")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("refresh-key", bytes("current")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("active-key", bytes("active")).has_value());

    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto* initial_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(initial_generation != nullptr);
    const std::string key{"refresh-key"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key(key)};
    glyphastore::RecordRef initial_reference;
    std::optional<glyphastore::detail::StoreAccess::PreparedGet> pending_read;
    {
        auto initial_record = initial_generation->prepare_durable(hashed);
        GLYPHA_REQUIRE(initial_record.has_value());
        initial_reference = initial_record->reference();
        auto prepared =
            glyphastore::detail::StoreAccess::prepare_published_durable_get(store, 0, *initial_record, 0);
        GLYPHA_REQUIRE(prepared.has_value());
        GLYPHA_REQUIRE(!prepared->value.has_value());
        GLYPHA_REQUIRE(prepared->cold.has_value());
        pending_read.emplace(std::move(*prepared));
    }
    const auto initial_epoch = initial_generation->epoch();
    const auto initial_revision = glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();
    compactor.join();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE((*compacted)->compacted);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   initial_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((*executor)->stats()[0].read_refresh_successes == 0 &&
           std::chrono::steady_clock::now() < refresh_deadline) {
        (*executor)->request_read_refresh(0);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    auto stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.read_refresh_attempts >= 1);
    GLYPHA_REQUIRE(stats.read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats.read_refresh_failures == 0);
    GLYPHA_REQUIRE(stats.read_catalog_revision > initial_revision);

    // Simulate one asynchronous cold read that still borrows the initial
    // generation. Reader may adopt the new pointer, but Writer must keep the
    // old generation until the explicit minimum lease epoch advances.
    const auto* refreshed_generation = (*executor)->adopt_read_generation(0, initial_epoch);
    GLYPHA_REQUIRE(refreshed_generation != nullptr);
    GLYPHA_REQUIRE(refreshed_generation->epoch() > initial_epoch);
    auto refreshed_record = refreshed_generation->prepare_durable(hashed);
    GLYPHA_REQUIRE(refreshed_record.has_value());
    GLYPHA_REQUIRE(refreshed_record->reference().sequence == initial_reference.sequence);
    GLYPHA_REQUIRE(refreshed_record->reference().segment_id != initial_reference.segment_id);

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.reader_safe_epoch == initial_epoch);
    GLYPHA_REQUIRE(stats.writer_epoch == refreshed_generation->epoch());
    GLYPHA_REQUIRE(stats.retired_generation_count == 1);

    // The cold task borrows both the key and the Segment generation from the
    // retired read generation. Completing it here proves that the advertised
    // safe epoch, rather than a per-request shared_ptr, pins the complete read
    // state across compaction publication and source retirement.
    GLYPHA_REQUIRE(pending_read.has_value());
    GLYPHA_REQUIRE(pending_read->cold.has_value());
    auto borrowed_value =
        glyphastore::detail::StoreAccess::complete_get_owned(store, 0, std::move(*pending_read->cold));
    GLYPHA_REQUIRE(borrowed_value.has_value());
    GLYPHA_REQUIRE(text(borrowed_value->view()) == "current");

    const auto* released_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(released_generation == refreshed_generation);

    const auto reclaim_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    do {
        stats = (*executor)->stats()[0];
        if (stats.retired_generation_count == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < reclaim_deadline);
    GLYPHA_REQUIRE(stats.generations_retired >= 1);
    GLYPHA_REQUIRE(stats.retired_generation_count == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Reader refreshes durable pins after a Writer-owned rotation") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 4,
                    .async_lane_payload_bytes = kTestMutationArenaBytes,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("rotation-base", bytes("base")).has_value());

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 4, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto* initial_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(initial_generation != nullptr);
    const auto initial_epoch = initial_generation->epoch();
    const auto initial_revision = glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);

    blocker.force_next_record_write_full();
    const std::string key{"rotation-published"};
    GLYPHA_REQUIRE((*executor)
                       ->try_submit({
                           .connection = {.slot = 1, .generation = 1},
                           .request_id = 701,
                           .worker_index = 0,
                           .kind = glyphastore::server::MutationKind::put,
                           .key = bytes(key),
                           .key_hash = glyphastore::hash_key(key),
                           .value = bytes("rotated"),
                           .completions = &completions,
                           .wakeup = &*wakeup,
                       })
                       .has_value());
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::optional<glyphastore::server::MutationCompletion> completion;
    while (!completion && std::chrono::steady_clock::now() < completion_deadline) {
        completion = completions.try_pop();
        if (!completion) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completion.has_value());
    GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
    GLYPHA_REQUIRE(!completion->error.has_value());
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   initial_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((*executor)->stats()[0].read_refresh_successes == 0 &&
           std::chrono::steady_clock::now() < refresh_deadline) {
        (*executor)->request_read_refresh(0);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const auto* refreshed_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(refreshed_generation != nullptr);
    GLYPHA_REQUIRE(refreshed_generation->epoch() >= initial_epoch + 2U);
    auto published = refreshed_generation->prepare_durable({.key = key, .hash = glyphastore::hash_key(key)});
    GLYPHA_REQUIRE(published.has_value());
    auto sealed = refreshed_generation->prepare_durable(
        {.key = "rotation-base", .hash = glyphastore::hash_key("rotation-base")});
    GLYPHA_REQUIRE(sealed.has_value());
    GLYPHA_REQUIRE(refreshed_generation->delta_entries() == 0);
    GLYPHA_REQUIRE(refreshed_generation->base_entries() == 2);
    const auto stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats.read_refresh_failures == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("durable read catalog refresh is isolated to the compacted shard pair") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 2},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = kTestMutationArenaBytes,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const auto key_for = [](const std::string_view prefix, const std::size_t worker) {
        for (std::size_t suffix = 0; suffix < 10'000; ++suffix) {
            auto key = std::string{prefix} + std::to_string(suffix);
            if (glyphastore::route_worker(key, 2) == worker) {
                return key;
            }
        }
        return std::string{};
    };
    const auto compacted_key = key_for("isolated-compact-", 0);
    const auto active_key = key_for("isolated-active-", 0);
    const auto other_key = key_for("isolated-other-", 1);
    GLYPHA_REQUIRE(!compacted_key.empty());
    GLYPHA_REQUIRE(!active_key.empty());
    GLYPHA_REQUIRE(!other_key.empty());

    GLYPHA_REQUIRE(store.put(compacted_key, bytes("v1")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put(compacted_key, bytes("v2")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put(active_key, bytes("active")).has_value());
    GLYPHA_REQUIRE(store.put(other_key, bytes("other")).has_value());

    auto executor = glyphastore::server::PairWriterPool::create(store, 2, 8, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto worker_zero_revision =
        glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);
    const auto worker_one_revision =
        glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 1);

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();
    compactor.join();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE((*compacted)->compacted);
    GLYPHA_REQUIRE((*compacted)->worker_index == 0);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   worker_zero_revision);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 1) ==
                   worker_one_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::vector<glyphastore::server::PairWriterStats> stats;
    do {
        (*executor)->request_read_refresh(0);
        (*executor)->request_read_refresh(1);
        stats = (*executor)->stats();
        if (stats[0].read_refresh_successes != 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < refresh_deadline);
    GLYPHA_REQUIRE(stats.size() == 2);
    GLYPHA_REQUIRE(stats[0].read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats[1].read_refresh_attempts == 0);
    GLYPHA_REQUIRE(stats[1].read_refresh_successes == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("server rejects unsupported worker counts and undersized protocol buffers") {
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .worker_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_input_bytes = glyphastore::server::kRequestHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_output_bytes = glyphastore::server::kResponseHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0,
                         .accepted_socket_send_buffer_bytes =
                             static_cast<std::size_t>(std::numeric_limits<int>::max()) + 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .disk_read_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_bytes = 0}).has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .disk_read_thread_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .worker_count = 2, .disk_read_thread_count = 1})
             .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create({.port = 0, .worker_count = 2},
                                                        {.worker_config = {.explicit_count = 1}})
                        .has_value());
}

GLYPHA_TEST("server StoreConfig persists acknowledged wire writes across restart") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 4},
            {.storage_mode = glyphastore::StorageMode::durable_sync,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(opened.has_value());
        auto& server = **opened;
        GLYPHA_REQUIRE(server.start().has_value());

        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 3,
            .key = bytes("durable-wire-key"),
            .value = bytes("durable-wire-value"),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto put_frame = receive_response(socket);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(socket));
        server.request_stop();
        GLYPHA_REQUIRE(server.join().has_value());
    }

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    auto& server = **reopened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 4,
        .key = bytes("durable-wire-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    const auto get_frame = receive_response(socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "durable-wire-value");
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP before INIT returns NOT_BOUND and creates no destination") {
    // BACKUP is Bound-state only; unbound frames must not run the fenced path.
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto backup_dir = temporary.store_path().parent_path() / "unbound-backup";
    const auto backup_path = backup_dir.string();
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 89,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.request_id == 89);
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::not_bound);
    GLYPHA_REQUIRE(!std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP copies a live durable Server catalog into an empty destination") {
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 90,
        .key = bytes("backup-live-key"),
        .value = bytes("backup-live-value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto backup_dir = temporary.store_path().parent_path() / "online-backup";
    const auto backup_path = backup_dir.string();
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 91,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(backup_response->frame.value);
    GLYPHA_REQUIRE(report.find("status=ok") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("admission_fence_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("catalog_copy_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("destination_verify_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("segment_copy_workers=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("source_crc_scanned=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("destination_crc_scanned=") != std::string_view::npos);

    // Offline tool still fails while the Server holds the lock.
    const auto contested = glyphastore::backup_durable_store(
        temporary.store_path(), temporary.store_path().parent_path() / "offline-contested");
    GLYPHA_REQUIRE(!contested.has_value());

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_dir = temporary.store_path().parent_path() / "restored";
    const auto restored = glyphastore::restore_durable_store(backup_dir, restored_dir);
    GLYPHA_REQUIRE(restored.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    const auto got = (*reopened)->get("backup-live-key");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(text(got->bytes) == "backup-live-value");
}

GLYPHA_TEST("wire BACKUP refuses before fence when OK report cannot fit output budget") {
    // Oversized OK report used to map to OVERLOADED after a successful fenced copy —
    // false known-not-committed polarity while the destination already held the backup.
    ServerTemporaryDirectory temporary;
    const auto backup_dir = temporary.store_path().parent_path() / "fit-refuse-backup";
    const auto backup_path = backup_dir.string();
    const auto estimated =
        glyphastore::server::reactor_detail::backup_ok_report_max_bytes(backup_path.size());
    GLYPHA_REQUIRE(estimated > 64);
    const auto max_output = glyphastore::server::kResponseHeaderBytes + 64;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .maximum_output_bytes = max_output},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 92,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.request_id == 92);
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(!std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP keeps OK after report formatting fails post-commit") {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Site::backup_report throws after backup_to succeeds. Probe must still return
    // success (minimal status=ok) — not INTERNAL_ERROR with destination already filled.
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 93,
        .key = bytes("backup-report-key"),
        .value = bytes("backup-report-value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto backup_dir = temporary.store_path().parent_path() / "report-fault-backup";
    const auto backup_path = backup_dir.string();
    glyphastore::fault::fail_once(glyphastore::fault::Site::backup_report);
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 94,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(backup_response->frame.value);
    GLYPHA_REQUIRE(report.find("status=ok") != std::string_view::npos);
    GLYPHA_REQUIRE(std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_dir = temporary.store_path().parent_path() / "report-fault-restored";
    const auto restored = glyphastore::restore_durable_store(backup_dir, restored_dir);
    GLYPHA_REQUIRE(restored.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    const auto got = (*reopened)->get("backup-report-key");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(text(got->bytes) == "backup-report-value");
#endif
}

GLYPHA_TEST("blocked durable mutation leaves its Reactor responsive with bounded FIFO admission") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .durable_mutation_queue_capacity = 2},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    const auto responsive_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(responsive_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(responsive_socket, 0, 1));

    blocker.arm();
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 70,
        .key = bytes("async-first"),
        .value = bytes("first"),
    });
    const auto ordered_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 74,
        .key = bytes("async-first"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(ordered_get.has_value());
    std::vector<std::byte> first_pipeline;
    first_pipeline.insert(first_pipeline.end(), first->begin(), first->end());
    first_pipeline.insert(first_pipeline.end(), ordered_get->begin(), ordered_get->end());
    GLYPHA_REQUIRE(send_all(first_socket, first_pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    // A second mutation must be admitted without waiting for the lane's slow
    // I/O, proving that its queue mutex is not an equivalent storage lock.
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 71,
        .key = bytes("async-second"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(send_all(second_socket, *second));

    // send_all only proves kernel admission. Wait until the second mutation has
    // consumed the remaining bounded lane slot before asserting that the next
    // connection is rejected; slow OpenBSD runners can otherwise schedule the
    // responsive socket first.
    bool lane_full = false;
    const auto admission_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < admission_deadline) {
        const auto stats = server.pair_writer_stats();
        if (stats.size() == 1 && stats[0].payload_slots_in_use == 2) {
            lane_full = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(lane_full);

    // The per-Worker admission budget is now exhausted. Rejection and the
    // following non-storage request are both handled while fsync is suspended.
    const auto rejected = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 72,
        .key = bytes("async-rejected"),
        .value = bytes("rejected"),
    });
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 73,
        .value = bytes("reactor-live"),
    });
    GLYPHA_REQUIRE(rejected.has_value());
    GLYPHA_REQUIRE(ping.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), rejected->begin(), rejected->end());
    pipeline.insert(pipeline.end(), ping->begin(), ping->end());
    GLYPHA_REQUIRE(send_all(responsive_socket, pipeline));
    const auto rejected_frame = receive_response(responsive_socket);
    const auto ping_frame = receive_response(responsive_socket);
    const auto rejected_response = glyphastore::server::decode_response(rejected_frame);
    const auto ping_response = glyphastore::server::decode_response(ping_frame);
    GLYPHA_REQUIRE(rejected_response.has_value());
    GLYPHA_REQUIRE(rejected_response->frame.request_id == 72);
    GLYPHA_REQUIRE(rejected_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(ping_response->frame.request_id == 73);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "reactor-live");

    blocker.release();
    const auto first_frame = receive_response(first_socket);
    const auto ordered_get_frame = receive_response(first_socket);
    const auto second_frame = receive_response(second_socket);
    const auto first_response = glyphastore::server::decode_response(first_frame);
    const auto ordered_get_response = glyphastore::server::decode_response(ordered_get_frame);
    const auto second_response = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first_response.has_value());
    GLYPHA_REQUIRE(ordered_get_response.has_value());
    GLYPHA_REQUIRE(second_response.has_value());
    GLYPHA_REQUIRE(first_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(ordered_get_response->frame.request_id == 74);
    GLYPHA_REQUIRE(ordered_get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ordered_get_response->frame.value) == "first");
    GLYPHA_REQUIRE(second_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto mutation_stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(mutation_stats.size() == 1);
    GLYPHA_REQUIRE(mutation_stats[0].queue_depth == 0);
    GLYPHA_REQUIRE(mutation_stats[0].queued_bytes == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queue_depth >= 1);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queued_bytes > 0);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slot_capacity == 2);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slots_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_slots_in_use == 2);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_capacity_bytes == 16U * 1024U * 1024U);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_storage_bytes >
                   mutation_stats[0].payload_arena_capacity_bytes);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_bytes_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_arena_bytes_in_use >= 34);
    GLYPHA_REQUIRE(mutation_stats[0].payload_admission_bytes_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_admission_bytes_in_use >= 290);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slot_full_total == 1);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_full_total == 0);
    GLYPHA_REQUIRE(mutation_stats[0].payload_too_large_total == 0);
    GLYPHA_REQUIRE(mutation_stats[0].admitted == 2);
    GLYPHA_REQUIRE(mutation_stats[0].rejected == 1);
    GLYPHA_REQUIRE(mutation_stats[0].expired_before_store == 0);
    GLYPHA_REQUIRE(mutation_stats[0].completed == 2);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retries == 0);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retry_commits == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_service_ns > 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    static_cast<void>(::close(responsive_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("mutation completion resumes a bounded pipeline without reordering decided responses") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 501,
        .key = bytes("resume-a"),
        .value = bytes("one"),
    });
    const auto get_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 502,
        .key = bytes("resume-a"),
    });
    const auto put_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 503,
        .key = bytes("resume-b"),
        .value = bytes("two"),
    });
    const auto get_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 504,
        .key = bytes("resume-b"),
    });
    GLYPHA_REQUIRE(put_a.has_value());
    GLYPHA_REQUIRE(get_a.has_value());
    GLYPHA_REQUIRE(put_b.has_value());
    GLYPHA_REQUIRE(get_b.has_value());

    std::vector<std::byte> pipeline;
    for (const auto* frame : {&*put_a, &*get_a, &*put_b, &*get_b}) {
        pipeline.insert(pipeline.end(), frame->begin(), frame->end());
    }
    // The second completion encounters this only after ACK/GET responses have
    // been decided. They must drain in order before the connection closes.
    std::array<std::byte, glyphastore::server::kRequestHeaderBytes> malformed{};
    malformed[0] = std::byte{static_cast<unsigned char>(glyphastore::server::kRequestHeaderBytes)};
    malformed[4] = std::byte{0xff};
    pipeline.insert(pipeline.end(), malformed.begin(), malformed.end());

    blocker.arm();
    GLYPHA_REQUIRE(send_all(socket, pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();

    for (const auto& [request_id, expected_value] :
         {std::pair{501ULL, std::string_view{}}, std::pair{502ULL, std::string_view{"one"}},
          std::pair{503ULL, std::string_view{}}, std::pair{504ULL, std::string_view{"two"}}}) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == request_id);
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        if (!expected_value.empty()) {
            GLYPHA_REQUIRE(text(response->frame.value) == expected_value);
        }
    }

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("pending output stops mutation completion pipeline resume until socket drain") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .accepted_socket_send_buffer_bytes = 4U * 1024U},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    int receive_buffer_bytes = 4 * 1024;
    GLYPHA_REQUIRE(::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &receive_buffer_bytes,
                                sizeof(receive_buffer_bytes)) == 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 511,
        .key = bytes("slow-a"),
        .value = bytes("one"),
    });
    constexpr std::size_t kPingBytes = 512U * 1024U;
    const std::vector<std::byte> ping_value(kPingBytes, std::byte{0x61});
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 512,
        .value = ping_value,
    });
    const auto put_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 513,
        .key = bytes("slow-b"),
        .value = bytes("two"),
    });
    const auto put_c = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 514,
        .key = bytes("slow-c"),
        .value = bytes("three"),
    });
    GLYPHA_REQUIRE(put_a.has_value());
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(put_b.has_value());
    GLYPHA_REQUIRE(put_c.has_value());

    std::vector<std::byte> pipeline;
    for (const auto* frame : {&*put_a, &*ping, &*put_b, &*put_c}) {
        pipeline.insert(pipeline.end(), frame->begin(), frame->end());
    }

    blocker.arm();
    GLYPHA_REQUIRE(send_all(socket, pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();

    // Completion A resumes the buffered PING and admits B. Its large response
    // cannot drain into the deliberately small TCP windows. Completion B must
    // therefore leave C buffered instead of advancing the Writer lane.
    bool two_completed = false;
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < completion_deadline) {
        const auto stats = server.pair_writer_stats();
        if (stats.size() == 1 && stats[0].completed == 2) {
            GLYPHA_REQUIRE(stats[0].admitted == 2);
            two_completed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(two_completed);
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 2);

    for (const auto request_id : {511ULL, 512ULL, 513ULL, 514ULL}) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == request_id);
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        if (request_id == 512) {
            GLYPHA_REQUIRE(response->frame.value.size() == kPingBytes);
        }
    }

    stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 3);
    GLYPHA_REQUIRE(stats[0].completed == 3);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("mutation payload arena applies byte backpressure independently of queue slots") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 4,
         .durable_mutation_queue_bytes = 300},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));

    const std::string value(64, 'v');
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 77,
        .key = bytes("arena-first"),
        .value = bytes(value),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 78,
        .key = bytes("arena-second"),
        .value = bytes(value),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    blocker.arm();
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(second_socket, *second));

    const auto rejected_frame = receive_response(second_socket);
    const auto rejected = glyphastore::server::decode_response(rejected_frame);
    GLYPHA_REQUIRE(rejected.has_value());
    GLYPHA_REQUIRE(rejected->frame.request_id == 78);
    GLYPHA_REQUIRE(rejected->frame.status == glyphastore::server::ResponseStatus::overloaded);
    auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].payload_slot_capacity == 4);
    GLYPHA_REQUIRE(stats[0].payload_slots_in_use == 1);
    GLYPHA_REQUIRE(stats[0].payload_arena_capacity_bytes == 300);
    GLYPHA_REQUIRE(stats[0].payload_arena_bytes_in_use == 75);
    GLYPHA_REQUIRE(stats[0].payload_admission_bytes_in_use == 203);
    GLYPHA_REQUIRE(stats[0].payload_slot_full_total == 0);
    GLYPHA_REQUIRE(stats[0].payload_arena_full_total == 1);

    blocker.release();
    const auto committed_frame = receive_response(first_socket);
    const auto committed = glyphastore::server::decode_response(committed_frame);
    GLYPHA_REQUIRE(committed.has_value());
    GLYPHA_REQUIRE(committed->frame.request_id == 77);
    GLYPHA_REQUIRE(committed->frame.status == glyphastore::server::ResponseStatus::ok);
    stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats[0].payload_slots_in_use == 0);
    GLYPHA_REQUIRE(stats[0].payload_arena_bytes_in_use == 0);
    GLYPHA_REQUIRE(stats[0].payload_admission_bytes_in_use == 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable mutation queue deadline rejects only before Store execution") {
#if defined(__OpenBSD__)
    // Hosted OpenBSD qemu VMs do not reliably reach the hooked durable sync barrier for
    // this multi-connection deadline race; Linux/FreeBSD/macOS remain the authority.
    return;
#endif
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 2,
         .durable_mutation_queue_wait_ms = 10},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());
    const auto blocked_socket = connect_to(server.port());
    const auto expiring_socket = connect_to(server.port());
    GLYPHA_REQUIRE(blocked_socket >= 0);
    GLYPHA_REQUIRE(expiring_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(blocked_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(expiring_socket, 0, 1));

    blocker.arm();
    const auto blocked = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 75,
        .key = bytes("deadline-blocker"),
        .value = bytes("committed"),
    });
    const auto expiring = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 76,
        .key = bytes("deadline-expired"),
        .value = bytes("must-not-commit"),
    });
    GLYPHA_REQUIRE(blocked.has_value());
    GLYPHA_REQUIRE(expiring.has_value());
    GLYPHA_REQUIRE(send_all(blocked_socket, *blocked));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(expiring_socket, *expiring));
    const auto expiry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{30};
    while (std::chrono::steady_clock::now() < expiry_deadline) {
        std::this_thread::yield();
    }
    blocker.release();

    const auto blocked_frame = receive_response(blocked_socket);
    const auto expired_frame = receive_response(expiring_socket);
    const auto blocked_response = glyphastore::server::decode_response(blocked_frame);
    const auto expired_response = glyphastore::server::decode_response(expired_frame);
    GLYPHA_REQUIRE(blocked_response.has_value());
    GLYPHA_REQUIRE(expired_response.has_value());
    GLYPHA_REQUIRE(blocked_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(expired_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    const auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 2);
    GLYPHA_REQUIRE(stats[0].expired_before_store == 1);
    GLYPHA_REQUIRE(stats[0].completed == 2);
    GLYPHA_REQUIRE(stats[0].maximum_queue_wait_ns >= 10'000'000U);

    static_cast<void>(::close(blocked_socket));
    static_cast<void>(::close(expiring_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->get("deadline-blocker").has_value());
    const auto absent = (*recovered)->get("deadline-expired");
    GLYPHA_REQUIRE(!absent.has_value());
    GLYPHA_REQUIRE(absent.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("volatile pair sticky fails READY with pair_fail_closed reason") {
    // Store catalog stays operational on volatile sticky; ready() already fails on
    // pair_writers_->healthy(), but classify_ready_loss must not report none.
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4, .worker_count = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.ready());
    GLYPHA_REQUIRE(server.store_operational());
    GLYPHA_REQUIRE(server.pair_writers_healthy());
    GLYPHA_REQUIRE(glyphastore::server::classify_ready_loss(server) ==
                   glyphastore::server::ReadyLossReason::none);

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    glyphastore::fault::reset();
    glyphastore::fault::configure(1, 0, 0);
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 91,
        .key = bytes("volatile-sticky"),
        .value = bytes("x"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.request_id == 91);
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok ||
                   put_response->frame.status == glyphastore::server::ResponseStatus::internal_error);
    GLYPHA_REQUIRE(put_response->frame.status != glyphastore::server::ResponseStatus::overloaded);

    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.store_operational());
    GLYPHA_REQUIRE(!server.pair_writers_healthy());
    GLYPHA_REQUIRE(!server.ready());
    GLYPHA_REQUIRE(glyphastore::server::classify_ready_loss(server) ==
                   glyphastore::server::ReadyLossReason::pair_fail_closed);

    const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 92);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 93);
    GLYPHA_REQUIRE(ready.has_value());
    GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
#endif
