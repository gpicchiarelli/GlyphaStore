#include "experimental/generation_slot_pool.hpp"
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
#include <tuple>
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

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("dedicated paired Writer gives admitted async work a turn within one large sync batch") {
    auto opened = open_paired_store_for_writer(1, 8);
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, kTestMutationArenaBytes,
                                                                std::chrono::milliseconds{0}, {});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());

    glyphastore::fault::reset();
    glyphastore::fault::arm_block(glyphastore::fault::Site::sync_lane_snapshot);
    std::vector<std::string> sync_keys;
    sync_keys.reserve(64);
    for (std::size_t index = 0; index < 64U; ++index) {
        sync_keys.push_back("large-sync-batch-" + std::to_string(index));
    }
    std::vector<glyphastore::Store::PutItem> sync_items;
    sync_items.reserve(sync_keys.size());
    for (const auto& key : sync_keys) {
        sync_items.push_back({.key = key, .value = bytes("sync")});
    }
    std::atomic_bool sync_batch_ok{};
    std::thread sync_batch{[&] {
        const auto statuses = store.put_batch(sync_items);
        sync_batch_ok.store(
            std::ranges::all_of(statuses, [](const auto& status) { return status.has_value(); }));
    }};
    const auto first_snapshot_blocked =
        glyphastore::fault::wait_until_blocked(glyphastore::fault::Site::sync_lane_snapshot);

    const auto async_admitted = (*executor)
                                    ->try_submit({
                                        .connection = {.slot = 1, .generation = 1},
                                        .request_id = 700,
                                        .worker_index = 0,
                                        .kind = glyphastore::server::MutationKind::put,
                                        .key = bytes("async-between-sync-snapshots"),
                                        .key_hash = glyphastore::hash_key("async-between-sync-snapshots"),
                                        .value = bytes("async"),
                                        .completions = &completions,
                                        .wakeup = &*wakeup,
                                    })
                                    .has_value();

    glyphastore::fault::release_block(glyphastore::fault::Site::sync_lane_snapshot);
    std::optional<glyphastore::server::MutationCompletion> async_completion;
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!async_completion && std::chrono::steady_clock::now() < completion_deadline) {
        async_completion = completions.try_pop();
        if (!async_completion) {
            std::this_thread::yield();
        }
    }
    sync_batch.join();
    glyphastore::fault::reset();

    GLYPHA_REQUIRE(first_snapshot_blocked);
    GLYPHA_REQUIRE(async_admitted);
    GLYPHA_REQUIRE(sync_batch_ok.load());
    GLYPHA_REQUIRE(async_completion.has_value());
    GLYPHA_REQUIRE(!async_completion->error.has_value());
    GLYPHA_REQUIRE((*executor)->release_payload(0, async_completion->payload_slot));
    const auto stats = (*executor)->stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].sync_drain_turns >= 2U);
    GLYPHA_REQUIRE(stats[0].sync_turn_splits >= 1U);
    GLYPHA_REQUIRE(stats[0].sync_async_fairness_turns >= 1U);
    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}
#endif

GLYPHA_TEST("paired Writer completes incremental read merge in bounded quanta") {
    const glyphastore::server::PairReadMergeConfig merge_config{
        .delta_entries = 4,
        .maximum_post_entries = 8,
        .quantum_slots = 4'096,
    };
    auto opened =
        open_paired_store_for_writer(1, 8, kTestMutationArenaBytes,
                                     {.async_writer_batch_max_records = 2,
                                      .merge_delta_entries = merge_config.delta_entries,
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
    const auto completion_stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(completion_stats.writer_batch_records == keys.size());
    GLYPHA_REQUIRE(completion_stats.writer_batches >= 1);
    GLYPHA_REQUIRE(completion_stats.writer_batches <= keys.size());
    GLYPHA_REQUIRE(completion_stats.maximum_writer_batch_records <= 2);
    GLYPHA_REQUIRE(completion_stats.publications == completion_stats.writer_batches);
    GLYPHA_REQUIRE(completion_stats.publication_records == keys.size());
    // One Reader owns this lane and drains every delivered completion after a wakeup. The
    // Writer therefore emits one notification per completed Writer batch, not per mutation.
    GLYPHA_REQUIRE(completion_stats.completion_notifications == completion_stats.writer_batches);

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
    GLYPHA_REQUIRE(stats.read_merge_remaining_slots == 0);
    GLYPHA_REQUIRE(stats.read_merge_post_capacity_remaining == 0);
    GLYPHA_REQUIRE(stats.maximum_read_merge_quantum_slots <= merge_config.quantum_slots);
    GLYPHA_REQUIRE(!stats.read_merge_active);
    GLYPHA_REQUIRE(stats.read_merge_post_entries == 0);
    GLYPHA_REQUIRE(stats.read_generation_memory.base_entries == keys.size());
    GLYPHA_REQUIRE(stats.read_generation_memory.base_record_storage_bytes == keys.size() * 64U);
    GLYPHA_REQUIRE(stats.read_generation_memory.base_record_mapped_storage_bytes == 0);
    GLYPHA_REQUIRE(stats.read_generation_memory.base_lookup_storage_bytes ==
                   stats.read_generation_memory.base_capacity * 5U);
    GLYPHA_REQUIRE(stats.read_generation_memory.current_allocated_lower_bound_bytes > 0);

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
    GLYPHA_REQUIRE(
        !open_paired_store_for_writer(1, 8, kTestMutationArenaBytes, {.async_writer_batch_max_records = 0})
             .has_value());
    GLYPHA_REQUIRE(
        !open_paired_store_for_writer(1, 8, kTestMutationArenaBytes, {.async_writer_batch_max_bytes = 0})
             .has_value());

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

GLYPHA_TEST("paired async Writer rejects retire pressure through completion before Store") {
    using glyphastore::store::paired::ShardPairRuntime;

    auto opened = open_paired_store_for_writer(1, 2, 64U * 1024U);
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{2};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor =
        glyphastore::server::PairWriterPool::create(store, 1, 2, 64U * 1024U, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());

    const auto* pinned_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(pinned_generation != nullptr);
    const auto pinned_epoch = pinned_generation->epoch();
    const auto submit_and_wait =
        [&](const std::uint64_t request_id, const std::string_view key,
            const std::string_view value) -> glyphastore::server::MutationCompletion {
        GLYPHA_REQUIRE((*executor)
                           ->try_submit({.connection = {.slot = 1, .generation = 1},
                                         .request_id = request_id,
                                         .worker_index = 0,
                                         .kind = glyphastore::server::MutationKind::put,
                                         .key = bytes(key),
                                         .key_hash = glyphastore::hash_key(key),
                                         .value = bytes(value),
                                         .completions = &completions,
                                         .wakeup = &*wakeup})
                           .has_value());
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        std::optional<glyphastore::server::MutationCompletion> completion;
        while (!completion && std::chrono::steady_clock::now() < deadline) {
            completion = completions.try_pop();
            if (!completion) {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
        }
        GLYPHA_REQUIRE(completion.has_value());
        GLYPHA_REQUIRE((*executor)->release_payload(0, completion->payload_slot));
        return std::move(*completion);
    };

    for (std::size_t publication = 0; publication < ShardPairRuntime::kMaximumRetiredReadGenerations;
         ++publication) {
        const auto value = "async-generation-" + std::to_string(publication);
        const auto completion = submit_and_wait(10'000U + publication, "async-retire-pressure", value);
        GLYPHA_REQUIRE(!completion.error.has_value());
    }
    auto stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.reader_safe_epoch == pinned_epoch);
    GLYPHA_REQUIRE(stats.retired_generation_count == ShardPairRuntime::kMaximumRetiredReadGenerations);

    const auto writer_epoch_before_rejection = stats.writer_epoch;
    const auto blocked = submit_and_wait(20'000U, "async-must-not-enter", "blocked");
    GLYPHA_REQUIRE(blocked.error.has_value());
    GLYPHA_REQUIRE(blocked.error->code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(blocked.error->message == "mutation rejected until paired Reader reaches quiescence");
    stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.writer_epoch == writer_epoch_before_rejection);
    GLYPHA_REQUIRE(stats.retired_generation_count == ShardPairRuntime::kMaximumRetiredReadGenerations);
    GLYPHA_REQUIRE(stats.generation_admission_backpressure_total == 1U);
    GLYPHA_REQUIRE(stats.expired_before_store == 1U);
    GLYPHA_REQUIRE(!store.get("async-must-not-enter").has_value());

    const auto* resumed_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(resumed_generation != nullptr);
    GLYPHA_REQUIRE(resumed_generation->epoch() > pinned_epoch);
    const auto resumed = submit_and_wait(30'000U, "async-must-not-enter", "after-quiescence");
    GLYPHA_REQUIRE(!resumed.error.has_value());
    stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.retired_generation_count < ShardPairRuntime::kMaximumRetiredReadGenerations);
    GLYPHA_REQUIRE(store.get("async-must-not-enter").has_value());
    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
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

GLYPHA_TEST("ADR 0036 V8 candidate preserves durable cold pin across compacted slot refresh") {
    using Generation = glyphastore::server::PairReadGeneration;
    using Pool = glyphastore::experimental::GenerationSlotPool<Generation, 4>;

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

    GLYPHA_REQUIRE(store.put("slot-refresh", bytes("old")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("slot-refresh", bytes("current")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("slot-active", bytes("active")).has_value());

    auto initial_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(initial_snapshot.has_value());
    auto initial_result = Generation::from_durable_snapshot(
        glyphastore::detail::StoreAccess::worker_routing(store), initial_snapshot->records);
    GLYPHA_REQUIRE(initial_result.has_value());
    auto initial = std::move(*initial_result);
    auto replacement_parent = initial;
    std::weak_ptr<const Generation> initial_lifetime = initial;
    auto pool = Pool::create(std::move(initial));
    GLYPHA_REQUIRE(pool.has_value());

    const auto* adopted_initial = (*pool)->adopt();
    GLYPHA_REQUIRE(adopted_initial != nullptr);
    const auto initial_epoch = adopted_initial->epoch();
    const std::string key{"slot-refresh"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key(key)};
    auto initial_record = adopted_initial->prepare_durable(hashed);
    GLYPHA_REQUIRE(initial_record.has_value());
    const auto initial_reference = initial_record->reference();
    auto pending =
        glyphastore::detail::StoreAccess::prepare_published_durable_get(store, 0, *initial_record, 0);
    GLYPHA_REQUIRE(pending.has_value());
    GLYPHA_REQUIRE(!pending->value.has_value());
    GLYPHA_REQUIRE(pending->cold.has_value());

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();
    compactor.join();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE((*compacted)->compacted);

    auto refreshed_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(refreshed_snapshot.has_value());
    auto refreshed_result =
        Generation::replace_durable_snapshot(replacement_parent, refreshed_snapshot->records);
    GLYPHA_REQUIRE(refreshed_result.has_value());
    replacement_parent.reset();
    initial_snapshot->records.clear();
    GLYPHA_REQUIRE((*pool)->try_publish(std::move(*refreshed_result)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);

    const auto* refreshed = (*pool)->adopt(initial_epoch);
    GLYPHA_REQUIRE(refreshed != nullptr);
    GLYPHA_REQUIRE(refreshed->epoch() > initial_epoch);
    auto refreshed_record = refreshed->prepare_durable(hashed);
    GLYPHA_REQUIRE(refreshed_record.has_value());
    GLYPHA_REQUIRE(refreshed_record->reference().sequence == initial_reference.sequence);
    GLYPHA_REQUIRE(refreshed_record->reference().segment_id != initial_reference.segment_id);
    (*pool)->reclaim();
    GLYPHA_REQUIRE(!initial_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().reader_safe_epoch == initial_epoch);

    auto value = glyphastore::detail::StoreAccess::complete_get_owned(store, 0, std::move(*pending->cold));
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(text(value->view()) == "current");
    GLYPHA_REQUIRE((*pool)->adopt() == refreshed);
    (*pool)->reclaim();
    GLYPHA_REQUIRE(initial_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);

    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("ADR 0036 V8 candidate publishes a Writer-owned rotation as one slot generation") {
    using Generation = glyphastore::server::PairReadGeneration;
    using Pool = glyphastore::experimental::GenerationSlotPool<Generation, 4>;

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
    GLYPHA_REQUIRE(store.put("slot-rotation-base", bytes("base")).has_value());

    auto initial_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(initial_snapshot.has_value());
    auto initial_result = Generation::from_durable_snapshot(
        glyphastore::detail::StoreAccess::worker_routing(store), initial_snapshot->records);
    GLYPHA_REQUIRE(initial_result.has_value());
    auto initial = std::move(*initial_result);
    auto replacement_parent = initial;
    std::weak_ptr<const Generation> initial_lifetime = initial;
    auto pool = Pool::create(std::move(initial));
    GLYPHA_REQUIRE(pool.has_value());
    const auto* initial_read = (*pool)->adopt();
    GLYPHA_REQUIRE(initial_read != nullptr);
    const auto initial_epoch = initial_read->epoch();

    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("slot-rotation-new", bytes("rotated")).has_value());
    auto rotated_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(rotated_snapshot.has_value());
    auto rotated_result = Generation::replace_durable_snapshot(replacement_parent, rotated_snapshot->records);
    GLYPHA_REQUIRE(rotated_result.has_value());
    replacement_parent.reset();
    initial_snapshot->records.clear();
    GLYPHA_REQUIRE((*pool)->try_publish(std::move(*rotated_result)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);

    const auto* rotated = (*pool)->adopt(initial_epoch);
    GLYPHA_REQUIRE(rotated != nullptr);
    GLYPHA_REQUIRE(rotated->epoch() == initial_epoch + 1U);
    GLYPHA_REQUIRE(rotated->delta_entries() == 0);
    GLYPHA_REQUIRE(rotated->base_entries() == 2);
    const glyphastore::HashedKey base{"slot-rotation-base", glyphastore::hash_key("slot-rotation-base")};
    const glyphastore::HashedKey added{"slot-rotation-new", glyphastore::hash_key("slot-rotation-new")};
    GLYPHA_REQUIRE(rotated->prepare_durable(base).has_value());
    GLYPHA_REQUIRE(rotated->prepare_durable(added).has_value());
    (*pool)->reclaim();
    GLYPHA_REQUIRE(!initial_lifetime.expired());

    GLYPHA_REQUIRE((*pool)->adopt() == rotated);
    (*pool)->reclaim();
    GLYPHA_REQUIRE(initial_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("ADR 0036 V5 candidate shutdown retires a real durable generation after Reader drain") {
    using Generation = glyphastore::server::PairReadGeneration;
    using Pool = glyphastore::experimental::GenerationSlotPool<Generation, 4>;

    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 4,
                                                       .async_lane_payload_bytes = kTestMutationArenaBytes,
                                                       .reader_epoch_lease = true},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = temporary.store_path(),
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("slot-v5-initial", bytes("initial")).has_value());

    auto initial_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(initial_snapshot.has_value());
    auto initial_result = Generation::from_durable_snapshot(
        glyphastore::detail::StoreAccess::worker_routing(store), initial_snapshot->records);
    GLYPHA_REQUIRE(initial_result.has_value());
    auto initial = std::move(*initial_result);
    auto replacement_parent = initial;
    std::weak_ptr<const Generation> initial_lifetime = initial;
    auto pool = Pool::create(std::move(initial));
    GLYPHA_REQUIRE(pool.has_value());
    const auto* adopted_initial = (*pool)->adopt();
    GLYPHA_REQUIRE(adopted_initial != nullptr);
    const auto borrowed_epoch = adopted_initial->epoch();

    GLYPHA_REQUIRE(store.put("slot-v5-final", bytes("final")).has_value());
    auto final_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(final_snapshot.has_value());
    auto final_result = Generation::replace_durable_snapshot(replacement_parent, final_snapshot->records);
    GLYPHA_REQUIRE(final_result.has_value());
    replacement_parent.reset();
    initial_snapshot->records.clear();
    GLYPHA_REQUIRE((*pool)->try_publish(std::move(*final_result)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt(borrowed_epoch) != nullptr);

    (*pool)->stop_admission();
    (*pool)->reclaim();
    GLYPHA_REQUIRE(!initial_lifetime.expired());
    GLYPHA_REQUIRE(!(*pool)->try_finish_shutdown());

    // The owner completes all output/cold borrows before this terminal edge.
    GLYPHA_REQUIRE((*pool)->mark_reader_quiescent());
    GLYPHA_REQUIRE((*pool)->try_finish_shutdown());
    GLYPHA_REQUIRE(initial_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("ADR 0036 V6 candidate fail-closes a committed mutation then snapshot-drains authority") {
    using Generation = glyphastore::server::PairReadGeneration;
    using Pool = glyphastore::experimental::GenerationSlotPool<Generation, 4>;

    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 4,
                                                       .async_lane_payload_bytes = kTestMutationArenaBytes,
                                                       .reader_epoch_lease = true},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = temporary.store_path(),
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("slot-v6-seed", bytes("seed")).has_value());

    auto initial_snapshot = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0);
    GLYPHA_REQUIRE(initial_snapshot.has_value());
    auto initial_result = Generation::from_durable_snapshot(
        glyphastore::detail::StoreAccess::worker_routing(store), initial_snapshot->records);
    GLYPHA_REQUIRE(initial_result.has_value());
    auto initial = std::move(*initial_result);
    auto replacement_parent = initial;
    auto pool =
        Pool::create(std::move(initial), {.context = &store, .fail_closed = [](void* context) noexcept {
                                              glyphastore::detail::StoreAccess::mark_fail_closed(
                                                  *static_cast<glyphastore::Store*>(context));
                                          }});
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    {
        auto reservation = (*pool)->try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        // This is the candidate ordering: capacity first, then Store entry.
        GLYPHA_REQUIRE(store.put("slot-v6-committed", bytes("authority")).has_value());
        reservation->mark_store_linearized();
        // Deterministically model generation construction/publication failure.
        GLYPHA_REQUIRE((*pool)->commit(*reservation, {}) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::invalid_generation);
    }
    GLYPHA_REQUIRE(!glyphastore::detail::StoreAccess::operational(store));
    GLYPHA_REQUIRE((*pool)->stats().unpublished_linearizations == 1);
    GLYPHA_REQUIRE((*pool)->stats().reserved_slots == 0);

    // Same recovery authority used by the production fail-closed epilogue:
    // snapshot is explicitly allowed after the durable catalog becomes sticky.
    auto drain = glyphastore::detail::StoreAccess::snapshot_durable_reads(store, 0, true);
    GLYPHA_REQUIRE(drain.has_value());
    auto drained_generation = Generation::replace_durable_snapshot(replacement_parent, drain->records);
    GLYPHA_REQUIRE(drained_generation.has_value());
    replacement_parent.reset();
    initial_snapshot->records.clear();
    GLYPHA_REQUIRE((*pool)->try_publish(std::move(*drained_generation)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    const auto* adopted = (*pool)->adopt();
    GLYPHA_REQUIRE(adopted != nullptr);
    GLYPHA_REQUIRE(
        adopted->prepare_durable({.key = "slot-v6-seed", .hash = glyphastore::hash_key("slot-v6-seed")})
            .has_value());
    GLYPHA_REQUIRE(adopted
                       ->prepare_durable(
                           {.key = "slot-v6-committed", .hash = glyphastore::hash_key("slot-v6-committed")})
                       .has_value());
    GLYPHA_REQUIRE(adopted->base_entries() == 2);
    GLYPHA_REQUIRE((*pool)->stats().publications == 1);

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
