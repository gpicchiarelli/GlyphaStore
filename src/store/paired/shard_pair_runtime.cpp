#include "glyphastore/store/paired/shard_pair_runtime.hpp"

#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/hot_path_phases.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/paired/bounded_spsc_queue.hpp"
#include "glyphastore/store/paired/completion_policy.hpp"
#include "glyphastore/store/paired/fail_closed_state.hpp"
#include "glyphastore/store/paired/lane_state.hpp"
#include "glyphastore/store/paired/mutation_batch.hpp"
#include "glyphastore/store/paired/mutation_execution.hpp"
#include "glyphastore/store/paired/mutation_recovery.hpp"
#include "glyphastore/store/paired/mutation_slot_pool.hpp"
#include "glyphastore/store/paired/mutation_state.hpp"
#include "glyphastore/store/paired/publication_coordinator.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace glyphastore::store::paired {
namespace {

[[nodiscard]] auto elapsed_ns(const std::chrono::steady_clock::time_point start,
                              const std::chrono::steady_clock::time_point end) noexcept -> std::uint64_t {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return count <= 0 ? 0U : static_cast<std::uint64_t>(count);
}

template <typename T> void atomic_max(std::atomic<T>& destination, const T value) noexcept {
    static_assert(std::is_unsigned_v<T>);
    auto observed = destination.load(std::memory_order_relaxed);
    while (observed < value && !destination.compare_exchange_weak(observed, value, std::memory_order_relaxed,
                                                                  std::memory_order_relaxed)) {
    }
}

void atomic_saturating_add(std::atomic<std::uint64_t>& destination, const std::uint64_t value) noexcept {
    auto observed = destination.load(std::memory_order_relaxed);
    for (;;) {
        const auto next = value > std::numeric_limits<std::uint64_t>::max() - observed
                              ? std::numeric_limits<std::uint64_t>::max()
                              : observed + value;
        if (destination.compare_exchange_weak(observed, next, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
            return;
        }
    }
}

struct AsyncMutationTask final {
    MutationContext context{};
    std::uint64_t request_id{};
    std::size_t shard{};
    MutationKind kind{};
    std::uint32_t payload_slot{};
    std::uint64_t key_hash{};
    std::uint64_t expire_at_ns{};
    std::size_t admission_bytes{};
    std::chrono::steady_clock::time_point admitted_at{};
    MutationSink sink{};
};

static_assert(std::is_nothrow_move_constructible_v<AsyncMutationTask>);

[[nodiscard]] auto deliver_outcome(const MutationSink& sink, MutationOutcome outcome) noexcept -> bool {
    if (sink.deliver == nullptr) {
        return false;
    }
    return sink.deliver(sink.completions, std::move(outcome));
}

void notify_sink(const MutationSink& sink) noexcept {
    if (sink.notify != nullptr && sink.wakeup != nullptr) {
        sink.notify(sink.wakeup);
    }
}

} // namespace

struct ShardPairRuntime::SyncMutation final {
    MutationKind kind{};
    const HashedKey* key{};
    std::span<const std::byte> value{};
    std::uint64_t expire_at_ns{};
    Status status{};
    std::atomic_bool done{};
    SyncMutation* next{};
};

struct ShardPairRuntime::Lane final {
    Lane(const std::size_t capacity, std::shared_ptr<const PairReadGeneration> initial,
         const std::uint64_t catalog_revision, const std::size_t payload_bytes,
         const std::size_t maximum_payload_bytes)
        : queue(std::max(capacity == 0 ? std::size_t{2} : capacity, std::size_t{2})),
          payloads(capacity == 0 ? 1U : queue.capacity(),
                   capacity == 0 ? 1U : std::max(payload_bytes, std::size_t{1}),
                   capacity == 0 ? 1U
                                 : (maximum_payload_bytes == 0 ? std::max(payload_bytes, std::size_t{1})
                                                               : maximum_payload_bytes)) {
        async.async_enabled = capacity != 0U;
        if (!initial) {
            throw std::runtime_error{"paired Writer has no initial read generation"};
        }
        generation.writer_generation = std::move(initial);
        generation.retired_generations.reserve(ShardPairRuntime::kMaximumRetiredReadGenerations);
        generation.writer_epoch.store(generation.writer_generation->epoch(), std::memory_order_relaxed);
        generation.delta_entries.store(generation.writer_generation->delta_entries(), std::memory_order_relaxed);
        generation.delta_record_versions.store(generation.writer_generation->delta_record_versions(),
                                               std::memory_order_relaxed);
        generation.delta_arena_record_bytes.store(generation.writer_generation->delta_arena_record_bytes(),
                                                  std::memory_order_relaxed);
        generation.delta_arena_key_bytes.store(generation.writer_generation->delta_arena_key_bytes(),
                                               std::memory_order_relaxed);
        generation.delta_arena_key_storage_bytes.store(
            generation.writer_generation->delta_arena_key_storage_bytes(), std::memory_order_relaxed);
        generation.published_catalog_revision.store(catalog_revision, std::memory_order_relaxed);
        publish_read_generation(generation.published_generation, generation.writer_generation.get());
    }

    BoundedSpscQueue<AsyncMutationTask> queue;
    MutationSlotPool payloads;
    std::thread thread;
    SyncMutation* sync_head{};
    AsyncLaneState async{};
    SyncLaneState sync{};
    GenerationState generation{};
    MergeState merge{};
    ReclamationState reclaim{};
    LaneMetrics metrics{};
};

ShardPairRuntime::ShardPairRuntime(Store& store, PairedConcurrencyConfig config,
                                   std::vector<std::shared_ptr<const PairReadGeneration>> initial_generations,
                                   std::vector<std::uint64_t> initial_catalog_revisions)
    : store_(store), config_(std::move(config)),
      maximum_queue_wait_(std::chrono::milliseconds{config_.async_queue_wait_ms}) {
    const auto shard_count = initial_generations.size();
    if (initial_catalog_revisions.size() != shard_count) {
        throw std::invalid_argument{"paired Writer initial generation count mismatch"};
    }
    lanes_.reserve(shard_count);
    for (std::size_t shard = 0; shard < shard_count; ++shard) {
        lanes_.push_back(
            std::make_unique<Lane>(config_.async_lane_capacity, std::move(initial_generations[shard]),
                                   initial_catalog_revisions[shard], config_.async_lane_payload_bytes,
                                   config_.async_maximum_payload_bytes));
    }
}

ShardPairRuntime::~ShardPairRuntime() {
    static_cast<void>(stop_and_drain());
}

auto ShardPairRuntime::async_lane_enabled() const noexcept -> bool {
    return !lanes_.empty() && lanes_.front()->async.async_enabled;
}

auto ShardPairRuntime::create(Store& store, const PairedConcurrencyConfig& config)
    -> Result<std::unique_ptr<ShardPairRuntime>> try {
    const auto shard_count = store.worker_count();
    if (shard_count == 0 || config.merge_delta_entries == 0 || config.merge_maximum_post_entries == 0 ||
        config.merge_quantum_slots == 0 ||
        config.merge_delta_entries > PairReadGeneration::kMaximumIncrementalDeltaEntries ||
        config.merge_maximum_post_entries >
            PairReadGeneration::kMaximumIncrementalDeltaEntries - config.merge_delta_entries ||
        (config.async_lane_capacity != 0 && config.async_lane_payload_bytes == 0)) {
        return fail(ErrorCode::invalid_argument, "paired runtime configuration is invalid");
    }
    const auto routing = detail::StoreAccess::worker_routing(store);
    std::vector<std::shared_ptr<const PairReadGeneration>> initial_generations;
    std::vector<std::uint64_t> initial_catalog_revisions;
    initial_generations.reserve(shard_count);
    initial_catalog_revisions.reserve(shard_count);
    for (std::size_t shard = 0; shard < shard_count; ++shard) {
        Result<std::shared_ptr<const PairReadGeneration>> initial = PairReadGeneration::empty(routing);
        if (detail::StoreAccess::is_durable(store)) {
            auto snapshot = detail::StoreAccess::snapshot_durable_reads(store, shard);
            if (!snapshot) {
                return unexpected(snapshot.error());
            }
            initial = PairReadGeneration::from_durable_snapshot(routing, snapshot->records);
            initial_catalog_revisions.push_back(snapshot->catalog_revision);
        } else {
            initial_catalog_revisions.push_back(0U);
        }
        if (!initial) {
            return unexpected(initial.error());
        }
        initial_generations.push_back(std::move(*initial));
    }
    return std::unique_ptr<ShardPairRuntime>(new ShardPairRuntime(
        store, config, std::move(initial_generations), std::move(initial_catalog_revisions)));
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "paired runtime allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "paired runtime construction failed");
}

auto ShardPairRuntime::start() -> Status try {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return fail(ErrorCode::invalid_argument, "paired mutation executor has already been started");
    }
    if (stopping_.load(std::memory_order_acquire)) {
        return fail(ErrorCode::unavailable, "paired mutation executor has been stopped");
    }
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        active_writers_.fetch_add(1U, std::memory_order_relaxed);
        try {
            lanes_[worker]->thread = std::thread{[this, worker] { run(worker); }};
        } catch (...) {
            active_writers_.fetch_sub(1U, std::memory_order_relaxed);
            throw;
        }
    }
    return {};
} catch (const std::exception& exception) {
    static_cast<void>(stop_and_drain());
    return fail(ErrorCode::io_error,
                std::string{"failed to start paired mutation executor: "} + exception.what());
} catch (...) {
    static_cast<void>(stop_and_drain());
    return fail(ErrorCode::io_error, "failed to start paired mutation executor");
}

auto ShardPairRuntime::begin_submission() noexcept -> bool {
    const auto previous = admission_state_.fetch_add(1U, std::memory_order_acq_rel);
    if ((previous & kAdmissionClosed) == 0) {
        return true;
    }
    finish_submission();
    return false;
}

void ShardPairRuntime::finish_submission() noexcept {
    const auto previous = admission_state_.fetch_sub(1U, std::memory_order_acq_rel);
    if ((previous & kAdmissionClosed) != 0 && (previous & kAdmissionCountMask) == 1U) {
        admission_state_.notify_all();
    }
}

auto ShardPairRuntime::mutation_admission_bytes(const std::size_t key_bytes,
                                                const std::size_t value_bytes) noexcept
    -> std::optional<std::size_t> {
    if (key_bytes > std::numeric_limits<std::size_t>::max() - value_bytes) {
        return std::nullopt;
    }
    const auto payload_bytes = key_bytes + value_bytes;
    if (payload_bytes > std::numeric_limits<std::size_t>::max() - kMutationAdmissionOverheadBytes) {
        return std::nullopt;
    }
    return kMutationAdmissionOverheadBytes + payload_bytes;
}

auto ShardPairRuntime::try_submit(const AsyncMutationRequest& request) noexcept
    -> std::optional<std::size_t> {
    if (request.shard >= lanes_.size()) {
        return std::nullopt;
    }
    auto& lane = *lanes_[request.shard];
    if (!lanes_[request.shard]->async.async_enabled || request.sink.deliver == nullptr) {
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    if (!begin_submission()) {
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    struct SubmissionGuard final {
        ShardPairRuntime& executor;
        ~SubmissionGuard() {
            executor.finish_submission();
        }
    } submission{*this};
    if (!healthy_.load(std::memory_order_acquire) || !started_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire) || lane.async.stopping.load(std::memory_order_acquire) ||
        expire_remaining_.load(std::memory_order_acquire) || !detail::StoreAccess::admissions_open(store_)) {
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto admission_bytes = mutation_admission_bytes(request.key.size(), request.value.size());
    if (!admission_bytes) {
        lane.metrics.payload_too_large_total.fetch_add(1U, std::memory_order_relaxed);
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto queued_bytes = lane.async.queued_bytes.load(std::memory_order_relaxed);
    if (*admission_bytes > std::numeric_limits<std::size_t>::max() - queued_bytes) {
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    auto acquired = lane.payloads.try_acquire(request.key, request.value, *admission_bytes);
    if (!acquired.lease) {
        switch (acquired.failure) {
        case MutationSlotPool::AcquireFailure::slot_exhausted:
            lane.metrics.payload_slot_full_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::byte_exhausted:
            lane.metrics.payload_arena_full_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::payload_too_large:
            lane.metrics.payload_too_large_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::none:
            break;
        }
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    AsyncMutationTask task{.context = request.context,
                           .request_id = request.request_id,
                           .shard = request.shard,
                           .kind = request.kind,
                           .payload_slot = acquired.lease->slot,
                           .key_hash = request.key_hash,
                           .expire_at_ns = request.expire_at_ns,
                           .admission_bytes = acquired.lease->admission_bytes,
                           .admitted_at = std::chrono::steady_clock::now(),
                           .sink = request.sink};
    const auto next_bytes =
        lane.async.queued_bytes.fetch_add(*admission_bytes, std::memory_order_relaxed) + *admission_bytes;
    GS_FAULT_SITE(enqueue);
    if (!lane.queue.try_push(std::move(task))) {
        lane.async.queued_bytes.fetch_sub(*admission_bytes, std::memory_order_relaxed);
        if (!lane.payloads.rollback(*acquired.lease)) {
            std::terminate();
        }
        lane.metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto slots_in_use = lane.payloads.slots_in_use();
    const auto arena_bytes_in_use = lane.payloads.payload_bytes_in_use();
    const auto admission_bytes_in_use = lane.payloads.admission_bytes_in_use();
    lane.metrics.payload_slots_in_use.store(slots_in_use, std::memory_order_relaxed);
    lane.metrics.payload_arena_bytes_in_use.store(arena_bytes_in_use, std::memory_order_relaxed);
    lane.metrics.payload_admission_bytes_in_use.store(admission_bytes_in_use, std::memory_order_relaxed);
    atomic_max(lane.metrics.maximum_payload_slots_in_use, slots_in_use);
    atomic_max(lane.metrics.maximum_payload_arena_bytes_in_use, arena_bytes_in_use);
    atomic_max(lane.metrics.maximum_payload_admission_bytes_in_use, admission_bytes_in_use);
    lane.metrics.admitted.fetch_add(1U, std::memory_order_relaxed);
    atomic_max(lane.metrics.maximum_queue_depth, lane.queue.size());
    atomic_max(lane.metrics.maximum_queued_bytes, next_bytes);
    wake(lane);
    return admission_bytes;
}

auto ShardPairRuntime::release_payload(const std::size_t shard, const std::uint32_t payload_slot) noexcept
    -> bool {
    if (shard >= lanes_.size()) {
        return false;
    }
    auto& lane = *lanes_[shard];
    if (!lane.payloads.release(payload_slot)) {
        return false;
    }
    lane.metrics.payload_slots_in_use.store(lane.payloads.slots_in_use(), std::memory_order_relaxed);
    lane.metrics.payload_arena_bytes_in_use.store(lane.payloads.payload_bytes_in_use(), std::memory_order_relaxed);
    lane.metrics.payload_admission_bytes_in_use.store(lane.payloads.admission_bytes_in_use(),
                                              std::memory_order_relaxed);
    return true;
}

auto ShardPairRuntime::adopt_read_generation(const std::size_t shard,
                                             const std::uint64_t minimum_leased_epoch) const noexcept
    -> const PairReadGeneration* {
    if (shard >= lanes_.size()) {
        return nullptr;
    }
    auto& lane = *lanes_[shard];
    GS_FAULT_SITE(adopt);
    const auto* generation = lane.generation.published_generation.load(std::memory_order_acquire);
    if (generation != nullptr) {
        // The current turn protects generation->epoch(); asynchronous I/O can
        // additionally keep an older epoch borrowed. The minimum is the sole
        // reclamation boundary published by Reader to Writer.
        const auto safe_epoch = std::min(generation->epoch(), minimum_leased_epoch);
        const auto previous = lane.generation.reader_safe_epoch.exchange(safe_epoch, std::memory_order_acq_rel);
        if (previous != safe_epoch && !lane.generation.reclaim_requested.exchange(true, std::memory_order_acq_rel)) {
            lane.async.signal.fetch_add(1U, std::memory_order_release);
            lane.async.signal.notify_one();
        }
    }
    return generation;
}

void ShardPairRuntime::request_read_refresh(const std::size_t shard) noexcept {
    if (shard >= lanes_.size()) {
        return;
    }
    auto& lane = *lanes_[shard];
    const auto current = detail::StoreAccess::durable_read_catalog_revision(store_, shard);
    if (current == 0U || current == lane.generation.published_catalog_revision.load(std::memory_order_acquire)) {
        return;
    }
    if (!lane.generation.refresh_requested.exchange(true, std::memory_order_acq_rel)) {
        lane.async.signal.fetch_add(1U, std::memory_order_release);
        lane.async.signal.notify_one();
    }
}

void ShardPairRuntime::note_rejected(const std::size_t shard) noexcept {
    if (shard < lanes_.size()) {
        lanes_[shard]->metrics.rejected.fetch_add(1U, std::memory_order_relaxed);
    }
}

auto ShardPairRuntime::stats() const -> std::vector<ShardPairStats> {
    std::vector<ShardPairStats> result;
    result.reserve(lanes_.size());
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        const auto& lane = *lanes_[worker];
        result.push_back(
            {.worker_index = worker,
             .reader_safe_epoch = lane.generation.reader_safe_epoch.load(std::memory_order_relaxed),
             .writer_epoch = lane.generation.writer_epoch.load(std::memory_order_relaxed),
             .queue_depth = lane.queue.size(),
             .queued_bytes = lane.async.queued_bytes.load(std::memory_order_relaxed),
             .maximum_queue_depth = lane.metrics.maximum_queue_depth.load(std::memory_order_relaxed),
             .maximum_queued_bytes = lane.metrics.maximum_queued_bytes.load(std::memory_order_relaxed),
             .payload_slot_capacity = lane.payloads.slot_capacity(),
             .payload_slots_in_use = lane.metrics.payload_slots_in_use.load(std::memory_order_relaxed),
             .maximum_payload_slots_in_use =
                 lane.metrics.maximum_payload_slots_in_use.load(std::memory_order_relaxed),
             .payload_arena_capacity_bytes = lane.payloads.byte_capacity(),
             .payload_arena_storage_bytes = lane.payloads.storage_bytes(),
             .payload_arena_bytes_in_use = lane.metrics.payload_arena_bytes_in_use.load(std::memory_order_relaxed),
             .maximum_payload_arena_bytes_in_use =
                 lane.metrics.maximum_payload_arena_bytes_in_use.load(std::memory_order_relaxed),
             .payload_admission_bytes_in_use =
                 lane.metrics.payload_admission_bytes_in_use.load(std::memory_order_relaxed),
             .maximum_payload_admission_bytes_in_use =
                 lane.metrics.maximum_payload_admission_bytes_in_use.load(std::memory_order_relaxed),
             .payload_slot_full_total = lane.metrics.payload_slot_full_total.load(std::memory_order_relaxed),
             .payload_arena_full_total = lane.metrics.payload_arena_full_total.load(std::memory_order_relaxed),
             .payload_too_large_total = lane.metrics.payload_too_large_total.load(std::memory_order_relaxed),
             .admitted = lane.metrics.admitted.load(std::memory_order_relaxed),
             .rejected = lane.metrics.rejected.load(std::memory_order_relaxed),
             .expired_before_store = lane.metrics.expired_before_store.load(std::memory_order_relaxed),
             .completed = lane.metrics.completed.load(std::memory_order_relaxed),
             .conflict_retries = lane.metrics.conflict_retries.load(std::memory_order_relaxed),
             .conflict_retry_commits = lane.metrics.conflict_retry_commits.load(std::memory_order_relaxed),
             .total_queue_wait_ns = lane.metrics.total_queue_wait_ns.load(std::memory_order_relaxed),
             .maximum_queue_wait_ns = lane.metrics.maximum_queue_wait_ns.load(std::memory_order_relaxed),
             .total_service_ns = lane.metrics.total_service_ns.load(std::memory_order_relaxed),
             .maximum_service_ns = lane.metrics.maximum_service_ns.load(std::memory_order_relaxed),
             .read_catalog_revision = lane.generation.published_catalog_revision.load(std::memory_order_relaxed),
             .read_refresh_attempts = lane.metrics.read_refresh_attempts.load(std::memory_order_relaxed),
             .read_refresh_successes = lane.metrics.read_refresh_successes.load(std::memory_order_relaxed),
             .read_refresh_failures = lane.metrics.read_refresh_failures.load(std::memory_order_relaxed),
             .read_refresh_deferrals = lane.metrics.read_refresh_deferrals.load(std::memory_order_relaxed),
             .generations_retired = lane.generation.generations_retired.load(std::memory_order_relaxed),
             .retired_generation_count = lane.generation.retired_generation_count.load(std::memory_order_relaxed),
             .delta_entries = lane.generation.delta_entries.load(std::memory_order_relaxed),
             .delta_record_versions = lane.generation.delta_record_versions.load(std::memory_order_relaxed),
             .delta_arena_record_bytes = lane.generation.delta_arena_record_bytes.load(std::memory_order_relaxed),
             .delta_arena_key_bytes = lane.generation.delta_arena_key_bytes.load(std::memory_order_relaxed),
             .delta_arena_key_storage_bytes =
                 lane.generation.delta_arena_key_storage_bytes.load(std::memory_order_relaxed),
             .read_merge_active = lane.merge.read_merge_active.load(std::memory_order_relaxed),
             .read_merge_post_entries = lane.merge.read_merge_post_entries.load(std::memory_order_relaxed),
             .read_merge_starts = lane.merge.read_merge_starts.load(std::memory_order_relaxed),
             .read_merge_completions = lane.merge.read_merge_completions.load(std::memory_order_relaxed),
             .read_merge_failures = lane.merge.read_merge_failures.load(std::memory_order_relaxed),
             .read_merge_backpressure = lane.merge.read_merge_backpressure.load(std::memory_order_relaxed),
             .read_merge_slots_processed = lane.merge.read_merge_slots_processed.load(std::memory_order_relaxed),
             .sync_admitted = lane.sync.sync_admitted.load(std::memory_order_relaxed),
             .queue_wait_histogram = lane.metrics.queue_wait_histogram.snapshot(),
             .service_histogram = lane.metrics.service_histogram.snapshot()});
    }
    return result;
}

void ShardPairRuntime::note_writer_exit() noexcept {
    if (active_writers_.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
        active_writers_.notify_all();
    }
}

auto ShardPairRuntime::stop_and_drain(const std::optional<std::chrono::milliseconds> deadline) -> Status {
    GS_FAULT_SITE(close);
    if (!stopping_.exchange(true, std::memory_order_acq_rel)) {
        auto observed = admission_state_.fetch_or(kAdmissionClosed, std::memory_order_acq_rel);
        while ((observed & kAdmissionCountMask) != 0) {
            admission_state_.wait(observed, std::memory_order_acquire);
            observed = admission_state_.load(std::memory_order_acquire);
        }
        for (auto& lane : lanes_) {
            lane->async.stopping.store(true, std::memory_order_release);
            lane->async.signal.fetch_add(1U, std::memory_order_release);
            lane->async.signal.notify_one();
        }
    }

    bool timed_out = false;
    if (deadline.has_value()) {
        const auto deadline_at = std::chrono::steady_clock::now() + *deadline;
        while (active_writers_.load(std::memory_order_acquire) != 0 &&
               std::chrono::steady_clock::now() < deadline_at) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        timed_out = active_writers_.load(std::memory_order_acquire) != 0;
        if (timed_out) {
            abandon_queued_mutations();
        }
    }

    for (auto& lane : lanes_) {
        if (lane->thread.joinable()) {
            lane->thread.join();
        }
    }
    if (timed_out) {
        return fail(ErrorCode::unavailable, "shutdown drain deadline exceeded");
    }
    return {};
}

void ShardPairRuntime::abandon_queued_mutations() noexcept {
    expire_remaining_.store(true, std::memory_order_release);
    for (auto& lane_ptr : lanes_) {
        auto& lane = *lane_ptr;
        lane.async.signal.fetch_add(1U, std::memory_order_release);
        lane.async.signal.notify_one();
        for (;;) {
            std::optional<AsyncMutationTask> task;
            {
                const std::lock_guard lock{lane.async.queue_consumer_mutex};
                task = lane.queue.try_pop();
            }
            if (!task) {
                break;
            }
            lane.async.queued_bytes.fetch_sub(task->admission_bytes, std::memory_order_relaxed);
            lane.metrics.expired_before_store.fetch_add(1U, std::memory_order_relaxed);
            lane.metrics.completed.fetch_add(1U, std::memory_order_relaxed);
            MutationOutcome outcome{.context = task->context,
                                    .request_id = task->request_id,
                                    .admission_bytes = task->admission_bytes,
                                    .payload_slot = task->payload_slot};
            outcome.error.emplace(ErrorCode::resource_exhausted,
                                  "mutation abandoned after shutdown drain deadline");
            if (!deliver_outcome(task->sink, std::move(outcome))) {
                std::terminate();
            }
            notify_sink(task->sink);
        }
    }
}

void ShardPairRuntime::run(const std::size_t shard) noexcept {
    auto& lane = *lanes_[shard];
    const auto payload_for = [&](const AsyncMutationTask& task) noexcept {
        auto payload = lane.payloads.view(task.payload_slot);
        if (!payload) {
            std::terminate();
        }
        return *payload;
    };
    struct ExitGuard final {
        ShardPairRuntime& executor;
        ~ExitGuard() {
            executor.note_writer_exit();
        }
    } exit_guard{*this};
    const auto pop_queued = [&lane]() noexcept -> std::optional<AsyncMutationTask> {
        const std::lock_guard lock{lane.async.queue_consumer_mutex};
        return lane.queue.try_pop();
    };

    const auto batch_config = detail::StoreAccess::durable_writer_batch_config(store_);
    const auto maximum_batch_records =
        batch_config ? static_cast<std::size_t>(batch_config->max_records) : std::size_t{32};
    std::vector<AsyncMutationTask> batch;
    std::vector<std::uint64_t> queue_waits;
    std::vector<std::chrono::steady_clock::time_point> service_started;
    std::vector<bool> expired;
    std::vector<MutationOutcome> completions;
    std::vector<detail::StoreAccess::DurableMutationView> durable_views;
    std::vector<std::size_t> durable_indices;
    std::vector<ReadMutation> read_mutations;
    std::vector<std::size_t> read_mutation_indices;
    std::optional<AsyncMutationTask> carried_task;
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
    std::vector<FailClosedLaneWake> fail_closed_wakes;
    fail_closed_wakes.reserve(lanes_.size());
    for (auto& affected_lane : lanes_) {
        fail_closed_wakes.push_back(FailClosedLaneWake{.signal = &affected_lane->async.signal});
    }
    const auto publish_fail_closed = [&]() noexcept {
        fail_closed.arm(fail_closed_wakes, FailClosedScope::pair_and_store);
    };
    const auto sticky_pair_before_durable_mark = [&]() noexcept {
        fail_closed.arm(fail_closed_wakes, FailClosedScope::pair_only);
    };
    const auto update_delta_stats = [&]() noexcept {
        lane.generation.delta_entries.store(lane.generation.writer_generation->delta_entries(), std::memory_order_relaxed);
        lane.generation.delta_record_versions.store(lane.generation.writer_generation->delta_record_versions(),
                                         std::memory_order_relaxed);
        lane.generation.delta_arena_record_bytes.store(lane.generation.writer_generation->delta_arena_record_bytes(),
                                            std::memory_order_relaxed);
        lane.generation.delta_arena_key_bytes.store(lane.generation.writer_generation->delta_arena_key_bytes(),
                                         std::memory_order_relaxed);
        lane.generation.delta_arena_key_storage_bytes.store(lane.generation.writer_generation->delta_arena_key_storage_bytes(),
                                                 std::memory_order_relaxed);
    };
    const auto reclaim_quiescent = [&]() noexcept {
        if (lane.generation.retired_generations.empty()) {
            lane.generation.retired_generation_count.store(0U, std::memory_order_relaxed);
            return;
        }
        const auto before = lane.generation.retired_generations.size();
        std::uint64_t quiescent_epoch{};
        if (config_.reader_epoch_lease) {
            quiescent_epoch = lane.generation.reader_safe_epoch.load(std::memory_order_acquire);
        } else {
            // Pair with ReadLease's fence between publishing its counted
            // presence and loading the generation pointer. If a Reader can
            // still observe the previous generation, this Writer must observe
            // its active lease; if the Writer observes no lease, the Reader
            // must observe the new generation.
            std::atomic_thread_fence(std::memory_order_seq_cst);
            quiescent_epoch = lane.reclaim.active_read_leases.load(std::memory_order_acquire) == 0
                                  ? lane.generation.writer_epoch.load(std::memory_order_relaxed)
                                  : std::uint64_t{0};
        }
        if (quiescent_epoch == 0) {
            lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(), std::memory_order_relaxed);
            return;
        }
        std::erase_if(lane.generation.retired_generations,
                      [&](const auto& retired) { return retired->epoch() < quiescent_epoch; });
        const auto retired = before - lane.generation.retired_generations.size();
        if (retired != 0U) {
            lane.generation.generations_retired.fetch_add(retired, std::memory_order_relaxed);
        }
        lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(), std::memory_order_relaxed);
    };
    // Drain durable Index authority into the published generation before sticky
    // close (allow_fail_closed: durable may already be unhealthy). Sync single-op
    // and batch catch use this so committed keys are not left unpublished.
    const auto try_drain_durable_snapshot = [&]() noexcept -> bool {
        try {
            if (glyphastore::fault::consume_fail(glyphastore::fault::Site::drain_snapshot)) {
                return false;
            }
            auto snapshot = detail::StoreAccess::snapshot_durable_reads(store_, shard, true);
            if (!snapshot) {
                return false;
            }
            auto next =
                PairReadGeneration::replace_durable_snapshot(lane.generation.writer_generation, snapshot->records);
            if (!next) {
                return false;
            }
            install_writer_generation(lane.generation.writer_generation, lane.generation.retired_generations,
                                      lane.generation.retired_generation_count, lane.generation.writer_epoch, std::move(*next));
            update_delta_stats();
            publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
            lane.generation.published_catalog_revision.store(snapshot->catalog_revision, std::memory_order_release);
            lane.merge.read_merge.reset();
            lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
            lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
            merge_retry_blocked = false;
            reclaim_quiescent();
            return true;
        } catch (...) {
            return false;
        }
    };
    // Publication produces one retired generation per successful incremental
    // publish. Reclaiming on every single-mutation publish dominated volatile
    // PUT ack time; reclaim proportionally when debt accumulates, and always
    // before hard retire limits / merge pressure checks.
    const auto reclaim_proportional = [&]() noexcept {
        constexpr std::size_t kReclaimPublishQuantum = 8;
        if (lane.generation.retired_generations.size() >= kReclaimPublishQuantum ||
            lane.generation.retired_generations.size() + 1U >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
            reclaim_quiescent();
        } else {
            lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(), std::memory_order_relaxed);
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
        if (lane.generation.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
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
        if (snapshot->catalog_revision == lane.generation.published_catalog_revision.load(std::memory_order_acquire)) {
            return;
        }
        auto next = PairReadGeneration::replace_durable_snapshot(lane.generation.writer_generation, snapshot->records);
        if (!next) {
            lane.metrics.read_refresh_failures.fetch_add(1U, std::memory_order_relaxed);
            if (next.error().code != ErrorCode::resource_exhausted) {
                publish_fail_closed();
            }
            return;
        }
        install_writer_generation(lane.generation.writer_generation, lane.generation.retired_generations,
                                  lane.generation.retired_generation_count, lane.generation.writer_epoch, std::move(*next));
        update_delta_stats();
        publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
        lane.generation.published_catalog_revision.store(snapshot->catalog_revision, std::memory_order_release);
        lane.merge.read_merge.reset();
        lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
        lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
        merge_retry_blocked = false;
        lane.metrics.read_refresh_successes.fetch_add(1U, std::memory_order_relaxed);
    };
    const auto process_merge = [&]() noexcept {
        if (!lane.merge.read_merge && !merge_retry_blocked &&
            (lane.generation.writer_generation->delta_entries() >= config_.merge_delta_entries ||
             lane.generation.writer_generation->delta_record_versions() >= config_.merge_delta_entries)) {
            auto started = PairReadGeneration::start_incremental_merge(lane.generation.writer_generation,
                                                                       config_.merge_maximum_post_entries);
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
            lane.merge.read_merge_starts.fetch_add(1U, std::memory_order_relaxed);
        }
        if (!lane.merge.read_merge) {
            return;
        }
        if (!PairReadGeneration::merge_ready(*lane.merge.read_merge)) {
            auto advanced =
                PairReadGeneration::advance_incremental_merge(*lane.merge.read_merge, config_.merge_quantum_slots);
            if (!advanced) {
                lane.merge.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
                if (advanced.error().code == ErrorCode::resource_exhausted) {
                    lane.merge.read_merge.reset();
                    lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
                    lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
                    merge_retry_blocked = true;
                } else {
                    publish_fail_closed();
                }
                return;
            }
            lane.merge.read_merge_slots_processed.fetch_add(*advanced, std::memory_order_relaxed);
        }
        if (!PairReadGeneration::merge_ready(*lane.merge.read_merge)) {
            return;
        }
        reclaim_quiescent();
        if (lane.generation.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
            return;
        }
        auto next = PairReadGeneration::finish_incremental_merge(lane.generation.writer_generation, *lane.merge.read_merge);
        if (!next) {
            lane.merge.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
            lane.merge.read_merge.reset();
            lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
            lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
            if (next.error().code == ErrorCode::resource_exhausted) {
                merge_retry_blocked = true;
            } else {
                publish_fail_closed();
            }
            return;
        }
        lane.generation.retired_generations.push_back(lane.generation.writer_generation);
        lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(), std::memory_order_relaxed);
        lane.generation.writer_generation = std::move(*next);
        update_delta_stats();
        lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(), std::memory_order_relaxed);
        publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
        lane.merge.read_merge.reset();
        lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
        lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
        lane.merge.read_merge_completions.fetch_add(1U, std::memory_order_relaxed);
        merge_retry_blocked = false;
        reclaim_quiescent();
    };

    for (;;) {
        process_reclamation();
        if (!lane.async.stopping.load(std::memory_order_acquire) && healthy_.load(std::memory_order_acquire)) {
            process_refresh();
            process_merge();
        }

        // Drain synchronous embedded mutations before asynchronous lane work.
        for (;;) {
            SyncMutation* sync_batch = nullptr;
            {
                const std::lock_guard lock{lane.sync.sync_mutex};
                sync_batch = lane.sync_head;
                lane.sync_head = nullptr;
            }
            if (sync_batch == nullptr) {
                break;
            }
            // Reverse to preserve FIFO-ish order from LIFO push.
            SyncMutation* rev = nullptr;
            while (sync_batch != nullptr) {
                auto* next = sync_batch->next;
                sync_batch->next = rev;
                rev = sync_batch;
                sync_batch = next;
            }
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
                continue;
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
                    const auto* published = lane.generation.published_generation.load(std::memory_order_acquire);
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
                            begin, nodes.size(), [&](const std::size_t index) -> const HashedKey& {
                                return *nodes[index]->key;
                            });
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
                        auto next = snapshot ? PairReadGeneration::replace_durable_snapshot(
                                                   lane.generation.writer_generation, snapshot->records)
                                             : Result<std::shared_ptr<const PairReadGeneration>>{
                                                   unexpected(snapshot.error())};
                        if (!next) {
                            publish_fail_closed();
                            for (auto* node : nodes) {
                                if (node->status) {
                                    node->status =
                                        Status{fail(ErrorCode::unavailable,
                                                    "read publication failed after durable Writer batch")};
                                }
                            }
                        } else {
                            lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                            lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                                std::memory_order_relaxed);
                            lane.generation.writer_generation = std::move(*next);
                            update_delta_stats();
                            lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(),
                                                    std::memory_order_relaxed);
                            publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
                            lane.generation.published_catalog_revision.store(snapshot->catalog_revision,
                                                                  std::memory_order_release);
                            lane.merge.read_merge.reset();
                            lane.merge.read_merge_active.store(false, std::memory_order_relaxed);
                            lane.merge.read_merge_post_entries.store(0U, std::memory_order_relaxed);
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
                    } else if (sticky_publication_failure) {
                        publish_fail_closed();
                    }
                } catch (const std::bad_alloc&) {
                    if (publication_required || durable_mutate_entered) {
                        sibling_snapshot_published = try_drain_durable_snapshot();
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
                        sibling_snapshot_published = try_drain_durable_snapshot();
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
                continue;
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
                    for (unsigned spin = 0; !PairReadGeneration::can_publish_incremental(
                                                *lane.generation.writer_generation, lane.merge.read_merge.get(), chunk_size) &&
                                            spin < 256U;
                         ++spin) {
                        reclaim_quiescent();
                        process_merge();
                    }
                    if (!PairReadGeneration::can_publish_incremental(*lane.generation.writer_generation,
                                                                     lane.merge.read_merge.get(), chunk_size)) {
                        lane.merge.read_merge_backpressure.fetch_add(1U, std::memory_order_relaxed);
                        for (std::size_t index = 0; index < chunk_size; ++index) {
                            // Never Store-entered — same polarity as async merge pressure.
                            chunk[index]->status =
                                Status{fail(ErrorCode::resource_exhausted,
                                            "mutation rejected until incremental read merge advances")};
                            chunk[index]->done.store(true, std::memory_order_release);
                            chunk[index]->done.notify_one();
                        }
                        continue;
                    }
                    std::array<ReadMutation, kMaximumPublicationBatch> publications{};
                    std::array<SyncMutation*, kMaximumPublicationBatch> published_nodes{};
                    std::size_t publication_count = 0;
                    bool store_mutated = false;
                    bool generation_published = false;
                    try {
                        {
                            GS_PHASE_PUT(worker_apply);
                            for (std::size_t index = 0; index < chunk_size; ++index) {
                                auto* node = chunk[index];
                                // Another lane may sticky-fail mid-chunk; do not Store-mutate further.
                                if (!healthy_.load(std::memory_order_acquire)) {
                                    for (; index < chunk_size; ++index) {
                                        // Never Store-entered after sticky — known not newly committed.
                                        chunk[index]->status = Status{fail(ErrorCode::resource_exhausted,
                                                                           "paired runtime is fail-closed")};
                                    }
                                    break;
                                }
                                GS_FAULT_SITE(mutate);
                                const auto& key = *node->key;
                                auto published =
                                    node->kind == MutationKind::put
                                        ? detail::StoreAccess::put_volatile_published(
                                              store_, shard, key, node->value, node->expire_at_ns,
                                              detail::StoreAccess::PublishedAdmission::caller_holds_guard)
                                        : detail::StoreAccess::erase_volatile_published(
                                              store_, shard, key,
                                              detail::StoreAccess::PublishedAdmission::caller_holds_guard);
                                if (!published) {
                                    bool sticky = false;
                                    auto error = classify_volatile_mutation_error(published.error(), sticky);
                                    if (sticky) {
                                        publish_fail_closed();
                                    }
                                    node->status = Status{unexpected(std::move(error))};
                                    continue;
                                }
                                store_mutated = true;
                                publications[publication_count] =
                                    ReadMutation{.key = key,
                                                 .record = published->record,
                                                 .segment = std::move(published->segment),
                                                 .opcode = published->opcode};
                                published_nodes[publication_count] = node;
                                ++publication_count;
                                // Litmus / cross-lane sticky: force fail-closed after a successful
                                // Store mutate so later siblings hit the mid-chunk reject above.
                                if (glyphastore::fault::consume_fail(glyphastore::fault::Site::mutate)) {
                                    publish_fail_closed();
                                }
                            }
                        }
                        if (publication_count != 0) {
                            Result<std::shared_ptr<const PairReadGeneration>> next{
                                fail(ErrorCode::internal_error, "read publication not attempted")};
                            {
                                GS_PHASE_PUT(publish);
                                next = PairReadGeneration::publish_incremental(
                                    lane.generation.writer_generation, std::span{publications.data(), publication_count},
                                    lane.merge.read_merge.get());
                                if (!next && next.error().code == ErrorCode::resource_exhausted) {
                                    reclaim_quiescent();
                                    process_merge();
                                    next = PairReadGeneration::publish_incremental(
                                        lane.generation.writer_generation,
                                        std::span{publications.data(), publication_count},
                                        lane.merge.read_merge.get());
                                }
                            }
                            if (!next) {
                                publish_fail_closed();
                                for (std::size_t index = 0; index < publication_count; ++index) {
                                    published_nodes[index]->status =
                                        Status{fail(ErrorCode::unavailable, "read publication failed")};
                                }
                            } else {
                                lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                                lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                                    std::memory_order_relaxed);
                                lane.generation.writer_generation = std::move(*next);
                                update_delta_stats();
                                lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(),
                                                        std::memory_order_relaxed);
                                publish_read_generation(lane.generation.published_generation,
                                                        lane.generation.writer_generation.get());
                                // Mark published before reclaim so catch cannot invert RAW.
                                generation_published = true;
                                for (std::size_t index = 0; index < publication_count; ++index) {
                                    published_nodes[index]->status = Status{};
                                }
                                if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                                    throw std::bad_alloc{};
                                }
                                reclaim_proportional();
                            }
                        }
                    } catch (const std::bad_alloc&) {
                        if (store_mutated) {
                            publish_fail_closed();
                        }
                        for (std::size_t index = 0; index < chunk_size; ++index) {
                            auto* node = chunk[index];
                            if (generation_published && node->status) {
                                continue;
                            }
                            bool store_entered = false;
                            for (std::size_t published = 0; published < publication_count; ++published) {
                                if (published_nodes[published] == node) {
                                    store_entered = true;
                                    break;
                                }
                            }
                            if (store_entered) {
                                // Mutated but not success-ACK'd — reconcile polarity.
                                node->status =
                                    Status{fail(ErrorCode::unavailable, "paired mutation allocation failed")};
                            } else if (!node->status) {
                                // Keep Store API / mid-chunk fail-closed polarity (known not committed).
                                continue;
                            } else if (store_mutated) {
                                // Default success but never Store-entered after sticky.
                                node->status = Status{
                                    fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                            } else {
                                node->status = Status{
                                    fail(ErrorCode::resource_exhausted, "paired mutation allocation failed")};
                            }
                        }
                    } catch (...) {
                        if (store_mutated) {
                            publish_fail_closed();
                        }
                        for (std::size_t index = 0; index < chunk_size; ++index) {
                            auto* node = chunk[index];
                            if (generation_published && node->status) {
                                continue;
                            }
                            bool store_entered = false;
                            for (std::size_t published = 0; published < publication_count; ++published) {
                                if (published_nodes[published] == node) {
                                    store_entered = true;
                                    break;
                                }
                            }
                            if (store_entered) {
                                node->status = Status{fail(ErrorCode::unavailable, "paired Writer failure")};
                            } else if (!node->status) {
                                continue;
                            } else if (store_mutated) {
                                node->status = Status{
                                    fail(ErrorCode::resource_exhausted, "paired runtime is fail-closed")};
                            } else {
                                node->status =
                                    Status{fail(ErrorCode::resource_exhausted, "paired Writer failure")};
                            }
                        }
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
                continue;
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
                            const auto* published = lane.generation.published_generation.load(std::memory_order_acquire);
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
                                generation_published = try_drain_durable_snapshot();
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
                                    generation_published = try_drain_durable_snapshot();
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
                                        generation_published = try_drain_durable_snapshot();
                                        shadow_mark_published(generation_published);
                                        publish_fail_closed();
                                        status = generation_published
                                                     ? ack_after_published_visibility()
                                                     : Status{fail(ErrorCode::unavailable,
                                                                   "read publication failed")};
                                        shadow_resolve_status();
                                    } else {
                                        lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                                        lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                                            std::memory_order_relaxed);
                                        lane.generation.writer_generation = std::move(*next);
                                        update_delta_stats();
                                        lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(),
                                                                std::memory_order_relaxed);
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
                                    generation_published = try_drain_durable_snapshot();
                                    shadow_mark_published(generation_published);
                                    publish_fail_closed();
                                    status =
                                        generation_published
                                            ? ack_after_published_visibility()
                                            : Status{fail(ErrorCode::unavailable, "read publication failed")};
                                    shadow_resolve_status();
                                } else {
                                    lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                                    lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                                        std::memory_order_relaxed);
                                    lane.generation.writer_generation = std::move(*next);
                                    update_delta_stats();
                                    lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(),
                                                            std::memory_order_relaxed);
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
                        generation_published = try_drain_durable_snapshot() || generation_published;
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
                        generation_published = try_drain_durable_snapshot() || generation_published;
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
                        generation_published = try_drain_durable_snapshot();
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
        }

        auto task = carried_task ? std::exchange(carried_task, std::nullopt) : pop_queued();
        if (!task) {
            if (lane.async.stopping.load(std::memory_order_acquire)) {
                return;
            }
            const auto observed = lane.async.signal.load(std::memory_order_acquire);
            task = pop_queued();
            bool sync_pending = false;
            {
                const std::lock_guard lock{lane.sync.sync_mutex};
                sync_pending = lane.sync_head != nullptr;
            }
            if (!task && !sync_pending && !lane.async.stopping.load(std::memory_order_acquire) &&
                !lane.generation.refresh_requested.load(std::memory_order_acquire) &&
                !lane.generation.reclaim_requested.load(std::memory_order_acquire) && !lane.merge.read_merge) {
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
                if (!woke) {
                    lane.async.signal.wait(observed, std::memory_order_acquire);
                }
            }
            if (!task) {
                continue;
            }
        }

        batch.clear();
        batch.push_back(std::move(*task));
        auto batch_bytes = batch.front().admission_bytes;
        const auto coalescing_started = std::chrono::steady_clock::now();
        const auto minimum_batch_deadline =
            batch_config ? coalescing_started + std::chrono::milliseconds{batch_config->max_wait_ms}
                         : std::chrono::steady_clock::time_point{};
        const auto burst_deadline =
            batch_config
                ? coalescing_started + std::min(std::chrono::duration_cast<std::chrono::microseconds>(
                                                    std::chrono::milliseconds{batch_config->max_wait_ms}),
                                                std::chrono::microseconds{250})
                : std::chrono::steady_clock::time_point{};
        std::size_t empty_polls{};
        while (batch.size() < maximum_batch_records) {
            auto next = pop_queued();
            if (!next) {
                // Drain deadline may arm expire while we wait for min_records /
                // burst coalescing — do not hold pre-Store work past abandon.
                if (expire_remaining_.load(std::memory_order_acquire)) {
                    break;
                }
                const auto deadline = batch_config && batch.size() < batch_config->min_records
                                          ? minimum_batch_deadline
                                          : burst_deadline;
                if (batch_config && std::chrono::steady_clock::now() < deadline) {
                    if (empty_polls++ < 64U) {
                        std::this_thread::yield();
                    } else {
                        std::this_thread::sleep_for(std::chrono::microseconds{50});
                    }
                    continue;
                }
                break;
            }
            empty_polls = 0;
            if (batch_config && (batch_bytes >= batch_config->max_bytes ||
                                 next->admission_bytes > batch_config->max_bytes - batch_bytes)) {
                carried_task = std::move(*next);
                break;
            }
            batch_bytes += next->admission_bytes;
            batch.push_back(std::move(*next));
        }

        queue_waits.clear();
        service_started.clear();
        expired.clear();
        completions.clear();
        durable_views.clear();
        durable_indices.clear();
        read_mutations.clear();
        read_mutation_indices.clear();
        bool post_commit_publication_failure{};
        bool durable_commit_observed = false;
        // Clean durable commits (no mutation error) eligible for ACK-after-drain when
        // capture/incremental publication fails but Index authority is snapshotted.
        std::vector<std::size_t> clean_durable_commit_indices;
        clean_durable_commit_indices.reserve(maximum_batch_records);
        // Sticky committed+error / Index-visible indeterminate eligible for ACK-after-visibility.
        std::vector<std::size_t> sticky_durable_commit_indices;
        sticky_durable_commit_indices.reserve(maximum_batch_records);
        const auto stage_durable_publication = [&](const std::size_t batch_index,
                                                   const DurableMutationResult& result) {
            auto& queued = batch[batch_index];
            auto& completion = completions[batch_index];
            if (!result.sequence) {
                completion.error.emplace(ErrorCode::unavailable,
                                         "committed durable mutation has no publication sequence");
                post_commit_publication_failure = true;
                return;
            }
            const auto payload = payload_for(queued);
            const HashedKey key{.key = payload.key, .hash = queued.key_hash};
            ReadMutation publication{
                .key = key,
                .record = RecordRef{.sequence = *result.sequence},
                .opcode = queued.kind == MutationKind::put ? Opcode::put : Opcode::erase,
            };
            if (queued.kind == MutationKind::put) {
                auto captured = detail::StoreAccess::capture_durable_read(store_, shard, key);
                if (!captured) {
                    // Keep sticky; ACK upgraded after successful drain-snapshot (mirror sync).
                    completion.error.emplace(std::move(captured.error()));
                    completion.error->code = ErrorCode::unavailable;
                    post_commit_publication_failure = true;
                    return;
                }
                publication.record = captured->reference();
                publication.durable.emplace(std::move(*captured));
            }
            read_mutations.push_back(std::move(publication));
            read_mutation_indices.push_back(batch_index);
        };
        const auto ack_clean_durable_after_drain = [&]() noexcept {
            for (const auto index : clean_durable_commit_indices) {
                completions[index].error.reset();
            }
            const auto* published = lane.generation.published_generation.load(std::memory_order_acquire);
            if (published == nullptr) {
                return;
            }
            for (const auto index : sticky_durable_commit_indices) {
                auto& completion = completions[index];
                if (!completion.error) {
                    continue;
                }
                const auto& queued = batch[index];
                const HashedKey key{.key = payload_for(queued).key, .hash = queued.key_hash};
                const auto view = published->prepare_durable(key);
                if (queued.kind == MutationKind::put) {
                    if (view.has_value()) {
                        completion.error.reset();
                    }
                } else if (!view.has_value() && view.error().code == ErrorCode::not_found) {
                    completion.error.reset();
                }
            }
        };
        const auto ack_staged_after_publish = [&]() noexcept {
            for (const auto index : read_mutation_indices) {
                completions[index].error.reset();
            }
        };
        const auto finish_published_generation = [&]() {
            // Authority is already visible; preserve staged success ACKs across reclaim /
            // injected post-publish faults (mirror sync volatile/durable catch).
            ack_staged_after_publish();
            if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                throw std::bad_alloc{};
            }
            if (lane.merge.read_merge) {
                lane.merge.read_merge_post_entries.store(PairReadGeneration::merge_post_entries(*lane.merge.read_merge),
                                                   std::memory_order_relaxed);
            }
            merge_retry_blocked = false;
            reclaim_quiescent();
        };
        reclaim_quiescent();
        const bool retire_pressure =
            lane.generation.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations;
        const bool merge_pressure = !PairReadGeneration::can_publish_incremental(
            *lane.generation.writer_generation, lane.merge.read_merge.get(), batch.size());
        const bool generation_pressure = retire_pressure || merge_pressure;
        const bool force_expire = expire_remaining_.load(std::memory_order_acquire);
        for (std::size_t index = 0; index < batch.size(); ++index) {
            auto& queued = batch[index];
            lane.async.queued_bytes.fetch_sub(queued.admission_bytes, std::memory_order_relaxed);
            const auto queue_wait_ns = elapsed_ns(queued.admitted_at, std::chrono::steady_clock::now());
            queue_waits.push_back(queue_wait_ns);
            atomic_saturating_add(lane.metrics.total_queue_wait_ns, queue_wait_ns);
            atomic_max(lane.metrics.maximum_queue_wait_ns, queue_wait_ns);
            lane.metrics.queue_wait_histogram.observe(queue_wait_ns);
            const bool task_expired =
                force_expire || generation_pressure ||
                (maximum_queue_wait_.count() != 0 &&
                 queue_wait_ns >=
                     static_cast<std::uint64_t>(
                         std::chrono::duration_cast<std::chrono::nanoseconds>(maximum_queue_wait_).count()));
            expired.push_back(task_expired);
            service_started.push_back(std::chrono::steady_clock::now());
            completions.push_back({.context = queued.context,
                                   .request_id = queued.request_id,
                                   .admission_bytes = queued.admission_bytes,
                                   .payload_slot = queued.payload_slot});
            if (task_expired) {
                if (merge_pressure && !retire_pressure && !force_expire) {
                    lane.merge.read_merge_backpressure.fetch_add(1U, std::memory_order_relaxed);
                }
                completions.back().error.emplace(
                    ErrorCode::resource_exhausted,
                    force_expire
                        ? "mutation abandoned after shutdown drain deadline"
                        : (retire_pressure
                               ? "mutation rejected until paired Reader reaches quiescence"
                               : (merge_pressure ? "mutation rejected until incremental read merge advances"
                                                 : "mutation expired before Store execution")));
            } else if (batch_config) {
                const auto payload = payload_for(queued);
                durable_views.push_back({.operation = queued.kind == MutationKind::put
                                                          ? detail::StoreAccess::MutationOperation::put
                                                          : detail::StoreAccess::MutationOperation::erase,
                                         .key = {.key = payload.key, .hash = queued.key_hash},
                                         .value = payload.value,
                                         .expire_at_ns = queued.expire_at_ns});
                durable_indices.push_back(index);
            }
        }

        if (batch_config && !durable_views.empty()) {
            if (expire_remaining_.load(std::memory_order_acquire)) {
                for (const auto batch_index : durable_indices) {
                    if (!completions[batch_index].error) {
                        expired[batch_index] = true;
                        completions[batch_index].error.emplace(
                            ErrorCode::resource_exhausted,
                            "mutation abandoned after shutdown drain deadline");
                    }
                }
                durable_views.clear();
                durable_indices.clear();
            }
        }
        if (batch_config && !durable_views.empty()) {
            bool durable_mutate_entered = false;
            std::size_t first_unprocessed = 0;
            std::size_t inflight_begin = 0;
            std::size_t inflight_end = 0;
            bool mutate_inflight = false;
            const auto upgrade_unprocessed = [&](const std::size_t view_index,
                                                 const ErrorCode before_mutate) -> ErrorCode {
                if (mutate_inflight && view_index >= inflight_begin && view_index < inflight_end) {
                    return ErrorCode::unavailable;
                }
                if (mutate_inflight && view_index >= inflight_end) {
                    return ErrorCode::resource_exhausted;
                }
                if (view_index >= first_unprocessed) {
                    return durable_mutate_entered || durable_commit_observed ||
                                   post_commit_publication_failure
                               ? ErrorCode::resource_exhausted
                               : before_mutate;
                }
                return ErrorCode::unavailable;
            };
            try {
                std::size_t begin = 0;
                while (begin < durable_views.size()) {
                    if (expire_remaining_.load(std::memory_order_acquire)) {
                        for (std::size_t index = begin; index < durable_views.size(); ++index) {
                            const auto batch_index = durable_indices[index];
                            expired[batch_index] = true;
                            if (!completions[batch_index].error) {
                                completions[batch_index].error.emplace(
                                    ErrorCode::resource_exhausted,
                                    "mutation abandoned after shutdown drain deadline");
                            }
                        }
                        break;
                    }
                    if (post_commit_publication_failure || !healthy_.load(std::memory_order_acquire) ||
                        !detail::StoreAccess::operational(store_)) {
                        post_commit_publication_failure = true;
                        for (std::size_t index = begin; index < durable_views.size(); ++index) {
                            auto& completion = completions[durable_indices[index]];
                            if (!completion.error) {
                                // Remaining siblings never entered mutate_durable_batch.
                                completion.error.emplace(ErrorCode::resource_exhausted,
                                                         "paired runtime is fail-closed");
                            }
                        }
                        break;
                    }
                    const std::size_t end = durable_subbatch_end(
                        begin, durable_views.size(), [&](const std::size_t index) -> const HashedKey& {
                            return durable_views[index].key;
                        });
                    GS_FAULT_SITE(mutate);
                    inflight_begin = begin;
                    inflight_end = end;
                    mutate_inflight = true;
                    durable_mutate_entered = true;
                    auto results = detail::StoreAccess::mutate_durable_batch(
                        store_, shard, std::span{durable_views}.subspan(begin, end - begin));
                    // Keep mutate_inflight through classification so post-return throws
                    // cannot stamp Store-entered siblings as never-started OVERLOADED.
                    if (glyphastore::fault::consume_fail(glyphastore::fault::Site::post_mutate)) {
                        throw std::bad_alloc{};
                    }
                    if (results.size() != end - begin) {
                        std::terminate();
                    }
                    std::vector<std::size_t> pending_stage_offsets;
                    pending_stage_offsets.reserve(results.size());
                    for (std::size_t offset = 0; offset < results.size(); ++offset) {
                        auto& result = results[offset];
                        const auto batch_index = durable_indices[begin + offset];
                        auto& completion = completions[batch_index];
                        if (result.conflict_retried) {
                            lane.metrics.conflict_retries.fetch_add(1U, std::memory_order_relaxed);
                            if (result.mutation.committed() && !result.mutation.error) {
                                lane.metrics.conflict_retry_commits.fetch_add(1U, std::memory_order_relaxed);
                            }
                        }
                        if (result.mutation.committed()) {
                            durable_commit_observed = true;
                        }
                        if (!result.mutation.committed() || result.mutation.error) {
                            auto error =
                                result.mutation.error
                                    ? std::move(*result.mutation.error)
                                    : Error{ErrorCode::io_error, "durable mutation failed without an error"};
                            if (result.mutation.committed() ||
                                result.mutation.outcome == DurableMutationOutcome::indeterminate) {
                                // Committed-without-publication or indeterminate (write boundary may
                                // have been crossed) must sticky-fail before later sub-batches.
                                error.code = ErrorCode::unavailable;
                                post_commit_publication_failure = true;
                                if (result.mutation.committed()) {
                                    sticky_durable_commit_indices.push_back(batch_index);
                                }
                            } else {
                                rewrite_known_not_committed_wire_error(error);
                            }
                            completion.error.emplace(std::move(error));
                        } else {
                            clean_durable_commit_indices.push_back(batch_index);
                            pending_stage_offsets.push_back(offset);
                        }
                    }
                    first_unprocessed = end;
                    mutate_inflight = false;
                    for (const auto offset : pending_stage_offsets) {
                        stage_durable_publication(durable_indices[begin + offset], results[offset].mutation);
                    }
                    begin = end;
                }
            } catch (const std::bad_alloc&) {
                // Exception after entering durable mutate must not ACK via a later
                // publish_incremental, and must not leave the pair healthy with possible
                // unpublished Store state (partial commit is indistinguishable here).
                if (durable_mutate_entered || durable_commit_observed || !read_mutations.empty() ||
                    post_commit_publication_failure) {
                    post_commit_publication_failure = true;
                }
                for (std::size_t view_index = 0; view_index < durable_indices.size(); ++view_index) {
                    const auto index = durable_indices[view_index];
                    // Preserve staged sibling successes; drain-snapshot publishes them.
                    // Overwriting clean ACKs here yields visible generation + error ACK.
                    if (!completions[index].error &&
                        std::find(read_mutation_indices.begin(), read_mutation_indices.end(), index) !=
                            read_mutation_indices.end()) {
                        continue;
                    }
                    if (!completions[index].error) {
                        completions[index].error.emplace(
                            upgrade_unprocessed(view_index, ErrorCode::resource_exhausted),
                            "paired mutation allocation failed");
                    }
                }
            } catch (...) {
                if (durable_mutate_entered || durable_commit_observed || !read_mutations.empty() ||
                    post_commit_publication_failure) {
                    post_commit_publication_failure = true;
                }
                for (std::size_t view_index = 0; view_index < durable_indices.size(); ++view_index) {
                    const auto index = durable_indices[view_index];
                    if (!completions[index].error &&
                        std::find(read_mutation_indices.begin(), read_mutation_indices.end(), index) !=
                            read_mutation_indices.end()) {
                        continue;
                    }
                    if (!completions[index].error) {
                        completions[index].error.emplace(
                            upgrade_unprocessed(view_index, ErrorCode::resource_exhausted),
                            "paired Writer failure");
                    }
                }
            }
            if (durable_mutate_entered && !detail::StoreAccess::operational(store_)) {
                post_commit_publication_failure = true;
            }
        } else {
            for (std::size_t index = 0; index < batch.size(); ++index) {
                if (expired[index]) {
                    continue;
                }
                auto& queued = batch[index];
                auto& completion = completions[index];
                // Same class as sync multi-chunk: after sticky post-commit failure, do not
                // Store-mutate later items in this coalesced async batch.
                if (post_commit_publication_failure || !healthy_.load(std::memory_order_acquire) ||
                    (detail::StoreAccess::is_durable(store_) && !detail::StoreAccess::operational(store_))) {
                    post_commit_publication_failure = true;
                    if (!completion.error) {
                        // Never Store-entered after sticky — known not newly committed.
                        completion.error.emplace(ErrorCode::resource_exhausted,
                                                 "paired runtime is fail-closed");
                    }
                    continue;
                }
                // Drain deadline may arm expire while this batch already left the queue;
                // re-check before Store so known-not-committed work never enters mutate.
                if (expire_remaining_.load(std::memory_order_acquire)) {
                    expired[index] = true;
                    completion.error.emplace(ErrorCode::resource_exhausted,
                                             "mutation abandoned after shutdown drain deadline");
                    continue;
                }
                bool current_committed = false;
                bool durable_mutate_entered = false;
                try {
                    const auto payload = payload_for(queued);
                    const HashedKey key{.key = payload.key, .hash = queued.key_hash};
                    if (detail::StoreAccess::is_durable(store_)) {
                        DurableMutationResult result;
                        bool conflict_retried{};
                        durable_mutate_entered = true;
                        for (unsigned attempt = 0; attempt < 2; ++attempt) {
                            result = queued.kind == MutationKind::put
                                         ? detail::StoreAccess::put_durable(store_, shard, key, payload.value,
                                                                            queued.expire_at_ns)
                                         : detail::StoreAccess::erase_durable(store_, shard, key);
                            if (!detail::StoreAccess::should_retry_durable_mutation(result, attempt)) {
                                break;
                            }
                            conflict_retried = true;
                        }
                        if (conflict_retried) {
                            lane.metrics.conflict_retries.fetch_add(1U, std::memory_order_relaxed);
                            if (result.committed() && !result.error) {
                                lane.metrics.conflict_retry_commits.fetch_add(1U, std::memory_order_relaxed);
                            }
                        }
                        if (result.committed()) {
                            current_committed = true;
                            durable_commit_observed = true;
                        }
                        if (!result.committed() || result.error) {
                            auto error = result.error ? std::move(*result.error)
                                                      : Error{ErrorCode::io_error,
                                                              "durable mutation failed without an error"};
                            if (result.committed() ||
                                result.outcome == DurableMutationOutcome::indeterminate) {
                                error.code = ErrorCode::unavailable;
                                post_commit_publication_failure = true;
                                if (result.committed()) {
                                    sticky_durable_commit_indices.push_back(index);
                                }
                            } else {
                                rewrite_known_not_committed_wire_error(error);
                            }
                            completion.error.emplace(std::move(error));
                        } else {
                            clean_durable_commit_indices.push_back(index);
                            stage_durable_publication(index, result);
                        }
                    } else {
                        GS_FAULT_SITE(mutate);
                        auto published =
                            queued.kind == MutationKind::put
                                ? detail::StoreAccess::put_volatile_published(
                                      store_, shard, key, payload.value, queued.expire_at_ns)
                                : detail::StoreAccess::erase_volatile_published(store_, shard, key);
                        if (!published) {
                            bool sticky = false;
                            auto error = classify_volatile_mutation_error(published.error(), sticky);
                            if (sticky) {
                                // Append crossed; mirror durable indeterminate sticky.
                                post_commit_publication_failure = true;
                            }
                            completion.error.emplace(std::move(error));
                        } else {
                            current_committed = true;
                            read_mutations.push_back({.key = key,
                                                      .record = published->record,
                                                      .segment = std::move(published->segment),
                                                      .opcode = published->opcode});
                            read_mutation_indices.push_back(index);
                        }
                    }
                } catch (const std::bad_alloc&) {
                    // If already staged into read_mutations, do not error-ACK here —
                    // sticky/happy publish must success-ACK once authority is visible.
                    const bool already_staged =
                        std::find(read_mutation_indices.begin(), read_mutation_indices.end(), index) !=
                        read_mutation_indices.end();
                    if (current_committed || durable_mutate_entered || post_commit_publication_failure) {
                        post_commit_publication_failure = true;
                    }
                    if (!already_staged) {
                        completion.error.emplace(post_commit_publication_failure
                                                     ? ErrorCode::unavailable
                                                     : ErrorCode::resource_exhausted,
                                                 "paired mutation allocation failed");
                    }
                } catch (...) {
                    const bool already_staged =
                        std::find(read_mutation_indices.begin(), read_mutation_indices.end(), index) !=
                        read_mutation_indices.end();
                    if (current_committed || durable_mutate_entered || post_commit_publication_failure) {
                        post_commit_publication_failure = true;
                    }
                    if (!already_staged) {
                        completion.error.emplace(post_commit_publication_failure
                                                     ? ErrorCode::unavailable
                                                     : ErrorCode::resource_exhausted,
                                                 "paired Writer failure");
                    }
                }
                if (durable_mutate_entered && !detail::StoreAccess::operational(store_)) {
                    post_commit_publication_failure = true;
                }
            }
        }

        // Linearization order for paired mutations:
        // Store append/index publication -> immutable generation publication
        // (release) -> completion queue -> client ACK. A publication failure is
        // sticky and fail-closed because the mutable Store has already changed.
        //
        // When a later item fails publication after earlier commits, still publish
        // durable authority (snapshot) or already-staged volatile mutations so
        // successful siblings are not left unpublished with an error ACK.
        // Snapshot/incremental MUST run before publish_fail_closed(): marking the
        // durable catalog fail-closed first makes snapshot_published_reads reject.
        // After a successful durable drain, clear errors on clean commits (ACK-after-
        // publish) — capture-failed items must not keep error ACK while GET-visible.
        // After a successful incremental publish, clear errors on staged indices so a
        // catch that raced staging cannot invert RAW (visible + error ACK).
        bool generation_published = false;
        try {
            if (post_commit_publication_failure && detail::StoreAccess::is_durable(store_) &&
                (durable_commit_observed || !read_mutations.empty())) {
                if (try_drain_durable_snapshot()) {
                    generation_published = true;
                    ack_clean_durable_after_drain();
                    if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                        throw std::bad_alloc{};
                    }
                } else {
                    for (const auto index : read_mutation_indices) {
                        if (!completions[index].error) {
                            completions[index].error.emplace(
                                ErrorCode::unavailable, "read publication failed after a committed mutation; "
                                                        "paired runtime is fail-closed");
                        }
                    }
                }
                publish_fail_closed();
            } else if (post_commit_publication_failure && !read_mutations.empty()) {
                auto next = PairReadGeneration::publish_incremental(lane.generation.writer_generation, read_mutations,
                                                                    lane.merge.read_merge.get());
                if (!next) {
                    for (const auto index : read_mutation_indices) {
                        if (!completions[index].error) {
                            completions[index].error.emplace(
                                ErrorCode::unavailable, "read publication failed after a committed mutation; "
                                                        "paired runtime is fail-closed");
                        }
                    }
                } else {
                    lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                    lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                        std::memory_order_relaxed);
                    lane.generation.writer_generation = std::move(*next);
                    update_delta_stats();
                    lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(), std::memory_order_relaxed);
                    publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
                    generation_published = true;
                    finish_published_generation();
                }
                publish_fail_closed();
            } else if (post_commit_publication_failure) {
                publish_fail_closed();
            } else if (!read_mutations.empty()) {
                auto next = PairReadGeneration::publish_incremental(lane.generation.writer_generation, read_mutations,
                                                                    lane.merge.read_merge.get());
                if (!next) {
                    // Durable happy-path incremental fail: drain Index before sticky close
                    // (mirror sync single-op). Volatile has nothing to drain.
                    if (detail::StoreAccess::is_durable(store_) && durable_commit_observed &&
                        try_drain_durable_snapshot()) {
                        generation_published = true;
                        ack_clean_durable_after_drain();
                        if (glyphastore::fault::consume_fail(glyphastore::fault::Site::publish)) {
                            throw std::bad_alloc{};
                        }
                    } else {
                        for (const auto index : read_mutation_indices) {
                            completions[index].error.emplace(
                                ErrorCode::unavailable,
                                "read publication failed after mutation linearization; "
                                "paired runtime is fail-closed");
                        }
                    }
                    publish_fail_closed();
                } else {
                    lane.generation.retired_generations.push_back(lane.generation.writer_generation);
                    lane.generation.retired_generation_count.store(lane.generation.retired_generations.size(),
                                                        std::memory_order_relaxed);
                    lane.generation.writer_generation = std::move(*next);
                    update_delta_stats();
                    lane.generation.writer_epoch.store(lane.generation.writer_generation->epoch(), std::memory_order_relaxed);
                    publish_read_generation(lane.generation.published_generation, lane.generation.writer_generation.get());
                    generation_published = true;
                    finish_published_generation();
                }
            }
        } catch (const std::bad_alloc&) {
            if (generation_published) {
                ack_staged_after_publish();
                if (detail::StoreAccess::is_durable(store_)) {
                    ack_clean_durable_after_drain();
                }
            } else if (!read_mutations.empty()) {
                for (const auto index : read_mutation_indices) {
                    if (!completions[index].error) {
                        completions[index].error.emplace(ErrorCode::unavailable,
                                                         "paired mutation allocation failed");
                    }
                }
            }
            publish_fail_closed();
        } catch (...) {
            if (generation_published) {
                ack_staged_after_publish();
                if (detail::StoreAccess::is_durable(store_)) {
                    ack_clean_durable_after_drain();
                }
            } else if (!read_mutations.empty()) {
                for (const auto index : read_mutation_indices) {
                    if (!completions[index].error) {
                        completions[index].error.emplace(ErrorCode::unavailable, "paired Writer failure");
                    }
                }
            }
            publish_fail_closed();
        }

        const auto completed_at = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < batch.size(); ++index) {
            const auto service_ns = expired[index] ? 0U : elapsed_ns(service_started[index], completed_at);
            if (!expired[index]) {
                const auto foreground_ns =
                    service_ns > std::numeric_limits<std::uint64_t>::max() - queue_waits[index]
                        ? std::numeric_limits<std::uint64_t>::max()
                        : service_ns + queue_waits[index];
                detail::StoreAccess::report_foreground_latency(store_, foreground_ns);
                lane.metrics.service_histogram.observe(service_ns);
            } else {
                lane.metrics.expired_before_store.fetch_add(1U, std::memory_order_relaxed);
            }
            lane.metrics.completed.fetch_add(1U, std::memory_order_relaxed);
            atomic_saturating_add(lane.metrics.total_service_ns, service_ns);
            atomic_max(lane.metrics.maximum_service_ns, service_ns);
            if (!deliver_outcome(batch[index].sink, std::move(completions[index]))) {
                std::terminate();
            }
            notify_sink(batch[index].sink);
        }
    }
}

void ShardPairRuntime::wake(Lane& lane) noexcept {
    lane.async.signal.fetch_add(1U, std::memory_order_release);
    lane.async.signal.notify_one();
}

auto ShardPairRuntime::mutate(const std::size_t shard, const MutationKind kind, const HashedKey& key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
    -> Status {
    if (shard >= lanes_.size()) {
        return fail(ErrorCode::invalid_argument, "paired mutation shard is out of range");
    }
    {
        GS_PHASE_PUT(admit);
        if (!begin_submission()) {
            return fail(ErrorCode::unavailable, "paired runtime is not accepting mutations");
        }
    }
    struct AdmissionGuard final {
        ShardPairRuntime& runtime;
        ~AdmissionGuard() {
            runtime.finish_submission();
        }
    } admission{*this};

    // Mirror async try_submit: sticky fail-closed / stop must reject sync embeds too.
    // Otherwise volatile pairs keep mutating after publish_fail_closed (durable is masked
    // by DurableRuntime::healthy checks on the Store API).
    if (!healthy_.load(std::memory_order_acquire) || !started_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire) ||
        lanes_[shard]->async.stopping.load(std::memory_order_acquire)) {
        return fail(ErrorCode::unavailable, "paired runtime is fail-closed");
    }

    SyncMutation node{.kind = kind, .key = &key, .value = value, .expire_at_ns = expire_at_ns};
    auto& lane = *lanes_[shard];
    GS_FAULT_SITE(enqueue);
    {
        GS_PHASE_PUT(enqueue);
        const std::lock_guard lock{lane.sync.sync_mutex};
        node.next = lane.sync_head;
        lane.sync_head = &node;
    }
    lane.sync.sync_admitted.fetch_add(1U, std::memory_order_relaxed);
    wake(lane);
    {
        GS_PHASE_PUT(ack);
        // Short adaptive spin before parking. Keep this bounded: on Apple Silicon
        // long dual-sided spins (caller + Writer) compete for the same P-cores and
        // can regress worker-affine multi-thread PUT.
        for (unsigned spin = 0; spin < 32U; ++spin) {
            if (node.done.load(std::memory_order_acquire)) {
                return node.status;
            }
#if defined(__aarch64__) || defined(_M_ARM64)
            __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(_M_X64)
            __builtin_ia32_pause();
#else
            std::this_thread::yield();
#endif
        }
        node.done.wait(false, std::memory_order_acquire);
    }
    return node.status;
}

auto ShardPairRuntime::mutate_batch(const std::size_t shard, const std::span<const SyncBatchItem> items,
                                    const std::span<Status> statuses) -> Status {
    if (shard >= lanes_.size()) {
        return fail(ErrorCode::invalid_argument, "paired mutation shard is out of range");
    }
    if (items.size() != statuses.size()) {
        return fail(ErrorCode::invalid_argument, "paired mutation batch status span size mismatch");
    }
    if (items.empty()) {
        return {};
    }
    for (const auto& item : items) {
        if (item.key == nullptr) {
            return fail(ErrorCode::invalid_argument, "paired mutation batch item has a null key");
        }
    }
    if (!begin_submission()) {
        for (auto& status : statuses) {
            status = Status{fail(ErrorCode::unavailable, "paired runtime is not accepting mutations")};
        }
        return fail(ErrorCode::unavailable, "paired runtime is not accepting mutations");
    }
    struct AdmissionGuard final {
        ShardPairRuntime& runtime;
        ~AdmissionGuard() {
            runtime.finish_submission();
        }
    } admission{*this};

    if (!healthy_.load(std::memory_order_acquire) || !started_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire) ||
        lanes_[shard]->async.stopping.load(std::memory_order_acquire)) {
        for (auto& status : statuses) {
            status = Status{fail(ErrorCode::unavailable, "paired runtime is fail-closed")};
        }
        return fail(ErrorCode::unavailable, "paired runtime is fail-closed");
    }

    // Stack nodes for the common ≤32 path; heap for larger caller batches.
    constexpr std::size_t kStackBatch = 32U;
    std::array<SyncMutation, kStackBatch> stack_nodes{};
    std::unique_ptr<SyncMutation[]> heap_nodes;
    SyncMutation* nodes = stack_nodes.data();
    if (items.size() > kStackBatch) {
        heap_nodes = std::make_unique<SyncMutation[]>(items.size());
        nodes = heap_nodes.get();
    }
    for (std::size_t index = 0; index < items.size(); ++index) {
        nodes[index].kind = items[index].kind;
        nodes[index].key = items[index].key;
        nodes[index].value = items[index].value;
        nodes[index].expire_at_ns = items[index].expire_at_ns;
        nodes[index].status = {};
        nodes[index].done.store(false, std::memory_order_relaxed);
        nodes[index].next = nullptr;
    }

    auto& lane = *lanes_[shard];
    GS_FAULT_SITE(enqueue);
    {
        const std::lock_guard lock{lane.sync.sync_mutex};
        // Same LIFO push as single mutate; Writer reverses to FIFO.
        for (std::size_t index = 0; index < items.size(); ++index) {
            nodes[index].next = lane.sync_head;
            lane.sync_head = &nodes[index];
        }
    }
    lane.sync.sync_admitted.fetch_add(items.size(), std::memory_order_relaxed);
    wake(lane);

    for (std::size_t index = 0; index < items.size(); ++index) {
        auto& node = nodes[index];
        for (unsigned spin = 0; spin < 32U; ++spin) {
            if (node.done.load(std::memory_order_acquire)) {
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
        if (!node.done.load(std::memory_order_acquire)) {
            node.done.wait(false, std::memory_order_acquire);
        }
        statuses[index] = node.status;
    }
    return {};
}

ShardPairRuntime::ReadLease::ReadLease(const ShardPairRuntime& runtime, const std::size_t shard) noexcept {
    if (shard >= runtime.lanes_.size()) {
        return;
    }
    runtime_ = &runtime;
    shard_ = shard;
    auto& lane = *runtime.lanes_[shard];
    // Publish the lease before adopting the pointer. The fence pairs with the
    // Writer's reclamation fence and closes the load-then-increment window that
    // could otherwise free a generation while a Reader was adopting it.
    lane.reclaim.active_read_leases.fetch_add(1U, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    GS_FAULT_SITE(adopt);
    generation_ = lane.generation.published_generation.load(std::memory_order_acquire);
    if (generation_ == nullptr) {
        lane.reclaim.active_read_leases.fetch_sub(1U, std::memory_order_release);
        runtime_ = nullptr;
    }
}

ShardPairRuntime::ReadLease::~ReadLease() {
    if (runtime_ == nullptr || generation_ == nullptr) {
        return;
    }
    auto& lane = *runtime_->lanes_[shard_];
    // Drop the lease without waking the Writer. Reclaim is opportunistic: the
    // Writer observes reclaim_requested on its next loop turn / park predicate.
    // Waking on every serial GET created a Reader→Writer ping-pong that dominated
    // embedded get_copy cost while contributing no correctness benefit (retired
    // generations are only produced by publication, which already wakes Writer).
    if (lane.reclaim.active_read_leases.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
        lane.generation.reclaim_requested.store(true, std::memory_order_release);
    }
}

} // namespace glyphastore::store::paired
