#include "glyphastore/store/paired/shard_pair_runtime.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/paired/bounded_spsc_queue.hpp"
#include "glyphastore/store/paired/mutation_slot_pool.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <limits>
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

void atomic_max(std::atomic<std::uint64_t>& destination, const std::uint64_t value) noexcept {
    auto observed = destination.load(std::memory_order_relaxed);
    while (observed < value && !destination.compare_exchange_weak(observed, value, std::memory_order_relaxed,
                                                                  std::memory_order_relaxed)) {
    }
}

void atomic_max(std::atomic<std::size_t>& destination, const std::size_t value) noexcept {
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

struct AtomicLatencyHistogram final {
    std::array<std::atomic<std::uint64_t>, LatencyHistogram::kBoundsNs.size()> counts{};
    std::atomic<std::uint64_t> observations{};
    std::atomic<std::uint64_t> sum_ns{};

    void observe(const std::uint64_t sample_ns) noexcept {
        for (std::size_t index = 0; index < LatencyHistogram::kBoundsNs.size(); ++index) {
            if (sample_ns <= LatencyHistogram::kBoundsNs[index]) {
                counts[index].fetch_add(1U, std::memory_order_relaxed);
                break;
            }
        }
        observations.fetch_add(1U, std::memory_order_relaxed);
        atomic_saturating_add(sum_ns, sample_ns);
    }

    [[nodiscard]] auto snapshot() const noexcept -> LatencyHistogram {
        LatencyHistogram result;
        for (std::size_t index = 0; index < counts.size(); ++index) {
            result.counts[index] = counts[index].load(std::memory_order_relaxed);
        }
        result.observations = observations.load(std::memory_order_relaxed);
        result.sum_ns = sum_ns.load(std::memory_order_relaxed);
        return result;
    }
};

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
                   capacity == 0
                       ? 1U
                       : (maximum_payload_bytes == 0 ? std::max(payload_bytes, std::size_t{1})
                                                     : maximum_payload_bytes)),
          async_enabled(capacity != 0U), writer_generation(std::move(initial)),
          published_catalog_revision(catalog_revision) {
        if (!writer_generation) {
            throw std::runtime_error{"paired Writer has no initial read generation"};
        }
        retired_generations.reserve(ShardPairRuntime::kMaximumRetiredReadGenerations);
        writer_epoch.store(writer_generation->epoch(), std::memory_order_relaxed);
        delta_entries.store(writer_generation->delta_entries(), std::memory_order_relaxed);
        delta_record_versions.store(writer_generation->delta_record_versions(), std::memory_order_relaxed);
        delta_arena_record_bytes.store(writer_generation->delta_arena_record_bytes(),
                                       std::memory_order_relaxed);
        delta_arena_key_bytes.store(writer_generation->delta_arena_key_bytes(), std::memory_order_relaxed);
        delta_arena_key_storage_bytes.store(writer_generation->delta_arena_key_storage_bytes(),
                                            std::memory_order_relaxed);
        published_generation.store(writer_generation.get(), std::memory_order_release);
    }

    BoundedSpscQueue<AsyncMutationTask> queue;
    MutationSlotPool payloads;
    alignas(128) std::atomic<std::uint64_t> signal{};
    alignas(128) std::atomic_bool stopping{};
    std::thread thread;
    std::atomic<std::size_t> queued_bytes{};
    std::atomic<std::size_t> maximum_queue_depth{};
    std::atomic<std::size_t> maximum_queued_bytes{};
    std::atomic<std::size_t> payload_slots_in_use{};
    std::atomic<std::size_t> maximum_payload_slots_in_use{};
    std::atomic<std::size_t> payload_arena_bytes_in_use{};
    std::atomic<std::size_t> maximum_payload_arena_bytes_in_use{};
    std::atomic<std::size_t> payload_admission_bytes_in_use{};
    std::atomic<std::size_t> maximum_payload_admission_bytes_in_use{};
    std::atomic<std::uint64_t> payload_slot_full_total{};
    std::atomic<std::uint64_t> payload_arena_full_total{};
    std::atomic<std::uint64_t> payload_too_large_total{};
    std::atomic<std::uint64_t> admitted{};
    std::atomic<std::uint64_t> rejected{};
    std::atomic<std::uint64_t> expired_before_store{};
    std::atomic<std::uint64_t> completed{};
    std::atomic<std::uint64_t> conflict_retries{};
    std::atomic<std::uint64_t> conflict_retry_commits{};
    std::atomic<std::uint64_t> total_queue_wait_ns{};
    std::atomic<std::uint64_t> maximum_queue_wait_ns{};
    std::atomic<std::uint64_t> total_service_ns{};
    std::atomic<std::uint64_t> maximum_service_ns{};
    std::atomic<std::uint64_t> read_refresh_attempts{};
    std::atomic<std::uint64_t> read_refresh_successes{};
    std::atomic<std::uint64_t> read_refresh_failures{};
    std::atomic<std::uint64_t> read_refresh_deferrals{};
    std::atomic<std::uint64_t> generations_retired{};
    std::atomic<std::size_t> retired_generation_count{};
    std::atomic<std::size_t> delta_entries{};
    std::atomic<std::size_t> delta_record_versions{};
    std::atomic<std::size_t> delta_arena_record_bytes{};
    std::atomic<std::size_t> delta_arena_key_bytes{};
    std::atomic<std::size_t> delta_arena_key_storage_bytes{};
    std::atomic_bool read_merge_active{};
    std::atomic<std::size_t> read_merge_post_entries{};
    std::atomic<std::uint64_t> read_merge_starts{};
    std::atomic<std::uint64_t> read_merge_completions{};
    std::atomic<std::uint64_t> read_merge_failures{};
    std::atomic<std::uint64_t> read_merge_backpressure{};
    std::atomic<std::uint64_t> read_merge_slots_processed{};
    AtomicLatencyHistogram queue_wait_histogram{};
    AtomicLatencyHistogram service_histogram{};
    std::shared_ptr<const PairReadGeneration> writer_generation;
    std::unique_ptr<PairReadMerge> read_merge;
    std::vector<std::shared_ptr<const PairReadGeneration>> retired_generations;
    alignas(128) std::atomic<const PairReadGeneration*> published_generation{};
    alignas(128) std::atomic<std::uint64_t> writer_epoch{};
    alignas(128) std::atomic<std::uint64_t> reader_safe_epoch{};
    alignas(128) std::atomic<std::uint64_t> published_catalog_revision{};
    alignas(128) std::atomic_bool refresh_requested{};
    alignas(128) std::atomic_bool reclaim_requested{};
    const bool async_enabled{};
    std::mutex sync_mutex{};
    SyncMutation* sync_head{};
    std::atomic_size_t active_read_leases{};
    std::atomic<std::uint64_t> sync_admitted{};
};

ShardPairRuntime::ShardPairRuntime(
    Store& store, PairedConcurrencyConfig config,
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
        lanes_.push_back(std::make_unique<Lane>(
            config_.async_lane_capacity, std::move(initial_generations[shard]),
            initial_catalog_revisions[shard], config_.async_lane_payload_bytes,
            config_.async_maximum_payload_bytes));
    }
}

ShardPairRuntime::~ShardPairRuntime() {
    static_cast<void>(stop_and_drain());
}

auto ShardPairRuntime::async_lane_enabled() const noexcept -> bool {
    return !lanes_.empty() && lanes_.front()->async_enabled;
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
    return std::unique_ptr<ShardPairRuntime>(
        new ShardPairRuntime(store, config, std::move(initial_generations),
                             std::move(initial_catalog_revisions)));
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

auto ShardPairRuntime::try_submit(const AsyncMutationRequest& request) noexcept -> std::optional<std::size_t> {
    if (request.shard >= lanes_.size()) {
        return std::nullopt;
    }
    auto& lane = *lanes_[request.shard];
    if (!lanes_[request.shard]->async_enabled || request.sink.deliver == nullptr) {
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    if (!begin_submission()) {
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    struct SubmissionGuard final {
        ShardPairRuntime& executor;
        ~SubmissionGuard() {
            executor.finish_submission();
        }
    } submission{*this};
    if (!healthy_.load(std::memory_order_acquire) || !started_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire) || lane.stopping.load(std::memory_order_acquire)) {
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto admission_bytes = mutation_admission_bytes(request.key.size(), request.value.size());
    if (!admission_bytes) {
        lane.payload_too_large_total.fetch_add(1U, std::memory_order_relaxed);
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto queued_bytes = lane.queued_bytes.load(std::memory_order_relaxed);
    if (*admission_bytes > std::numeric_limits<std::size_t>::max() - queued_bytes) {
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    auto acquired = lane.payloads.try_acquire(request.key, request.value, *admission_bytes);
    if (!acquired.lease) {
        switch (acquired.failure) {
        case MutationSlotPool::AcquireFailure::slot_exhausted:
            lane.payload_slot_full_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::byte_exhausted:
            lane.payload_arena_full_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::payload_too_large:
            lane.payload_too_large_total.fetch_add(1U, std::memory_order_relaxed);
            break;
        case MutationSlotPool::AcquireFailure::none:
            break;
        }
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
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
        lane.queued_bytes.fetch_add(*admission_bytes, std::memory_order_relaxed) + *admission_bytes;
    if (!lane.queue.try_push(std::move(task))) {
        lane.queued_bytes.fetch_sub(*admission_bytes, std::memory_order_relaxed);
        if (!lane.payloads.rollback(*acquired.lease)) {
            std::terminate();
        }
        lane.rejected.fetch_add(1U, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto slots_in_use = lane.payloads.slots_in_use();
    const auto arena_bytes_in_use = lane.payloads.payload_bytes_in_use();
    const auto admission_bytes_in_use = lane.payloads.admission_bytes_in_use();
    lane.payload_slots_in_use.store(slots_in_use, std::memory_order_relaxed);
    lane.payload_arena_bytes_in_use.store(arena_bytes_in_use, std::memory_order_relaxed);
    lane.payload_admission_bytes_in_use.store(admission_bytes_in_use, std::memory_order_relaxed);
    atomic_max(lane.maximum_payload_slots_in_use, slots_in_use);
    atomic_max(lane.maximum_payload_arena_bytes_in_use, arena_bytes_in_use);
    atomic_max(lane.maximum_payload_admission_bytes_in_use, admission_bytes_in_use);
    lane.admitted.fetch_add(1U, std::memory_order_relaxed);
    atomic_max(lane.maximum_queue_depth, lane.queue.size());
    atomic_max(lane.maximum_queued_bytes, next_bytes);
    lane.signal.fetch_add(1U, std::memory_order_release);
    lane.signal.notify_one();
    return admission_bytes;
}

auto ShardPairRuntime::release_payload(const std::size_t shard,
                                     const std::uint32_t payload_slot) noexcept -> bool {
    if (shard >= lanes_.size()) {
        return false;
    }
    auto& lane = *lanes_[shard];
    if (!lane.payloads.release(payload_slot)) {
        return false;
    }
    lane.payload_slots_in_use.store(lane.payloads.slots_in_use(), std::memory_order_relaxed);
    lane.payload_arena_bytes_in_use.store(lane.payloads.payload_bytes_in_use(), std::memory_order_relaxed);
    lane.payload_admission_bytes_in_use.store(lane.payloads.admission_bytes_in_use(),
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
    const auto* generation = lane.published_generation.load(std::memory_order_acquire);
    if (generation != nullptr) {
        // The current turn protects generation->epoch(); asynchronous I/O can
        // additionally keep an older epoch borrowed. The minimum is the sole
        // reclamation boundary published by Reader to Writer.
        const auto safe_epoch = std::min(generation->epoch(), minimum_leased_epoch);
        const auto previous = lane.reader_safe_epoch.exchange(safe_epoch, std::memory_order_acq_rel);
        if (previous != safe_epoch && !lane.reclaim_requested.exchange(true, std::memory_order_acq_rel)) {
            lane.signal.fetch_add(1U, std::memory_order_release);
            lane.signal.notify_one();
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
    if (current == 0U || current == lane.published_catalog_revision.load(std::memory_order_acquire)) {
        return;
    }
    if (!lane.refresh_requested.exchange(true, std::memory_order_acq_rel)) {
        lane.signal.fetch_add(1U, std::memory_order_release);
        lane.signal.notify_one();
    }
}

void ShardPairRuntime::note_rejected(const std::size_t shard) noexcept {
    if (shard < lanes_.size()) {
        lanes_[shard]->rejected.fetch_add(1U, std::memory_order_relaxed);
    }
}

auto ShardPairRuntime::stats() const -> std::vector<ShardPairStats> {
    std::vector<ShardPairStats> result;
    result.reserve(lanes_.size());
    for (std::size_t worker = 0; worker < lanes_.size(); ++worker) {
        const auto& lane = *lanes_[worker];
        result.push_back(
            {.worker_index = worker,
             .reader_safe_epoch = lane.reader_safe_epoch.load(std::memory_order_relaxed),
             .writer_epoch = lane.writer_epoch.load(std::memory_order_relaxed),
             .queue_depth = lane.queue.size(),
             .queued_bytes = lane.queued_bytes.load(std::memory_order_relaxed),
             .maximum_queue_depth = lane.maximum_queue_depth.load(std::memory_order_relaxed),
             .maximum_queued_bytes = lane.maximum_queued_bytes.load(std::memory_order_relaxed),
             .payload_slot_capacity = lane.payloads.slot_capacity(),
             .payload_slots_in_use = lane.payload_slots_in_use.load(std::memory_order_relaxed),
             .maximum_payload_slots_in_use =
                 lane.maximum_payload_slots_in_use.load(std::memory_order_relaxed),
             .payload_arena_capacity_bytes = lane.payloads.byte_capacity(),
             .payload_arena_storage_bytes = lane.payloads.storage_bytes(),
             .payload_arena_bytes_in_use = lane.payload_arena_bytes_in_use.load(std::memory_order_relaxed),
             .maximum_payload_arena_bytes_in_use =
                 lane.maximum_payload_arena_bytes_in_use.load(std::memory_order_relaxed),
             .payload_admission_bytes_in_use =
                 lane.payload_admission_bytes_in_use.load(std::memory_order_relaxed),
             .maximum_payload_admission_bytes_in_use =
                 lane.maximum_payload_admission_bytes_in_use.load(std::memory_order_relaxed),
             .payload_slot_full_total = lane.payload_slot_full_total.load(std::memory_order_relaxed),
             .payload_arena_full_total = lane.payload_arena_full_total.load(std::memory_order_relaxed),
             .payload_too_large_total = lane.payload_too_large_total.load(std::memory_order_relaxed),
             .admitted = lane.admitted.load(std::memory_order_relaxed),
             .rejected = lane.rejected.load(std::memory_order_relaxed),
             .expired_before_store = lane.expired_before_store.load(std::memory_order_relaxed),
             .completed = lane.completed.load(std::memory_order_relaxed),
             .conflict_retries = lane.conflict_retries.load(std::memory_order_relaxed),
             .conflict_retry_commits = lane.conflict_retry_commits.load(std::memory_order_relaxed),
             .total_queue_wait_ns = lane.total_queue_wait_ns.load(std::memory_order_relaxed),
             .maximum_queue_wait_ns = lane.maximum_queue_wait_ns.load(std::memory_order_relaxed),
             .total_service_ns = lane.total_service_ns.load(std::memory_order_relaxed),
             .maximum_service_ns = lane.maximum_service_ns.load(std::memory_order_relaxed),
             .read_catalog_revision = lane.published_catalog_revision.load(std::memory_order_relaxed),
             .read_refresh_attempts = lane.read_refresh_attempts.load(std::memory_order_relaxed),
             .read_refresh_successes = lane.read_refresh_successes.load(std::memory_order_relaxed),
             .read_refresh_failures = lane.read_refresh_failures.load(std::memory_order_relaxed),
             .read_refresh_deferrals = lane.read_refresh_deferrals.load(std::memory_order_relaxed),
             .generations_retired = lane.generations_retired.load(std::memory_order_relaxed),
             .retired_generation_count = lane.retired_generation_count.load(std::memory_order_relaxed),
             .delta_entries = lane.delta_entries.load(std::memory_order_relaxed),
             .delta_record_versions = lane.delta_record_versions.load(std::memory_order_relaxed),
             .delta_arena_record_bytes = lane.delta_arena_record_bytes.load(std::memory_order_relaxed),
             .delta_arena_key_bytes = lane.delta_arena_key_bytes.load(std::memory_order_relaxed),
             .delta_arena_key_storage_bytes =
                 lane.delta_arena_key_storage_bytes.load(std::memory_order_relaxed),
             .read_merge_active = lane.read_merge_active.load(std::memory_order_relaxed),
             .read_merge_post_entries = lane.read_merge_post_entries.load(std::memory_order_relaxed),
             .read_merge_starts = lane.read_merge_starts.load(std::memory_order_relaxed),
             .read_merge_completions = lane.read_merge_completions.load(std::memory_order_relaxed),
             .read_merge_failures = lane.read_merge_failures.load(std::memory_order_relaxed),
             .read_merge_backpressure = lane.read_merge_backpressure.load(std::memory_order_relaxed),
             .read_merge_slots_processed = lane.read_merge_slots_processed.load(std::memory_order_relaxed),
            .sync_admitted = lane.sync_admitted.load(std::memory_order_relaxed),
             .queue_wait_histogram = lane.queue_wait_histogram.snapshot(),
             .service_histogram = lane.service_histogram.snapshot()});
    }
    return result;
}

void ShardPairRuntime::note_writer_exit() noexcept {
    if (active_writers_.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
        active_writers_.notify_all();
    }
}

auto ShardPairRuntime::stop_and_drain(const std::optional<std::chrono::milliseconds> deadline) -> Status {
    if (!stopping_.exchange(true, std::memory_order_acq_rel)) {
        auto observed = admission_state_.fetch_or(kAdmissionClosed, std::memory_order_acq_rel);
        while ((observed & kAdmissionCountMask) != 0) {
            admission_state_.wait(observed, std::memory_order_acquire);
            observed = admission_state_.load(std::memory_order_acquire);
        }
        for (auto& lane : lanes_) {
            lane->stopping.store(true, std::memory_order_release);
            lane->signal.fetch_add(1U, std::memory_order_release);
            lane->signal.notify_one();
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
            expire_remaining_.store(true, std::memory_order_release);
            for (auto& lane : lanes_) {
                lane->signal.fetch_add(1U, std::memory_order_release);
                lane->signal.notify_one();
            }
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

    const auto publish_fail_closed = [&]() noexcept {
        healthy_.store(false, std::memory_order_release);
        detail::StoreAccess::mark_fail_closed(store_);
        expire_remaining_.store(true, std::memory_order_release);
        for (auto& affected_lane : lanes_) {
            affected_lane->signal.fetch_add(1U, std::memory_order_release);
            affected_lane->signal.notify_one();
        }
    };
    const auto update_delta_stats = [&]() noexcept {
        lane.delta_entries.store(lane.writer_generation->delta_entries(), std::memory_order_relaxed);
        lane.delta_record_versions.store(lane.writer_generation->delta_record_versions(),
                                         std::memory_order_relaxed);
        lane.delta_arena_record_bytes.store(lane.writer_generation->delta_arena_record_bytes(),
                                            std::memory_order_relaxed);
        lane.delta_arena_key_bytes.store(lane.writer_generation->delta_arena_key_bytes(),
                                         std::memory_order_relaxed);
        lane.delta_arena_key_storage_bytes.store(lane.writer_generation->delta_arena_key_storage_bytes(),
                                                 std::memory_order_relaxed);
    };
    const auto reclaim_quiescent = [&]() noexcept {
        const auto before = lane.retired_generations.size();
        const auto quiescent_epoch =
            config_.reader_epoch_lease
                ? lane.reader_safe_epoch.load(std::memory_order_acquire)
                : (lane.active_read_leases.load(std::memory_order_acquire) == 0
                       ? lane.writer_epoch.load(std::memory_order_relaxed)
                       : std::uint64_t{0});
        std::erase_if(lane.retired_generations,
                      [&](const auto& retired) { return retired->epoch() < quiescent_epoch; });
        const auto retired = before - lane.retired_generations.size();
        if (retired != 0U) {
            lane.generations_retired.fetch_add(retired, std::memory_order_relaxed);
        }
        lane.retired_generation_count.store(lane.retired_generations.size(), std::memory_order_relaxed);
    };
    const auto process_reclamation = [&]() noexcept {
        if (lane.reclaim_requested.exchange(false, std::memory_order_acq_rel)) {
            reclaim_quiescent();
        }
    };
    const auto process_refresh = [&]() noexcept {
        if (!lane.refresh_requested.exchange(false, std::memory_order_acq_rel)) {
            return;
        }
        reclaim_quiescent();
        if (lane.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
            lane.read_refresh_deferrals.fetch_add(1U, std::memory_order_relaxed);
            return;
        }
        lane.read_refresh_attempts.fetch_add(1U, std::memory_order_relaxed);
        auto snapshot = detail::StoreAccess::snapshot_durable_reads(store_, shard);
        if (!snapshot) {
            lane.read_refresh_failures.fetch_add(1U, std::memory_order_relaxed);
            if (snapshot.error().code != ErrorCode::resource_exhausted &&
                !(lane.stopping.load(std::memory_order_acquire) &&
                  snapshot.error().code == ErrorCode::unavailable)) {
                publish_fail_closed();
            }
            return;
        }
        if (snapshot->catalog_revision == lane.published_catalog_revision.load(std::memory_order_acquire)) {
            return;
        }
        auto next = PairReadGeneration::replace_durable_snapshot(lane.writer_generation, snapshot->records);
        if (!next) {
            lane.read_refresh_failures.fetch_add(1U, std::memory_order_relaxed);
            if (next.error().code != ErrorCode::resource_exhausted) {
                publish_fail_closed();
            }
            return;
        }
        lane.retired_generations.push_back(lane.writer_generation);
        lane.retired_generation_count.store(lane.retired_generations.size(), std::memory_order_relaxed);
        lane.writer_generation = std::move(*next);
        update_delta_stats();
        lane.writer_epoch.store(lane.writer_generation->epoch(), std::memory_order_relaxed);
        lane.published_generation.store(lane.writer_generation.get(), std::memory_order_release);
        lane.published_catalog_revision.store(snapshot->catalog_revision, std::memory_order_release);
        lane.read_merge.reset();
        lane.read_merge_active.store(false, std::memory_order_relaxed);
        lane.read_merge_post_entries.store(0U, std::memory_order_relaxed);
        merge_retry_blocked = false;
        lane.read_refresh_successes.fetch_add(1U, std::memory_order_relaxed);
    };
    const auto process_merge = [&]() noexcept {
        if (!lane.read_merge && !merge_retry_blocked &&
            (lane.writer_generation->delta_entries() >= config_.merge_delta_entries ||
             lane.writer_generation->delta_record_versions() >= config_.merge_delta_entries)) {
            auto started = PairReadGeneration::start_incremental_merge(
                lane.writer_generation, config_.merge_maximum_post_entries);
            if (!started) {
                lane.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
                if (started.error().code == ErrorCode::resource_exhausted) {
                    merge_retry_blocked = true;
                } else {
                    publish_fail_closed();
                }
                return;
            }
            lane.read_merge = std::move(*started);
            lane.read_merge_active.store(true, std::memory_order_relaxed);
            lane.read_merge_post_entries.store(0U, std::memory_order_relaxed);
            lane.read_merge_starts.fetch_add(1U, std::memory_order_relaxed);
        }
        if (!lane.read_merge) {
            return;
        }
        if (!PairReadGeneration::merge_ready(*lane.read_merge)) {
            auto advanced = PairReadGeneration::advance_incremental_merge(*lane.read_merge,
                                                                          config_.merge_quantum_slots);
            if (!advanced) {
                lane.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
                if (advanced.error().code == ErrorCode::resource_exhausted) {
                    lane.read_merge.reset();
                    lane.read_merge_active.store(false, std::memory_order_relaxed);
                    lane.read_merge_post_entries.store(0U, std::memory_order_relaxed);
                    merge_retry_blocked = true;
                } else {
                    publish_fail_closed();
                }
                return;
            }
            lane.read_merge_slots_processed.fetch_add(*advanced, std::memory_order_relaxed);
        }
        if (!PairReadGeneration::merge_ready(*lane.read_merge)) {
            return;
        }
        reclaim_quiescent();
        if (lane.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations) {
            return;
        }
        auto next = PairReadGeneration::finish_incremental_merge(lane.writer_generation, *lane.read_merge);
        if (!next) {
            lane.read_merge_failures.fetch_add(1U, std::memory_order_relaxed);
            lane.read_merge.reset();
            lane.read_merge_active.store(false, std::memory_order_relaxed);
            lane.read_merge_post_entries.store(0U, std::memory_order_relaxed);
            if (next.error().code == ErrorCode::resource_exhausted) {
                merge_retry_blocked = true;
            } else {
                publish_fail_closed();
            }
            return;
        }
        lane.retired_generations.push_back(lane.writer_generation);
        lane.retired_generation_count.store(lane.retired_generations.size(), std::memory_order_relaxed);
        lane.writer_generation = std::move(*next);
        update_delta_stats();
        lane.writer_epoch.store(lane.writer_generation->epoch(), std::memory_order_relaxed);
        lane.published_generation.store(lane.writer_generation.get(), std::memory_order_release);
        lane.read_merge.reset();
        lane.read_merge_active.store(false, std::memory_order_relaxed);
        lane.read_merge_post_entries.store(0U, std::memory_order_relaxed);
        lane.read_merge_completions.fetch_add(1U, std::memory_order_relaxed);
        merge_retry_blocked = false;
        reclaim_quiescent();
    };

    for (;;) {
        process_reclamation();
        if (!lane.stopping.load(std::memory_order_acquire)) {
            process_refresh();
            process_merge();
        }

        // Drain synchronous embedded mutations before asynchronous lane work.
        for (;;) {
            SyncMutation* sync_batch = nullptr;
            {
                const std::lock_guard lock{lane.sync_mutex};
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
            while (rev != nullptr) {
                auto* node = rev;
                rev = rev->next;
                Status status{};
                try {
                    const auto& key = *node->key;
                    if (detail::StoreAccess::is_durable(store_)) {
                        DurableMutationResult result;
                        for (unsigned attempt = 0; attempt < 2; ++attempt) {
                            result = node->kind == MutationKind::put
                                         ? detail::StoreAccess::put_durable(store_, shard, key, node->value,
                                                                            node->expire_at_ns)
                                         : detail::StoreAccess::erase_durable(store_, shard, key);
                            if (!detail::StoreAccess::should_retry_durable_mutation(result, attempt)) {
                                break;
                            }
                        }
                        if (!result.committed() || result.error) {
                            auto error = result.error
                                             ? *result.error
                                             : Error{ErrorCode::io_error, "durable mutation failed"};
                            if (result.committed()) {
                                error.code = ErrorCode::unavailable;
                            }
                            status = Status{unexpected(std::move(error))};
                        } else {
                            ReadMutation publication{.key = key,
                                                     .record = RecordRef{.sequence = *result.sequence},
                                                     .opcode = node->kind == MutationKind::put ? Opcode::put
                                                                                               : Opcode::erase};
                            if (node->kind == MutationKind::put) {
                                auto captured = detail::StoreAccess::capture_durable_read(store_, shard, key);
                                if (!captured) {
                                    publish_fail_closed();
                                    status = Status{fail(ErrorCode::unavailable, "durable read capture failed")};
                                } else {
                                    publication.record = captured->reference();
                                    publication.durable.emplace(std::move(*captured));
                                    auto next = PairReadGeneration::publish_incremental(
                                        lane.writer_generation, std::span{&publication, 1}, lane.read_merge.get());
                                    if (!next) {
                                        publish_fail_closed();
                                        status = Status{fail(ErrorCode::unavailable, "read publication failed")};
                                    } else {
                                        lane.retired_generations.push_back(lane.writer_generation);
                                        lane.retired_generation_count.store(lane.retired_generations.size(),
                                                                            std::memory_order_relaxed);
                                        lane.writer_generation = std::move(*next);
                                        update_delta_stats();
                                        lane.writer_epoch.store(lane.writer_generation->epoch(),
                                                               std::memory_order_relaxed);
                                        lane.published_generation.store(lane.writer_generation.get(),
                                                                        std::memory_order_release);
                                        reclaim_quiescent();
                                    }
                                }
                            } else {
                                auto next = PairReadGeneration::publish_incremental(
                                    lane.writer_generation, std::span{&publication, 1}, lane.read_merge.get());
                                if (!next) {
                                    publish_fail_closed();
                                    status = Status{fail(ErrorCode::unavailable, "read publication failed")};
                                } else {
                                    lane.retired_generations.push_back(lane.writer_generation);
                                    lane.retired_generation_count.store(lane.retired_generations.size(),
                                                                        std::memory_order_relaxed);
                                    lane.writer_generation = std::move(*next);
                                    update_delta_stats();
                                    lane.writer_epoch.store(lane.writer_generation->epoch(),
                                                           std::memory_order_relaxed);
                                    lane.published_generation.store(lane.writer_generation.get(),
                                                                    std::memory_order_release);
                                    reclaim_quiescent();
                                }
                            }
                        }
                    } else {
                        auto published =
                            node->kind == MutationKind::put
                                ? detail::StoreAccess::put_volatile_published(store_, shard, key, node->value,
                                                                             node->expire_at_ns)
                                : detail::StoreAccess::erase_volatile_published(store_, shard, key);
                        if (!published) {
                            status = Status{unexpected(published.error())};
                        } else {
                            ReadMutation publication{.key = key,
                                                     .record = published->record,
                                                     .segment = std::move(published->segment),
                                                     .opcode = published->opcode};
                            auto next = PairReadGeneration::publish_incremental(
                                lane.writer_generation, std::span{&publication, 1}, lane.read_merge.get());
                            if (!next) {
                                publish_fail_closed();
                                status = Status{fail(ErrorCode::unavailable, "read publication failed")};
                            } else {
                                lane.retired_generations.push_back(lane.writer_generation);
                                lane.retired_generation_count.store(lane.retired_generations.size(),
                                                                    std::memory_order_relaxed);
                                lane.writer_generation = std::move(*next);
                                update_delta_stats();
                                lane.writer_epoch.store(lane.writer_generation->epoch(),
                                                       std::memory_order_relaxed);
                                lane.published_generation.store(lane.writer_generation.get(),
                                                                std::memory_order_release);
                                reclaim_quiescent();
                            }
                        }
                    }
                } catch (const std::bad_alloc&) {
                    status = Status{fail(ErrorCode::resource_exhausted, "paired mutation allocation failed")};
                } catch (...) {
                    status = Status{fail(ErrorCode::internal_error, "paired Writer failure")};
                }
                node->status = status;
                node->done.store(true, std::memory_order_release);
                node->done.notify_one();
            }
        }

        auto task = carried_task ? std::exchange(carried_task, std::nullopt) : lane.queue.try_pop();
        if (!task) {
            if (lane.stopping.load(std::memory_order_acquire)) {
                return;
            }
            const auto observed = lane.signal.load(std::memory_order_acquire);
            task = lane.queue.try_pop();
            bool sync_pending = false;
            {
                const std::lock_guard lock{lane.sync_mutex};
                sync_pending = lane.sync_head != nullptr;
            }
            if (!task && !sync_pending && !lane.stopping.load(std::memory_order_acquire) &&
                !lane.refresh_requested.load(std::memory_order_acquire) &&
                !lane.reclaim_requested.load(std::memory_order_acquire) && !lane.read_merge) {
                lane.signal.wait(observed, std::memory_order_acquire);
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
            auto next = lane.queue.try_pop();
            if (!next) {
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
        reclaim_quiescent();
        const bool retire_pressure =
            lane.retired_generations.size() >= ShardPairRuntime::kMaximumRetiredReadGenerations;
        const bool merge_pressure = !PairReadGeneration::can_publish_incremental(
            *lane.writer_generation, lane.read_merge.get(), batch.size());
        const bool generation_pressure = retire_pressure || merge_pressure;
        const bool force_expire = expire_remaining_.load(std::memory_order_acquire);
        for (std::size_t index = 0; index < batch.size(); ++index) {
            auto& queued = batch[index];
            lane.queued_bytes.fetch_sub(queued.admission_bytes, std::memory_order_relaxed);
            const auto queue_wait_ns = elapsed_ns(queued.admitted_at, std::chrono::steady_clock::now());
            queue_waits.push_back(queue_wait_ns);
            atomic_saturating_add(lane.total_queue_wait_ns, queue_wait_ns);
            atomic_max(lane.maximum_queue_wait_ns, queue_wait_ns);
            lane.queue_wait_histogram.observe(queue_wait_ns);
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
                    lane.read_merge_backpressure.fetch_add(1U, std::memory_order_relaxed);
                }
                completions.back().error.emplace(
                    ErrorCode::unavailable,
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
            try {
                auto results = detail::StoreAccess::mutate_durable_batch(store_, shard, durable_views);
                if (results.size() != durable_views.size()) {
                    std::terminate();
                }
                for (std::size_t result_index = 0; result_index < results.size(); ++result_index) {
                    auto& result = results[result_index];
                    auto& completion = completions[durable_indices[result_index]];
                    if (result.conflict_retried) {
                        lane.conflict_retries.fetch_add(1U, std::memory_order_relaxed);
                        if (result.mutation.committed() && !result.mutation.error) {
                            lane.conflict_retry_commits.fetch_add(1U, std::memory_order_relaxed);
                        }
                    }
                    if (!result.mutation.committed() || result.mutation.error) {
                        auto error =
                            result.mutation.error
                                ? std::move(*result.mutation.error)
                                : Error{ErrorCode::io_error, "durable mutation failed without an error"};
                        if (result.mutation.committed()) {
                            error.code = ErrorCode::unavailable;
                        }
                        completion.error.emplace(std::move(error));
                    } else {
                        stage_durable_publication(durable_indices[result_index], result.mutation);
                    }
                }
            } catch (const std::bad_alloc&) {
                for (const auto index : durable_indices) {
                    completions[index].error.emplace(ErrorCode::resource_exhausted,
                                                     "paired mutation allocation failed");
                }
            } catch (...) {
                for (const auto index : durable_indices) {
                    completions[index].error.emplace(ErrorCode::internal_error, "paired Writer failure");
                }
            }
        } else {
            for (std::size_t index = 0; index < batch.size(); ++index) {
                if (expired[index]) {
                    continue;
                }
                auto& queued = batch[index];
                auto& completion = completions[index];
                try {
                    const auto payload = payload_for(queued);
                    const HashedKey key{.key = payload.key, .hash = queued.key_hash};
                    if (detail::StoreAccess::is_durable(store_)) {
                        DurableMutationResult result;
                        bool conflict_retried{};
                        for (unsigned attempt = 0; attempt < 2; ++attempt) {
                            result = queued.kind == MutationKind::put
                                         ? detail::StoreAccess::put_durable(
                                               store_, shard, key, payload.value, queued.expire_at_ns)
                                         : detail::StoreAccess::erase_durable(store_, shard, key);
                            if (!detail::StoreAccess::should_retry_durable_mutation(result, attempt)) {
                                break;
                            }
                            conflict_retried = true;
                        }
                        if (conflict_retried) {
                            lane.conflict_retries.fetch_add(1U, std::memory_order_relaxed);
                            if (result.committed() && !result.error) {
                                lane.conflict_retry_commits.fetch_add(1U, std::memory_order_relaxed);
                            }
                        }
                        if (!result.committed() || result.error) {
                            auto error = result.error ? std::move(*result.error)
                                                      : Error{ErrorCode::io_error,
                                                              "durable mutation failed without an error"};
                            if (result.committed()) {
                                error.code = ErrorCode::unavailable;
                            }
                            completion.error.emplace(std::move(error));
                        } else {
                            stage_durable_publication(index, result);
                        }
                    } else {
                        auto published =
                            queued.kind == MutationKind::put
                                ? detail::StoreAccess::put_volatile_published(
                                      store_, shard, key, payload.value, queued.expire_at_ns)
                                : detail::StoreAccess::erase_volatile_published(store_, shard, key);
                        if (!published) {
                            completion.error.emplace(std::move(published.error()));
                        } else {
                            read_mutations.push_back({.key = key,
                                                      .record = published->record,
                                                      .segment = std::move(published->segment),
                                                      .opcode = published->opcode});
                            read_mutation_indices.push_back(index);
                        }
                    }
                } catch (const std::bad_alloc&) {
                    completion.error.emplace(ErrorCode::resource_exhausted,
                                             "paired mutation allocation failed");
                } catch (...) {
                    completion.error.emplace(ErrorCode::internal_error, "paired Writer failure");
                }
            }
        }

        if (post_commit_publication_failure) {
            publish_fail_closed();
            for (const auto index : read_mutation_indices) {
                if (!completions[index].error) {
                    completions[index].error.emplace(
                        ErrorCode::unavailable, "read publication batch aborted after a committed mutation");
                }
            }
        }

        // Linearization order for paired mutations:
        // Store append/index publication -> immutable generation publication
        // (release) -> completion queue -> client ACK. A publication failure is
        // sticky and fail-closed because the mutable Store has already changed.
        if (!post_commit_publication_failure && !read_mutations.empty()) {
            auto next = PairReadGeneration::publish_incremental(lane.writer_generation, read_mutations,
                                                                lane.read_merge.get());
            if (!next) {
                publish_fail_closed();
                for (const auto index : read_mutation_indices) {
                    completions[index].error.emplace(ErrorCode::unavailable,
                                                     "read publication failed after mutation linearization; "
                                                     "paired runtime is fail-closed");
                }
            } else {
                lane.retired_generations.push_back(lane.writer_generation);
                lane.retired_generation_count.store(lane.retired_generations.size(),
                                                    std::memory_order_relaxed);
                lane.writer_generation = std::move(*next);
                update_delta_stats();
                lane.writer_epoch.store(lane.writer_generation->epoch(), std::memory_order_relaxed);
                lane.published_generation.store(lane.writer_generation.get(), std::memory_order_release);
                if (lane.read_merge) {
                    lane.read_merge_post_entries.store(
                        PairReadGeneration::merge_post_entries(*lane.read_merge), std::memory_order_relaxed);
                }
                merge_retry_blocked = false;
                reclaim_quiescent();
            }
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
                lane.service_histogram.observe(service_ns);
            } else {
                lane.expired_before_store.fetch_add(1U, std::memory_order_relaxed);
            }
            lane.completed.fetch_add(1U, std::memory_order_relaxed);
            atomic_saturating_add(lane.total_service_ns, service_ns);
            atomic_max(lane.maximum_service_ns, service_ns);
            if (!deliver_outcome(batch[index].sink, std::move(completions[index]))) {
                std::terminate();
            }
            notify_sink(batch[index].sink);
        }
    }
}

void ShardPairRuntime::wake(Lane& lane) noexcept {
    lane.signal.fetch_add(1U, std::memory_order_release);
    lane.signal.notify_one();
}

auto ShardPairRuntime::mutate(const std::size_t shard, const MutationKind kind, const HashedKey& key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
    -> Status {
    if (shard >= lanes_.size()) {
        return fail(ErrorCode::invalid_argument, "paired mutation shard is out of range");
    }
    if (!begin_submission()) {
        return fail(ErrorCode::unavailable, "paired runtime is not accepting mutations");
    }
    struct AdmissionGuard final {
        ShardPairRuntime& runtime;
        ~AdmissionGuard() { runtime.finish_submission(); }
    } admission{*this};

    SyncMutation node{.kind = kind, .key = &key, .value = value, .expire_at_ns = expire_at_ns};
    auto& lane = *lanes_[shard];
    {
        const std::lock_guard lock{lane.sync_mutex};
        node.next = lane.sync_head;
        lane.sync_head = &node;
    }
    lane.sync_admitted.fetch_add(1U, std::memory_order_relaxed);
    wake(lane);
    node.done.wait(false, std::memory_order_acquire);
    return node.status;
}

ShardPairRuntime::ReadLease::ReadLease(const ShardPairRuntime& runtime, const std::size_t shard) noexcept {
    if (shard >= runtime.lanes_.size()) {
        return;
    }
    runtime_ = &runtime;
    shard_ = shard;
    auto& lane = *runtime.lanes_[shard];
    generation_ = lane.published_generation.load(std::memory_order_acquire);
    if (generation_ != nullptr) {
        lane.active_read_leases.fetch_add(1U, std::memory_order_acq_rel);
    } else {
        runtime_ = nullptr;
    }
}

ShardPairRuntime::ReadLease::~ReadLease() {
    if (runtime_ == nullptr || generation_ == nullptr) {
        return;
    }
    auto& lane = *runtime_->lanes_[shard_];
    if (lane.active_read_leases.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
        lane.reclaim_requested.store(true, std::memory_order_release);
        lane.signal.fetch_add(1U, std::memory_order_release);
        lane.signal.notify_one();
    }
}


} // namespace glyphastore::store::paired
