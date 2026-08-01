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

auto DurableRuntimeCatalog::fail_closed(Error error) -> Unexpected {
    healthy_.store(false, std::memory_order_release);
    return unexpected(std::move(error));
}

auto DurableRuntimeCatalog::get(const std::string_view key, const std::uint64_t now_ns)
    -> Result<OwnedValue> {
    return get(std::as_bytes(std::span{key}), now_ns);
}

auto DurableRuntimeCatalog::get(const std::span<const std::byte> key, const std::uint64_t now_ns)
    -> Result<OwnedValue> {
    return get(HashedKey{.key = as_string_view(key), .hash = hash_key_routing(key, worker_routing_)}, now_ns);
}

auto DurableRuntimeCatalog::get(const HashedKey& key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    auto prepared = prepare_get(key, now_ns);
    if (!prepared) {
        return unexpected(prepared.error());
    }
    if (prepared->value) {
        return std::move(*prepared->value);
    }
    if (!prepared->cold) {
        return fail(ErrorCode::internal_error, "durable GET preparation produced no result");
    }
    return complete_get(std::move(*prepared->cold));
}

auto DurableRuntimeCatalog::prepare_get(const HashedKey& key, const std::uint64_t now_ns)
    -> Result<PreparedRead> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const auto worker_index = route_worker(key.hash, workers_.size());
    auto& worker = *workers_[worker_index];
    auto& metrics = worker.get_path_metrics;
    RecordRef cold_reference;
    std::shared_ptr<const RuntimeSegmentGeneration> cold_pin;
    std::optional<HotRecordSnapshot> hot;
    std::uint64_t mutex_wait_ns = 0;
    std::uint64_t prepare_hold_ns = 0;
    std::uint64_t index_lookup_ns = 0;
    std::uint64_t hot_cache_lookup_ns = 0;
    std::uint64_t generation_pin_lookup_ns = 0;
    std::uint64_t local_hits = 0;
    std::uint64_t local_misses = 0;
    std::uint64_t local_stale = 0;
    ScopeExit publish_prepare_metrics{[&]() noexcept {
        metrics.prepare_calls.fetch_add(1U, std::memory_order_relaxed);
        if (local_hits != 0) {
            metrics.hot_hits.fetch_add(local_hits, std::memory_order_relaxed);
        }
        if (local_misses != 0) {
            metrics.hot_misses.fetch_add(local_misses, std::memory_order_relaxed);
        }
        if (local_stale != 0) {
            metrics.hot_stale_hits.fetch_add(local_stale, std::memory_order_relaxed);
        }
        if constexpr (kGetPathTimingEnabled) {
            atomic_saturating_add(metrics.mutex_wait_ns, mutex_wait_ns);
            atomic_saturating_add(metrics.prepare_hold_ns, prepare_hold_ns);
            atomic_saturating_add(metrics.index_lookup_ns, index_lookup_ns);
            atomic_saturating_add(metrics.hot_cache_lookup_ns, hot_cache_lookup_ns);
            atomic_saturating_add(metrics.generation_pin_lookup_ns, generation_pin_lookup_ns);
        }
    }};
    {
        const auto wait_started = timing_now();
        const std::lock_guard worker_lock{worker.mutex};
        const auto locked_at = timing_now();
        mutex_wait_ns = timing_duration_ns(wait_started, locked_at);
        ScopeExit observe_hold{[&]() noexcept { prepare_hold_ns = timing_elapsed_ns(locked_at); }};
        // Hot path only needs the Worker mutex + atomic health. Catalog shared lock
        // is taken below solely for cold-miss generation pin / manifest identity.
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }

        // Drain deferred TTL only when the backlog is non-empty, and before Index
        // lookup so a reclaimed key becomes an Index miss (not a second cold read).
        // Empty-backlog hot GETs skip this entirely.
        if (!worker.deferred_ttl_reclaims.empty()) {
            if (auto drained =
                    worker.drain_deferred_ttl(std::min<std::size_t>(8, worker.deferred_ttl_reclaims.size()));
                !drained) {
                return fail_closed(drained.error());
            }
        }

        // Ordinary hot path under mutex: Index lookup, hot match + snapshot, unlock.
        // No I/O/CRC/value copy under the lock.
        const auto index_started = timing_now();
        const auto reference = worker.index.find(key);
        index_lookup_ns = timing_elapsed_ns(index_started);
        if (!reference) {
            return fail(ErrorCode::not_found, "key is not present");
        }

        const auto hot_started = timing_now();
        if (const auto* cached = worker.hot_records.find(key); cached != nullptr) {
            if (hot_record_matches(*cached, *reference)) {
                ++local_hits;
                if (cached->expire_at_ns != 0 && now_ns != 0 && cached->expire_at_ns <= now_ns) {
                    if (auto reclaimed = worker.defer_or_reclaim_expired(
                            key, *reference, options_.limits.max_deferred_ttl_reclaims_per_worker);
                        !reclaimed) {
                        hot_cache_lookup_ns = timing_elapsed_ns(hot_started);
                        return fail_closed(reclaimed.error());
                    }
                    hot_cache_lookup_ns = timing_elapsed_ns(hot_started);
                    return fail(ErrorCode::not_found, "key has expired");
                }
                hot.emplace(HotRecordSnapshot::from_entry(*cached));
            } else {
                ++local_stale;
                worker.erase_hot_record(key);
            }
        }
        hot_cache_lookup_ns = timing_elapsed_ns(hot_started);

        if (!hot) {
            ++local_misses;

            const auto pin_started = timing_now();
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }
            const auto catalog_index = catalog_index_for_segment(reference->segment_id);
            if (!catalog_index) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Index references a Segment absent from the catalog"});
            }
            const auto& found = manifest_.segments[*catalog_index];
            if (found.generation != reference->generation || found.owner_worker != worker.worker_id) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(
                    Error{ErrorCode::corrupted_data,
                          "durable Index reference disagrees with catalog identity or ownership"});
            }
            const auto& pin = generation_pins_[*catalog_index];
            if (!pin || pin->identity.segment_id != reference->segment_id ||
                pin->identity.generation != reference->generation ||
                pin->identity.owner_worker != worker.worker_id) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Segment generation pin disagrees with the Index"});
            }
            generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
            cold_reference = *reference;
            cold_pin = pin;
        }
    }
    if (hot) {
        return PreparedRead{.value = owned_value_from_hot(*hot)};
    }
    return PreparedRead{.cold = PinnedRead{std::string{key.key}, key.hash, now_ns, worker_index,
                                           cold_reference, std::move(cold_pin)}};
}

auto DurableRuntimeCatalog::snapshot_published_reads(const std::size_t worker_index)
    -> Result<PublishedReadSnapshot> try {
    if (worker_index >= workers_.size()) {
        return fail(ErrorCode::invalid_argument,
                    "durable read-generation snapshot targets an invalid Worker");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    auto& worker = *workers_[worker_index];
    const std::lock_guard worker_lock{worker.mutex};
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    auto entries = worker.index.entries();
    PublishedReadSnapshot result;
    result.records.reserve(entries.size());
    for (auto& entry : entries) {
        const auto catalog_index = catalog_index_for_segment(entry.record.segment_id);
        if (!catalog_index) {
            return fail_closed(Error{ErrorCode::corrupted_data,
                                     "durable read-generation snapshot references an absent Segment"});
        }
        const auto& manifest_entry = manifest_.segments[*catalog_index];
        const auto& pin = generation_pins_[*catalog_index];
        if (!pin || manifest_entry.owner_worker != worker.worker_id ||
            manifest_entry.generation != entry.record.generation ||
            pin->identity.segment_id != entry.record.segment_id ||
            pin->identity.generation != entry.record.generation ||
            pin->identity.owner_worker != worker.worker_id) {
            return fail_closed(Error{ErrorCode::corrupted_data,
                                     "durable read-generation snapshot has no exact generation pin"});
        }
        const auto key_hash = hash_key_routing(entry.key, worker_routing_);
        if (route_worker(key_hash, workers_.size()) != worker_index) {
            return fail_closed(
                Error{ErrorCode::corrupted_data, "durable read-generation snapshot contains a foreign key"});
        }
        result.records.push_back(PublishedReadRecord::bind(std::move(entry.key), key_hash, entry.record,
                                                           PublishedReadPin{worker_index, pin}));
    }
    result.catalog_revision = worker.read_catalog_revision.load(std::memory_order_relaxed);
    return result;
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "durable read-generation snapshot allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "durable read-generation snapshot failed");
}

auto DurableRuntimeCatalog::read_catalog_revision(const std::size_t worker_index) const noexcept
    -> std::uint64_t {
    return worker_index < workers_.size()
               ? workers_[worker_index]->read_catalog_revision.load(std::memory_order_acquire)
               : 0U;
}

auto DurableRuntimeCatalog::advance_read_catalog_revision(RuntimeWorker& worker) noexcept -> bool {
    auto revision = worker.read_catalog_revision.load(std::memory_order_relaxed);
    while (revision != std::numeric_limits<std::uint64_t>::max()) {
        if (worker.read_catalog_revision.compare_exchange_weak(
                revision, revision + 1U, std::memory_order_release, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

auto DurableRuntimeCatalog::capture_published_read(const std::size_t worker_index, const HashedKey& key)
    -> Result<PublishedReadRecord> try {
    if (worker_index >= workers_.size() || route_worker(key.hash, workers_.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "durable read publication targets the wrong Worker owner");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    auto& worker = *workers_[worker_index];
    std::unique_lock worker_lock{worker.mutex, std::defer_lock};
    if (!options_.exclusive_writer || flusher_ != nullptr) {
        worker_lock.lock();
    } else {
#ifndef NDEBUG
        if (!worker_lock.try_lock()) {
            worker_lock.lock();
        }
        worker_lock.unlock();
#endif
    }
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const auto reference = worker.index.find(key);
    if (!reference) {
        return fail(ErrorCode::not_found, "durable read publication key is not present");
    }
    const auto catalog_index = catalog_index_for_segment(reference->segment_id);
    if (!catalog_index) {
        return fail_closed(
            Error{ErrorCode::corrupted_data, "durable read publication references an absent Segment"});
    }
    const auto& manifest_entry = manifest_.segments[*catalog_index];
    const auto& pin = generation_pins_[*catalog_index];
    if (!pin || manifest_entry.owner_worker != worker.worker_id ||
        manifest_entry.generation != reference->generation ||
        pin->identity.segment_id != reference->segment_id ||
        pin->identity.generation != reference->generation || pin->identity.owner_worker != worker.worker_id) {
        return fail_closed(
            Error{ErrorCode::corrupted_data, "durable read publication has no exact generation pin"});
    }
    return PublishedReadRecord::bind(std::string{key.key}, key.hash, *reference,
                                     PublishedReadPin{worker_index, pin});
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "durable read publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "durable read publication capture failed");
}

auto DurableRuntimeCatalog::prepare_published_get(PublishedReadRecord read, const std::uint64_t now_ns)
    -> Result<PreparedRead> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const auto worker_index = read.pin_.worker_index_;
    if (worker_index >= workers_.size() || route_worker(read.key_hash_, workers_.size()) != worker_index ||
        !read.pin_.matches(read.reference_) ||
        read.pin_.generation_->identity.owner_worker != workers_[worker_index]->worker_id) {
        return fail_closed(Error{ErrorCode::corrupted_data,
                                 "immutable durable read publication is internally inconsistent"});
    }
    return PreparedRead{.cold = PinnedRead{std::move(read.key_), read.key_hash_, now_ns, worker_index,
                                           read.reference_, std::move(read.pin_.generation_), true}};
}

auto DurableRuntimeCatalog::prepare_published_get(const PublishedReadView read, const std::uint64_t now_ns)
    -> Result<PreparedRead> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    if (read.pin_ == nullptr || !read.pin_->generation_) {
        return fail_closed(Error{ErrorCode::corrupted_data, "borrowed durable read has no generation pin"});
    }
    const auto worker_index = read.pin_->worker_index_;
    if (worker_index >= workers_.size() || route_worker(read.key_hash_, workers_.size()) != worker_index ||
        !read.pin_->matches(read.reference_) ||
        read.pin_->generation_->identity.owner_worker != workers_[worker_index]->worker_id) {
        return fail_closed(
            Error{ErrorCode::corrupted_data, "borrowed durable read publication is inconsistent"});
    }
    return PreparedRead{.borrowed_cold = BorrowedPinnedRead{read.key_, read.key_hash_, now_ns, worker_index,
                                                            read.reference_, read.pin_->generation_.get()}};
}

auto DurableRuntimeCatalog::complete_generation_get(
    const std::string_view key, const std::uint64_t key_hash, const std::uint64_t now_ns,
    const std::size_t worker_index, const RecordRef& reference, const RuntimeSegmentGeneration& generation,
    const detail::ColdReadCancellation* cancellation, std::vector<std::byte>* scratch) -> Result<OwnedValue> {
    if (cancellation != nullptr && cancellation->cancelled()) {
        return fail(ErrorCode::unavailable, "durable cold read was cancelled");
    }
    auto& metrics = workers_[worker_index]->get_path_metrics;
    metrics.complete_calls.fetch_add(1U, std::memory_order_relaxed);
    std::vector<std::byte> local_scratch;
    auto& read_scratch = scratch == nullptr ? local_scratch : *scratch;
    const auto key_bytes =
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.data()), key.size()};
    ReadContext context{.expected_key = key_bytes, .expected_hash = key_hash, .now_ns = now_ns};
    const auto cold_started = timing_now();
    auto visited =
        generation.file.visit_runtime_record(reference, read_scratch, &context, &copy_verified_value);
    if constexpr (kGetPathTimingEnabled) {
        atomic_saturating_add(metrics.cold_read_ns, timing_elapsed_ns(cold_started));
        atomic_saturating_add(metrics.crc_value_copy_ns, context.crc_value_copy_ns);
    }
    if (!visited) {
        if (visited.error().code == ErrorCode::not_found) {
            return unexpected(visited.error());
        }
        return fail_closed(visited.error());
    }
    return std::move(context.value);
}

auto DurableRuntimeCatalog::complete_get(BorrowedPinnedRead read,
                                         const detail::ColdReadCancellation* cancellation,
                                         std::vector<std::byte>* scratch) -> Result<OwnedValue> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    if (read.worker_index_ >= workers_.size() || read.generation_ == nullptr ||
        route_worker(read.key_hash_, workers_.size()) != read.worker_index_ ||
        read.generation_->identity.owner_worker != workers_[read.worker_index_]->worker_id ||
        read.generation_->identity.segment_id != read.reference_.segment_id ||
        read.generation_->identity.generation != read.reference_.generation) {
        return fail_closed(
            Error{ErrorCode::corrupted_data, "borrowed cold read lost its generation identity"});
    }
    return complete_generation_get(read.key_, read.key_hash_, read.now_ns_, read.worker_index_,
                                   read.reference_, *read.generation_, cancellation, scratch);
}

auto DurableRuntimeCatalog::complete_get(PinnedRead read, const detail::ColdReadCancellation* cancellation,
                                         std::vector<std::byte>* scratch) -> Result<OwnedValue> {
    constexpr std::size_t kMaximumRelinearizationAttempts = 8;
    if (read.generation_linearized_) {
        return complete_generation_get(read.key_, read.key_hash_, read.now_ns_, read.worker_index_,
                                       read.reference_, *read.generation_, cancellation, scratch);
    }
    std::vector<std::byte> local_scratch;
    auto& read_scratch = scratch == nullptr ? local_scratch : *scratch;
    for (std::size_t attempt = 0; attempt < kMaximumRelinearizationAttempts; ++attempt) {
        if (cancellation != nullptr && cancellation->cancelled()) {
            return fail(ErrorCode::unavailable, "durable cold read was cancelled");
        }
        auto& worker = *workers_[read.worker_index_];
        auto& metrics = worker.get_path_metrics;
        metrics.complete_calls.fetch_add(1U, std::memory_order_relaxed);
        const auto key_bytes = std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(read.key_.data()), read.key_.size()};
        ReadContext context{
            .expected_key = key_bytes, .expected_hash = read.key_hash_, .now_ns = read.now_ns_};
        const auto cold_started = timing_now();
        auto visited = read.generation_->file.visit_runtime_record(read.reference_, read_scratch, &context,
                                                                   &copy_verified_value);
        if constexpr (kGetPathTimingEnabled) {
            atomic_saturating_add(metrics.cold_read_ns, timing_elapsed_ns(cold_started));
            atomic_saturating_add(metrics.crc_value_copy_ns, context.crc_value_copy_ns);
        }

        // Linearization point for a cold GET: the Worker index must still
        // designate both the exact RecordRef and immutable generation pin
        // captured by prepare_get(). No file I/O or CRC work holds either lock.
        bool still_current{};
        {
            const auto wait_started = timing_now();
            const std::lock_guard worker_lock{worker.mutex};
            const auto locked_at = timing_now();
            if constexpr (kGetPathTimingEnabled) {
                atomic_saturating_add(metrics.mutex_wait_ns, timing_duration_ns(wait_started, locked_at));
            }
            ScopeExit observe_hold{[&]() noexcept {
                if constexpr (kGetPathTimingEnabled) {
                    atomic_saturating_add(metrics.complete_revalidate_hold_ns, timing_elapsed_ns(locked_at));
                }
            }};
            if (!healthy()) {
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }
            const HashedKey key{.key = read.key_, .hash = read.key_hash_};
            const auto index_started = timing_now();
            const auto current = worker.index.find(key);
            if constexpr (kGetPathTimingEnabled) {
                atomic_saturating_add(metrics.index_lookup_ns, timing_elapsed_ns(index_started));
            }
            // Catalog lock only when the Index still names the captured RecordRef;
            // churned keys skip pin identity work and catalog shared-lock contention.
            if (current && *current == read.reference_) {
                const auto pin_started = timing_now();
                const std::shared_lock catalog_lock{catalog_mutex_};
                if (!healthy()) {
                    return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
                }
                const auto catalog_index = catalog_index_for_segment(current->segment_id);
                still_current =
                    catalog_index.has_value() && generation_pins_[*catalog_index] == read.generation_;
                if constexpr (kGetPathTimingEnabled) {
                    atomic_saturating_add(metrics.generation_pin_lookup_ns, timing_elapsed_ns(pin_started));
                }
            }
        }
        if (!still_current) {
            if (attempt + 1U == kMaximumRelinearizationAttempts) {
                return fail(ErrorCode::sequence_conflict,
                            "durable cold read exceeded its relinearization retry budget");
            }
            atomic_saturating_add(metrics.relinearization_retries, 1U);
            const HashedKey key{.key = read.key_, .hash = read.key_hash_};
            auto prepared = prepare_get(key, read.now_ns_);
            if (!prepared) {
                return unexpected(prepared.error());
            }
            if (prepared->value) {
                return std::move(*prepared->value);
            }
            if (!prepared->cold) {
                return fail(ErrorCode::internal_error, "durable GET retry produced no result");
            }
            read = std::move(*prepared->cold);
            continue;
        }
        if (!visited) {
            if (visited.error().code == ErrorCode::not_found) {
                // Visitor not_found is validated expiry only. Defer Index reclaim
                // while verifying the exact RecordRef when the backlog drains.
                {
                    const auto wait_started = timing_now();
                    const std::lock_guard worker_lock{worker.mutex};
                    const auto locked_at = timing_now();
                    if constexpr (kGetPathTimingEnabled) {
                        atomic_saturating_add(metrics.mutex_wait_ns,
                                              timing_duration_ns(wait_started, locked_at));
                    }
                    ScopeExit observe_hold{[&]() noexcept {
                        if constexpr (kGetPathTimingEnabled) {
                            atomic_saturating_add(metrics.complete_revalidate_hold_ns,
                                                  timing_elapsed_ns(locked_at));
                        }
                    }};
                    // Index + deferred TTL reclaim are Worker-local; no catalog pin work.
                    if (healthy()) {
                        const HashedKey key{.key = read.key_, .hash = read.key_hash_};
                        const auto current = worker.index.find(key);
                        if (current && *current == read.reference_) {
                            if (auto reclaimed = worker.defer_or_reclaim_expired(
                                    key, read.reference_,
                                    options_.limits.max_deferred_ttl_reclaims_per_worker);
                                !reclaimed) {
                                return fail_closed(reclaimed.error());
                            }
                        }
                    }
                }
                return unexpected(visited.error());
            }
            return fail_closed(visited.error());
        }
        return std::move(context.value);
    }
    return fail(ErrorCode::internal_error, "durable cold read retry loop escaped its bound");
}

auto DurableRuntimeCatalog::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                                const std::uint64_t expire_at_ns, const ValueType type,
                                const std::uint32_t flags) -> DurableMutationResult {
    return mutate(key, value, Opcode::put, hash_key_routing(key, worker_routing_), expire_at_ns, type, flags);
}

auto DurableRuntimeCatalog::put(const HashedKey& key, const std::span<const std::byte> value,
                                const std::uint64_t expire_at_ns, const ValueType type,
                                const std::uint32_t flags) -> DurableMutationResult {
    return mutate(
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()}, value,
        Opcode::put, key.hash, expire_at_ns, type, flags);
}

auto DurableRuntimeCatalog::erase(const std::span<const std::byte> key) -> DurableMutationResult {
    return mutate(key, {}, Opcode::erase, hash_key_routing(key, worker_routing_), 0, ValueType::bytes, 0);
}

auto DurableRuntimeCatalog::erase(const HashedKey& key) -> DurableMutationResult {
    return mutate(
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()}, {},
        Opcode::erase, key.hash, 0, ValueType::bytes, 0);
}

auto DurableRuntimeCatalog::rotate_active(RuntimeWorker& worker, std::unique_lock<std::mutex>& worker_lock)
    -> DurableMutationResult {
    // Linearization protocol (runtime contract):
    //  1. caller owns worker.mutex (legacy / flusher paths) or is the paired
    //     exclusive Writer with the mutex elided; publication + catalog locks
    //     capture one manifest generation, its immutable generation pin, and
    //     the writable Segment handle;
    //  2. mutation_io_active serializes writers while worker/catalog mutexes are
    //     released. GET remains lock-independent through the captured pin;
    //  3. after durable manifest publication, worker then catalog mutex are
    //     reacquired (when held) and the prepared state is installed without
    //     allocation.
    // No RecordRef, file, or Segment generation crosses phase 1 without the
    // exact shared generation pin captured below.
    GS_FAULT_SITE(rotate);
    if (!worker_lock.owns_lock() && !options_.exclusive_writer) {
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::internal_error, "rotation requires the owning Worker mutex"});
    }
    const auto rotation_started = std::chrono::steady_clock::now();
    rotation_attempts_.fetch_add(1U, std::memory_order_relaxed);
    const auto observed_active_segment = worker.active_segment;
    const auto observed_next_sequence = worker.next_sequence;
    bool counted_compaction_wait{};
    for (;;) {
        std::unique_lock publication_lock{manifest_publication_mutex_};
        if (!compaction_publication_active_ && !rotation_publication_active_) {
            rotation_publication_active_ = true;
            publication_lock.unlock();
            break;
        }
        if (compaction_publication_active_ && !counted_compaction_wait) {
            rotation_compaction_waits_.fetch_add(1U, std::memory_order_relaxed);
            counted_compaction_wait = true;
        }
        // A manifest publisher may perform durable I/O. Do not turn its
        // logical lease into equivalent Worker head-of-line blocking.
        publication_lock.unlock();
        if (worker_lock.owns_lock()) {
            worker_lock.unlock();
        }
        // Exclusive Writer: drop hot_path_depth while blocked on another
        // publisher (compaction). Compaction waits for depth==0 before the
        // Index swap; keeping depth here deadlocks against that wait.
        struct ExclusiveHotPathPause final {
            RuntimeWorker* worker{};
            bool paused{};

            explicit ExclusiveHotPathPause(RuntimeWorker* owner, const bool enable) noexcept
                : worker(enable ? owner : nullptr) {
                if (worker == nullptr) {
                    return;
                }
                if (worker->hot_path_depth.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
                    worker->hot_path_depth.notify_all();
                }
                paused = true;
            }
            ~ExclusiveHotPathPause() {
                if (paused && worker != nullptr) {
                    worker->hot_path_depth.fetch_add(1U, std::memory_order_acq_rel);
                }
            }
            ExclusiveHotPathPause(const ExclusiveHotPathPause&) = delete;
            auto operator=(const ExclusiveHotPathPause&) -> ExclusiveHotPathPause& = delete;
        } hot_path_pause{&worker, options_.exclusive_writer};
        {
            std::unique_lock wait_lock{manifest_publication_mutex_};
            manifest_publication_changed_.wait(wait_lock, [&] {
                return (!compaction_publication_active_ && !rotation_publication_active_) || !healthy();
            });
        }
        if (!options_.exclusive_writer) {
            worker_lock.lock();
        }
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        if (worker.next_sequence != observed_next_sequence) {
            return mutation_failure(DurableMutationOutcome::not_committed,
                                    Error{ErrorCode::sequence_conflict,
                                          "Worker advanced while mutation waited for manifest publication"});
        }
        if (worker.active_segment != observed_active_segment) {
            // Another same-Worker writer completed the required rotation while
            // this caller slept. The caller can retry its final append against
            // the newly linearized active generation.
            return {.outcome = DurableMutationOutcome::committed,
                    .sequence = std::nullopt,
                    .error = std::nullopt};
        }
    }
    ScopeExit release_publication_reservation{[this]() noexcept {
        {
            const std::lock_guard lock{manifest_publication_mutex_};
            rotation_publication_active_ = false;
        }
        manifest_publication_changed_.notify_all();
    }};
    const auto execution_started = std::chrono::steady_clock::now();
    bool rotation_committed{};
    std::uint64_t seal_ns{};
    std::uint64_t create_ns{};
    std::uint64_t manifest_publication_ns{};
    ScopeExit telemetry{[&, this]() noexcept {
        const auto finished = std::chrono::steady_clock::now();
        const auto publication_wait_ns = steady_duration_ns(rotation_started, execution_started);
        const auto execution_ns = steady_duration_ns(execution_started, finished);
        const auto total_ns = steady_duration_ns(rotation_started, finished);
        const auto previous_version = begin_atomic_stats_publication(rotation_stats_version_);
        if (rotation_committed) {
            rotations_committed_.fetch_add(1U, std::memory_order_relaxed);
        }
        last_rotation_publication_wait_ns_.store(publication_wait_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_publication_wait_ns_, publication_wait_ns);
        atomic_observe_max(maximum_rotation_publication_wait_ns_, publication_wait_ns);
        last_rotation_seal_ns_.store(seal_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_seal_ns_, seal_ns);
        atomic_observe_max(maximum_rotation_seal_ns_, seal_ns);
        last_rotation_create_ns_.store(create_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_create_ns_, create_ns);
        atomic_observe_max(maximum_rotation_create_ns_, create_ns);
        last_rotation_manifest_publication_ns_.store(manifest_publication_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_manifest_publication_ns_, manifest_publication_ns);
        atomic_observe_max(maximum_rotation_manifest_publication_ns_, manifest_publication_ns);
        last_rotation_execution_ns_.store(execution_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_execution_ns_, execution_ns);
        atomic_observe_max(maximum_rotation_execution_ns_, execution_ns);
        last_rotation_total_ns_.store(total_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_ns_, total_ns);
        atomic_observe_max(maximum_rotation_total_ns_, total_ns);
        end_atomic_stats_publication(rotation_stats_version_, previous_version);
    }};
    std::unique_lock catalog_lock{catalog_mutex_};
    const auto old_position =
        std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), worker.active_segment,
                         [](const ManifestSegmentEntry& entry, const SegmentId id) {
                             return entry.segment_id.value < id.value;
                         });
    if (old_position == manifest_.segments.end() || old_position->segment_id != worker.active_segment ||
        old_position->owner_worker != worker.worker_id || old_position->role != ManifestSegmentRole::active) {
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::corrupted_data, "runtime active Segment disagrees with the manifest"});
    }
    const auto old_index = static_cast<std::size_t>(old_position - manifest_.segments.begin());
    const auto old_entry = *old_position;
    auto next_manifest = rotation_manifest(manifest_, old_entry, options_.limits);
    if (!next_manifest) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_manifest.error());
    }
    const auto next_manifest_bytes = durable_manifest_bytes(next_manifest->segments.size());
    if (!next_manifest_bytes) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_manifest_bytes.error());
    }
    if (*next_manifest_bytes > std::numeric_limits<std::uint64_t>::max() - kSegmentSizeBytes) {
        return mutation_failure(
            DurableMutationOutcome::not_committed,
            Error{ErrorCode::arithmetic_overflow, "rotation free-space requirement overflow"});
    }
    auto next_pin_slots = prepare_pin_slot_index(*next_manifest);
    if (!next_pin_slots) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_pin_slots.error());
    }
    segments_.reserve(segments_.size() + 1U);
    generation_pins_.reserve(generation_pins_.size() + 1U);

    const SegmentHeaderIdentity old_identity{.store_id = manifest_.store_id,
                                             .segment_id = old_entry.segment_id,
                                             .generation = old_entry.generation,
                                             .owner_worker = old_entry.owner_worker};
    if (old_index >= generation_pins_.size() || !generation_pins_[old_index] ||
        generation_pins_[old_index]->identity != old_identity) {
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::corrupted_data, "active Segment has no exact generation pin"});
    }
    const auto old_pin = generation_pins_[old_index];
    const auto old_selected = segments_[old_index].selected;
    std::optional<DurableSegmentFile> active_file;
    if (worker.cached_file && worker.cached_file->identity() == old_identity && worker.cached_writable) {
        active_file.emplace(std::move(*worker.cached_file));
    }
    worker.cached_file.reset();
    worker.cached_writable = false;
    const auto& replacement_entry = next_manifest->segments.back();
    const SegmentHeaderIdentity replacement_identity{
        .store_id = manifest_.store_id,
        .segment_id = replacement_entry.segment_id,
        .generation = replacement_entry.generation,
        .owner_worker = replacement_entry.owner_worker,
    };
    worker.mutation_io_active = true;
    catalog_lock.unlock();
    if (worker_lock.owns_lock()) {
        worker_lock.unlock();
    }
    SegmentFileCreationResult created;
    SelectedSegmentCommit sealed_selected{};
    SelectedSegmentCommit replacement_selected{};
    std::shared_ptr<const RuntimeSegmentGeneration> sealed_generation;
    std::shared_ptr<const RuntimeSegmentGeneration> replacement_generation;
    DurableMutationResult io_result{
        .outcome = DurableMutationOutcome::not_committed, .sequence = std::nullopt, .error = std::nullopt};
    try {
        if (auto available = require_durable_available_space(
                directory_, kSegmentSizeBytes + *next_manifest_bytes, options_.limits);
            !available) {
            io_result = mutation_failure(DurableMutationOutcome::not_committed, available.error());
        } else {
            if (!active_file) {
                auto opened =
                    DurableSegmentFile::open(directory_, old_identity, SegmentFileOpenMode::read_write);
                if (!opened || opened->selected_commit() != old_selected) {
                    io_result = mutation_failure(
                        DurableMutationOutcome::indeterminate,
                        opened ? Error{ErrorCode::corrupted_data, "active Segment changed after reservation"}
                               : opened.error());
                } else {
                    active_file.emplace(std::move(*opened));
                }
            }
            if (!io_result.error) {
                const auto seal_started = std::chrono::steady_clock::now();
                ScopeExit observe_seal{[&]() noexcept { seal_ns = steady_elapsed_ns(seal_started); }};
                if (active_file->selected_commit().commit.state != PersistedSegmentState::sealed) {
                    const auto sealed = active_file->seal();
                    if (!sealed.committed()) {
                        io_result = mutation_failure(
                            sealed.outcome == SegmentCommitOutcome::indeterminate
                                ? DurableMutationOutcome::indeterminate
                                : DurableMutationOutcome::not_committed,
                            sealed.error.value_or(Error{ErrorCode::io_error, "Segment seal failed"}));
                    }
                }
                sealed_selected = active_file->selected_commit();
            }
            if (!io_result.error) {
                auto sealed_reader =
                    DurableSegmentFile::open(directory_, old_identity, SegmentFileOpenMode::read_only);
                if (!sealed_reader || sealed_reader->selected_commit() != sealed_selected) {
                    io_result = mutation_failure(
                        DurableMutationOutcome::not_committed,
                        sealed_reader ? Error{ErrorCode::corrupted_data,
                                              "sealed Segment changed before generation-pin preparation"}
                                      : sealed_reader.error());
                } else {
                    sealed_generation =
                        std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
                            .identity = old_identity,
                            .selected = sealed_selected,
                            .file = std::move(*sealed_reader),
                        });
                }
            }
            if (!io_result.error) {
                const auto create_started = std::chrono::steady_clock::now();
                ScopeExit observe_create{[&]() noexcept { create_ns = steady_elapsed_ns(create_started); }};
                created = DurableSegmentFile::create(directory_, replacement_identity);
                if (!created.durable()) {
                    io_result =
                        mutation_failure(created.outcome == SegmentFileCreationOutcome::indeterminate
                                             ? DurableMutationOutcome::indeterminate
                                             : DurableMutationOutcome::not_committed,
                                         created.error.value_or(Error{
                                             ErrorCode::io_error, "replacement Segment creation failed"}));
                }
            }
            if (!io_result.error) {
                replacement_selected = created.file->selected_commit();
                auto replacement_reader = DurableSegmentFile::open(directory_, replacement_identity,
                                                                   SegmentFileOpenMode::read_only);
                if (!replacement_reader || replacement_reader->selected_commit() != replacement_selected) {
                    io_result = mutation_failure(
                        DurableMutationOutcome::not_committed,
                        replacement_reader
                            ? Error{ErrorCode::corrupted_data,
                                    "new active Segment changed before generation-pin preparation"}
                            : replacement_reader.error());
                } else {
                    replacement_generation =
                        std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
                            .identity = replacement_identity,
                            .selected = replacement_selected,
                            .file = std::move(*replacement_reader),
                        });
                }
            }
            if (!io_result.error) {
                const auto manifest_started = std::chrono::steady_clock::now();
                ScopeExit observe_manifest{
                    [&]() noexcept { manifest_publication_ns = steady_elapsed_ns(manifest_started); }};
                const auto published =
                    directory_.publish_manifest(*next_manifest, options_.limits.max_manifest_bytes);
                if (!published.durable()) {
                    io_result =
                        mutation_failure(published.outcome == ManifestPublicationOutcome::indeterminate
                                             ? DurableMutationOutcome::indeterminate
                                             : DurableMutationOutcome::not_committed,
                                         published.error.value_or(Error{
                                             ErrorCode::io_error, "rotation manifest publication failed"}));
                } else {
                    io_result = {.outcome = DurableMutationOutcome::committed,
                                 .sequence = std::nullopt,
                                 .error = std::nullopt};
                }
            }
        }
    } catch (const std::bad_alloc&) {
        // Rotation starts only after an append attempt reached the persistent
        // write boundary. Unexpected failures in this phase are conservative.
        io_result =
            mutation_failure(DurableMutationOutcome::indeterminate, Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        io_result =
            mutation_failure(DurableMutationOutcome::indeterminate, Error{ErrorCode::internal_error, {}});
    }

    if (!options_.exclusive_writer) {
        worker_lock.lock();
    }
    catalog_lock.lock();
    const auto clear_reservation = [&]() noexcept {
        worker.mutation_io_active = false;
        worker.mutation_io_finished.notify_all();
    };
    if (!io_result.committed()) {
        if (active_file && !worker.cached_file) {
            worker.cached_file.emplace(std::move(*active_file));
            worker.cached_writable = true;
        }
        if (io_result.outcome == DurableMutationOutcome::indeterminate || !directory_.healthy()) {
            healthy_.store(false, std::memory_order_release);
        }
        clear_reservation();
        return io_result;
    }

    const auto current_position =
        std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), old_entry.segment_id,
                         [](const ManifestSegmentEntry& entry, const SegmentId id) {
                             return entry.segment_id.value < id.value;
                         });
    if (!healthy() || current_position == manifest_.segments.end() || *current_position != old_entry ||
        worker.active_segment != old_entry.segment_id || generation_pins_[old_index] != old_pin ||
        worker.cached_file || !worker.mutation_io_active) {
        healthy_.store(false, std::memory_order_release);
        clear_reservation();
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::corrupted_data, "rotation reservation failed final relinearization"});
    }
    const auto active_live_record_bytes = worker.active_live_record_bytes.load(std::memory_order_relaxed);
    const auto sealed_live_record_bytes = worker.sealed_live_record_bytes.load(std::memory_order_relaxed);
    if (active_live_record_bytes > std::numeric_limits<std::uint64_t>::max() - sealed_live_record_bytes) {
        healthy_.store(false, std::memory_order_release);
        clear_reservation();
        return mutation_failure(DurableMutationOutcome::indeterminate,
                                Error{ErrorCode::arithmetic_overflow,
                                      "durable rotation live Record byte count overflows after publication"});
    }
    const auto next_sealed_live_record_bytes = sealed_live_record_bytes + active_live_record_bytes;

    const auto replacement_segment_id = replacement_entry.segment_id;
    manifest_ = std::move(*next_manifest);
    segments_[old_index].selected = sealed_selected;
    segments_.push_back({.selected = replacement_selected});
    generation_pins_[old_index] = std::move(sealed_generation);
    generation_pins_.push_back(std::move(replacement_generation));
    pin_slot_by_segment_id_ = std::move(*next_pin_slots);
    worker.active_segment = replacement_segment_id;
    worker.active_live_record_bytes.store(0, std::memory_order_release);
    worker.sealed_live_record_bytes.store(next_sealed_live_record_bytes, std::memory_order_release);
    worker.cached_file.emplace(std::move(*created.file));
    worker.cached_writable = true;
    worker.hot_records.erase_if([&](const std::string& key, std::uint64_t, HotRecordEntry& entry) {
        if (entry.reference.segment_id != old_entry.segment_id) {
            return false;
        }
        subtract_hot_record_accounting(worker.hot_record_resident_bytes, key, entry);
        worker.get_path_metrics.hot_evictions.fetch_add(1U, std::memory_order_relaxed);
        return true;
    });
    if (!advance_read_catalog_revision(worker)) {
        healthy_.store(false, std::memory_order_release);
        clear_reservation();
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::arithmetic_overflow, "durable read catalog revision exhausted"});
    }
    clear_reservation();
    rotation_committed = true;
    return {.outcome = DurableMutationOutcome::committed, .sequence = std::nullopt, .error = std::nullopt};
}

auto DurableRuntimeCatalog::mutate(const std::span<const std::byte> key,
                                   const std::span<const std::byte> value, const Opcode opcode,
                                   const std::uint64_t key_hash, const std::uint64_t expire_at_ns,
                                   const ValueType type, const std::uint32_t flags, const bool writer_batch)
    -> DurableMutationResult {
    auto exception_outcome = DurableMutationOutcome::not_committed;
    std::optional<std::chrono::steady_clock::time_point> final_record_commit_started;
    bool final_record_committed{};
    ScopeExit final_record_telemetry{[&, this]() noexcept {
        if (final_record_commit_started) {
            record_rotation_final_commit(steady_elapsed_ns(*final_record_commit_started),
                                         final_record_committed);
        }
    }};
    try {
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        const auto worker_index = route_worker(key_hash, workers_.size());
        auto& worker = *workers_[worker_index];
        const bool strict_batch = options_.batch.has_value() && options_.strict_ack;
        struct GroupAdmission final {
            std::atomic_size_t* active{};

            explicit GroupAdmission(std::atomic_size_t* counter) noexcept : active(counter) {
                if (active) {
                    active->fetch_add(1U, std::memory_order_relaxed);
                }
            }
            ~GroupAdmission() {
                if (active) {
                    active->fetch_sub(1U, std::memory_order_relaxed);
                }
            }
            GroupAdmission(const GroupAdmission&) = delete;
            auto operator=(const GroupAdmission&) -> GroupAdmission& = delete;
        } admission{strict_batch ? &worker.active_group_mutations : nullptr};
        // Paired exclusive Writer (no background flusher): skip Worker mutex on the
        // ordinary mutate hot path. Compaction observes hot_path_depth instead.
        // Group/periodic paths keep the mutex because the flush coordinator shares
        // pending-batch and file state with mutate.
        const bool elide_worker_mutex = options_.exclusive_writer && flusher_ == nullptr;
        std::unique_lock worker_lock{worker.mutex, std::defer_lock};
        struct ExclusiveHotPathGuard final {
            RuntimeWorker* worker{};

            explicit ExclusiveHotPathGuard(RuntimeWorker* owner) noexcept : worker(owner) {
                if (worker != nullptr) {
                    worker->hot_path_depth.fetch_add(1U, std::memory_order_acq_rel);
                }
            }
            ~ExclusiveHotPathGuard() {
                if (worker == nullptr) {
                    return;
                }
                if (worker->hot_path_depth.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
                    worker->hot_path_depth.notify_all();
                }
            }
            ExclusiveHotPathGuard(const ExclusiveHotPathGuard&) = delete;
            auto operator=(const ExclusiveHotPathGuard&) -> ExclusiveHotPathGuard& = delete;
        } hot_path{elide_worker_mutex ? &worker : nullptr};
        if (elide_worker_mutex) {
#ifndef NDEBUG
            // Debug: the exclusive Writer must not find the mutex already held by
            // compaction/verify/backup. If it is, wait then release — those paths
            // retain the lock by design.
            if (!worker_lock.try_lock()) {
                worker_lock.lock();
            }
            worker_lock.unlock();
#endif
            if (worker.compaction_commit_active.load(std::memory_order_acquire)) {
                return mutation_failure(
                    DurableMutationOutcome::not_committed,
                    Error{ErrorCode::sequence_conflict,
                          "durable mutation conflicts with compaction manifest publication"});
            }
        } else {
            worker_lock.lock();
            // Both waits release the Worker mutex. Recheck both gates after every
            // wake so a producer cannot enter a closing batch while another
            // producer has just returned the writable handle (or vice versa).
            while (healthy() &&
                   (worker.mutation_io_active || (dedicated_commit_executor_ && worker.batch_closing))) {
                if (dedicated_commit_executor_ && worker.batch_closing) {
                    worker.batch_closed.wait(worker_lock);
                } else {
                    worker.mutation_io_finished.wait(worker_lock);
                }
            }
        }
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        if (auto drained = worker.drain_deferred_ttl(worker.deferred_ttl_reclaims.size()); !drained) {
            return mutation_failure(DurableMutationOutcome::indeterminate, drained.error());
        }
        if (worker.compaction_commit_active.load(std::memory_order_acquire)) {
            return mutation_failure(DurableMutationOutcome::not_committed,
                                    Error{ErrorCode::sequence_conflict,
                                          "durable mutation conflicts with compaction manifest publication"});
        }
        if (worker.next_sequence.value == 0 ||
            worker.next_sequence.value == std::numeric_limits<std::uint64_t>::max()) {
            return mutation_failure(
                DurableMutationOutcome::not_committed,
                Error{ErrorCode::arithmetic_overflow, "Worker sequence space is exhausted"});
        }

        const HashedKey hashed{.key = as_string_view(key), .hash = key_hash};
        bool key_present = worker.index.find(hashed).has_value();
        if (strict_batch) {
            const auto pending = std::find_if(
                worker.pending_group_mutations.rbegin(), worker.pending_group_mutations.rend(),
                [&](const PendingGroupMutation& mutation) { return mutation.key == hashed.key; });
            if (pending != worker.pending_group_mutations.rend()) {
                key_present = pending->opcode == Opcode::put;
            }
        }
        if (opcode == Opcode::erase && !key_present) {
            return mutation_failure(DurableMutationOutcome::not_committed,
                                    Error{ErrorCode::not_found, "key is not present"});
        }
        std::size_t prospective_group_insertions = worker.pending_group_insertions;
        std::size_t prospective_group_heap_key_bytes = worker.pending_group_heap_key_bytes;
        if (opcode == Opcode::put) {
            if (!key_present) {
                const auto live_key_limit = durable_worker_live_key_limit(worker_index, workers_.size(),
                                                                          options_.limits.max_live_keys);
                const auto current_size = worker.index.stats().size;
                if (current_size >= live_key_limit ||
                    prospective_group_insertions >= live_key_limit - current_size) {
                    return mutation_failure(
                        DurableMutationOutcome::not_committed,
                        Error{ErrorCode::resource_exhausted, "durable Worker live-key budget is exhausted"});
                }
            }
            if (auto prepared = worker.index.prepare_insert(hashed); !prepared) {
                return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
            }
            if (strict_batch && !key_present) {
                if (prospective_group_insertions == std::numeric_limits<std::size_t>::max() ||
                    (key.size() > kSwissInlineKeyBytes &&
                     key.size() >
                         std::numeric_limits<std::size_t>::max() - prospective_group_heap_key_bytes)) {
                    return mutation_failure(
                        DurableMutationOutcome::not_committed,
                        Error{ErrorCode::arithmetic_overflow, "group publication capacity overflow"});
                }
                ++prospective_group_insertions;
                if (key.size() > kSwissInlineKeyBytes) {
                    prospective_group_heap_key_bytes += key.size();
                }
                if (auto prepared = worker.index.prepare_batch_insert(prospective_group_insertions,
                                                                      prospective_group_heap_key_bytes);
                    !prepared) {
                    return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
                }
            }
        }
        const RecordInput input{.sequence = worker.next_sequence,
                                .opcode = opcode,
                                .type = type,
                                .flags = flags,
                                .key_hash = key_hash,
                                .expire_at_ns = expire_at_ns,
                                .key = key,
                                .value = value};
        const auto committed_sequence = worker.next_sequence;
        const auto encoded_size = encoded_record_size(input);
        if (!encoded_size) {
            return mutation_failure(DurableMutationOutcome::not_committed, encoded_size.error());
        }
        worker.encode_scratch.resize(*encoded_size);
        if (auto encoded = encode_record(worker.encode_scratch, input, *encoded_size); !encoded) {
            return mutation_failure(DurableMutationOutcome::not_committed, encoded.error());
        }
        // The mutation, not the Worker, owns encoded bytes across every
        // unlocked I/O or publication wait. Move preserves scratch capacity on
        // the uncontended path and prevents a later same-Worker producer from
        // overwriting a sleeping mutation's final Record.
        std::vector<std::byte> encoded_record{std::move(worker.encode_scratch)};
        ScopeExit restore_encode_scratch{[&]() noexcept {
            if (worker_lock.owns_lock() || elide_worker_mutex) {
                worker.encode_scratch = std::move(encoded_record);
            }
        }};
        PendingGroupMutation group_mutation;
        PreparedHotRecord prepared_hot_record;
        if (opcode == Opcode::put) {
            constexpr auto group_publication_fixed_bytes =
                static_cast<std::uint64_t>(sizeof(PendingGroupMutation));
            const auto publication_staging_bytes =
                strict_batch
                    ? (key.size() > std::numeric_limits<std::uint64_t>::max() - group_publication_fixed_bytes
                           ? std::numeric_limits<std::uint64_t>::max()
                           : group_publication_fixed_bytes + static_cast<std::uint64_t>(key.size()))
                    : 0U;
            auto prepared =
                worker.prepare_hot_record(worker_index, workers_.size(), options_.limits, hashed.key,
                                          hashed.hash, value, expire_at_ns, publication_staging_bytes);
            if (!prepared) {
                return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
            }
            prepared_hot_record = std::move(*prepared);
        }
        if (strict_batch) {
            group_mutation.key.assign(hashed.key);
            group_mutation.hot_record = std::move(prepared_hot_record);
            group_mutation.opcode = opcode;
            group_mutation.key_hash = key_hash;
            group_mutation.expire_at_ns = expire_at_ns;
            worker.pending_group_mutations.reserve(worker.pending_group_mutations.size() + 1U);
        }

        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            std::shared_lock catalog_lock{catalog_mutex_};
            const auto position =
                std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), worker.active_segment,
                                 [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                     return entry.segment_id.value < id.value;
                                 });
            if (position == manifest_.segments.end() || position->segment_id != worker.active_segment) {
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(
                    DurableMutationOutcome::indeterminate,
                    Error{ErrorCode::corrupted_data, "runtime active Segment is absent from the manifest"});
            }
            const auto catalog_index = static_cast<std::size_t>(position - manifest_.segments.begin());
            const SegmentHeaderIdentity identity{.store_id = manifest_.store_id,
                                                 .segment_id = position->segment_id,
                                                 .generation = position->generation,
                                                 .owner_worker = position->owner_worker};
            if (catalog_index >= generation_pins_.size() || !generation_pins_[catalog_index] ||
                generation_pins_[catalog_index]->identity != identity) {
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(
                    DurableMutationOutcome::indeterminate,
                    Error{ErrorCode::corrupted_data, "active Segment has no exact generation pin"});
            }
            const auto active_pin = generation_pins_[catalog_index];
            const auto expected_selected = segments_[catalog_index].selected;
            const bool batch_enabled = options_.batch.has_value();
            const bool deferred_commit = options_.commit_sync == SegmentCommitSync::deferred;
            SegmentCommitResult appended{};
            std::optional<DurableSegmentFile> io_file;
            if (worker.cached_file && worker.cached_file->identity() == identity && worker.cached_writable) {
                io_file.emplace(std::move(*worker.cached_file));
            }
            worker.cached_file.reset();
            worker.cached_writable = false;
            worker.mutation_io_active = true;
            catalog_lock.unlock();
            if (worker_lock.owns_lock()) {
                worker_lock.unlock();
            }

            std::uint32_t offset{};
            // From the first persistent write until coherent runtime publication,
            // an unexpected exception has a conservative indeterminate outcome.
            exception_outcome = DurableMutationOutcome::indeterminate;
            try {
                if (!io_file) {
                    auto opened =
                        DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_write);
                    if (!opened || opened->selected_commit() != expected_selected) {
                        appended = {
                            .outcome = SegmentCommitOutcome::indeterminate,
                            .error = opened ? Error{ErrorCode::corrupted_data,
                                                    "active Segment changed after I/O reservation"}
                                            : opened.error(),
                        };
                    } else {
                        io_file.emplace(std::move(*opened));
                    }
                }
                if (!appended.error) {
                    offset = io_file->selected_commit().commit.committed_end;
                    appended = batch_enabled || deferred_commit
                                   ? io_file->append_record(encoded_record)
                                   : io_file->append(encoded_record, options_.commit_sync);
                }
            } catch (const std::bad_alloc&) {
                appended = {.outcome = SegmentCommitOutcome::indeterminate,
                            .error = Error{ErrorCode::resource_exhausted, {}}};
            } catch (...) {
                appended = {.outcome = SegmentCommitOutcome::indeterminate,
                            .error = Error{ErrorCode::internal_error, {}}};
            }

            if (!elide_worker_mutex) {
                worker_lock.lock();
            }
            catalog_lock.lock();
            const auto current_position =
                std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), identity.segment_id,
                                 [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                     return entry.segment_id.value < id.value;
                                 });
            const auto current_pin_index = catalog_index_for_segment(identity.segment_id);
            const bool reservation_valid = current_position != manifest_.segments.end() &&
                                           current_position->segment_id == identity.segment_id &&
                                           current_position->generation == identity.generation &&
                                           current_position->owner_worker == identity.owner_worker &&
                                           current_position->role == ManifestSegmentRole::active &&
                                           worker.active_segment == identity.segment_id &&
                                           worker.mutation_io_active && current_pin_index.has_value() &&
                                           generation_pins_[*current_pin_index] == active_pin;
            if (io_file) {
                worker.cached_file.emplace(std::move(*io_file));
                worker.cached_writable = true;
            }
            worker.mutation_io_active = false;
            worker.mutation_io_finished.notify_all();
            if (!reservation_valid) {
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(
                    DurableMutationOutcome::indeterminate,
                    Error{ErrorCode::corrupted_data, "mutation I/O reservation failed relinearization"});
            }
            const auto current_catalog_index =
                static_cast<std::size_t>(current_position - manifest_.segments.begin());
            if (!appended.committed()) {
                if (appended.error &&
                    (appended.error->code == ErrorCode::segment_full ||
                     appended.error->code == ErrorCode::segment_sealed) &&
                    attempt == 0) {
                    if (strict_batch && !worker.pending_group_mutations.empty()) {
                        if (dedicated_commit_executor_) {
                            worker.batch_closing = true;
                            catalog_lock.unlock();
                            flusher_->request_flush();
                            worker.batch_closed.wait(worker_lock, [&] {
                                return worker.pending_group_mutations.empty() || !healthy();
                            });
                            if (!healthy()) {
                                return mutation_failure(
                                    DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                            }
                        } else if (auto flushed = flush_worker_batch(worker, worker_lock, catalog_lock,
                                                                     SegmentCommitSync::immediate);
                                   !flushed) {
                            return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                        }
                        prospective_group_insertions = opcode == Opcode::put && !key_present ? 1U : 0U;
                        prospective_group_heap_key_bytes =
                            prospective_group_insertions != 0 && key.size() > kSwissInlineKeyBytes
                                ? key.size()
                                : 0U;
                    }
                    if (catalog_lock.owns_lock()) {
                        catalog_lock.unlock();
                    }
                    const auto rotated = rotate_active(worker, worker_lock);
                    if (!rotated.committed()) {
                        return rotated;
                    }
                    final_record_commit_started = std::chrono::steady_clock::now();
                    continue;
                }
                if (appended.outcome == SegmentCommitOutcome::indeterminate || !directory_.healthy()) {
                    healthy_.store(false, std::memory_order_release);
                    worker.pending_group_mutations.clear();
                    worker.pending_group_insertions = 0;
                    worker.pending_group_heap_key_bytes = 0;
                    worker.batch_closing = false;
                    worker.batch_closed.notify_all();
                }
                return mutation_failure(
                    appended.outcome == SegmentCommitOutcome::indeterminate
                        ? DurableMutationOutcome::indeterminate
                        : DurableMutationOutcome::not_committed,
                    appended.error.value_or(Error{ErrorCode::io_error, "Record append failed"}));
            }
            if (!worker.cached_file) {
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(
                    DurableMutationOutcome::indeterminate,
                    Error{ErrorCode::corrupted_data, "committed mutation lost its writable Segment handle"});
            }

            segments_[current_catalog_index].selected = worker.cached_file->selected_commit();
            ++worker.next_sequence.value;

            const RecordRef reference{.segment_id = identity.segment_id,
                                      .offset = RecordOffset{offset},
                                      .size = RecordSize{static_cast<std::uint32_t>(encoded_record.size())},
                                      .sequence = committed_sequence,
                                      .generation = identity.generation};
            if (strict_batch) {
                group_mutation.reference = reference;
                worker.pending_group_mutations.push_back(std::move(group_mutation));
                worker.pending_group_insertions = prospective_group_insertions;
                worker.pending_group_heap_key_bytes = prospective_group_heap_key_bytes;
            }

            if (batch_enabled) {
                worker.batch_metrics.pending_records.store(
                    static_cast<std::size_t>(worker.cached_file->pending_record_count()),
                    std::memory_order_relaxed);
                worker.batch_metrics.pending_bytes.store(worker.cached_file->pending_bytes(),
                                                         std::memory_order_relaxed);
                if (worker.batch_started == std::chrono::steady_clock::time_point{}) {
                    worker.batch_started = std::chrono::steady_clock::now();
                    if (dedicated_commit_executor_ && flusher_) {
                        flusher_->request_flush_at(worker.batch_started +
                                                   std::chrono::milliseconds{options_.batch->max_wait_ms});
                    }
                }
                if (options_.strict_ack && !writer_batch) {
                    if (should_flush_batch(worker)) {
                        if (dedicated_commit_executor_) {
                            worker.batch_closing = true;
                            catalog_lock.unlock();
                            flusher_->request_flush();
                            wait_for_batch_close(worker, committed_sequence, worker_lock);
                            if (!healthy() || worker.durable_through.value < committed_sequence.value) {
                                return mutation_failure(
                                    DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                            }
                        } else if (auto flushed = flush_worker_batch(worker, worker_lock, catalog_lock,
                                                                     SegmentCommitSync::immediate);
                                   !flushed) {
                            return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                        }
                    } else {
                        catalog_lock.unlock();
                        wait_for_batch_close(worker, committed_sequence, worker_lock);
                        if (!healthy() || worker.durable_through.value < committed_sequence.value) {
                            return mutation_failure(
                                DurableMutationOutcome::indeterminate,
                                Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                        }
                    }
                } else if (should_flush_batch(worker)) {
                    if (auto flushed = flush_worker_batch(worker, worker_lock, catalog_lock,
                                                          SegmentCommitSync::deferred);
                        !flushed) {
                        return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                    }
                } else if (flusher_) {
                    flusher_->notify_batch_activity();
                }
            } else if (deferred_commit && flusher_) {
                flusher_->notify_batch_activity();
            }

            if (strict_batch) {
                final_record_committed = true;
                return {.outcome = DurableMutationOutcome::committed,
                        .sequence = committed_sequence,
                        .error = std::nullopt};
            }
            if (opcode == Opcode::put) {
                const auto published = worker.index.insert_or_assign(hashed, reference);
                if (!published) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = published.error()};
                }
                if (auto counted = worker.update_live_record_bytes(published->previous, reference);
                    !counted) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = counted.error()};
                }
                if (prepared_hot_record.empty()) {
                    worker.erase_hot_record(hashed);
                } else if (auto hot_published = worker.publish_hot_record(prepared_hot_record, reference);
                           !hot_published) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = hot_published.error()};
                }
            } else {
                const auto erased = worker.index.erase_no_compact(hashed);
                if (auto counted = worker.update_live_record_bytes(erased.previous, std::nullopt); !counted) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = counted.error()};
                }
                worker.erase_hot_record(hashed);
            }
            final_record_committed = true;
            return {.outcome = DurableMutationOutcome::committed,
                    .sequence = committed_sequence,
                    .error = std::nullopt};
        }
        return mutation_failure(
            DurableMutationOutcome::not_committed,
            Error{ErrorCode::segment_full, "Record does not fit after one durable rotation"});
    } catch (const std::bad_alloc&) {
        if (exception_outcome != DurableMutationOutcome::not_committed) {
            abandon_pending_batches();
        }
        return mutation_failure(exception_outcome, Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        if (exception_outcome != DurableMutationOutcome::not_committed) {
            abandon_pending_batches();
        }
        return mutation_failure(exception_outcome, Error{ErrorCode::internal_error, {}});
    }
}
} // namespace glyphastore
