#include "glyphastore/store/store.hpp"

#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/bootstrap.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/store_verify.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/paired/shard_pair_runtime.hpp"
#include "glyphastore/store/value.hpp"
#include "glyphastore/worker/pool.hpp"
#include "glyphastore/worker/topology.hpp"
#include "store/store_impl.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace glyphastore {

[[nodiscard]] auto start_paired_runtime(Store& store, StoreConfig config) -> Status {
    return detail::StoreAccess::attach_paired_runtime(store, config);
}

Store::Store(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Store::~Store() {
    try {
        static_cast<void>(close());
    } catch (...) {
    }
}

auto Store::open(const StoreConfig& config) -> Result<std::unique_ptr<Store>> try {
    if (config.storage_mode != StorageMode::volatile_memory &&
        config.storage_mode != StorageMode::durable_sync &&
        config.storage_mode != StorageMode::durable_periodic &&
        config.storage_mode != StorageMode::durable_group) {
        return fail(ErrorCode::invalid_argument, "storage mode is unsupported");
    }
    if (config.concurrency != StoreConcurrencyMode::paired &&
        config.concurrency != StoreConcurrencyMode::legacy_mutex) {
        return fail(ErrorCode::invalid_argument, "store concurrency mode is unsupported");
    }
    if (config.durable_open_mode != DurableOpenMode::open_or_create &&
        config.durable_open_mode != DurableOpenMode::open_existing &&
        config.durable_open_mode != DurableOpenMode::create_new) {
        return fail(ErrorCode::invalid_argument, "durable open mode is unsupported");
    }
    if (config.worker_config.maximum_workers == 0 ||
        config.worker_config.maximum_workers > kMaximumWorkerCount) {
        return fail(ErrorCode::invalid_argument, "maximum worker count is outside the supported range");
    }
    if (config.worker_config.explicit_count.has_value() &&
        (*config.worker_config.explicit_count == 0 ||
         *config.worker_config.explicit_count > config.worker_config.maximum_workers)) {
        return fail(ErrorCode::invalid_argument, "explicit worker count is outside the supported range");
    }
    if (config.recovery_now_ns != 0) {
        return fail(ErrorCode::invalid_argument,
                    "recovery_now_ns is no longer supported; inject StoreConfig::clock instead");
    }
    if (auto valid = validate_maintenance_config(config.maintenance); !valid) {
        return unexpected(valid.error());
    }
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    if (config.storage_mode == StorageMode::volatile_memory) {
        if (config.data_directory || config.durable_open_mode != DurableOpenMode::open_or_create ||
            config.durable_limits != DurableResourceLimits{}) {
            return fail(ErrorCode::invalid_argument,
                        "volatile storage cannot use durable-only configuration");
        }
        if (auto routing = validate_worker_routing_state(config.worker_routing.state()); !routing) {
            return unexpected(routing.error());
        }
        const auto initial_now_ns = store_detail::sample_clock(config.clock);
        auto impl = std::make_unique<Impl>(
            std::make_unique<VolatileStoreRuntime>(SegmentId{1}, count, config.worker_routing.state()),
            config.worker_routing.state(), config.clock, initial_now_ns);
        auto store = std::unique_ptr<Store>(new Store(std::move(impl)));
        store->impl_->maintenance = std::make_unique<MaintenanceController>(config.maintenance);
        Store* const raw = store.get();
        store->impl_->maintenance->bind_compact(
            [raw](const std::optional<std::size_t> preferred_worker,
                  const std::uint64_t max_copy_bytes) -> Result<CompactionResult> {
                return raw->compact_for_maintenance(preferred_worker, max_copy_bytes);
            });
        store->impl_->maintenance->bind_observe(
            [](MaintenanceObserveRequest) -> Result<MaintenanceObservation> {
                return MaintenanceObservation{.durable = false};
            });
        store->impl_->maintenance->start();
        store->impl_->concurrency = config.concurrency;
        store->impl_->close_drain_deadline_ms = config.close_drain_deadline_ms;
        if (auto paired = start_paired_runtime(*store, config); !paired) {
            static_cast<void>(store->close());
            return unexpected(paired.error());
        }
        return store;
    }
    if (!config.data_directory || config.data_directory->empty()) {
        return fail(ErrorCode::invalid_argument, "durable storage requires a data directory");
    }
    if (auto valid = validate_durable_resource_limits(config.durable_limits); !valid) {
        return unexpected(valid.error());
    }
    if (config.storage_mode == StorageMode::durable_periodic &&
        config.durable_periodic.sync_interval_ms == 0) {
        return fail(ErrorCode::invalid_argument,
                    "durable_periodic requires sync_interval_ms greater than zero");
    }
    if (config.storage_mode == StorageMode::durable_group) {
        if (auto valid = store_detail::validate_batch_config(config.durable_group); !valid) {
            return unexpected(valid.error());
        }
    }
    if (config.storage_mode == StorageMode::durable_periodic && config.durable_periodic.batch) {
        if (auto valid = store_detail::validate_batch_config(*config.durable_periodic.batch); !valid) {
            return unexpected(valid.error());
        }
    }
    auto directory = DataDirectory::open_and_lock(
        *config.data_directory, store_detail::data_directory_mode(config.durable_open_mode), config.filesystem_hooks);
    if (!directory) {
        return unexpected(directory.error());
    }
    if (auto prepared = prepare_durable_store(*directory, config.durable_open_mode, count,
                                              config.worker_config.explicit_count, config.durable_limits,
                                              config.worker_routing);
        !prepared) {
        return unexpected(prepared.error());
    }
    DurableRuntimeOptions runtime_options{};
    runtime_options.limits = config.durable_limits;
    if (config.concurrency == StoreConcurrencyMode::paired) {
        // Generation-only ordinary reads (ADR 0032).
        runtime_options.limits.hot_cache_enabled = false;
        runtime_options.exclusive_writer = true;
    }
    if (config.storage_mode == StorageMode::durable_periodic) {
        runtime_options.commit_sync = SegmentCommitSync::deferred;
        runtime_options.sync_interval_ms = config.durable_periodic.sync_interval_ms;
        runtime_options.batch = config.durable_periodic.batch;
    } else if (config.storage_mode == StorageMode::durable_group) {
        runtime_options.commit_sync = SegmentCommitSync::immediate;
        runtime_options.batch = config.durable_group;
        runtime_options.strict_ack = true;
        runtime_options.sync_interval_ms = config.durable_group.max_wait_ms;
    }
    const auto recovery_now_ns = store_detail::sample_clock(config.clock);
    auto runtime =
        DurableRuntimeCatalog::open_locked(std::move(*directory), recovery_now_ns, runtime_options);
    if (!runtime) {
        return unexpected(runtime.error());
    }
    auto impl = std::make_unique<Impl>(std::move(*runtime), config.clock, recovery_now_ns);
    auto store = std::unique_ptr<Store>(new Store(std::move(impl)));
    store->impl_->maintenance = std::make_unique<MaintenanceController>(config.maintenance);
    Store* const raw = store.get();
    store->impl_->maintenance->bind_compact(
        [raw](const std::optional<std::size_t> preferred_worker,
              const std::uint64_t max_copy_bytes) -> Result<CompactionResult> {
            return raw->compact_for_maintenance(preferred_worker, max_copy_bytes);
        });
    store->impl_->maintenance->bind_observe(
        [raw](MaintenanceObserveRequest req) -> Result<MaintenanceObservation> {
            if (!raw->impl_ || !raw->impl_->durable_runtime) {
                return fail(ErrorCode::unavailable, "durable runtime is unavailable");
            }
            auto observation = raw->impl_->durable_runtime->maintenance_observation(
                raw->impl_->next_compaction_worker.load(std::memory_order_relaxed), raw->impl_->now_ns(),
                req.probe_unread_expired_ttl);
            if (observation && observation->compaction_candidate_worker && !req.probe_unread_expired_ttl) {
                raw->impl_->next_compaction_worker.store((*observation->compaction_candidate_worker + 1U) %
                                                             raw->impl_->worker_count_value,
                                                         std::memory_order_relaxed);
            }
            return observation;
        });
    store->impl_->maintenance->start();
    store->impl_->concurrency = config.concurrency;
    store->impl_->close_drain_deadline_ms = config.close_drain_deadline_ms;
    if (auto paired = start_paired_runtime(*store, config); !paired) {
        static_cast<void>(store->close());
        return unexpected(paired.error());
    }
    return store;
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    return store_detail::internal_failure();
}

auto Store::worker_count() const noexcept -> std::size_t {
    return impl_->worker_count_value;
}

auto Store::get(const std::string_view key) -> Result<OwnedValue> {
    return get_copy(key);
}

auto Store::get(const std::span<const std::byte> key) -> Result<OwnedValue> {
    return get_copy(key);
}

auto Store::get_copy(const std::string_view key) -> Result<OwnedValue> try {
    const HashedKey hashed{key, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    const auto now_ns = impl_->now_ns();
    if (impl_->pair_runtime) {
        store::paired::ShardPairRuntime::ReadLease lease{*impl_->pair_runtime, shard};
        if (!lease) {
            return fail(ErrorCode::unavailable, "paired read generation is unavailable");
        }
        if (impl_->durable_runtime) {
            auto view = lease.generation()->prepare_durable(hashed);
            if (!view) {
                return unexpected(view.error());
            }
            auto prepared = detail::StoreAccess::prepare_published_durable_get(*this, shard, *view, now_ns);
            if (!prepared) {
                return unexpected(prepared.error());
            }
            if (prepared->value) {
                return std::move(*prepared->value);
            }
            if (prepared->cold) {
                return detail::StoreAccess::complete_get_owned(*this, shard, std::move(*prepared->cold));
            }
            return fail(ErrorCode::internal_error, "paired durable GET produced no result");
        }
        return lease.generation()->get(hashed, now_ns);
    }
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(hashed, now_ns);
    }
    auto& worker = impl_->volatile_runtime->workers.route(hashed);
    const std::lock_guard lock{worker.mutex_};
    auto record = worker.get_locked(hashed, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return store_detail::copy_value(*record);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::get_copy(const std::span<const std::byte> key) -> Result<OwnedValue> try {
    const auto key_view = store_detail::as_string_view(key);
    const HashedKey hashed{key_view, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    const auto now_ns = impl_->now_ns();
    if (impl_->pair_runtime) {
        store::paired::ShardPairRuntime::ReadLease lease{*impl_->pair_runtime, shard};
        if (!lease) {
            return fail(ErrorCode::unavailable, "paired read generation is unavailable");
        }
        if (impl_->durable_runtime) {
            auto view = lease.generation()->prepare_durable(hashed);
            if (!view) {
                return unexpected(view.error());
            }
            auto prepared = detail::StoreAccess::prepare_published_durable_get(*this, shard, *view, now_ns);
            if (!prepared) {
                return unexpected(prepared.error());
            }
            if (prepared->value) {
                return std::move(*prepared->value);
            }
            if (prepared->cold) {
                return detail::StoreAccess::complete_get_owned(*this, shard, std::move(*prepared->cold));
            }
            return fail(ErrorCode::internal_error, "paired durable GET produced no result");
        }
        return lease.generation()->get(hashed, now_ns);
    }
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(hashed, now_ns);
    }
    auto& worker = impl_->volatile_runtime->workers.route(hashed);
    const std::lock_guard lock{worker.mutex_};
    auto record = worker.get_locked(hashed, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return store_detail::copy_value(*record);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::put(const std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    const HashedKey hashed{key, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->pair_runtime) {
        return impl_->pair_runtime->mutate(shard, store::paired::MutationKind::put, hashed, value,
                                           expire_at_ns);
    }
    if (impl_->durable_runtime) {
        return store_detail::durable_status(impl_->durable_runtime->put(hashed, value, expire_at_ns));
    }
    return impl_->volatile_runtime->workers.route(hashed).put(hashed, value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    const auto key_view = store_detail::as_string_view(key);
    const HashedKey hashed{key_view, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->pair_runtime) {
        return impl_->pair_runtime->mutate(shard, store::paired::MutationKind::put, hashed, value,
                                           expire_at_ns);
    }
    if (impl_->durable_runtime) {
        return store_detail::durable_status(impl_->durable_runtime->put(hashed, value, expire_at_ns));
    }
    return impl_->volatile_runtime->workers.route(hashed).put(hashed, value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::erase(const std::string_view key) -> Status try {
    const HashedKey hashed{key, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->pair_runtime) {
        return impl_->pair_runtime->mutate(shard, store::paired::MutationKind::erase, hashed, {}, 0);
    }
    if (impl_->durable_runtime) {
        return store_detail::durable_status(impl_->durable_runtime->erase(hashed));
    }
    return impl_->volatile_runtime->workers.route(hashed).erase(hashed);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::erase(const std::span<const std::byte> key) -> Status try {
    const auto key_view = store_detail::as_string_view(key);
    const HashedKey hashed{key_view, hash_key_routing(key, impl_->routing)};
    const auto shard = route_worker(hashed.hash, impl_->worker_count_value);
    Impl::OperationGuard operation{*impl_, shard};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->pair_runtime) {
        return impl_->pair_runtime->mutate(shard, store::paired::MutationKind::erase, hashed, {}, 0);
    }
    if (impl_->durable_runtime) {
        return store_detail::durable_status(impl_->durable_runtime->erase(hashed));
    }
    return impl_->volatile_runtime->workers.route(hashed).erase(hashed);
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::flush() -> Status try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->flush();
    }
    return {};
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::backup_to(const std::filesystem::path& destination, const bool scan_records)
    -> Result<DurableStoreBackupReport> try {
    if (impl_->lifecycle.load(std::memory_order_acquire) != Impl::LifecycleState::open) {
        return store_detail::closed_store();
    }
    if (!impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "online backup requires a durable Store");
    }

    const auto fence_started = std::chrono::steady_clock::now();
    // Fence admissions before taking compaction_mutex so a compact that already holds the mutex
    // (and an OperationGuard) cannot deadlock against wait_for_active_operations.
    impl_->close_admission();
    struct AdmissionResume final {
        Impl* impl;
        ~AdmissionResume() {
            if (impl != nullptr) {
                impl->resume_admission_if_open();
            }
        }
        void release() noexcept {
            if (impl != nullptr) {
                impl->resume_admission_if_open();
                impl = nullptr;
            }
        }
    } resume{impl_.get()};

    if (!impl_->wait_for_active_operations()) {
        return fail(ErrorCode::unavailable, "backup timed out waiting for active operations");
    }

    if (impl_->lifecycle.load(std::memory_order_acquire) != Impl::LifecycleState::open) {
        return store_detail::closed_store();
    }

    auto copied = [&]() -> Result<DurableStoreBackupReport> {
        std::unique_lock compaction_lock{impl_->compaction_mutex};

        if (!impl_->durable_runtime || !impl_->durable_runtime->healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is unavailable");
        }

        return impl_->durable_runtime->backup_to(destination, scan_records);
    }();
    // Resume writers before destination verify: the destination is a private empty directory and
    // does not share catalog locks with the live Store. Source CRC is not re-run on the live catalog
    // after resume (writers may mutate); destination CRC (when scan_records) is the promotion gate
    // (ADR 0034 shorter-fence incremental).
    const auto fence_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fence_started)
            .count());
    resume.release();

    if (!copied) {
        return unexpected(copied.error());
    }
    copied->admission_fence_ns = fence_ns;

    const auto verify_started = std::chrono::steady_clock::now();
    auto destination_verification = verify_durable_store_path(destination, scan_records);
    if (!destination_verification) {
        return unexpected(destination_verification.error());
    }
    copied->destination_verification = std::move(*destination_verification);
    copied->destination_crc_scanned = scan_records;
    copied->destination_verify_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - verify_started)
            .count());
    return copied;
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::compact() -> Result<CompactionResult> {
    return compact_for_maintenance(std::nullopt, 0);
}

auto Store::compact_for_maintenance(const std::optional<std::size_t> preferred_worker,
                                    const std::uint64_t max_copy_bytes) -> Result<CompactionResult> try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return store_detail::closed_store();
    }
    GS_FAULT_SITE(compact);
    // try_to_lock is the compact-admission progress bound: a second caller fails
    // closed with sequence_conflict instead of waiting unbounded (GS-CONCUR-LIVE-001).
    std::unique_lock maintenance_lock{impl_->compaction_mutex, std::try_to_lock};
    if (!maintenance_lock.owns_lock()) {
        return fail(ErrorCode::sequence_conflict, "another Store compaction is already running");
    }
    if (impl_->volatile_runtime) {
        const auto now_ns = impl_->now_ns();
        for (std::size_t attempted = 0; attempted < impl_->worker_count_value; ++attempted) {
            const auto worker_index = impl_->next_compaction_worker.load(std::memory_order_relaxed);
            impl_->next_compaction_worker.store((worker_index + 1U) % impl_->worker_count_value,
                                                std::memory_order_relaxed);
            auto result = impl_->volatile_runtime->workers.worker(worker_index).compact(now_ns);
            if (!result) {
                return unexpected(result.error());
            }
            if (*result) {
                return store_detail::public_compaction_result(worker_index, **result);
            }
        }
        return CompactionResult{};
    }

    if (preferred_worker) {
        if (*preferred_worker >= impl_->worker_count_value) {
            return fail(ErrorCode::invalid_argument, "maintenance compaction Worker is outside the Store");
        }
        impl_->next_compaction_worker.store((*preferred_worker + 1U) % impl_->worker_count_value,
                                            std::memory_order_relaxed);
        auto result =
            impl_->durable_runtime->compact_worker(*preferred_worker, impl_->now_ns(), max_copy_bytes);
        if (result.outcome == DurableCompactionOutcome::not_compacted && result.error.has_value() &&
            result.error->code == ErrorCode::not_found) {
            return CompactionResult{};
        }
        return store_detail::public_compaction_result(*preferred_worker, std::move(result));
    }

    std::optional<std::size_t> first_candidate;
    std::optional<CompactionResult> last_no_gain;
    for (;;) {
        auto candidate = impl_->durable_runtime->next_compaction_worker(
            impl_->next_compaction_worker.load(std::memory_order_relaxed));
        if (!candidate) {
            return unexpected(candidate.error());
        }
        if (!*candidate || (first_candidate && **candidate == *first_candidate)) {
            if (last_no_gain) {
                return *last_no_gain;
            }
            return CompactionResult{};
        }
        const auto worker_index = **candidate;
        if (!first_candidate) {
            first_candidate = worker_index;
        }
        impl_->next_compaction_worker.store((worker_index + 1U) % impl_->worker_count_value,
                                            std::memory_order_relaxed);
        auto result = impl_->durable_runtime->compact_worker(worker_index, impl_->now_ns(), 0);
        if (result.outcome == DurableCompactionOutcome::not_beneficial) {
            auto mapped = store_detail::public_compaction_result(worker_index, std::move(result));
            if (!mapped) {
                return unexpected(mapped.error());
            }
            last_no_gain = *mapped;
            continue;
        }
        return store_detail::public_compaction_result(worker_index, std::move(result));
    }
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}

auto Store::maintenance_snapshot() const -> MaintenanceSnapshot {
    if (!impl_ || !impl_->maintenance) {
        return MaintenanceSnapshot{};
    }
    auto snapshot = impl_->maintenance->snapshot();
    if (impl_->durable_runtime) {
        snapshot.rotation = impl_->durable_runtime->rotation_stats();
    }
    return snapshot;
}

auto Store::close() -> Status try { return impl_->close(); } catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    return store_detail::internal_failure();
}

auto Store::verify_index() const -> Status try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->verify_index();
    }
    auto& runtime = *impl_->volatile_runtime;
    std::vector<std::unique_lock<std::mutex>> worker_locks;
    worker_locks.reserve(runtime.workers.size());
    for (std::size_t index = 0; index < runtime.workers.size(); ++index) {
        worker_locks.emplace_back(runtime.workers.worker(index).mutex_);
    }

    const auto segments = runtime.segment_manager.segments();
    const auto rebuilt = rebuild_index_from_segments(segments, 0, impl_->routing);
    if (!rebuilt) {
        return unexpected(rebuilt.error());
    }
    if (rebuilt->index.stats().size != rebuilt->stats.records_visible) {
        return fail(ErrorCode::corrupted_data, "rebuilt index size does not match visible record count");
    }
    std::size_t worker_entry_count{};
    for (std::size_t index = 0; index < runtime.workers.size(); ++index) {
        const auto& worker = runtime.workers.worker(index);
        if (worker.index().stats().size > rebuilt->index.stats().size) {
            return fail(ErrorCode::corrupted_data, "worker index exceeds rebuilt index size");
        }
        for (const auto& entry : worker.index().entries()) {
            ++worker_entry_count;
            if (route_worker(hash_key_routing(entry.key, impl_->routing), runtime.workers.size()) != index) {
                return fail(ErrorCode::corrupted_data, "worker index entry is in the wrong partition");
            }
            const auto ref = rebuilt->index.find(entry.key);
            if (!ref || *ref != entry.record) {
                return fail(ErrorCode::corrupted_data, "worker index entry does not match rebuilt index");
            }
        }
    }
    if (worker_entry_count != rebuilt->index.stats().size) {
        return fail(ErrorCode::corrupted_data, "worker indexes do not contain every rebuilt entry");
    }
    for (const auto& entry : rebuilt->index.entries()) {
        const auto worker_index =
            route_worker(hash_key_routing(entry.key, impl_->routing), runtime.workers.size());
        const auto ref = runtime.workers.worker(worker_index).index().find(entry.key);
        if (!ref || *ref != entry.record) {
            return fail(ErrorCode::corrupted_data, "rebuilt entry is missing from its worker index");
        }
    }
    return {};
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return store_detail::internal_failure();
}


} // namespace glyphastore
