#include "glyphastore/store/store.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/bootstrap.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/store/maintenance.hpp"
#include "glyphastore/store/value.hpp"
#include "glyphastore/worker/pool.hpp"
#include "glyphastore/worker/topology.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore {
namespace {

[[nodiscard]] auto as_string_view(const std::span<const std::byte> bytes) noexcept -> std::string_view {
    if (bytes.empty()) {
        return {};
    }
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

[[nodiscard]] auto copy_value(const RecordView& record) -> OwnedValue {
    return OwnedValue{
        .bytes = std::vector<std::byte>{record.value.begin(), record.value.end()},
        .sequence = record.sequence.value,
        .expire_at_ns = record.expire_at_ns,
    };
}

auto durable_status(DurableMutationResult result) -> Status {
    if (result.committed() && !result.error) {
        return {};
    }
    auto error =
        result.error ? std::move(*result.error) : Error{ErrorCode::io_error, "durable mutation failed"};
    if (result.committed()) {
        error.code = ErrorCode::unavailable;
    }
    return unexpected(std::move(error));
}

auto public_compaction_result(const std::size_t worker_index, DurableCompactionResult result)
    -> Result<CompactionResult> {
    if (!result.compacted()) {
        auto error =
            result.error ? std::move(*result.error) : Error{ErrorCode::io_error, "durable compaction failed"};
        if (result.outcome == DurableCompactionOutcome::recovery_required) {
            error.code = ErrorCode::unavailable;
        }
        return unexpected(std::move(error));
    }
    return CompactionResult{
        .compacted = true,
        .worker_index = worker_index,
        .source_records_verified = result.stats.source_index_records_verified,
        .source_bytes_verified = result.stats.source_bytes_verified,
        .records_copied = result.stats.records_copied,
        .bytes_copied = result.stats.bytes_copied,
        .expired_records_dropped = result.stats.expired_records_dropped,
    };
}

auto public_compaction_result(const std::size_t worker_index, const VacuumStats& stats) -> CompactionResult {
    return CompactionResult{
        .compacted = true,
        .worker_index = worker_index,
        .source_records_verified = stats.source_records_verified,
        .source_bytes_verified = stats.source_bytes_verified,
        .records_copied = stats.records_copied,
        .bytes_copied = stats.bytes_copied,
        .expired_records_dropped = stats.expired_records_dropped,
    };
}

[[nodiscard]] auto resource_exhausted() -> Unexpected {
    return unexpected(Error{ErrorCode::resource_exhausted, {}});
}

[[nodiscard]] auto internal_failure() -> Unexpected {
    return unexpected(Error{ErrorCode::internal_error, {}});
}

[[nodiscard]] auto closed_store() -> Unexpected {
    return unexpected(Error{ErrorCode::unavailable, {}});
}

[[nodiscard]] auto reject_if_maintenance_emergency(MaintenanceController* controller) -> Status {
    if (controller != nullptr && controller->mutations_rejected()) {
        return fail(ErrorCode::storage_exhausted, std::string{kMaintenanceEmergencyMutationMessage});
    }
    return {};
}

auto data_directory_mode(const DurableOpenMode mode) noexcept -> DataDirectoryOpenMode {
    switch (mode) {
    case DurableOpenMode::open_existing:
        return DataDirectoryOpenMode::existing;
    case DurableOpenMode::create_new:
        return DataDirectoryOpenMode::create_new;
    case DurableOpenMode::open_or_create:
        return DataDirectoryOpenMode::open_or_create;
    }
    return DataDirectoryOpenMode::existing;
}

[[nodiscard]] auto validate_batch_config(const DurableGroupConfig& batch) -> Status {
    if (batch.max_records == 0 || batch.max_bytes == 0 || batch.max_wait_ms == 0 || batch.min_records == 0) {
        return fail(ErrorCode::invalid_argument,
                    "durable batching requires record, byte, and wait limits greater than zero");
    }
    if (batch.min_records > batch.max_records) {
        return fail(ErrorCode::invalid_argument,
                    "durable batching requires min_records no greater than max_records");
    }
    return {};
}

[[nodiscard]] auto system_time_ns() noexcept -> std::uint64_t {
    using namespace std::chrono;
    const auto elapsed = system_clock::now().time_since_epoch();
    if (elapsed <= system_clock::duration::zero()) {
        return 0;
    }

    const auto whole_seconds = duration_cast<seconds>(elapsed);
    const auto fractional_ns = duration_cast<nanoseconds>(elapsed - whole_seconds);
    constexpr auto kNanosecondsPerSecond = std::uint64_t{1'000'000'000};
    constexpr auto kMaximumSeconds = std::numeric_limits<std::uint64_t>::max() / kNanosecondsPerSecond;
    const auto seconds_count = static_cast<std::uint64_t>(whole_seconds.count());
    if (seconds_count > kMaximumSeconds) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    const auto base = seconds_count * kNanosecondsPerSecond;
    const auto fraction = static_cast<std::uint64_t>(fractional_ns.count());
    if (fraction > std::numeric_limits<std::uint64_t>::max() - base) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return base + fraction;
}

[[nodiscard]] auto sample_clock(const std::shared_ptr<const StoreClock>& clock) noexcept -> std::uint64_t {
    return clock ? clock->now_ns() : system_time_ns();
}

} // namespace

struct VolatileStoreRuntime {
    VolatileStoreRuntime(const SegmentId first_segment_id, const std::size_t worker_count)
        : segment_manager(first_segment_id), workers(segment_manager, worker_count) {}

    GlobalSegmentManager segment_manager;
    WorkerPool workers;
};

struct Store::Impl {
    enum class LifecycleState : std::uint8_t { open, closing, closed };

    // 128 bytes covers the widest cache line of the currently supported desktop/server targets.
    // Keeping admission counters on separate lines prevents unrelated Workers from contending on
    // Store lifecycle bookkeeping in the steady-state data path.
    struct alignas(128) ActiveOperationCounter {
        std::atomic<std::size_t> state{};
    };

    static constexpr auto kAdmissionClosed = std::size_t{1}
                                             << (std::numeric_limits<std::size_t>::digits - 1U);
    static constexpr auto kAdmissionCountMask = kAdmissionClosed - 1U;

    class OperationGuard final {
      public:
        OperationGuard(Impl& impl, const std::size_t shard) noexcept
            : impl_(impl.begin_operation(shard) ? &impl : nullptr), shard_(shard) {}
        ~OperationGuard() {
            if (impl_ != nullptr) {
                impl_->finish_operation(shard_);
            }
        }

        OperationGuard(const OperationGuard&) = delete;
        auto operator=(const OperationGuard&) -> OperationGuard& = delete;

        explicit operator bool() const noexcept {
            return impl_ != nullptr;
        }

      private:
        Impl* impl_{};
        std::size_t shard_{};
    };

    explicit Impl(std::unique_ptr<VolatileStoreRuntime> runtime, std::shared_ptr<const StoreClock> clock,
                  const std::uint64_t initial_now_ns)
        : worker_count_value(runtime->workers.size()),
          active_operations(std::make_unique<ActiveOperationCounter[]>(worker_count_value + 1)),
          volatile_runtime(std::move(runtime)), clock(std::move(clock)), latest_now_ns(initial_now_ns) {}
    explicit Impl(std::unique_ptr<DurableRuntimeCatalog> runtime, std::shared_ptr<const StoreClock> clock,
                  const std::uint64_t initial_now_ns)
        : worker_count_value(runtime->worker_count()),
          active_operations(std::make_unique<ActiveOperationCounter[]>(worker_count_value + 1)),
          durable_runtime(std::move(runtime)), clock(std::move(clock)), latest_now_ns(initial_now_ns) {}

    [[nodiscard]] auto control_shard() const noexcept -> std::size_t {
        return worker_count_value;
    }

    [[nodiscard]] auto begin_operation(const std::size_t shard) noexcept -> bool {
        // The RMW reads its immediate predecessor in this shard's modification order. It therefore
        // either precedes close_admission() and is counted, or follows it and observes the closed bit.
        const auto previous = active_operations[shard].state.fetch_add(1, std::memory_order_relaxed);
        if ((previous & kAdmissionClosed) == 0) {
            return true;
        }
        finish_operation(shard);
        return false;
    }

    void finish_operation(const std::size_t shard) noexcept {
        const auto previous = active_operations[shard].state.fetch_sub(1, std::memory_order_acq_rel);
        if ((previous & kAdmissionClosed) != 0 && (previous & kAdmissionCountMask) == 1U) {
            lifecycle_changed.notify_all();
        }
    }

    void close_admission() noexcept {
        // Each RMW is totally ordered with admission on that shard. Completion of this loop is the
        // close linearization point: every later attempt observes its shard's closed bit.
        for (std::size_t shard = 0; shard <= worker_count_value; ++shard) {
            active_operations[shard].state.fetch_or(kAdmissionClosed, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] auto no_active_operations() const noexcept -> bool {
        for (std::size_t shard = 0; shard <= worker_count_value; ++shard) {
            if ((active_operations[shard].state.load(std::memory_order_acquire) & kAdmissionCountMask) != 0) {
                return false;
            }
        }
        return true;
    }

    void mark_durable_fail_closed() noexcept {
        OperationGuard operation{*this, control_shard()};
        if (operation && durable_runtime) {
            durable_runtime->mark_fail_closed();
        }
    }

    [[nodiscard]] auto close_status_locked() const -> Status {
        if (!close_error) {
            return {};
        }
        try {
            return unexpected(*close_error);
        } catch (const std::bad_alloc&) {
            return resource_exhausted();
        } catch (...) {
            return internal_failure();
        }
    }

    [[nodiscard]] auto operational() const noexcept -> bool {
        if (lifecycle.load(std::memory_order_acquire) != LifecycleState::open) {
            return false;
        }
        return !durable_runtime || durable_runtime->healthy();
    }

    [[nodiscard]] auto close() -> Status {
        auto expected = LifecycleState::open;
        if (!lifecycle.compare_exchange_strong(expected, LifecycleState::closing, std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
            std::unique_lock lock{lifecycle_mutex};
            lifecycle_changed.wait(
                lock, [&] { return lifecycle.load(std::memory_order_acquire) == LifecycleState::closed; });
            return close_status_locked();
        }

        struct CloseFinalizer {
            Impl* impl;

            ~CloseFinalizer() {
                if (impl == nullptr) {
                    return;
                }
                try {
                    if (!impl->close_error) {
                        impl->close_error.emplace(Error{ErrorCode::internal_error, {}});
                    }
                } catch (...) {
                }
                impl->lifecycle.store(LifecycleState::closed, std::memory_order_release);
                impl->lifecycle_changed.notify_all();
            }
        } finalizer{this};

        close_admission();
        // Stop new maintenance evaluations, then drain admitted ops (including compact),
        // then join the controller — matching DurableFlushCoordinator shutdown ordering.
        if (maintenance) {
            maintenance->request_stop();
        }
        Status result;
        try {
            if (durable_runtime) {
                durable_runtime->request_close_flush();
            }
        } catch (const std::bad_alloc&) {
            result = resource_exhausted();
        } catch (...) {
            result = internal_failure();
        }
        if (!result && durable_runtime) {
            // Release any strict-group producer even when posting the close flush itself failed.
            durable_runtime->mark_fail_closed();
        }
        {
            std::unique_lock lock{lifecycle_mutex};
            lifecycle_changed.wait(lock, [&] { return no_active_operations(); });
        }
        if (maintenance) {
            maintenance->join();
        }

        try {
            if (durable_runtime) {
                auto closed = durable_runtime->close();
                if (result && !closed) {
                    result = std::move(closed);
                }
            }
        } catch (const std::bad_alloc&) {
            if (result) {
                result = resource_exhausted();
            }
        } catch (...) {
            if (result) {
                result = internal_failure();
            }
        }
        durable_runtime.reset();
        volatile_runtime.reset();
        maintenance.reset();
        clock.reset();

        Status final_status;
        {
            const std::lock_guard lock{lifecycle_mutex};
            if (!result) {
                close_error.emplace(std::move(result.error()));
            }
            lifecycle.store(LifecycleState::closed, std::memory_order_release);
            final_status = close_status_locked();
        }
        finalizer.impl = nullptr;
        lifecycle_changed.notify_all();
        return final_status;
    }

    [[nodiscard]] auto now_ns() noexcept -> std::uint64_t {
        const auto sampled = sample_clock(clock);
        auto observed = latest_now_ns.load(std::memory_order_relaxed);
        while (observed < sampled &&
               !latest_now_ns.compare_exchange_weak(observed, sampled, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
        }
        return std::max(observed, sampled);
    }

    std::size_t worker_count_value{};
    std::unique_ptr<ActiveOperationCounter[]> active_operations;
    std::unique_ptr<VolatileStoreRuntime> volatile_runtime;
    std::unique_ptr<DurableRuntimeCatalog> durable_runtime;
    std::shared_ptr<const StoreClock> clock;
    std::atomic<std::uint64_t> latest_now_ns{};
    std::mutex compaction_mutex;
    std::atomic_size_t next_compaction_worker{};
    std::unique_ptr<MaintenanceController> maintenance;
    std::atomic<LifecycleState> lifecycle{LifecycleState::open};
    mutable std::mutex lifecycle_mutex;
    std::condition_variable lifecycle_changed;
    std::optional<Error> close_error;
};

struct detail::PreparedColdRead::State final {
    explicit State(DurableRuntimeCatalog::PinnedRead prepared) : prepared(std::move(prepared)) {}
    DurableRuntimeCatalog::PinnedRead prepared;
};

detail::PreparedColdRead::PreparedColdRead(std::unique_ptr<State> state) noexcept
    : state_(std::move(state)) {}

detail::PreparedColdRead::PreparedColdRead(PreparedColdRead&&) noexcept = default;

auto detail::PreparedColdRead::operator=(PreparedColdRead&&) noexcept -> PreparedColdRead& = default;

detail::PreparedColdRead::~PreparedColdRead() = default;

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
        const auto initial_now_ns = sample_clock(config.clock);
        auto impl = std::make_unique<Impl>(std::make_unique<VolatileStoreRuntime>(SegmentId{1}, count),
                                           config.clock, initial_now_ns);
        auto store = std::unique_ptr<Store>(new Store(std::move(impl)));
        store->impl_->maintenance = std::make_unique<MaintenanceController>(config.maintenance);
        Store* const raw = store.get();
        store->impl_->maintenance->bind_compact(
            [raw](const std::optional<std::size_t> preferred_worker,
                  const std::uint64_t max_copy_bytes) -> Result<CompactionResult> {
                return raw->compact_for_maintenance(preferred_worker, max_copy_bytes);
            });
        store->impl_->maintenance->bind_observe(
            []() -> Result<MaintenanceObservation> { return MaintenanceObservation{.durable = false}; });
        store->impl_->maintenance->start();
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
        if (auto valid = validate_batch_config(config.durable_group); !valid) {
            return unexpected(valid.error());
        }
    }
    if (config.storage_mode == StorageMode::durable_periodic && config.durable_periodic.batch) {
        if (auto valid = validate_batch_config(*config.durable_periodic.batch); !valid) {
            return unexpected(valid.error());
        }
    }
    auto directory = DataDirectory::open_and_lock(
        *config.data_directory, data_directory_mode(config.durable_open_mode), config.filesystem_hooks);
    if (!directory) {
        return unexpected(directory.error());
    }
    if (auto prepared = prepare_durable_store(*directory, config.durable_open_mode, count,
                                              config.worker_config.explicit_count, config.durable_limits);
        !prepared) {
        return unexpected(prepared.error());
    }
    DurableRuntimeOptions runtime_options{};
    runtime_options.limits = config.durable_limits;
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
    const auto recovery_now_ns = sample_clock(config.clock);
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
    store->impl_->maintenance->bind_observe([raw]() -> Result<MaintenanceObservation> {
        if (!raw->impl_ || !raw->impl_->durable_runtime) {
            return fail(ErrorCode::unavailable, "durable runtime is unavailable");
        }
        auto observation = raw->impl_->durable_runtime->maintenance_observation(
            raw->impl_->next_compaction_worker.load(std::memory_order_relaxed));
        if (observation && observation->compaction_candidate_worker) {
            raw->impl_->next_compaction_worker.store((*observation->compaction_candidate_worker + 1U) %
                                                         raw->impl_->worker_count_value,
                                                     std::memory_order_relaxed);
        }
        return observation;
    });
    store->impl_->maintenance->start();
    return store;
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    return internal_failure();
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
    const auto hashed = HashedKey::compute(key);
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    const auto now_ns = impl_->now_ns();
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(hashed, now_ns);
    }
    auto& worker = impl_->volatile_runtime->workers.route(hashed);
    const std::lock_guard lock{worker.mutex_};
    auto record = worker.get_locked(hashed, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return copy_value(*record);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::get_copy(const std::span<const std::byte> key) -> Result<OwnedValue> try {
    const auto hashed = HashedKey::compute(as_string_view(key));
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    const auto now_ns = impl_->now_ns();
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(hashed, now_ns);
    }
    auto& worker = impl_->volatile_runtime->workers.route(hashed);
    const std::lock_guard lock{worker.mutex_};
    auto record = worker.get_locked(hashed, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return copy_value(*record);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::put(const std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    const auto hashed = HashedKey::compute(key);
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->put(hashed, value, expire_at_ns));
    }
    return impl_->volatile_runtime->workers.route(hashed).put(hashed, value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    const auto hashed = HashedKey::compute(as_string_view(key));
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->put(hashed, value, expire_at_ns));
    }
    return impl_->volatile_runtime->workers.route(hashed).put(hashed, value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::erase(const std::string_view key) -> Status try {
    const auto hashed = HashedKey::compute(key);
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->erase(hashed));
    }
    return impl_->volatile_runtime->workers.route(hashed).erase(hashed);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::erase(const std::span<const std::byte> key) -> Status try {
    const auto hashed = HashedKey::compute(as_string_view(key));
    Impl::OperationGuard operation{*impl_, route_worker(hashed.hash, impl_->worker_count_value)};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->erase(hashed));
    }
    return impl_->volatile_runtime->workers.route(hashed).erase(hashed);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::flush() -> Status try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return closed_store();
    }
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->flush();
    }
    return {};
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto Store::compact() -> Result<CompactionResult> {
    return compact_for_maintenance(std::nullopt, 0);
}

auto Store::compact_for_maintenance(const std::optional<std::size_t> preferred_worker,
                                    const std::uint64_t max_copy_bytes) -> Result<CompactionResult> try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return closed_store();
    }
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
                return public_compaction_result(worker_index, **result);
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
        if (result.outcome == DurableCompactionOutcome::not_beneficial ||
            (result.outcome == DurableCompactionOutcome::not_compacted && result.error.has_value() &&
             result.error->code == ErrorCode::not_found)) {
            return CompactionResult{};
        }
        return public_compaction_result(*preferred_worker, std::move(result));
    }

    std::optional<std::size_t> first_candidate;
    for (;;) {
        auto candidate = impl_->durable_runtime->next_compaction_worker(
            impl_->next_compaction_worker.load(std::memory_order_relaxed));
        if (!candidate) {
            return unexpected(candidate.error());
        }
        if (!*candidate || (first_candidate && **candidate == *first_candidate)) {
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
            continue;
        }
        return public_compaction_result(worker_index, std::move(result));
    }
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
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
    return resource_exhausted();
} catch (...) {
    return internal_failure();
}

auto Store::verify_index() const -> Status try {
    Impl::OperationGuard operation{*impl_, impl_->control_shard()};
    if (!operation) {
        return closed_store();
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
    const auto rebuilt = rebuild_index_from_segments(segments);
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
            if (route_worker(entry.key, runtime.workers.size()) != index) {
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
        const auto worker_index = route_worker(entry.key, runtime.workers.size());
        const auto ref = runtime.workers.worker(worker_index).index().find(entry.key);
        if (!ref || *ref != entry.record) {
            return fail(ErrorCode::corrupted_data, "rebuilt entry is missing from its worker index");
        }
    }
    return {};
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    impl_->mark_durable_fail_closed();
    return internal_failure();
}

auto detail::StoreAccess::get_owned(Store& store, const std::size_t worker_index, const HashedKey& key,
                                    const std::uint64_t now_ns) -> Result<OwnedValue> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return closed_store();
    }
    if (store.impl_->durable_runtime) {
        return store.impl_->durable_runtime->get(key, now_ns);
    }
    auto record = store.impl_->volatile_runtime->workers.worker(worker_index).get_locked(key, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return copy_value(*record);
}

auto detail::StoreAccess::prepare_get_owned(Store& store, const std::size_t worker_index,
                                            const HashedKey& key, const std::uint64_t now_ns)
    -> Result<PreparedGet> try {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return closed_store();
    }
    if (store.impl_->durable_runtime) {
        auto prepared = store.impl_->durable_runtime->prepare_get(key, now_ns);
        if (!prepared) {
            return unexpected(prepared.error());
        }
        if (prepared->value) {
            return PreparedGet{.value = std::move(prepared->value)};
        }
        if (!prepared->cold) {
            return fail(ErrorCode::internal_error, "durable GET preparation produced no result");
        }
        auto state = std::make_unique<PreparedColdRead::State>(std::move(*prepared->cold));
        return PreparedGet{.cold = PreparedColdRead{std::move(state)}};
    }
    auto record = store.impl_->volatile_runtime->workers.worker(worker_index).get_locked(key, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return PreparedGet{.value = copy_value(*record)};
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    return internal_failure();
}

auto detail::StoreAccess::complete_get_owned(Store& store, const std::size_t worker_index,
                                             PreparedColdRead read, const std::atomic_bool* cancelled)
    -> Result<OwnedValue> {
    if (worker_index >= store.worker_count()) {
        return fail(ErrorCode::invalid_argument, "cold get targets an invalid Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return closed_store();
    }
    if (!store.impl_->durable_runtime || !read.state_ ||
        read.state_->prepared.worker_index_ != worker_index) {
        return fail(ErrorCode::invalid_argument, "cold get has no matching durable Worker owner");
    }
    return store.impl_->durable_runtime->complete_get(std::move(read.state_->prepared), cancelled);
}

auto detail::StoreAccess::put(Store& store, const std::size_t worker_index, const HashedKey& key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
    -> Status {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive put targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(store.impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (store.impl_->durable_runtime) {
        return durable_status(store.impl_->durable_runtime->put(key, value, expire_at_ns));
    }
    return store.impl_->volatile_runtime->workers.worker(worker_index).put_locked(key, value, expire_at_ns);
}

auto detail::StoreAccess::erase(Store& store, const std::size_t worker_index, const HashedKey& key)
    -> Status {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive erase targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return closed_store();
    }
    if (auto rejected = reject_if_maintenance_emergency(store.impl_->maintenance.get()); !rejected) {
        return rejected;
    }
    if (store.impl_->durable_runtime) {
        return durable_status(store.impl_->durable_runtime->erase(key));
    }
    return store.impl_->volatile_runtime->workers.worker(worker_index).erase_locked(key);
}

auto detail::StoreAccess::is_durable(const Store& store) noexcept -> bool {
    return store.impl_->durable_runtime != nullptr;
}

auto detail::StoreAccess::batch_stats(const Store& store) -> std::vector<DurableBatchWorkerStats> {
    return store.impl_->durable_runtime ? store.impl_->durable_runtime->batch_stats()
                                        : std::vector<DurableBatchWorkerStats>{};
}

auto detail::StoreAccess::maintenance_controller(Store& store) noexcept -> MaintenanceController* {
    return store.impl_->maintenance.get();
}

auto detail::StoreAccess::maintenance_mutations_rejected(const Store& store) noexcept -> bool {
    return store.impl_->maintenance != nullptr && store.impl_->maintenance->mutations_rejected();
}

auto detail::StoreAccess::operational(const Store& store) noexcept -> bool {
    return store.impl_ != nullptr && store.impl_->operational();
}

auto detail::StoreAccess::worker(const Store& store, const std::size_t index) noexcept -> const Worker& {
    if (!store.impl_->volatile_runtime) {
        std::terminate();
    }
    return store.impl_->volatile_runtime->workers.worker(index);
}

auto detail::StoreAccess::segments(const Store& store) -> std::vector<SegmentPtr> {
    if (!store.impl_->volatile_runtime) {
        return {};
    }
    return store.impl_->volatile_runtime->segment_manager.segments();
}

} // namespace glyphastore
