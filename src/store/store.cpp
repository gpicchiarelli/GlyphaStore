#include "glyphastore/store/store.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/bootstrap.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/store/value.hpp"
#include "glyphastore/worker/pool.hpp"
#include "glyphastore/worker/topology.hpp"
#include "store/store_internal.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>
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

[[nodiscard]] auto as_bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
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

[[nodiscard]] auto resource_exhausted() -> Unexpected {
    return unexpected(Error{ErrorCode::resource_exhausted, {}});
}

[[nodiscard]] auto internal_failure() -> Unexpected {
    return unexpected(Error{ErrorCode::internal_error, {}});
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
    if (batch.max_records == 0 || batch.max_bytes == 0 || batch.max_wait_ms == 0) {
        return fail(ErrorCode::invalid_argument,
                    "durable batching requires max_records, max_bytes, and max_wait_ms greater than zero");
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
    explicit Impl(std::unique_ptr<VolatileStoreRuntime> runtime, std::shared_ptr<const StoreClock> clock,
                  const std::uint64_t initial_now_ns)
        : volatile_runtime(std::move(runtime)), clock(std::move(clock)), latest_now_ns(initial_now_ns) {}
    explicit Impl(std::unique_ptr<DurableRuntimeCatalog> runtime, std::shared_ptr<const StoreClock> clock,
                  const std::uint64_t initial_now_ns)
        : durable_runtime(std::move(runtime)), clock(std::move(clock)), latest_now_ns(initial_now_ns) {}

    [[nodiscard]] auto now_ns() noexcept -> std::uint64_t {
        const auto sampled = sample_clock(clock);
        auto observed = latest_now_ns.load(std::memory_order_relaxed);
        while (observed < sampled &&
               !latest_now_ns.compare_exchange_weak(observed, sampled, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
        }
        return std::max(observed, sampled);
    }

    std::unique_ptr<VolatileStoreRuntime> volatile_runtime;
    std::unique_ptr<DurableRuntimeCatalog> durable_runtime;
    std::shared_ptr<const StoreClock> clock;
    std::atomic<std::uint64_t> latest_now_ns{};
};

Store::Store(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Store::~Store() = default;

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
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    if (config.storage_mode == StorageMode::volatile_memory) {
        if (config.data_directory || config.durable_open_mode != DurableOpenMode::open_or_create) {
            return fail(ErrorCode::invalid_argument,
                        "volatile storage cannot use durable-only configuration");
        }
        const auto initial_now_ns = sample_clock(config.clock);
        auto impl = std::make_unique<Impl>(std::make_unique<VolatileStoreRuntime>(SegmentId{1}, count),
                                           config.clock, initial_now_ns);
        return std::unique_ptr<Store>(new Store(std::move(impl)));
    }
    if (!config.data_directory || config.data_directory->empty()) {
        return fail(ErrorCode::invalid_argument, "durable storage requires a data directory");
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
    auto directory =
        DataDirectory::open_and_lock(*config.data_directory, data_directory_mode(config.durable_open_mode));
    if (!directory) {
        return unexpected(directory.error());
    }
    if (auto prepared = prepare_durable_store(*directory, config.durable_open_mode, count,
                                              config.worker_config.explicit_count);
        !prepared) {
        return unexpected(prepared.error());
    }
    DurableRuntimeOptions runtime_options{};
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
    return std::unique_ptr<Store>(new Store(std::move(impl)));
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    return internal_failure();
}

auto Store::worker_count() const noexcept -> std::size_t {
    return impl_->volatile_runtime ? impl_->volatile_runtime->workers.size()
                                   : impl_->durable_runtime->worker_count();
}

auto Store::get(const std::string_view key) -> Result<OwnedValue> {
    return get_copy(key);
}

auto Store::get(const std::span<const std::byte> key) -> Result<OwnedValue> {
    return get_copy(key);
}

auto Store::get_copy(const std::string_view key) -> Result<OwnedValue> try {
    const auto now_ns = impl_->now_ns();
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(key, now_ns);
    }
    const auto hashed = HashedKey::compute(key);
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
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::get_copy(const std::span<const std::byte> key) -> Result<OwnedValue> try {
    const auto now_ns = impl_->now_ns();
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->get(key, now_ns);
    }
    const auto hashed = HashedKey::compute(as_string_view(key));
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
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::put(const std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->put(as_bytes(key), value, expire_at_ns));
    }
    const auto hashed = HashedKey::compute(key);
    return impl_->volatile_runtime->workers.route(hashed).put(hashed, value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status try {
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->put(key, value, expire_at_ns));
    }
    return put(as_string_view(key), value, expire_at_ns);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::erase(const std::string_view key) -> Status try {
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->erase(as_bytes(key)));
    }
    const auto hashed = HashedKey::compute(key);
    return impl_->volatile_runtime->workers.route(hashed).erase(hashed);
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::erase(const std::span<const std::byte> key) -> Status try {
    if (impl_->durable_runtime) {
        return durable_status(impl_->durable_runtime->erase(key));
    }
    return erase(as_string_view(key));
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::flush() -> Status try {
    if (impl_->durable_runtime) {
        return impl_->durable_runtime->flush();
    }
    return {};
} catch (const std::bad_alloc&) {
    return resource_exhausted();
} catch (...) {
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto Store::verify_index() const -> Status try {
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
    if (impl_->durable_runtime) {
        impl_->durable_runtime->mark_fail_closed();
    }
    return internal_failure();
}

auto detail::StoreAccess::get_owned(Store& store, const std::size_t worker_index, const HashedKey& key,
                                    const std::uint64_t now_ns) -> Result<OwnedValue> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
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

auto detail::StoreAccess::put(Store& store, const std::size_t worker_index, const HashedKey& key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
    -> Status {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive put targets the wrong Worker owner");
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
    if (store.impl_->durable_runtime) {
        return durable_status(store.impl_->durable_runtime->erase(key));
    }
    return store.impl_->volatile_runtime->workers.worker(worker_index).erase_locked(key);
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
