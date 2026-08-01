#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/paired/shard_pair_runtime.hpp"
#include "glyphastore/store/prepared_read.hpp"
#include "glyphastore/store/store.hpp"
#include "glyphastore/store/value.hpp"
#include "glyphastore/worker/topology.hpp"
#include "store/store_impl.hpp"
#include "store/store_internal.hpp"

#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace glyphastore {

auto detail::StoreAccess::get_owned(Store& store, const std::size_t worker_index, const HashedKey& key,
                                    const std::uint64_t now_ns) -> Result<OwnedValue> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (store.impl_->durable_runtime) {
        return store.impl_->durable_runtime->get(key, now_ns);
    }
    auto record = store.impl_->volatile_runtime->workers.worker(worker_index).get_locked(key, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return store_detail::copy_value(*record);
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
        return store_detail::closed_store();
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
        return PreparedGet{.cold = PreparedColdRead{PreparedColdRead::State{std::move(*prepared->cold)}}};
    }
    auto record = store.impl_->volatile_runtime->workers.worker(worker_index).get_locked(key, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return PreparedGet{.value = store_detail::copy_value(*record)};
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    return store_detail::internal_failure();
}

auto detail::StoreAccess::complete_get_owned(Store& store, const std::size_t worker_index,
                                             PreparedColdRead read, const ColdReadCancellation* cancellation,
                                             std::vector<std::byte>* scratch) -> Result<OwnedValue> {
    if (worker_index >= store.worker_count()) {
        return fail(ErrorCode::invalid_argument, "cold get targets an invalid Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (!store.impl_->durable_runtime || !read.engaged_) {
        return fail(ErrorCode::invalid_argument, "cold get has no matching durable Worker owner");
    }
    return std::visit(
        [&](auto& prepared) -> Result<OwnedValue> {
            if (prepared.worker_index_ != worker_index) {
                return fail(ErrorCode::invalid_argument, "cold get has no matching durable Worker owner");
            }
            return store.impl_->durable_runtime->complete_get(std::move(prepared), cancellation, scratch);
        },
        read.state()->prepared);
}

auto detail::StoreAccess::snapshot_durable_reads(Store& store, const std::size_t worker_index)
    -> Result<DurableReadSnapshot> {
    if (worker_index >= store.worker_count()) {
        return fail(ErrorCode::invalid_argument,
                    "durable read-generation snapshot targets an invalid Worker");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (!store.impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "durable read-generation snapshot requires a durable Store");
    }
    return store.impl_->durable_runtime->snapshot_published_reads(worker_index);
}

auto detail::StoreAccess::durable_read_catalog_revision(const Store& store,
                                                        const std::size_t worker_index) noexcept
    -> std::uint64_t {
    return store.impl_ && store.impl_->durable_runtime
               ? store.impl_->durable_runtime->read_catalog_revision(worker_index)
               : 0U;
}

auto detail::StoreAccess::capture_durable_read(Store& store, const std::size_t worker_index,
                                               const HashedKey& key) -> Result<DurablePublishedRead> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "durable read publication targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (!store.impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "durable read publication requires a durable Store");
    }
    return store.impl_->durable_runtime->capture_published_read(worker_index, key);
}

auto detail::StoreAccess::prepare_published_durable_get(Store& store, const std::size_t worker_index,
                                                        DurablePublishedReadView read,
                                                        const std::uint64_t now_ns)
    -> Result<PreparedGet> try {
    if (worker_index >= store.worker_count() || read.worker_index() != worker_index) {
        return fail(ErrorCode::invalid_argument, "immutable durable GET targets the wrong Worker owner");
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return store_detail::closed_store();
    }
    if (!store.impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "immutable durable GET requires a durable Store");
    }
    auto prepared = store.impl_->durable_runtime->prepare_published_get(read, now_ns);
    if (!prepared) {
        return unexpected(prepared.error());
    }
    if (prepared->value) {
        return PreparedGet{.value = std::move(prepared->value)};
    }
    if (!prepared->borrowed_cold) {
        return fail(ErrorCode::internal_error, "immutable durable GET preparation produced no result");
    }
    return PreparedGet{.cold =
                           PreparedColdRead{PreparedColdRead::State{std::move(*prepared->borrowed_cold)}}};
} catch (const std::bad_alloc&) {
    return store_detail::resource_exhausted();
} catch (...) {
    return store_detail::internal_failure();
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
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
        !rejected) {
        return rejected;
    }
    if (store.impl_->durable_runtime) {
        return store_detail::durable_status(store.impl_->durable_runtime->put(key, value, expire_at_ns));
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
        return store_detail::closed_store();
    }
    if (auto rejected = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
        !rejected) {
        return rejected;
    }
    if (store.impl_->durable_runtime) {
        return store_detail::durable_status(store.impl_->durable_runtime->erase(key));
    }
    return store.impl_->volatile_runtime->workers.worker(worker_index).erase_locked(key);
}

auto detail::StoreAccess::put_volatile_published(Store& store, const std::size_t worker_index,
                                                 const HashedKey& key, const std::span<const std::byte> value,
                                                 const std::uint64_t expire_at_ns,
                                                 const PublishedAdmission admission)
    -> Result<VolatileMutationPublication> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "published put targets the wrong Worker owner");
    }
    std::optional<Store::Impl::OperationGuard> operation;
    if (admission == PublishedAdmission::check_admission) {
        operation.emplace(*store.impl_, worker_index);
        if (!*operation) {
            return store_detail::closed_store();
        }
        if (auto rejected = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
            !rejected) {
            return unexpected(rejected.error());
        }
    }
    if (!store.impl_->volatile_runtime) {
        return fail(ErrorCode::invalid_argument, "published volatile put requires a volatile Store");
    }
    auto published = store.impl_->volatile_runtime->workers.worker(worker_index)
                         .put_locked_published(key, value, expire_at_ns);
    if (!published) {
        return unexpected(published.error());
    }
    return VolatileMutationPublication{
        .record = published->record, .segment = std::move(published->segment), .opcode = published->opcode};
}

auto detail::StoreAccess::erase_volatile_published(Store& store, const std::size_t worker_index,
                                                   const HashedKey& key,
                                                   const PublishedAdmission admission)
    -> Result<VolatileMutationPublication> {
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "published erase targets the wrong Worker owner");
    }
    std::optional<Store::Impl::OperationGuard> operation;
    if (admission == PublishedAdmission::check_admission) {
        operation.emplace(*store.impl_, worker_index);
        if (!*operation) {
            return store_detail::closed_store();
        }
        if (auto rejected = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
            !rejected) {
            return unexpected(rejected.error());
        }
    }
    if (!store.impl_->volatile_runtime) {
        return fail(ErrorCode::invalid_argument, "published volatile erase requires a volatile Store");
    }
    auto published = store.impl_->volatile_runtime->workers.worker(worker_index).erase_locked_published(key);
    if (!published) {
        return unexpected(published.error());
    }
    return VolatileMutationPublication{
        .record = published->record, .segment = std::move(published->segment), .opcode = published->opcode};
}

auto detail::StoreAccess::put_durable(Store& store, const std::size_t worker_index, const HashedKey& key,
                                      const std::span<const std::byte> value,
                                      const std::uint64_t expire_at_ns) -> DurableMutationResult {
    const auto rejected = [](Error error) {
        return DurableMutationResult{.outcome = DurableMutationOutcome::not_committed,
                                     .sequence = std::nullopt,
                                     .error = std::move(error)};
    };
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return rejected(Error{ErrorCode::invalid_argument, "exclusive put targets the wrong Worker owner"});
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return rejected(Error{ErrorCode::unavailable, "Store is closed"});
    }
    if (auto allowed = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
        !allowed) {
        return rejected(allowed.error());
    }
    if (!store.impl_->durable_runtime) {
        return rejected(Error{ErrorCode::invalid_argument, "durable put requires a durable Store"});
    }
    return store.impl_->durable_runtime->put(key, value, expire_at_ns);
}

auto detail::StoreAccess::erase_durable(Store& store, const std::size_t worker_index, const HashedKey& key)
    -> DurableMutationResult {
    const auto rejected = [](Error error) {
        return DurableMutationResult{.outcome = DurableMutationOutcome::not_committed,
                                     .sequence = std::nullopt,
                                     .error = std::move(error)};
    };
    if (worker_index >= store.worker_count() ||
        route_worker(key.hash, store.worker_count()) != worker_index) {
        return rejected(Error{ErrorCode::invalid_argument, "exclusive erase targets the wrong Worker owner"});
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        return rejected(Error{ErrorCode::unavailable, "Store is closed"});
    }
    if (auto allowed = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
        !allowed) {
        return rejected(allowed.error());
    }
    if (!store.impl_->durable_runtime) {
        return rejected(Error{ErrorCode::invalid_argument, "durable erase requires a durable Store"});
    }
    return store.impl_->durable_runtime->erase(key);
}

auto detail::StoreAccess::durable_writer_batch_config(const Store& store) noexcept
    -> std::optional<DurableGroupConfig> {
    if (!store.impl_->durable_runtime) {
        return std::nullopt;
    }
    return store.impl_->durable_runtime->writer_batch_config();
}

auto detail::StoreAccess::mutate_durable_batch(Store& store, const std::size_t worker_index,
                                               const std::span<const DurableMutationView> mutations)
    -> std::vector<DurableWriterBatchResult> {
    std::vector<DurableWriterBatchResult> results;
    results.reserve(mutations.size());
    const auto reject_remaining = [&](const Error& error) {
        while (results.size() < mutations.size()) {
            results.push_back({.mutation = {.outcome = DurableMutationOutcome::not_committed,
                                            .sequence = std::nullopt,
                                            .error = error}});
        }
    };
    if (mutations.empty()) {
        return results;
    }
    if (worker_index >= store.worker_count()) {
        reject_remaining(Error{ErrorCode::invalid_argument, "Writer batch targets an invalid Worker"});
        return results;
    }
    for (const auto& mutation : mutations) {
        if (route_worker(mutation.key.hash, store.worker_count()) != worker_index) {
            reject_remaining(
                Error{ErrorCode::invalid_argument, "Writer batch contains a key owned by another Worker"});
            return results;
        }
    }
    Store::Impl::OperationGuard operation{*store.impl_, worker_index};
    if (!operation) {
        reject_remaining(Error{ErrorCode::unavailable, "Store is closed"});
        return results;
    }
    if (auto allowed = store_detail::reject_if_maintenance_emergency(store.impl_->maintenance.get());
        !allowed) {
        reject_remaining(allowed.error());
        return results;
    }
    if (!store.impl_->durable_runtime || !store.impl_->durable_runtime->writer_batch_config().has_value()) {
        reject_remaining(Error{ErrorCode::invalid_argument, "Writer batching requires durable-group mode"});
        return results;
    }

    for (const auto& mutation : mutations) {
        const auto key_bytes = std::as_bytes(std::span{mutation.key.key.data(), mutation.key.key.size()});
        DurableWriterBatchResult result;
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            result.mutation = store.impl_->durable_runtime->mutate(
                key_bytes, mutation.value,
                mutation.operation == MutationOperation::put ? Opcode::put : Opcode::erase, mutation.key.hash,
                mutation.expire_at_ns, ValueType::bytes, 0, true);
            if (!should_retry_durable_mutation(result.mutation, attempt)) {
                break;
            }
            result.conflict_retried = true;
        }
        results.push_back(std::move(result));
        if (!store.impl_->durable_runtime->healthy()) {
            reject_remaining(Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
            break;
        }
    }

    const auto committed = store.impl_->durable_runtime->commit_writer_batch(worker_index);
    if (!committed) {
        for (auto& result : results) {
            if (result.mutation.committed() && !result.mutation.error) {
                result.mutation.outcome = DurableMutationOutcome::indeterminate;
                result.mutation.error = committed.error();
            }
        }
    }
    return results;
}

auto detail::StoreAccess::is_durable(const Store& store) noexcept -> bool {
    return store.impl_->durable_runtime != nullptr;
}

auto detail::StoreAccess::worker_routing(const Store& store) noexcept -> WorkerRoutingState {
    return store.impl_->routing;
}

auto detail::StoreAccess::batch_stats(const Store& store) -> std::vector<DurableBatchWorkerStats> {
    return store.impl_->durable_runtime ? store.impl_->durable_runtime->batch_stats()
                                        : std::vector<DurableBatchWorkerStats>{};
}

auto detail::StoreAccess::maintenance_controller(Store& store) noexcept -> MaintenanceController* {
    return store.impl_->maintenance.get();
}

void detail::StoreAccess::report_foreground_latency(Store& store, const std::uint64_t latency_ns) noexcept {
    if (store.impl_->maintenance) {
        store.impl_->maintenance->report_foreground_latency(latency_ns);
    }
}

auto detail::StoreAccess::maintenance_mutations_rejected(const Store& store) noexcept -> bool {
    return store.impl_->maintenance != nullptr && store.impl_->maintenance->mutations_rejected();
}

auto detail::StoreAccess::operational(const Store& store) noexcept -> bool {
    return store.impl_ != nullptr && store.impl_->operational();
}

void detail::StoreAccess::mark_fail_closed(Store& store) noexcept {
    if (store.impl_ != nullptr) {
        store.impl_->mark_durable_fail_closed();
    }
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

auto detail::StoreAccess::snapshot_live_keys(Store& store) -> Result<std::vector<std::string>> {
    if (!store.impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "live-key snapshot requires a durable Store");
    }
    Store::Impl::OperationGuard operation{*store.impl_, store.impl_->control_shard()};
    if (!operation) {
        return store_detail::closed_store();
    }
    return store.impl_->durable_runtime->snapshot_live_keys();
}

auto detail::StoreAccess::durable_manifest(const Store& store) -> Result<Manifest> {
    if (!store.impl_->durable_runtime) {
        return fail(ErrorCode::invalid_argument, "durable manifest requires a durable Store");
    }
    return store.impl_->durable_runtime->manifest();
}

[[nodiscard]] auto detail::StoreAccess::attach_paired_runtime(Store& store, const StoreConfig& config)
    -> Status {
    if (config.concurrency != StoreConcurrencyMode::paired) {
        store.impl_->concurrency = config.concurrency;
        return {};
    }
    if (store.impl_->volatile_runtime) {
        for (std::size_t index = 0; index < store.impl_->worker_count_value; ++index) {
            store.impl_->volatile_runtime->workers.worker(index).set_exclusive_writer(true);
        }
    }
    auto runtime = store::paired::ShardPairRuntime::create(store, config.paired);
    if (!runtime) {
        return unexpected(runtime.error());
    }
    if (auto started = (*runtime)->start(); !started) {
        return started;
    }
    store.impl_->concurrency = StoreConcurrencyMode::paired;
    store.impl_->pair_runtime = std::move(*runtime);
    return {};
}

auto detail::StoreAccess::shard_pair_runtime(Store& store) noexcept -> store::paired::ShardPairRuntime* {
    return store.impl_->pair_runtime.get();
}

auto detail::StoreAccess::concurrency(const Store& store) noexcept -> StoreConcurrencyMode {
    return store.impl_->concurrency;
}

} // namespace glyphastore
