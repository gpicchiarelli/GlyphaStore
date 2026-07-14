#include "glyphastore/store/store.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/store/value.hpp"
#include "glyphastore/worker/pool.hpp"
#include "glyphastore/worker/topology.hpp"
#include "store/store_internal.hpp"

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

} // namespace

struct Store::Impl {
    Impl(const SegmentId first_segment_id, const std::size_t worker_count)
        : segment_manager(first_segment_id), workers(segment_manager, worker_count) {}

    GlobalSegmentManager segment_manager;
    WorkerPool workers;
};

Store::Store(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Store::~Store() = default;

auto Store::open(const StoreConfig& config) -> Result<std::unique_ptr<Store>> {
    if (config.storage_mode != StorageMode::volatile_memory) {
        return fail(ErrorCode::invalid_argument,
                    "durable_sync storage is specified but not implemented in the current prototype");
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
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    auto impl = std::make_unique<Impl>(SegmentId{1}, count);
    return std::unique_ptr<Store>(new Store(std::move(impl)));
}

auto Store::worker_count() const noexcept -> std::size_t {
    return impl_->workers.size();
}

auto Store::get(const std::string_view key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    return get_copy(key, now_ns);
}

auto Store::get(const std::span<const std::byte> key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    return get_copy(key, now_ns);
}

auto Store::get_copy(const std::string_view key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    const auto hashed = HashedKey::compute(key);
    auto& worker = impl_->workers.route(hashed);
    const std::lock_guard lock{worker.mutex_};
    auto record = worker.get_locked(hashed, now_ns);
    if (!record) {
        return unexpected(record.error());
    }
    return copy_value(*record);
}

auto Store::get_copy(const std::span<const std::byte> key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    return get_copy(as_string_view(key), now_ns);
}

auto Store::put(const std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status {
    const auto hashed = HashedKey::compute(key);
    return impl_->workers.route(hashed).put(hashed, value, expire_at_ns);
}

auto Store::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status {
    return put(as_string_view(key), value, expire_at_ns);
}

auto Store::erase(const std::string_view key) -> Status {
    const auto hashed = HashedKey::compute(key);
    return impl_->workers.route(hashed).erase(hashed);
}

auto Store::erase(const std::span<const std::byte> key) -> Status {
    return erase(as_string_view(key));
}

auto Store::verify_index() const -> Status {
    std::vector<std::unique_lock<std::mutex>> worker_locks;
    worker_locks.reserve(impl_->workers.size());
    for (std::size_t index = 0; index < impl_->workers.size(); ++index) {
        worker_locks.emplace_back(impl_->workers.worker(index).mutex_);
    }

    const auto segments = impl_->segment_manager.segments();
    const auto rebuilt = rebuild_index_from_segments(segments);
    if (!rebuilt) {
        return unexpected(rebuilt.error());
    }
    if (rebuilt->index.stats().size != rebuilt->stats.records_visible) {
        return fail(ErrorCode::corrupted_data, "rebuilt index size does not match visible record count");
    }
    std::size_t worker_entry_count{};
    for (std::size_t index = 0; index < impl_->workers.size(); ++index) {
        const auto& worker = impl_->workers.worker(index);
        if (worker.index().stats().size > rebuilt->index.stats().size) {
            return fail(ErrorCode::corrupted_data, "worker index exceeds rebuilt index size");
        }
        for (const auto& entry : worker.index().entries()) {
            ++worker_entry_count;
            if (route_worker(entry.key, impl_->workers.size()) != index) {
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
        const auto worker_index = route_worker(entry.key, impl_->workers.size());
        const auto ref = impl_->workers.worker(worker_index).index().find(entry.key);
        if (!ref || *ref != entry.record) {
            return fail(ErrorCode::corrupted_data, "rebuilt entry is missing from its worker index");
        }
    }
    return {};
}

auto detail::StoreAccess::get_view(Store& store, const std::size_t worker_index, const HashedKey& key,
                                   const std::uint64_t now_ns) -> Result<RecordView> {
    if (worker_index >= store.impl_->workers.size() ||
        route_worker(key.hash, store.impl_->workers.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
    }
    return store.impl_->workers.worker(worker_index).get_locked(key, now_ns);
}

auto detail::StoreAccess::put(Store& store, const std::size_t worker_index, const HashedKey& key,
                              const std::span<const std::byte> value, const std::uint64_t expire_at_ns)
    -> Status {
    if (worker_index >= store.impl_->workers.size() ||
        route_worker(key.hash, store.impl_->workers.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive put targets the wrong Worker owner");
    }
    return store.impl_->workers.worker(worker_index).put_locked(key, value, expire_at_ns);
}

auto detail::StoreAccess::erase(Store& store, const std::size_t worker_index, const HashedKey& key)
    -> Status {
    if (worker_index >= store.impl_->workers.size() ||
        route_worker(key.hash, store.impl_->workers.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive erase targets the wrong Worker owner");
    }
    return store.impl_->workers.worker(worker_index).erase_locked(key);
}

auto detail::StoreAccess::worker(const Store& store, const std::size_t index) noexcept -> const Worker& {
    return store.impl_->workers.worker(index);
}

auto detail::StoreAccess::segments(const Store& store) -> std::vector<SegmentPtr> {
    return store.impl_->segment_manager.segments();
}

} // namespace glyphastore
