#include "glyphastore/store/store.hpp"

#include "glyphastore/core/key_hash.hpp"

#include <memory>
#include <mutex>
#include <vector>

namespace glyphastore {

Store::Store(const SegmentId first_segment_id, const std::size_t worker_count)
    : segment_manager_(first_segment_id), workers_(segment_manager_, worker_count) {}

auto Store::open(const StoreConfig& config) -> Result<std::unique_ptr<Store>> {
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    return std::unique_ptr<Store>(new Store(config.first_segment_id, count));
}

auto Store::get(std::string_view key, const std::uint64_t now_ns) -> Result<RecordView> {
    return get(HashedKey::compute(key), now_ns);
}

auto Store::get(const HashedKey& key, const std::uint64_t now_ns) -> Result<RecordView> {
    return workers_.route(key).get(key, now_ns);
}

auto Store::put(std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status {
    return put(HashedKey::compute(key), value, expire_at_ns);
}

auto Store::put(const HashedKey& key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status {
    return workers_.route(key).put(key, value, expire_at_ns);
}

auto Store::erase(std::string_view key) -> Status {
    return erase(HashedKey::compute(key));
}

auto Store::erase(const HashedKey& key) -> Status {
    return workers_.route(key).erase(key);
}

auto Store::get_owned(const std::size_t worker_index, const HashedKey& key, const std::uint64_t now_ns)
    -> Result<RecordView> {
    if (worker_index >= workers_.size() || route_worker(key.hash, workers_.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive get targets the wrong Worker owner");
    }
    return workers_.worker(worker_index).get_locked(key, now_ns);
}

auto Store::put_owned(const std::size_t worker_index, const HashedKey& key,
                      const std::span<const std::byte> value, const std::uint64_t expire_at_ns) -> Status {
    if (worker_index >= workers_.size() || route_worker(key.hash, workers_.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive put targets the wrong Worker owner");
    }
    return workers_.worker(worker_index).put_locked(key, value, expire_at_ns);
}

auto Store::erase_owned(const std::size_t worker_index, const HashedKey& key) -> Status {
    if (worker_index >= workers_.size() || route_worker(key.hash, workers_.size()) != worker_index) {
        return fail(ErrorCode::invalid_argument, "exclusive erase targets the wrong Worker owner");
    }
    return workers_.worker(worker_index).erase_locked(key);
}

auto Store::verify_index() const -> Status {
    std::vector<std::unique_lock<std::mutex>> worker_locks;
    worker_locks.reserve(workers_.size());
    for (std::size_t index = 0; index < workers_.size(); ++index) {
        worker_locks.emplace_back(workers_.worker(index).mutex_);
    }

    const auto segments = segment_manager_.segments();
    const auto rebuilt = rebuild_index_from_segments(segments);
    if (!rebuilt) {
        return unexpected(rebuilt.error());
    }
    if (rebuilt->index.stats().size != rebuilt->stats.records_visible) {
        return fail(ErrorCode::corrupted_data, "rebuilt index size does not match visible record count");
    }
    std::size_t worker_entry_count{};
    for (std::size_t index = 0; index < workers_.size(); ++index) {
        const auto& worker = workers_.worker(index);
        if (worker.index().stats().size > rebuilt->index.stats().size) {
            return fail(ErrorCode::corrupted_data, "worker index exceeds rebuilt index size");
        }
        for (const auto& entry : worker.index().entries()) {
            ++worker_entry_count;
            if (route_worker(entry.key, workers_.size()) != index) {
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
        const auto worker_index = route_worker(entry.key, workers_.size());
        const auto ref = workers_.worker(worker_index).index().find(entry.key);
        if (!ref || *ref != entry.record) {
            return fail(ErrorCode::corrupted_data, "rebuilt entry is missing from its worker index");
        }
    }
    return {};
}

} // namespace glyphastore
