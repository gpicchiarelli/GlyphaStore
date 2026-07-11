#include "glyphastore/store/store.hpp"

#include "glyphastore/core/key_hash.hpp"

#include <memory>

namespace glyphastore {

Store::Store(SegmentManager manager, std::size_t worker_count) : manager_(std::move(manager)) {
    workers_.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        workers_.emplace_back(WorkerId{static_cast<std::uint32_t>(index)}, manager_);
    }
}

auto Store::open(const StoreConfig& config) -> Result<std::unique_ptr<Store>> {
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    return std::unique_ptr<Store>(new Store(SegmentManager{config.first_segment_id}, count));
}

auto Store::worker_for(std::string_view key) const -> const Worker& {
    return workers_[route_worker(key, workers_.size())];
}

auto Store::worker_for(std::string_view key) -> Worker& {
    return workers_[route_worker(key, workers_.size())];
}

auto Store::get(std::string_view key, std::uint64_t now_ns) const -> Result<RecordView> {
    return worker_for(key).get(key, now_ns);
}

auto Store::put(std::string_view key, std::span<const std::byte> value, std::uint64_t expire_at_ns)
    -> Status {
    return worker_for(key).put(key, value, expire_at_ns);
}

auto Store::erase(std::string_view key) -> Status {
    return worker_for(key).erase(key);
}

auto Store::verify_index() const -> Status {
    const auto rebuilt = rebuild_index_from_segments(manager_.segments());
    if (!rebuilt) {
        return unexpected(rebuilt.error());
    }
    if (rebuilt->index.stats().size != rebuilt->stats.records_visible) {
        return fail(ErrorCode::corrupted_data, "rebuilt index size does not match visible record count");
    }
    for (const auto& worker : workers_) {
        if (worker.index().stats().size > rebuilt->index.stats().size) {
            return fail(ErrorCode::corrupted_data, "worker index exceeds rebuilt index size");
        }
        for (const auto& entry : worker.index().entries()) {
            const auto ref = rebuilt->index.find(entry.key);
            if (!ref || *ref != entry.record) {
                return fail(ErrorCode::corrupted_data, "worker index entry does not match rebuilt index");
            }
        }
    }
    return {};
}

} // namespace glyphastore
