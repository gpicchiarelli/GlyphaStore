#include "glyphastore/store/store.hpp"

#include <memory>

namespace glyphastore {

Store::Store(const SegmentId first_segment_id, const std::size_t worker_count)
    : segment_manager_(first_segment_id), workers_(segment_manager_, worker_count) {}

auto Store::open(const StoreConfig& config) -> Result<std::unique_ptr<Store>> {
    const auto topology = detect_worker_topology();
    const auto count = WorkerCountPolicy::choose(topology, config.worker_config);
    return std::unique_ptr<Store>(new Store(config.first_segment_id, count));
}

auto Store::get(std::string_view key, const std::uint64_t now_ns) -> Result<RecordView> {
    return workers_.route(key).get(key, now_ns);
}

auto Store::put(std::string_view key, const std::span<const std::byte> value,
                const std::uint64_t expire_at_ns) -> Status {
    return workers_.route(key).put(key, value, expire_at_ns);
}

auto Store::erase(std::string_view key) -> Status {
    return workers_.route(key).erase(key);
}

auto Store::verify_index() const -> Status {
    const auto rebuilt = rebuild_index_from_segments(segment_manager_.segments());
    if (!rebuilt) {
        return unexpected(rebuilt.error());
    }
    if (rebuilt->index.stats().size != rebuilt->stats.records_visible) {
        return fail(ErrorCode::corrupted_data, "rebuilt index size does not match visible record count");
    }
    for (std::size_t index = 0; index < workers_.size(); ++index) {
        const auto& worker = workers_.worker(index);
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
