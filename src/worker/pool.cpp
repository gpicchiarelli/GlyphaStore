#include "glyphastore/worker/pool.hpp"

namespace glyphastore {

WorkerPool::WorkerPool(GlobalSegmentManager& manager, const std::size_t worker_count) : manager_(manager) {
    workers_.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        workers_.emplace_back(WorkerId{static_cast<std::uint32_t>(index)}, manager_);
    }
}

} // namespace glyphastore
