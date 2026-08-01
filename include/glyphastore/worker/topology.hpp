#pragma once

#include "glyphastore/store/config.hpp"

#include <cstddef>

namespace glyphastore {

struct WorkerTopology {
    std::size_t logical_cpus{1};
    std::size_t physical_cores{1};
    std::size_t available_cpus{1};
    std::size_t available_memory_bytes{};
};

class WorkerCountPolicy final {
  public:
    [[nodiscard]] static auto choose(const WorkerTopology& topology, const WorkerCountConfig& config) noexcept
        -> std::size_t;
};

[[nodiscard]] auto detect_worker_topology() noexcept -> WorkerTopology;

} // namespace glyphastore
