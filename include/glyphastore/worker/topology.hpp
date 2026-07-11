#pragma once

#include "glyphastore/core/types.hpp"

#include <cstddef>
#include <optional>

namespace glyphastore {

struct WorkerTopology {
    std::size_t logical_cpus{1};
    std::size_t physical_cores{1};
    std::size_t available_cpus{1};
    std::size_t available_memory_bytes{};
};

struct WorkerCountConfig {
    std::optional<std::size_t> explicit_count;
    std::size_t reserved_cores{1};
    std::size_t maximum_workers{256};
    std::size_t minimum_memory_per_worker{kSegmentSizeBytes};
};

class WorkerCountPolicy final {
  public:
    [[nodiscard]] static auto choose(const WorkerTopology& topology, const WorkerCountConfig& config) noexcept
        -> std::size_t;
};

[[nodiscard]] auto detect_worker_topology() noexcept -> WorkerTopology;

} // namespace glyphastore
