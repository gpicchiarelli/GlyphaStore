#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace glyphastore {

inline constexpr std::size_t kMaximumWorkerCount = 256;
inline constexpr std::size_t kDefaultMinimumMemoryPerWorker = 64ULL * 1024ULL * 1024ULL;

enum class StorageMode : std::uint8_t {
    volatile_memory,
    durable_sync,
};

struct WorkerCountConfig {
    std::optional<std::size_t> explicit_count{};
    std::size_t reserved_cores{1};
    std::size_t maximum_workers{kMaximumWorkerCount};
    std::size_t minimum_memory_per_worker{kDefaultMinimumMemoryPerWorker};
};

struct StoreConfig {
    WorkerCountConfig worker_config{};
    StorageMode storage_mode{StorageMode::volatile_memory};
};

} // namespace glyphastore
