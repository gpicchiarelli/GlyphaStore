#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>

namespace glyphastore {

inline constexpr std::size_t kMaximumWorkerCount = 256;
inline constexpr std::size_t kDefaultMinimumMemoryPerWorker = 64ULL * 1024ULL * 1024ULL;

struct DurableGroupConfig {
    std::uint32_t max_records{32};
    std::uint32_t max_bytes{65536};
    std::uint32_t max_wait_ms{10};
};

enum class StorageMode : std::uint8_t {
    volatile_memory,
    durable_sync,
    durable_periodic,
    durable_group,
};

enum class DurableOpenMode : std::uint8_t {
    open_or_create,
    open_existing,
    create_new,
};

struct DurablePeriodicConfig {
    std::uint32_t sync_interval_ms{1000};
    std::optional<DurableGroupConfig> batch{
        DurableGroupConfig{.max_records = 4096, .max_bytes = 4U * 1024U * 1024U, .max_wait_ms = 1000}};
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
    std::optional<std::filesystem::path> data_directory{};
    DurableOpenMode durable_open_mode{DurableOpenMode::open_or_create};
    DurablePeriodicConfig durable_periodic{};
    DurableGroupConfig durable_group{};
    std::uint64_t recovery_now_ns{};
};

} // namespace glyphastore
