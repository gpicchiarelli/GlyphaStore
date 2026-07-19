#pragma once

#include "glyphastore/persistence/filesystem_hooks.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace glyphastore {

class StoreClock {
  public:
    virtual ~StoreClock() = default;

    // Implementations must be thread-safe and must not throw. Values are
    // absolute nanoseconds since the Unix epoch; zero represents the epoch.
    [[nodiscard]] virtual auto now_ns() const noexcept -> std::uint64_t = 0;
};

inline constexpr std::size_t kMaximumWorkerCount = 256;
inline constexpr std::size_t kDefaultMinimumMemoryPerWorker = 64ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultMaximumDurableStoreBytes = 8ULL * 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultReservedFreeBytes = 256ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kDefaultMaximumDurableSegments = 127;
inline constexpr std::size_t kDefaultMaximumManifestBytes = 1024ULL * 1024ULL;
inline constexpr std::size_t kDefaultMaximumDurableOpenFiles = 512;
inline constexpr std::uint64_t kDefaultMaximumRecoveryMemoryBytes = 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kDefaultMaximumLiveKeys = 10'000'000;
inline constexpr std::uint64_t kDefaultMaximumTemporaryCompactionBytes = 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultMaximumHotCacheBytes = 256ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultMaximumHotCacheBytesPerWorker = 64ULL * 1024ULL * 1024ULL;
inline constexpr std::uint64_t kDefaultMaximumHotCacheStagingBytesPerWorker = 16ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kDefaultMaximumHotCacheEntriesPerWorker = 1'000'000;

struct DurableResourceLimits {
    std::uint64_t max_store_bytes{kDefaultMaximumDurableStoreBytes};
    std::uint64_t reserved_free_bytes{kDefaultReservedFreeBytes};
    std::size_t max_segment_count{kDefaultMaximumDurableSegments};
    std::size_t max_manifest_bytes{kDefaultMaximumManifestBytes};
    std::size_t max_open_files{kDefaultMaximumDurableOpenFiles};
    std::uint64_t max_recovery_memory_bytes{kDefaultMaximumRecoveryMemoryBytes};
    std::size_t max_live_keys{kDefaultMaximumLiveKeys};
    std::uint64_t max_temporary_compaction_bytes{kDefaultMaximumTemporaryCompactionBytes};
    std::uint32_t max_write_amplification{4};
    // Zero disables hot admission without affecting correctness. The global
    // budget is partitioned across Workers, then capped by the per-Worker limit.
    std::uint64_t max_hot_cache_bytes{kDefaultMaximumHotCacheBytes};
    std::uint64_t max_hot_cache_bytes_per_worker{kDefaultMaximumHotCacheBytesPerWorker};
    std::uint64_t max_hot_cache_staging_bytes_per_worker{kDefaultMaximumHotCacheStagingBytesPerWorker};
    std::size_t max_hot_cache_entries_per_worker{kDefaultMaximumHotCacheEntriesPerWorker};

    auto operator==(const DurableResourceLimits&) const -> bool = default;
};

struct DurableGroupConfig {
    std::uint32_t max_records{32};
    std::uint32_t max_bytes{65536};
    std::uint32_t max_wait_ms{10};
    std::uint32_t min_records{1};
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
    DurableResourceLimits durable_limits{};
    // Deprecated compatibility field. Nonzero values are rejected; inject a
    // StoreClock instead so reads and recovery observe one time source.
    std::uint64_t recovery_now_ns{};
    std::shared_ptr<const StoreClock> clock{};
    // Deterministic filesystem seam used by crash, fault-injection, and
    // Reactor cold-I/O tests. Production configurations leave this empty.
    FilesystemHooks filesystem_hooks{};
};

} // namespace glyphastore
