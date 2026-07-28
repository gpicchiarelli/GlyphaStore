#pragma once

#include "glyphastore/core/worker_routing.hpp"
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
inline constexpr std::uint64_t kDefaultMaximumHotCacheValueBytes = 64ULL * 1024ULL;
inline constexpr std::size_t kDefaultMaximumDeferredTtlReclaimsPerWorker = 1'024;
inline constexpr std::uint64_t kDefaultMaintenanceMaxCopyBytesPerCycle = 128ULL * 1024ULL * 1024ULL;

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
    // hot_cache_enabled=false is an explicit kill switch equivalent to a zero
    // byte budget for admission (cold pinned reads remain correct).
    bool hot_cache_enabled{true};
    std::uint64_t max_hot_cache_bytes{kDefaultMaximumHotCacheBytes};
    std::uint64_t max_hot_cache_bytes_per_worker{kDefaultMaximumHotCacheBytesPerWorker};
    std::uint64_t max_hot_cache_staging_bytes_per_worker{kDefaultMaximumHotCacheStagingBytesPerWorker};
    std::size_t max_hot_cache_entries_per_worker{kDefaultMaximumHotCacheEntriesPerWorker};
    // Values larger than this are never admitted; they always take the cold path.
    std::uint64_t max_hot_cache_value_bytes{kDefaultMaximumHotCacheValueBytes};
    // Bounded Index/hot-cache TTL reclaim backlog drained by Worker maintenance
    // paths (mutate, prepare_get). Zero forces synchronous reclaim on expire.
    std::size_t max_deferred_ttl_reclaims_per_worker{kDefaultMaximumDeferredTtlReclaimsPerWorker};

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

enum class MaintenanceMode : std::uint8_t {
    cooperative,
    background,
    disabled,
};

struct MaintenanceConfig {
    MaintenanceMode mode{MaintenanceMode::cooperative};
    // Preflight limit for one normal-mode evaluation/compaction. Zero explicitly
    // disables this limit; pressure and emergency always bypass it.
    std::uint64_t max_copy_bytes_per_cycle{kDefaultMaintenanceMaxCopyBytesPerCycle};
    // Inclusive per-second copy budget across normal-mode compact work in the
    // current one-second steady_clock window. Zero disables the rate limit;
    // pressure and emergency bypass it.
    std::uint64_t max_copy_bytes_per_sec{};
    std::uint32_t max_segments_per_cycle{1};
    // Inclusive CPU budget for compact work inside the same one-second window
    // (milliseconds of wall time charged to compact). Zero disables; pressure
    // and emergency bypass it.
    std::uint32_t max_cpu_ms_per_window{};
    std::uint32_t min_eval_interval_ms{1'000};
    std::uint32_t max_eval_interval_ms{60'000};
    // Inclusive daemon foreground mutation p99 threshold for deferring normal
    // compaction. Zero disables; pressure/emergency bypass. Embedded Stores do
    // not produce samples unless an internal host reports them.
    std::uint32_t suspend_on_p99_latency_ms{};
    // A latency decision requires this many completed foreground mutations in
    // the consumed window. Low-traffic windows are reclaim opportunities.
    std::uint32_t suspend_on_p99_min_samples{32};
    // Maximum continuous normal-mode deferral before one reclaim attempt is
    // admitted despite the latency guard. Zero permits indefinite deferral
    // until pressure; pressure/emergency always bypass independently.
    std::uint32_t max_latency_deferral_ms{30'000};
    std::uint32_t max_no_gain_attempts{8};
    std::uint32_t dead_byte_ratio_bp_normal{5'000};
    std::uint32_t segment_count_pressure_pct{80};
    std::uint64_t free_bytes_pressure_margin{};
    // When true (default), pressure/emergency evaluations probe the candidate
    // Worker's sealed Index for unread expired puts and populate observation
    // counters.
    bool unread_ttl_pressure_probe{true};
    // When false (default), normal scheduling stays conservative: unread expired
    // sealed puts remain Index-live until GET, recovery, or pressure. When true,
    // normal evaluations also probe and treat unread expired bytes as reclaimable
    // dead space for the inclusive dead-byte threshold only (same Store::compact()
    // path; copy budget still uses exact Index-referenced live bytes).
    bool unread_ttl_normal_scheduling{false};

    auto operator==(const MaintenanceConfig&) const -> bool = default;
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

struct WorkerRoutingConfig {
    RoutingAlgorithm algorithm{RoutingAlgorithm::fnv1a64_v1};
    std::uint64_t seed{kDefaultWorkerHashSeed};
    bool seed_explicit{};

    [[nodiscard]] auto state() const noexcept -> WorkerRoutingState {
        return WorkerRoutingState{.algorithm = algorithm, .seed = seed};
    }
};

struct StoreConfig {
    WorkerCountConfig worker_config{};
    WorkerRoutingConfig worker_routing{};
    StorageMode storage_mode{StorageMode::volatile_memory};
    std::optional<std::filesystem::path> data_directory{};
    DurableOpenMode durable_open_mode{DurableOpenMode::open_or_create};
    DurablePeriodicConfig durable_periodic{};
    DurableGroupConfig durable_group{};
    DurableResourceLimits durable_limits{};
    MaintenanceConfig maintenance{};
    // Deprecated compatibility field. Nonzero values are rejected; inject a
    // StoreClock instead so reads and recovery observe one time source.
    std::uint64_t recovery_now_ns{};
    std::shared_ptr<const StoreClock> clock{};
    // Deterministic filesystem seam used by crash, fault-injection, and
    // Reactor cold-I/O tests. Production configurations leave this empty.
    FilesystemHooks filesystem_hooks{};
};

} // namespace glyphastore
