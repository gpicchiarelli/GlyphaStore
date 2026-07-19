#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/persistence/compaction_builder.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/persistence/recovery.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/value.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore {

class DurableFlushCoordinator;
namespace detail {
class StoreAccess;
}

enum class DurableMutationOutcome { committed, not_committed, indeterminate };
enum class DurableCompactionOutcome { compacted, not_compacted, not_beneficial, recovery_required };

struct DurableRuntimeOptions {
    SegmentCommitSync commit_sync{SegmentCommitSync::immediate};
    std::uint32_t sync_interval_ms{1000};
    std::optional<DurableGroupConfig> batch{};
    bool strict_ack{false};
    DurableResourceLimits limits{};
};

struct DurableMutationResult {
    DurableMutationOutcome outcome{DurableMutationOutcome::not_committed};
    std::optional<SequenceNumber> sequence;
    std::optional<Error> error;

    [[nodiscard]] auto committed() const noexcept -> bool {
        return outcome == DurableMutationOutcome::committed;
    }
};

struct DurableCompactionResult {
    DurableCompactionOutcome outcome{DurableCompactionOutcome::not_compacted};
    DurableCompactionCopyStats stats{};
    std::optional<Error> error;

    [[nodiscard]] auto compacted() const noexcept -> bool {
        return outcome == DurableCompactionOutcome::compacted;
    }
};

struct DurableHotCacheWorkerStats {
    WorkerId worker_id{};
    std::size_t resident_entries{};
    std::uint64_t resident_bytes{};
    std::size_t staged_entries{};
    std::uint64_t staged_bytes{};
    std::uint64_t bucket_bytes{};
    std::uint64_t total_accounted_bytes{};
    std::uint64_t byte_budget{};
    std::uint64_t staging_byte_budget{};
    std::size_t entry_budget{};
    std::uint64_t hits{};
    std::uint64_t misses{};
    std::uint64_t admission_bypasses{};
};

// Internal materialization of one recovered durable Store. It keeps the
// directory lock for its complete lifetime. Mutable Segment handles remain
// Worker-owned; immutable generation pins keep cold-read handles alive across
// catalog publication and Segment retirement.
class DurableRuntimeCatalog final {
    struct PendingGroupMutation;
    struct RuntimeWorker;
    struct RuntimeSegmentGeneration;

  public:
    class PinnedRead final {
      public:
        PinnedRead(PinnedRead&&) noexcept = default;
        auto operator=(PinnedRead&&) noexcept -> PinnedRead& = default;
        PinnedRead(const PinnedRead&) = delete;
        auto operator=(const PinnedRead&) -> PinnedRead& = delete;

      private:
        PinnedRead(std::string key, std::uint64_t key_hash, std::uint64_t now_ns, std::size_t worker_index,
                   RecordRef reference, std::shared_ptr<const RuntimeSegmentGeneration> generation)
            : key_(std::move(key)), key_hash_(key_hash), now_ns_(now_ns), worker_index_(worker_index),
              reference_(reference), generation_(std::move(generation)) {}

        std::string key_;
        std::uint64_t key_hash_{};
        std::uint64_t now_ns_{};
        std::size_t worker_index_{};
        RecordRef reference_;
        std::shared_ptr<const RuntimeSegmentGeneration> generation_;

        friend class DurableRuntimeCatalog;
        friend class detail::StoreAccess;
    };

    struct PreparedRead final {
        std::optional<OwnedValue> value;
        std::optional<PinnedRead> cold;
    };

    [[nodiscard]] static auto open_existing(const std::filesystem::path& path,
                                            std::uint64_t recovery_now_ns = 0, FilesystemHooks hooks = {})
        -> Result<std::unique_ptr<DurableRuntimeCatalog>>;
    [[nodiscard]] static auto open_locked(DataDirectory directory, std::uint64_t recovery_now_ns = 0,
                                          DurableRuntimeOptions options = {})
        -> Result<std::unique_ptr<DurableRuntimeCatalog>>;

    ~DurableRuntimeCatalog();
    DurableRuntimeCatalog(const DurableRuntimeCatalog&) = delete;
    auto operator=(const DurableRuntimeCatalog&) -> DurableRuntimeCatalog& = delete;
    DurableRuntimeCatalog(DurableRuntimeCatalog&&) = delete;
    auto operator=(DurableRuntimeCatalog&&) -> DurableRuntimeCatalog& = delete;

    [[nodiscard]] auto get(std::string_view key, std::uint64_t now_ns = 0) -> Result<OwnedValue>;
    [[nodiscard]] auto get(std::span<const std::byte> key, std::uint64_t now_ns = 0) -> Result<OwnedValue>;
    [[nodiscard]] auto get(const HashedKey& key, std::uint64_t now_ns = 0) -> Result<OwnedValue>;
    [[nodiscard]] auto put(std::span<const std::byte> key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0, ValueType type = ValueType::bytes,
                           std::uint32_t flags = 0) -> DurableMutationResult;
    [[nodiscard]] auto put(const HashedKey& key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0, ValueType type = ValueType::bytes,
                           std::uint32_t flags = 0) -> DurableMutationResult;
    [[nodiscard]] auto erase(std::span<const std::byte> key) -> DurableMutationResult;
    [[nodiscard]] auto erase(const HashedKey& key) -> DurableMutationResult;

    [[nodiscard]] auto healthy() const noexcept -> bool;
    [[nodiscard]] auto worker_count() const noexcept -> std::size_t;
    [[nodiscard]] auto manifest() const -> Manifest;
    [[nodiscard]] auto namespace_audit() const -> NamespaceAuditReport;
    [[nodiscard]] auto recovery_stats() const noexcept -> const DurableRecoveryStats&;
    [[nodiscard]] auto hot_cache_stats() const -> std::vector<DurableHotCacheWorkerStats>;
    [[nodiscard]] auto next_sequence(std::size_t worker_index) const -> Result<SequenceNumber>;
    [[nodiscard]] auto active_segment(std::size_t worker_index) const -> Result<SegmentId>;
    [[nodiscard]] auto next_compaction_worker(std::size_t start_worker) const
        -> Result<std::optional<std::size_t>>;
    [[nodiscard]] auto compact_worker(std::size_t worker_index, std::uint64_t now_ns)
        -> DurableCompactionResult;
    [[nodiscard]] auto verify_index() -> Status;
    [[nodiscard]] auto flush() -> Status;
    void request_close_flush();
    [[nodiscard]] auto close() -> Status;
    void mark_fail_closed() noexcept;

  private:
    DurableRuntimeCatalog(DataDirectory directory, DurableRecoveryState recovered,
                          DurableRuntimeOptions options);
    [[nodiscard]] auto initialize_generation_pins() -> Status;
    [[nodiscard]] auto flush_pending_batches(SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto flush_due_batches(SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto flush_dirty_segments() -> Status;
    [[nodiscard]] auto flush_worker_batch(RuntimeWorker& worker, SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto should_flush_batch(RuntimeWorker& worker) const noexcept -> bool;
    void abandon_pending_batches() noexcept;
    void wait_for_batch_close(RuntimeWorker& worker, PendingGroupMutation& mutation,
                              std::unique_lock<std::mutex>& lock);
    [[nodiscard]] auto fail_closed(Error error) -> Unexpected;
    [[nodiscard]] auto mutate(std::span<const std::byte> key, std::span<const std::byte> value, Opcode opcode,
                              std::uint64_t key_hash, std::uint64_t expire_at_ns, ValueType type,
                              std::uint32_t flags) -> DurableMutationResult;
    [[nodiscard]] auto rotate_active(RuntimeWorker& worker) -> DurableMutationResult;
    [[nodiscard]] auto prepare_get(const HashedKey& key, std::uint64_t now_ns) -> Result<PreparedRead>;
    [[nodiscard]] auto complete_get(PinnedRead read, const std::atomic_bool* cancelled = nullptr)
        -> Result<OwnedValue>;

    // Declared first so cached Segment handles are destroyed before the lock
    // and anchored directory descriptor.
    DataDirectory directory_;
    Manifest manifest_;
    NamespaceAuditReport namespace_audit_;
    std::vector<RecoveredSegmentState> segments_;
    std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> generation_pins_;
    std::vector<std::unique_ptr<RuntimeWorker>> workers_;
    DurableRecoveryStats recovery_stats_;
    DurableRuntimeOptions options_;
    std::unique_ptr<DurableFlushCoordinator> flusher_;
    bool dedicated_commit_executor_{};
    std::atomic_bool healthy_{true};
    std::atomic_bool closed_{false};
    std::mutex close_mutex_;
    std::optional<Error> close_error_;
    // Guarded by manifest_publication_mutex_. A compaction lease excludes
    // manifest-changing rotations without keeping the serializer locked while
    // replacement Records are scanned and copied.
    bool compaction_publication_active_{};
    std::mutex manifest_publication_mutex_;
    mutable std::shared_mutex catalog_mutex_;

    friend class detail::StoreAccess;
};

} // namespace glyphastore
