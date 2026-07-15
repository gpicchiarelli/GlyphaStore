#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
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
#include <string_view>
#include <vector>

namespace glyphastore {

class DurableFlushCoordinator;

enum class DurableMutationOutcome { committed, not_committed, indeterminate };

struct DurableRuntimeOptions {
    SegmentCommitSync commit_sync{SegmentCommitSync::immediate};
    std::uint32_t sync_interval_ms{1000};
    std::optional<DurableGroupConfig> batch{};
    bool strict_ack{false};
};

struct DurableMutationResult {
    DurableMutationOutcome outcome{DurableMutationOutcome::not_committed};
    std::optional<SequenceNumber> sequence;
    std::optional<Error> error;

    [[nodiscard]] auto committed() const noexcept -> bool {
        return outcome == DurableMutationOutcome::committed;
    }
};

// Internal materialization of one recovered durable Store. It keeps the
// directory lock for its complete lifetime and caches at most one Segment
// descriptor per Worker.
class DurableRuntimeCatalog final {
  public:
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
    [[nodiscard]] auto namespace_audit() const noexcept -> const NamespaceAuditReport&;
    [[nodiscard]] auto recovery_stats() const noexcept -> const DurableRecoveryStats&;
    [[nodiscard]] auto next_sequence(std::size_t worker_index) const -> Result<SequenceNumber>;
    [[nodiscard]] auto active_segment(std::size_t worker_index) const -> Result<SegmentId>;
    [[nodiscard]] auto verify_index() -> Status;
    [[nodiscard]] auto flush() -> Status;

  private:
    struct PendingGroupMutation;
    struct RuntimeWorker;

    DurableRuntimeCatalog(DataDirectory directory, DurableRecoveryState recovered,
                          DurableRuntimeOptions options);
    [[nodiscard]] auto flush_pending_batches(SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto flush_due_batches(SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto flush_dirty_segments() -> Status;
    [[nodiscard]] auto flush_worker_batch(RuntimeWorker& worker, SegmentCommitSync sync) -> Status;
    [[nodiscard]] auto should_flush_batch(const RuntimeWorker& worker) const noexcept -> bool;
    void wait_for_batch_close(RuntimeWorker& worker, PendingGroupMutation& mutation,
                              std::unique_lock<std::mutex>& lock);
    [[nodiscard]] auto fail_closed(Error error) -> Unexpected;
    [[nodiscard]] auto mutate(std::span<const std::byte> key, std::span<const std::byte> value, Opcode opcode,
                              std::uint64_t key_hash, std::uint64_t expire_at_ns, ValueType type,
                              std::uint32_t flags) -> DurableMutationResult;
    [[nodiscard]] auto rotate_active(RuntimeWorker& worker) -> DurableMutationResult;

    // Declared first so cached Segment handles are destroyed before the lock
    // and anchored directory descriptor.
    DataDirectory directory_;
    Manifest manifest_;
    NamespaceAuditReport namespace_audit_;
    std::vector<RecoveredSegmentState> segments_;
    std::vector<std::unique_ptr<RuntimeWorker>> workers_;
    DurableRecoveryStats recovery_stats_;
    DurableRuntimeOptions options_;
    std::unique_ptr<DurableFlushCoordinator> flusher_;
    bool dedicated_commit_executor_{};
    std::atomic_bool healthy_{true};
    mutable std::shared_mutex catalog_mutex_;
};

} // namespace glyphastore
