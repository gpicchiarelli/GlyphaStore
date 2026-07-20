#pragma once

#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace glyphastore {

struct RecordView;

enum class SegmentFileCreationOutcome { durable, not_published, indeterminate };
enum class SegmentCommitOutcome { committed, not_committed, indeterminate };
enum class SegmentFileOpenMode { read_only, read_write };
enum class SegmentCommitSync { immediate, deferred };
using CommittedRecordVisitor = Status (*)(void* context, const RecordRef& reference,
                                          const RecordView& record);
using RecordVisitor = Status (*)(void* context, const RecordView& record);

struct SegmentFileCreationResult;

struct DurableSegmentInspectReport {
    std::filesystem::path path;
    SegmentHeaderIdentity identity{};
    SelectedSegmentCommit selected{};
    std::uint64_t scanned_records{};
    // Set when the basename parses as a canonical Segment name; true iff it
    // matches header identity. Unset when the basename is not canonical.
    std::optional<bool> filename_matches_identity;
};

struct SegmentCommitResult {
    SegmentCommitOutcome outcome{SegmentCommitOutcome::not_committed};
    std::optional<Error> error;

    [[nodiscard]] auto committed() const noexcept -> bool {
        return outcome == SegmentCommitOutcome::committed;
    }
};

class DurableSegmentFile final {
  public:
    [[nodiscard]] static auto create(DataDirectory& directory, const SegmentHeaderIdentity& identity)
        -> SegmentFileCreationResult;
    [[nodiscard]] static auto open(DataDirectory& directory, const SegmentHeaderIdentity& expected_identity,
                                   SegmentFileOpenMode mode = SegmentFileOpenMode::read_write)
        -> Result<DurableSegmentFile>;
    // Read-only open of an absolute or relative Segment path without a data-directory
    // lock. Used by offline inspection; never opens writable.
    [[nodiscard]] static auto open_path(const std::filesystem::path& path) -> Result<DurableSegmentFile>;

    DurableSegmentFile(const DurableSegmentFile&) = delete;
    auto operator=(const DurableSegmentFile&) -> DurableSegmentFile& = delete;
    DurableSegmentFile(DurableSegmentFile&&) noexcept = default;
    auto operator=(DurableSegmentFile&&) noexcept -> DurableSegmentFile& = default;

    [[nodiscard]] auto append(std::span<const std::byte> encoded_record,
                              SegmentCommitSync sync = SegmentCommitSync::immediate) -> SegmentCommitResult;
    [[nodiscard]] auto append_record(std::span<const std::byte> encoded_record) -> SegmentCommitResult;
    [[nodiscard]] auto flush_pending_commit(SegmentCommitSync sync) -> SegmentCommitResult;
    [[nodiscard]] auto has_pending_commit() const noexcept -> bool;
    [[nodiscard]] auto pending_record_count() const noexcept -> std::uint64_t;
    [[nodiscard]] auto pending_bytes() const noexcept -> std::uint64_t;
    [[nodiscard]] auto seal() -> SegmentCommitResult;
    [[nodiscard]] auto sync_file() -> SegmentCommitResult;
    [[nodiscard]] auto is_dirty() const noexcept -> bool;
    [[nodiscard]] auto persisted_commit() const noexcept -> const SelectedSegmentCommit& {
        return persisted_;
    }
    [[nodiscard]] auto visit_committed_records(void* context, CommittedRecordVisitor visitor) const -> Status;
    [[nodiscard]] auto scan_committed() const -> Result<std::vector<RecordRef>>;
    [[nodiscard]] auto read_record(const RecordRef& reference) const -> Result<std::vector<std::byte>>;
    // The RecordView is valid only for the synchronous visitor invocation.
    [[nodiscard]] auto visit_record(const RecordRef& reference, void* context, RecordVisitor visitor) const
        -> Status;
    // Reuses caller-owned storage across verified reads. The scratch bytes and
    // RecordView remain valid only for the synchronous visitor invocation.
    [[nodiscard]] auto visit_record(const RecordRef& reference, std::vector<std::byte>& scratch,
                                    void* context, RecordVisitor visitor) const -> Status;
    // Runtime-only path for a RecordRef already obtained from the authoritative
    // Worker Index together with an exact generation pin. Unlike visit_record,
    // this permits a reference committed after this read-only handle's opening
    // boundary; the caller must revalidate Index and pin identity after I/O.
    [[nodiscard]] auto visit_runtime_record(const RecordRef& reference, void* context,
                                            RecordVisitor visitor) const -> Status;

    [[nodiscard]] auto identity() const noexcept -> const SegmentHeaderIdentity& {
        return identity_;
    }
    [[nodiscard]] auto selected_commit() const noexcept -> const SelectedSegmentCommit& {
        return selected_;
    }
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return directory_health_ && directory_health_->load(std::memory_order_acquire);
    }

  private:
    DurableSegmentFile(FileDescriptor file, SegmentHeaderIdentity identity, SelectedSegmentCommit selected,
                       FilesystemHooks hooks, std::shared_ptr<std::atomic_bool> directory_health,
                       bool writable) noexcept
        : file_(std::move(file)), identity_(identity), selected_(selected), persisted_(selected),
          hooks_(hooks), directory_health_(std::move(directory_health)), writable_(writable) {}

    [[nodiscard]] auto before(FilesystemOperation operation) const -> Status;
    void rollback_pending_metadata() noexcept;
    void after(FilesystemOperation operation) const noexcept;
    [[nodiscard]] auto publish_commit(const SegmentCommit& commit,
                                      SegmentCommitSync sync = SegmentCommitSync::immediate)
        -> SegmentCommitResult;
    [[nodiscard]] auto read_record_into(const RecordRef& reference, std::vector<std::byte>& bytes,
                                        RecordView& record) const -> Status;
    [[nodiscard]] auto read_record_into_extent(const RecordRef& reference, std::uint64_t readable_end,
                                               std::vector<std::byte>& bytes, RecordView& record) const
        -> Status;
    void poison() noexcept;

    FileDescriptor file_;
    SegmentHeaderIdentity identity_;
    SelectedSegmentCommit selected_;
    SelectedSegmentCommit persisted_;
    FilesystemHooks hooks_{};
    std::shared_ptr<std::atomic_bool> directory_health_;
    bool writable_{};
    bool dirty_{false};
    std::uint64_t pending_record_count_{0};
    std::uint64_t pending_bytes_{0};
};

struct SegmentFileCreationResult {
    SegmentFileCreationOutcome outcome{SegmentFileCreationOutcome::not_published};
    std::optional<DurableSegmentFile> file;
    std::optional<Error> error;

    [[nodiscard]] auto durable() const noexcept -> bool {
        return outcome == SegmentFileCreationOutcome::durable;
    }
};

[[nodiscard]] auto segment_filename(const SegmentHeaderIdentity& identity) -> std::string;

// Read-only validation of one Segment file: private regular file, exact size,
// header/commit decode, optional committed-extent CRC scan. Fail-closed on any
// disagreement; does not modify the file or take a Store lock.
[[nodiscard]] auto inspect_durable_segment(const std::filesystem::path& path, bool scan_records = true)
    -> Result<DurableSegmentInspectReport>;

} // namespace glyphastore
