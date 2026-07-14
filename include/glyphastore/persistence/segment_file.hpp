#pragma once

#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace glyphastore {

struct RecordView;

enum class SegmentFileCreationOutcome { durable, not_published, indeterminate };
enum class SegmentCommitOutcome { committed, not_committed, indeterminate };
using CommittedRecordVisitor = Status (*)(void* context, const RecordRef& reference,
                                          const RecordView& record);

struct SegmentFileCreationResult;

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
    [[nodiscard]] static auto open(DataDirectory& directory, const SegmentHeaderIdentity& expected_identity)
        -> Result<DurableSegmentFile>;

    DurableSegmentFile(const DurableSegmentFile&) = delete;
    auto operator=(const DurableSegmentFile&) -> DurableSegmentFile& = delete;
    DurableSegmentFile(DurableSegmentFile&&) noexcept = default;
    auto operator=(DurableSegmentFile&&) noexcept -> DurableSegmentFile& = default;

    [[nodiscard]] auto append(std::span<const std::byte> encoded_record) -> SegmentCommitResult;
    [[nodiscard]] auto seal() -> SegmentCommitResult;
    [[nodiscard]] auto visit_committed_records(void* context, CommittedRecordVisitor visitor) const -> Status;
    [[nodiscard]] auto scan_committed() const -> Result<std::vector<RecordRef>>;
    [[nodiscard]] auto read_record(const RecordRef& reference) const -> Result<std::vector<std::byte>>;

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
                       FilesystemHooks hooks, std::shared_ptr<std::atomic_bool> directory_health) noexcept
        : file_(std::move(file)), identity_(identity), selected_(selected), hooks_(hooks),
          directory_health_(std::move(directory_health)) {}

    [[nodiscard]] auto before(FilesystemOperation operation) const -> Status;
    [[nodiscard]] auto publish_commit(const SegmentCommit& commit) -> SegmentCommitResult;
    void poison() noexcept;

    FileDescriptor file_;
    SegmentHeaderIdentity identity_;
    SelectedSegmentCommit selected_;
    FilesystemHooks hooks_{};
    std::shared_ptr<std::atomic_bool> directory_health_;
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

} // namespace glyphastore
