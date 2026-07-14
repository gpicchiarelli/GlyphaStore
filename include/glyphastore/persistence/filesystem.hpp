#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/manifest.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <utility>

namespace glyphastore {

inline constexpr auto kManifestFilename = "manifest.glypha";
inline constexpr auto kManifestTemporaryFilename = ".manifest.glypha.tmp";
inline constexpr auto kStoreLockFilename = ".glyphastore.lock";
inline constexpr auto kBootstrapIntentFilename = ".glyphastore.bootstrap";
inline constexpr auto kBootstrapTemporaryFilename = ".glyphastore.bootstrap.tmp";

enum class FileSyncMode { data, full };
enum class DataDirectoryOpenMode { existing, open_or_create, create_new };

class FileDescriptor final {
  public:
    FileDescriptor() = default;
    explicit FileDescriptor(int descriptor) noexcept : descriptor_(descriptor) {}
    ~FileDescriptor();

    FileDescriptor(const FileDescriptor&) = delete;
    auto operator=(const FileDescriptor&) -> FileDescriptor& = delete;
    FileDescriptor(FileDescriptor&& other) noexcept;
    auto operator=(FileDescriptor&& other) noexcept -> FileDescriptor&;

    [[nodiscard]] auto valid() const noexcept -> bool {
        return descriptor_ >= 0;
    }
    [[nodiscard]] auto get() const noexcept -> int {
        return descriptor_;
    }
    [[nodiscard]] auto release() noexcept -> int;
    void reset(int descriptor = -1) noexcept;

    [[nodiscard]] auto size() const -> Result<std::uint64_t>;
    [[nodiscard]] auto write_all_at(std::span<const std::byte> bytes, std::uint64_t offset) const -> Status;
    [[nodiscard]] auto read_exact_at(std::span<std::byte> bytes, std::uint64_t offset) const -> Status;
    [[nodiscard]] auto sync(FileSyncMode mode) const -> Status;

  private:
    int descriptor_{-1};
};

enum class FilesystemOperation {
    write_manifest,
    sync_manifest,
    rename_manifest,
    sync_directory,
    preallocate_segment,
    write_segment_header,
    sync_segment_file,
    rename_segment,
    write_record,
    sync_record,
    write_commit_slot,
    sync_commit_slot,
    write_bootstrap,
    sync_bootstrap,
    rename_bootstrap,
    remove_bootstrap,
};

struct FilesystemHooks {
    // Internal fault-injection seam. Production callers leave this empty.
    void* context{};
    auto (*before)(void* context, FilesystemOperation operation) -> Status{};
};

enum class ManifestPublicationOutcome { durable, not_published, indeterminate };

struct ManifestPublicationResult {
    ManifestPublicationOutcome outcome{ManifestPublicationOutcome::not_published};
    std::optional<Error> error;

    [[nodiscard]] auto durable() const noexcept -> bool {
        return outcome == ManifestPublicationOutcome::durable;
    }
};

class DurableSegmentFile;

class DataDirectory final {
  public:
    [[nodiscard]] static auto open_and_lock(const std::filesystem::path& path, FilesystemHooks hooks = {})
        -> Result<DataDirectory>;
    [[nodiscard]] static auto open_and_lock(const std::filesystem::path& path, DataDirectoryOpenMode mode,
                                            FilesystemHooks hooks = {}) -> Result<DataDirectory>;

    ~DataDirectory();
    DataDirectory(const DataDirectory&) = delete;
    auto operator=(const DataDirectory&) -> DataDirectory& = delete;
    DataDirectory(DataDirectory&&) noexcept = default;
    auto operator=(DataDirectory&& other) noexcept -> DataDirectory&;

    [[nodiscard]] auto publish_manifest(const Manifest& manifest) -> ManifestPublicationResult;
    [[nodiscard]] auto read_manifest() const -> Result<Manifest>;
    [[nodiscard]] auto publish_bootstrap_intent(const Manifest& manifest) -> Status;
    [[nodiscard]] auto read_bootstrap_intent() const -> Result<Manifest>;
    [[nodiscard]] auto finish_bootstrap() -> Status;
    [[nodiscard]] auto pristine_for_bootstrap() const -> Result<bool>;
    // Returns an independently positioned descriptor for descriptor-relative
    // namespace enumeration. The caller owns the returned descriptor.
    [[nodiscard]] auto open_directory_for_enumeration() const -> Result<FileDescriptor>;
    [[nodiscard]] auto healthy() const noexcept -> bool {
        return health_ && health_->load(std::memory_order_acquire);
    }

  private:
    friend class DurableSegmentFile;

    DataDirectory(FileDescriptor directory, FileDescriptor lock, FilesystemHooks hooks)
        : directory_(std::move(directory)), lock_(std::move(lock)), hooks_(hooks),
          health_(std::make_shared<std::atomic_bool>(true)) {}

    [[nodiscard]] auto before(FilesystemOperation operation) const -> Status;
    [[nodiscard]] auto sync_directory() const -> Status;

    FileDescriptor directory_;
    FileDescriptor lock_;
    FilesystemHooks hooks_{};
    std::shared_ptr<std::atomic_bool> health_;
};

} // namespace glyphastore
