#include "glyphastore/persistence/filesystem.hpp"

#include "glyphastore/persistence/segment_file.hpp"
#include "system_error.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <vector>

namespace glyphastore {
namespace {

auto interrupted_open(const char* path, int flags) -> int {
    int descriptor{};
    do {
        descriptor = ::open(path, flags);
    } while (descriptor < 0 && errno == EINTR);
    return descriptor;
}

auto interrupted_open_at(int directory, const char* name, int flags, mode_t mode = 0) -> int {
    int descriptor{};
    do {
        descriptor = ::openat(directory, name, flags, mode);
    } while (descriptor < 0 && errno == EINTR);
    return descriptor;
}

auto interrupted_mkdir_at(int directory, const char* name, mode_t mode) -> int {
    int result{};
    do {
        result = ::mkdirat(directory, name, mode);
    } while (result != 0 && errno == EINTR);
    return result;
}

auto descriptor_stat(int descriptor) -> Result<struct stat> {
    struct stat status{};
    if (::fstat(descriptor, &status) != 0) {
        return persistence_system_error("fstat");
    }
    return status;
}

auto validate_directory_descriptor(int descriptor) -> Status {
    const auto status = descriptor_stat(descriptor);
    if (!status) {
        return unexpected(status.error());
    }
    if (!S_ISDIR(status->st_mode)) {
        return fail(ErrorCode::invalid_argument, "data directory path is not a directory");
    }
    if (status->st_uid != ::geteuid()) {
        return fail(ErrorCode::invalid_argument, "data directory is not owned by the effective user");
    }
    if ((status->st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return fail(ErrorCode::invalid_argument, "data directory is writable by group or other users");
    }
    return {};
}

auto validate_private_regular_file(int descriptor, std::string_view description) -> Status {
    const auto status = descriptor_stat(descriptor);
    if (!status) {
        return unexpected(status.error());
    }
    if (!S_ISREG(status->st_mode) || status->st_nlink != 1 || status->st_uid != ::geteuid() ||
        (status->st_mode & (S_IRWXG | S_IRWXO)) != 0) {
        return fail(ErrorCode::invalid_argument,
                    std::string{description} + " must be a private, singly linked regular file");
    }
    return {};
}

auto lock_exclusive_nonblocking(int descriptor) -> Status {
    int result{};
    do {
        result = ::flock(descriptor, LOCK_EX | LOCK_NB);
    } while (result != 0 && errno == EINTR);
    if (result == 0) {
        return {};
    }
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
        return fail(ErrorCode::io_error, "data directory is already locked by another Store");
    }
    return persistence_system_error("flock(LOCK_EX | LOCK_NB)");
}

auto unlink_temporary(int directory) -> Status {
    if (::unlinkat(directory, kManifestTemporaryFilename, 0) == 0 || errno == ENOENT) {
        return {};
    }
    return persistence_system_error("unlinkat(manifest temporary)");
}

auto publication_failure(ManifestPublicationOutcome outcome, Error error) -> ManifestPublicationResult {
    return {.outcome = outcome, .error = std::move(error)};
}

auto compaction_publication_failure(CompactionIntentPublicationOutcome outcome, Error error)
    -> CompactionIntentPublicationResult {
    return {.outcome = outcome, .error = std::move(error)};
}

auto compaction_removal_failure(CompactionIntentRemovalOutcome outcome, Error error)
    -> CompactionIntentRemovalResult {
    return {.outcome = outcome, .error = std::move(error)};
}

auto compaction_retirement_failure(CompactionSegmentRetirementOutcome outcome, Error error)
    -> CompactionSegmentRetirementResult {
    return {.outcome = outcome, .error = std::move(error)};
}

auto read_private_manifest_file(int directory, const char* name, std::string_view description,
                                const std::size_t max_bytes, const FileIoHooks io_hooks) -> Result<Manifest> {
    FileDescriptor file{interrupted_open_at(directory, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK),
                        io_hooks};
    if (!file.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, std::string{description} + " does not exist");
        }
        return persistence_system_error("openat(" + std::string{description} + ')');
    }
    if (auto valid = validate_private_regular_file(file.get(), description); !valid) {
        return unexpected(valid.error());
    }
    const auto size = file.size();
    if (!size) {
        return unexpected(size.error());
    }
    if (*size < kManifestHeaderBytes || *size > kMaximumManifestBytes) {
        return fail(ErrorCode::invalid_record,
                    std::string{description} + " size is outside manifest v1 bounds");
    }
    if (*size > max_bytes) {
        return fail(ErrorCode::storage_exhausted,
                    std::string{description} + " exceeds the configured byte budget");
    }
    std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
    if (auto read = file.read_exact_at(bytes, 0); !read) {
        return unexpected(read.error());
    }
    const auto size_after_read = file.size();
    if (!size_after_read || *size_after_read != *size) {
        return fail(ErrorCode::corrupted_data, std::string{description} + " changed while being read");
    }
    return decode_manifest(bytes);
}

auto read_private_compaction_intent_file(const int directory, const std::size_t max_manifest_bytes,
                                         const FileIoHooks io_hooks) -> Result<DurableCompactionIntent> {
    if (max_manifest_bytes > kMaximumManifestBytes ||
        max_manifest_bytes > (std::numeric_limits<std::size_t>::max() - kCompactionIntentHeaderBytes) / 2U) {
        return fail(ErrorCode::invalid_argument,
                    "compaction intent manifest byte budget is outside supported bounds");
    }
    const auto max_bytes = kCompactionIntentHeaderBytes + 2U * max_manifest_bytes;
    FileDescriptor file{interrupted_open_at(directory, kCompactionIntentFilename,
                                            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK),
                        io_hooks};
    if (!file.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, "compaction intent does not exist");
        }
        return persistence_system_error("openat(compaction intent)");
    }
    if (auto valid = validate_private_regular_file(file.get(), "compaction intent"); !valid) {
        return unexpected(valid.error());
    }
    const auto size = file.size();
    if (!size) {
        return unexpected(size.error());
    }
    if (*size < kCompactionIntentHeaderBytes || *size > kMaximumCompactionIntentBytes) {
        return fail(ErrorCode::invalid_record, "compaction intent size is outside its format bounds");
    }
    if (*size > max_bytes) {
        return fail(ErrorCode::storage_exhausted,
                    "compaction intent exceeds the configured manifest byte budget");
    }
    std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
    if (auto read = file.read_exact_at(bytes, 0); !read) {
        return unexpected(read.error());
    }
    const auto size_after_read = file.size();
    if (!size_after_read || *size_after_read != *size) {
        return fail(ErrorCode::corrupted_data, "compaction intent changed while being read");
    }
    return decode_compaction_intent(bytes);
}

} // namespace

FileDescriptor::~FileDescriptor() {
    reset();
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept
    : descriptor_(other.release()), io_hooks_(other.io_hooks_) {
    other.io_hooks_ = {};
}

auto FileDescriptor::operator=(FileDescriptor&& other) noexcept -> FileDescriptor& {
    if (this != &other) {
        reset(other.release());
        io_hooks_ = other.io_hooks_;
        other.io_hooks_ = {};
    }
    return *this;
}

auto FileDescriptor::release() noexcept -> int {
    const auto descriptor = descriptor_;
    descriptor_ = -1;
    return descriptor;
}

void FileDescriptor::reset(const int descriptor, const FileIoHooks io_hooks) noexcept {
    if (descriptor_ >= 0) {
        static_cast<void>(::close(descriptor_));
    }
    descriptor_ = descriptor;
    io_hooks_ = io_hooks;
}

auto FileDescriptor::size() const -> Result<std::uint64_t> {
    const auto status = descriptor_stat(descriptor_);
    if (!status) {
        return unexpected(status.error());
    }
    if (status->st_size < 0) {
        return fail(ErrorCode::io_error, "file reports a negative size");
    }
    return static_cast<std::uint64_t>(status->st_size);
}

auto FileDescriptor::write_all_at(const std::span<const std::byte> bytes, const std::uint64_t offset) const
    -> Status {
    constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
    if (offset > kMaximumOffset || bytes.size() > kMaximumOffset - offset) {
        return fail(ErrorCode::invalid_argument, "positional write extent exceeds off_t");
    }

    std::size_t written{};
    while (written < bytes.size()) {
        const auto remaining = bytes.size() - written;
        const auto chunk = std::min(remaining, static_cast<std::size_t>(SSIZE_MAX));
        const auto current = bytes.subspan(written, chunk);
        const auto result =
            io_hooks_.write_some_at != nullptr
                ? io_hooks_.write_some_at(io_hooks_.context, descriptor_, current, offset + written)
                : static_cast<std::ptrdiff_t>(::pwrite(descriptor_, current.data(), current.size(),
                                                       static_cast<off_t>(offset + written)));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return persistence_system_error("pwrite");
        }
        if (result == 0) {
            return fail(ErrorCode::io_error, "pwrite made no forward progress");
        }
        if (static_cast<std::size_t>(result) > chunk) {
            return fail(ErrorCode::internal_error,
                        "positional write hook returned more bytes than requested");
        }
        written += static_cast<std::size_t>(result);
    }
    return {};
}

auto FileDescriptor::read_exact_at(const std::span<std::byte> bytes, const std::uint64_t offset) const
    -> Status {
    constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
    if (offset > kMaximumOffset || bytes.size() > kMaximumOffset - offset) {
        return fail(ErrorCode::invalid_argument, "positional read extent exceeds off_t");
    }

    std::size_t read{};
    while (read < bytes.size()) {
        const auto remaining = bytes.size() - read;
        const auto chunk = std::min(remaining, static_cast<std::size_t>(SSIZE_MAX));
        const auto current = bytes.subspan(read, chunk);
        const auto result =
            io_hooks_.read_some_at != nullptr
                ? io_hooks_.read_some_at(io_hooks_.context, descriptor_, current, offset + read)
                : static_cast<std::ptrdiff_t>(::pread(descriptor_, current.data(), current.size(),
                                                      static_cast<off_t>(offset + read)));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return persistence_system_error("pread");
        }
        if (result == 0) {
            return fail(ErrorCode::corrupted_data, "file ended before the requested extent");
        }
        if (static_cast<std::size_t>(result) > chunk) {
            return fail(ErrorCode::internal_error, "positional read hook returned more bytes than requested");
        }
        read += static_cast<std::size_t>(result);
    }
    return {};
}

auto FileDescriptor::sync(const FileSyncMode mode) const -> Status {
    int result{};
    do {
        if (io_hooks_.sync_file != nullptr) {
            result = io_hooks_.sync_file(io_hooks_.context, descriptor_, mode);
            continue;
        }
#if defined(__APPLE__)
        result = ::fcntl(descriptor_, mode == FileSyncMode::ordered ? F_BARRIERFSYNC : F_FULLFSYNC, 0);
#else
        result = mode == FileSyncMode::full ? ::fsync(descriptor_) : ::fdatasync(descriptor_);
#endif
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
#if defined(__APPLE__)
        return persistence_system_error(mode == FileSyncMode::ordered ? "fcntl(F_BARRIERFSYNC)"
                                                                      : "fcntl(F_FULLFSYNC)");
#else
        return persistence_system_error(mode == FileSyncMode::full ? "fsync" : "fdatasync");
#endif
    }
    return {};
}

DataDirectory::~DataDirectory() {
    if (health_) {
        health_->store(false, std::memory_order_release);
    }
}

auto DataDirectory::operator=(DataDirectory&& other) noexcept -> DataDirectory& {
    if (this != &other) {
        if (health_) {
            health_->store(false, std::memory_order_release);
        }
        directory_ = std::move(other.directory_);
        lock_ = std::move(other.lock_);
        hooks_ = other.hooks_;
        health_ = std::move(other.health_);
    }
    return *this;
}

auto DataDirectory::open_and_lock(const std::filesystem::path& path, const FilesystemHooks hooks)
    -> Result<DataDirectory> {
    return open_and_lock(path, DataDirectoryOpenMode::existing, hooks);
}

auto DataDirectory::open_and_lock(const std::filesystem::path& path, const DataDirectoryOpenMode mode,
                                  const FilesystemHooks hooks) -> Result<DataDirectory> {
    if (path.empty()) {
        return fail(ErrorCode::invalid_argument, "data directory path cannot be empty");
    }
    FileDescriptor directory;
    if (mode == DataDirectoryOpenMode::existing) {
        directory.reset(interrupted_open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW),
                        hooks.file_io);
    } else {
        const auto leaf = path.filename();
        auto parent = path.parent_path();
        if (leaf.empty() || leaf == "." || leaf == "..") {
            return fail(ErrorCode::invalid_argument,
                        "creatable data directory must have a canonical leaf name");
        }
        if (parent.empty()) {
            parent = ".";
        }
        FileDescriptor parent_directory{
            interrupted_open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW), hooks.file_io};
        if (!parent_directory.valid()) {
            return persistence_system_error("open(data-directory parent)");
        }
        struct stat existing{};
        int inspected{};
        do {
            inspected = ::fstatat(parent_directory.get(), leaf.c_str(), &existing, AT_SYMLINK_NOFOLLOW);
        } while (inspected != 0 && errno == EINTR);
        if (inspected == 0 && mode == DataDirectoryOpenMode::create_new) {
            return fail(ErrorCode::sequence_conflict, "data directory already exists");
        }
        if (inspected != 0 && errno != ENOENT) {
            return persistence_system_error("fstatat(data directory)");
        }
        if (inspected != 0) {
            if (auto allowed =
                    detail::invoke_filesystem_before(hooks, FilesystemOperation::create_data_directory);
                !allowed) {
                return unexpected(allowed.error());
            }
            if (interrupted_mkdir_at(parent_directory.get(), leaf.c_str(), S_IRWXU) == 0) {
                detail::invoke_filesystem_after(hooks, FilesystemOperation::create_data_directory);
                if (auto allowed =
                        detail::invoke_filesystem_before(hooks, FilesystemOperation::sync_parent_directory);
                    !allowed) {
                    return unexpected(allowed.error());
                }
                if (auto synced = parent_directory.sync(FileSyncMode::full); !synced) {
                    return unexpected(synced.error());
                }
                detail::invoke_filesystem_after(hooks, FilesystemOperation::sync_parent_directory);
            } else if (errno != EEXIST || mode == DataDirectoryOpenMode::create_new) {
                if (errno == EEXIST) {
                    return fail(ErrorCode::sequence_conflict, "data directory already exists");
                }
                return persistence_system_error("mkdirat(data directory)");
            }
        }
        directory.reset(interrupted_open_at(parent_directory.get(), leaf.c_str(),
                                            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW),
                        hooks.file_io);
    }
    if (!directory.valid()) {
        return persistence_system_error("open(data directory)");
    }
    if (auto valid = validate_directory_descriptor(directory.get()); !valid) {
        return unexpected(valid.error());
    }

    FileDescriptor lock{interrupted_open_at(directory.get(), kStoreLockFilename,
                                            O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                                            S_IRUSR | S_IWUSR)};
    if (!lock.valid()) {
        return persistence_system_error("openat(Store lock)");
    }
    if (auto valid = validate_private_regular_file(lock.get(), "Store lock"); !valid) {
        return unexpected(valid.error());
    }
    if (auto acquired = lock_exclusive_nonblocking(lock.get()); !acquired) {
        return unexpected(acquired.error());
    }
    return DataDirectory{std::move(directory), std::move(lock), hooks};
}

auto DataDirectory::before(const FilesystemOperation operation) const -> Status {
    return detail::invoke_filesystem_before(hooks_, operation);
}

void DataDirectory::after(const FilesystemOperation operation) const noexcept {
    detail::invoke_filesystem_after(hooks_, operation);
}

auto DataDirectory::sync_directory() const -> Status {
    return directory_.sync(FileSyncMode::full);
}

auto DataDirectory::open_directory_for_enumeration() const -> Result<FileDescriptor> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot enumerate through a poisoned data-directory instance");
    }
    FileDescriptor enumeration{interrupted_open_at(
        directory_.get(), ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)};
    if (!enumeration.valid()) {
        return persistence_system_error("openat(data directory for enumeration)");
    }
    if (auto valid = validate_directory_descriptor(enumeration.get()); !valid) {
        return unexpected(valid.error());
    }
    return enumeration;
}

auto DataDirectory::available_space_bytes() const -> Result<std::uint64_t> {
    if (hooks_.available_space_bytes != nullptr) {
        return hooks_.available_space_bytes(hooks_.context);
    }
    struct statvfs status{};
    if (::fstatvfs(directory_.get(), &status) != 0) {
        return persistence_system_error("fstatvfs(data directory)");
    }
    const auto block_size = status.f_frsize != 0 ? status.f_frsize : status.f_bsize;
    const auto available_blocks = status.f_bavail;
    if (block_size != 0 && available_blocks > std::numeric_limits<std::uint64_t>::max() / block_size) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return static_cast<std::uint64_t>(available_blocks) * static_cast<std::uint64_t>(block_size);
}

auto DataDirectory::publish_manifest(const Manifest& manifest, const std::size_t max_bytes)
    -> ManifestPublicationResult {
    if (!healthy()) {
        return publication_failure(
            ManifestPublicationOutcome::indeterminate,
            Error{ErrorCode::io_error, "data directory is poisoned by an indeterminate publication"});
    }
    const auto encoded = encode_manifest(manifest);
    if (!encoded) {
        return publication_failure(ManifestPublicationOutcome::not_published, encoded.error());
    }
    if (encoded->size() > max_bytes) {
        return publication_failure(
            ManifestPublicationOutcome::not_published,
            Error{ErrorCode::storage_exhausted, "manifest exceeds the configured byte budget"});
    }
    const auto current = read_manifest(max_bytes);
    if (current) {
        if (manifest.manifest_generation <= current->manifest_generation) {
            return publication_failure(ManifestPublicationOutcome::not_published,
                                       Error{ErrorCode::sequence_conflict,
                                             "manifest publication generation must advance monotonically"});
        }
    } else if (current.error().code != ErrorCode::not_found) {
        return publication_failure(ManifestPublicationOutcome::not_published, current.error());
    }
    if (auto removed = unlink_temporary(directory_.get()); !removed) {
        return publication_failure(ManifestPublicationOutcome::not_published, removed.error());
    }

    FileDescriptor temporary{
        interrupted_open_at(directory_.get(), kManifestTemporaryFilename,
                            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                            S_IRUSR | S_IWUSR),
        hooks_.file_io};
    if (!temporary.valid()) {
        return publication_failure(ManifestPublicationOutcome::not_published,
                                   persistence_system_error("openat(manifest temporary)").error);
    }

    if (auto allowed = before(FilesystemOperation::write_manifest); !allowed) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, allowed.error());
    }
    if (auto written = temporary.write_all_at(*encoded, 0); !written) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, written.error());
    }
    after(FilesystemOperation::write_manifest);
    if (auto allowed = before(FilesystemOperation::sync_manifest); !allowed) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, allowed.error());
    }
    if (auto synced = temporary.sync(FileSyncMode::full); !synced) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, synced.error());
    }
    after(FilesystemOperation::sync_manifest);
    if (auto allowed = before(FilesystemOperation::rename_manifest); !allowed) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, allowed.error());
    }

    if (::renameat(directory_.get(), kManifestTemporaryFilename, directory_.get(), kManifestFilename) != 0) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate,
                                   persistence_system_error("renameat(manifest)").error);
    }
    after(FilesystemOperation::rename_manifest);
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate, allowed.error());
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate, synced.error());
    }
    after(FilesystemOperation::sync_directory);
    return {.outcome = ManifestPublicationOutcome::durable, .error = std::nullopt};
}

auto DataDirectory::publish_bootstrap_intent(const Manifest& manifest, const std::size_t max_bytes)
    -> Status {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot publish bootstrap intent through a poisoned directory");
    }
    auto encoded = encode_manifest(manifest);
    if (!encoded) {
        return unexpected(encoded.error());
    }
    if (encoded->size() > max_bytes) {
        return fail(ErrorCode::storage_exhausted, "bootstrap intent exceeds the configured byte budget");
    }
    struct stat existing{};
    if (::fstatat(directory_.get(), kBootstrapIntentFilename, &existing, AT_SYMLINK_NOFOLLOW) == 0) {
        return fail(ErrorCode::sequence_conflict, "bootstrap intent already exists");
    }
    if (errno != ENOENT) {
        return persistence_system_error("fstatat(bootstrap intent)");
    }
    if (::unlinkat(directory_.get(), kBootstrapTemporaryFilename, 0) != 0 && errno != ENOENT) {
        return persistence_system_error("unlinkat(bootstrap temporary)");
    }
    FileDescriptor intent{
        interrupted_open_at(directory_.get(), kBootstrapTemporaryFilename,
                            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                            S_IRUSR | S_IWUSR),
        hooks_.file_io};
    if (!intent.valid()) {
        return persistence_system_error("openat(bootstrap temporary)");
    }
    const auto cleanup = [this]() -> Status {
        if (::unlinkat(directory_.get(), kBootstrapTemporaryFilename, 0) != 0 && errno != ENOENT) {
            return persistence_system_error("unlinkat(bootstrap temporary cleanup)");
        }
        return sync_directory();
    };
    if (auto allowed = before(FilesystemOperation::write_bootstrap); !allowed) {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return cleaned;
        }
        return allowed;
    }
    if (auto written = intent.write_all_at(*encoded, 0); !written) {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return cleaned;
        }
        return unexpected(written.error());
    }
    after(FilesystemOperation::write_bootstrap);
    if (auto allowed = before(FilesystemOperation::sync_bootstrap); !allowed) {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return cleaned;
        }
        return allowed;
    }
    if (auto synced = intent.sync(FileSyncMode::full); !synced) {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return cleaned;
        }
        return unexpected(synced.error());
    }
    after(FilesystemOperation::sync_bootstrap);
    if (auto allowed = before(FilesystemOperation::rename_bootstrap); !allowed) {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return cleaned;
        }
        return allowed;
    }
    if (::renameat(directory_.get(), kBootstrapTemporaryFilename, directory_.get(),
                   kBootstrapIntentFilename) != 0) {
        health_->store(false, std::memory_order_release);
        return persistence_system_error("renameat(bootstrap intent)");
    }
    after(FilesystemOperation::rename_bootstrap);
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return allowed;
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return synced;
    }
    after(FilesystemOperation::sync_directory);
    return {};
}

auto DataDirectory::read_bootstrap_intent(const std::size_t max_bytes) const -> Result<Manifest> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read bootstrap intent through a poisoned directory");
    }
    return read_private_manifest_file(directory_.get(), kBootstrapIntentFilename, "bootstrap intent",
                                      max_bytes, hooks_.file_io);
}

auto DataDirectory::finish_bootstrap() -> Status {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot finish bootstrap through a poisoned directory");
    }
    if (auto allowed = before(FilesystemOperation::remove_bootstrap); !allowed) {
        return allowed;
    }
    if (::unlinkat(directory_.get(), kBootstrapIntentFilename, 0) != 0) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, "bootstrap intent does not exist");
        }
        return persistence_system_error("unlinkat(bootstrap intent)");
    }
    after(FilesystemOperation::remove_bootstrap);
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return allowed;
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return synced;
    }
    after(FilesystemOperation::sync_directory);
    return {};
}

auto DataDirectory::publish_compaction_intent(const DurableCompactionIntent& compaction,
                                              const std::size_t max_manifest_bytes)
    -> CompactionIntentPublicationResult {
    if (!healthy()) {
        return compaction_publication_failure(
            CompactionIntentPublicationOutcome::indeterminate,
            Error{ErrorCode::io_error, "cannot publish compaction intent through a poisoned directory"});
    }
    if (max_manifest_bytes > kMaximumManifestBytes) {
        return compaction_publication_failure(
            CompactionIntentPublicationOutcome::not_published,
            Error{ErrorCode::invalid_argument,
                  "compaction intent manifest byte budget exceeds the supported format bound"});
    }
    const auto old_size = encoded_manifest_size(compaction.old_manifest);
    const auto next_size = encoded_manifest_size(compaction.next_manifest);
    const auto encoded = encode_compaction_intent(compaction);
    if (!old_size || !next_size || !encoded) {
        if (!old_size) {
            return compaction_publication_failure(CompactionIntentPublicationOutcome::not_published,
                                                  old_size.error());
        }
        return compaction_publication_failure(CompactionIntentPublicationOutcome::not_published,
                                              !next_size ? next_size.error() : encoded.error());
    }
    if (*old_size > max_manifest_bytes || *next_size > max_manifest_bytes) {
        return compaction_publication_failure(
            CompactionIntentPublicationOutcome::not_published,
            Error{ErrorCode::storage_exhausted,
                  "compaction intent exceeds the configured manifest byte budget"});
    }
    struct stat existing{};
    int inspected{};
    do {
        inspected = ::fstatat(directory_.get(), kCompactionIntentFilename, &existing, AT_SYMLINK_NOFOLLOW);
    } while (inspected != 0 && errno == EINTR);
    if (inspected == 0) {
        return compaction_publication_failure(
            CompactionIntentPublicationOutcome::not_published,
            Error{ErrorCode::sequence_conflict, "compaction intent already exists"});
    }
    if (errno != ENOENT) {
        return compaction_publication_failure(CompactionIntentPublicationOutcome::not_published,
                                              persistence_system_error("fstatat(compaction intent)").error);
    }
    if (::unlinkat(directory_.get(), kCompactionTemporaryFilename, 0) != 0 && errno != ENOENT) {
        return compaction_publication_failure(
            CompactionIntentPublicationOutcome::not_published,
            persistence_system_error("unlinkat(compaction temporary)").error);
    }

    FileDescriptor intent{
        interrupted_open_at(directory_.get(), kCompactionTemporaryFilename,
                            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                            S_IRUSR | S_IWUSR),
        hooks_.file_io};
    if (!intent.valid()) {
        return compaction_publication_failure(CompactionIntentPublicationOutcome::not_published,
                                              persistence_system_error("openat(compaction temporary)").error);
    }
    const auto cleanup = [this]() -> Status {
        if (::unlinkat(directory_.get(), kCompactionTemporaryFilename, 0) != 0 && errno != ENOENT) {
            return persistence_system_error("unlinkat(compaction temporary cleanup)");
        }
        return sync_directory();
    };
    const auto fail_before_rename = [&](Error error) -> CompactionIntentPublicationResult {
        if (auto cleaned = cleanup(); !cleaned) {
            health_->store(false, std::memory_order_release);
            return compaction_publication_failure(CompactionIntentPublicationOutcome::indeterminate,
                                                  cleaned.error());
        }
        return compaction_publication_failure(CompactionIntentPublicationOutcome::not_published,
                                              std::move(error));
    };

    if (auto allowed = before(FilesystemOperation::write_compaction_intent); !allowed) {
        return fail_before_rename(allowed.error());
    }
    if (auto written = intent.write_all_at(*encoded, 0); !written) {
        return fail_before_rename(written.error());
    }
    after(FilesystemOperation::write_compaction_intent);
    if (auto allowed = before(FilesystemOperation::sync_compaction_intent); !allowed) {
        return fail_before_rename(allowed.error());
    }
    if (auto synced = intent.sync(FileSyncMode::full); !synced) {
        return fail_before_rename(synced.error());
    }
    after(FilesystemOperation::sync_compaction_intent);
    if (auto allowed = before(FilesystemOperation::rename_compaction_intent); !allowed) {
        return fail_before_rename(allowed.error());
    }
    if (::renameat(directory_.get(), kCompactionTemporaryFilename, directory_.get(),
                   kCompactionIntentFilename) != 0) {
        health_->store(false, std::memory_order_release);
        return compaction_publication_failure(CompactionIntentPublicationOutcome::indeterminate,
                                              persistence_system_error("renameat(compaction intent)").error);
    }
    after(FilesystemOperation::rename_compaction_intent);
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return compaction_publication_failure(CompactionIntentPublicationOutcome::indeterminate,
                                              allowed.error());
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return compaction_publication_failure(CompactionIntentPublicationOutcome::indeterminate,
                                              synced.error());
    }
    after(FilesystemOperation::sync_directory);
    return {.outcome = CompactionIntentPublicationOutcome::durable, .error = std::nullopt};
}

auto DataDirectory::read_compaction_intent(const std::size_t max_manifest_bytes) const
    -> Result<DurableCompactionIntent> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read compaction intent through a poisoned directory");
    }
    return read_private_compaction_intent_file(directory_.get(), max_manifest_bytes, hooks_.file_io);
}

auto DataDirectory::remove_compaction_intent() -> CompactionIntentRemovalResult {
    if (!healthy()) {
        return compaction_removal_failure(
            CompactionIntentRemovalOutcome::indeterminate,
            Error{ErrorCode::io_error, "cannot remove compaction intent through a poisoned directory"});
    }
    if (::unlinkat(directory_.get(), kCompactionTemporaryFilename, 0) != 0 && errno != ENOENT) {
        health_->store(false, std::memory_order_release);
        return compaction_removal_failure(
            CompactionIntentRemovalOutcome::indeterminate,
            persistence_system_error("unlinkat(compaction temporary during completion)").error);
    }
    if (auto allowed = before(FilesystemOperation::remove_compaction_intent); !allowed) {
        return compaction_removal_failure(CompactionIntentRemovalOutcome::not_removed, allowed.error());
    }
    if (::unlinkat(directory_.get(), kCompactionIntentFilename, 0) != 0) {
        if (errno == ENOENT) {
            return compaction_removal_failure(
                CompactionIntentRemovalOutcome::not_removed,
                Error{ErrorCode::not_found, "compaction intent does not exist"});
        }
        health_->store(false, std::memory_order_release);
        return compaction_removal_failure(CompactionIntentRemovalOutcome::indeterminate,
                                          persistence_system_error("unlinkat(compaction intent)").error);
    }
    after(FilesystemOperation::remove_compaction_intent);
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return compaction_removal_failure(CompactionIntentRemovalOutcome::indeterminate, allowed.error());
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return compaction_removal_failure(CompactionIntentRemovalOutcome::indeterminate, synced.error());
    }
    after(FilesystemOperation::sync_directory);
    return {.outcome = CompactionIntentRemovalOutcome::durable, .error = std::nullopt};
}

auto DataDirectory::retire_compaction_segments(const StoreId& store_id,
                                               const std::span<const ManifestSegmentEntry> segments)
    -> CompactionSegmentRetirementResult {
    if (!healthy()) {
        return compaction_retirement_failure(
            CompactionSegmentRetirementOutcome::indeterminate,
            Error{ErrorCode::io_error, "cannot retire Segments through a poisoned directory"});
    }
    bool removed_any{};
    for (const auto& entry : segments) {
        const auto name = segment_filename({.store_id = store_id,
                                            .segment_id = entry.segment_id,
                                            .generation = entry.generation,
                                            .owner_worker = entry.owner_worker});
        if (auto allowed = before(FilesystemOperation::remove_compaction_segment); !allowed) {
            if (removed_any) {
                health_->store(false, std::memory_order_release);
            }
            return compaction_retirement_failure(removed_any
                                                     ? CompactionSegmentRetirementOutcome::indeterminate
                                                     : CompactionSegmentRetirementOutcome::not_removed,
                                                 allowed.error());
        }
        if (::unlinkat(directory_.get(), name.c_str(), 0) != 0) {
            if (errno == ENOENT) {
                continue;
            }
            health_->store(false, std::memory_order_release);
            return compaction_retirement_failure(
                CompactionSegmentRetirementOutcome::indeterminate,
                persistence_system_error("unlinkat(compaction Segment)").error);
        }
        removed_any = true;
        after(FilesystemOperation::remove_compaction_segment);
    }
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return compaction_retirement_failure(CompactionSegmentRetirementOutcome::indeterminate,
                                             allowed.error());
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return compaction_retirement_failure(CompactionSegmentRetirementOutcome::indeterminate,
                                             synced.error());
    }
    after(FilesystemOperation::sync_directory);
    return {.outcome = CompactionSegmentRetirementOutcome::durable, .error = std::nullopt};
}

auto DataDirectory::pristine_for_bootstrap() const -> Result<bool> {
    auto descriptor = open_directory_for_enumeration();
    if (!descriptor) {
        return unexpected(descriptor.error());
    }
    auto* stream = ::fdopendir(descriptor->get());
    if (!stream) {
        return persistence_system_error("fdopendir(bootstrap namespace)");
    }
    static_cast<void>(descriptor->release());
    const std::unique_ptr<DIR, int (*)(DIR*)> owned{stream, &::closedir};
    for (;;) {
        errno = 0;
        const auto* entry = ::readdir(owned.get());
        if (!entry) {
            if (errno != 0) {
                return persistence_system_error("readdir(bootstrap namespace)");
            }
            return true;
        }
        const std::string_view name{entry->d_name};
        if (name != "." && name != ".." && name != kStoreLockFilename &&
            name != kBootstrapTemporaryFilename) {
            return false;
        }
    }
}

auto DataDirectory::read_manifest(const std::size_t max_bytes) const -> Result<Manifest> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read through a poisoned data-directory instance");
    }
    return read_private_manifest_file(directory_.get(), kManifestFilename, "manifest", max_bytes,
                                      hooks_.file_io);
}

} // namespace glyphastore
