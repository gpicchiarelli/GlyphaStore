#include "glyphastore/persistence/filesystem.hpp"

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

auto read_private_manifest_file(int directory, const char* name, std::string_view description)
    -> Result<Manifest> {
    FileDescriptor file{interrupted_open_at(directory, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)};
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

} // namespace

FileDescriptor::~FileDescriptor() {
    reset();
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept : descriptor_(other.release()) {}

auto FileDescriptor::operator=(FileDescriptor&& other) noexcept -> FileDescriptor& {
    if (this != &other) {
        reset(other.release());
    }
    return *this;
}

auto FileDescriptor::release() noexcept -> int {
    const auto descriptor = descriptor_;
    descriptor_ = -1;
    return descriptor;
}

void FileDescriptor::reset(const int descriptor) noexcept {
    if (descriptor_ >= 0) {
        static_cast<void>(::close(descriptor_));
    }
    descriptor_ = descriptor;
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
        const auto result =
            ::pwrite(descriptor_, bytes.data() + written, chunk, static_cast<off_t>(offset + written));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return persistence_system_error("pwrite");
        }
        if (result == 0) {
            return fail(ErrorCode::io_error, "pwrite made no forward progress");
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
        const auto result =
            ::pread(descriptor_, bytes.data() + read, chunk, static_cast<off_t>(offset + read));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return persistence_system_error("pread");
        }
        if (result == 0) {
            return fail(ErrorCode::corrupted_data, "file ended before the requested extent");
        }
        read += static_cast<std::size_t>(result);
    }
    return {};
}

auto FileDescriptor::sync(const FileSyncMode mode) const -> Status {
    int result{};
    do {
#if defined(__APPLE__)
        static_cast<void>(mode);
        result = ::fcntl(descriptor_, F_FULLFSYNC, 0);
#else
        result = mode == FileSyncMode::data ? ::fdatasync(descriptor_) : ::fsync(descriptor_);
#endif
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
#if defined(__APPLE__)
        return persistence_system_error("fcntl(F_FULLFSYNC)");
#else
        return persistence_system_error(mode == FileSyncMode::data ? "fdatasync" : "fsync");
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
        directory.reset(interrupted_open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
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
            interrupted_open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)};
        if (!parent_directory.valid()) {
            return persistence_system_error("open(data-directory parent)");
        }
        if (interrupted_mkdir_at(parent_directory.get(), leaf.c_str(), S_IRWXU) == 0) {
            if (auto synced = parent_directory.sync(FileSyncMode::full); !synced) {
                return unexpected(synced.error());
            }
        } else if (errno != EEXIST || mode == DataDirectoryOpenMode::create_new) {
            if (errno == EEXIST) {
                return fail(ErrorCode::sequence_conflict, "data directory already exists");
            }
            return persistence_system_error("mkdirat(data directory)");
        }
        directory.reset(interrupted_open_at(parent_directory.get(), leaf.c_str(),
                                            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
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
    if (!hooks_.before) {
        return {};
    }
    return hooks_.before(hooks_.context, operation);
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

auto DataDirectory::publish_manifest(const Manifest& manifest) -> ManifestPublicationResult {
    if (!healthy()) {
        return publication_failure(
            ManifestPublicationOutcome::indeterminate,
            Error{ErrorCode::io_error, "data directory is poisoned by an indeterminate publication"});
    }
    const auto encoded = encode_manifest(manifest);
    if (!encoded) {
        return publication_failure(ManifestPublicationOutcome::not_published, encoded.error());
    }
    const auto current = read_manifest();
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

    FileDescriptor temporary{interrupted_open_at(
        directory_.get(), kManifestTemporaryFilename,
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, S_IRUSR | S_IWUSR)};
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
    if (auto allowed = before(FilesystemOperation::sync_manifest); !allowed) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, allowed.error());
    }
    if (auto synced = temporary.sync(FileSyncMode::full); !synced) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, synced.error());
    }
    if (auto allowed = before(FilesystemOperation::rename_manifest); !allowed) {
        static_cast<void>(unlink_temporary(directory_.get()));
        return publication_failure(ManifestPublicationOutcome::not_published, allowed.error());
    }

    if (::renameat(directory_.get(), kManifestTemporaryFilename, directory_.get(), kManifestFilename) != 0) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate,
                                   persistence_system_error("renameat(manifest)").error);
    }
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate, allowed.error());
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return publication_failure(ManifestPublicationOutcome::indeterminate, synced.error());
    }
    return {.outcome = ManifestPublicationOutcome::durable, .error = std::nullopt};
}

auto DataDirectory::publish_bootstrap_intent(const Manifest& manifest) -> Status {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot publish bootstrap intent through a poisoned directory");
    }
    auto encoded = encode_manifest(manifest);
    if (!encoded) {
        return unexpected(encoded.error());
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
    FileDescriptor intent{interrupted_open_at(
        directory_.get(), kBootstrapTemporaryFilename,
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, S_IRUSR | S_IWUSR)};
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
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return allowed;
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return synced;
    }
    return {};
}

auto DataDirectory::read_bootstrap_intent() const -> Result<Manifest> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read bootstrap intent through a poisoned directory");
    }
    return read_private_manifest_file(directory_.get(), kBootstrapIntentFilename, "bootstrap intent");
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
    if (auto allowed = before(FilesystemOperation::sync_directory); !allowed) {
        health_->store(false, std::memory_order_release);
        return allowed;
    }
    if (auto synced = sync_directory(); !synced) {
        health_->store(false, std::memory_order_release);
        return synced;
    }
    return {};
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

auto DataDirectory::read_manifest() const -> Result<Manifest> {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read through a poisoned data-directory instance");
    }
    return read_private_manifest_file(directory_.get(), kManifestFilename, "manifest");
}

} // namespace glyphastore
