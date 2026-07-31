#include "glyphastore/persistence/store_backup.hpp"

#include "glyphastore/persistence/segment_file.hpp"
#include "system_error.hpp"

#include <array>
#include <cerrno>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace glyphastore {
namespace {

[[nodiscard]] auto interrupted_open(const char* path, int flags) -> int {
    int descriptor{};
    do {
        descriptor = ::open(path, flags);
    } while (descriptor < 0 && errno == EINTR);
    return descriptor;
}

[[nodiscard]] auto interrupted_open_at(int directory, const char* name, int flags, mode_t mode = 0) -> int {
    int descriptor{};
    do {
        descriptor = mode == 0 ? ::openat(directory, name, flags) : ::openat(directory, name, flags, mode);
    } while (descriptor < 0 && errno == EINTR);
    return descriptor;
}

[[nodiscard]] auto copy_named_file(int source_directory, int destination_directory, const char* name)
    -> Result<std::uint64_t> {
    FileDescriptor source{interrupted_open_at(source_directory, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
    if (!source.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, std::string{"backup source file is missing: "} + name);
        }
        return persistence_system_error("openat(backup source file)");
    }
    FileDescriptor destination{interrupted_open_at(destination_directory, name,
                                                   O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                                                   S_IRUSR | S_IWUSR)};
    if (!destination.valid()) {
        if (errno == EEXIST) {
            return fail(ErrorCode::sequence_conflict,
                        std::string{"backup destination already contains: "} + name);
        }
        return persistence_system_error("openat(backup destination file)");
    }

    std::array<std::byte, 1U << 20> buffer{};
    std::uint64_t copied{};
    for (;;) {
        ssize_t read_count{};
        do {
            read_count = ::read(source.get(), buffer.data(), buffer.size());
        } while (read_count < 0 && errno == EINTR);
        if (read_count < 0) {
            return persistence_system_error("read(backup source file)");
        }
        if (read_count == 0) {
            break;
        }
        std::size_t offset{};
        const auto total = static_cast<std::size_t>(read_count);
        while (offset < total) {
            ssize_t written{};
            do {
                written = ::write(destination.get(), buffer.data() + offset, total - offset);
            } while (written < 0 && errno == EINTR);
            if (written < 0) {
                return persistence_system_error("write(backup destination file)");
            }
            offset += static_cast<std::size_t>(written);
        }
        copied += static_cast<std::uint64_t>(read_count);
    }
    if (auto synced = destination.sync(FileSyncMode::full); !synced) {
        return unexpected(synced.error());
    }
    return copied;
}

} // namespace

auto backup_durable_store_from_open_directory(DataDirectory& source, const Manifest& catalog_manifest,
                                              const std::filesystem::path& destination,
                                              const bool scan_records, const DurableResourceLimits& limits)
    -> Result<DurableStoreBackupReport> {
    if (destination.empty()) {
        return fail(ErrorCode::invalid_argument, "backup destination path is required");
    }
    if (!source.healthy()) {
        return fail(ErrorCode::unavailable, "cannot backup through a poisoned data-directory instance");
    }

    DurableStoreBackupReport report{.destination = destination};

    auto source_verification = verify_durable_store(source, scan_records, limits);
    if (!source_verification) {
        return unexpected(source_verification.error());
    }
    // Prefer the caller's catalog snapshot when verify rebuilt an equivalent Manifest.
    if (source_verification->manifest.store_id != catalog_manifest.store_id ||
        source_verification->manifest.segments.size() != catalog_manifest.segments.size()) {
        return fail(ErrorCode::corrupted_data,
                    "open-store backup Manifest disagrees with the live catalog snapshot");
    }
    report.source_verification = std::move(*source_verification);

    {
        auto destination_locked =
            DataDirectory::open_and_lock(destination, DataDirectoryOpenMode::create_new);
        if (!destination_locked) {
            return unexpected(destination_locked.error());
        }
        if (auto pristine = destination_locked->pristine_for_bootstrap(); !pristine) {
            return unexpected(pristine.error());
        } else if (!*pristine) {
            return fail(ErrorCode::invalid_argument, "backup destination data directory is not empty");
        }

        FileDescriptor destination_root{
            interrupted_open(destination.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)};
        if (!destination_root.valid()) {
            return persistence_system_error("open(backup destination directory for copy)");
        }

        const int source_fd = source.directory_descriptor();
        for (const auto& entry : catalog_manifest.segments) {
            const SegmentHeaderIdentity identity{
                .store_id = catalog_manifest.store_id,
                .segment_id = entry.segment_id,
                .generation = entry.generation,
                .owner_worker = entry.owner_worker,
            };
            const auto name = segment_filename(identity);
            auto copied = copy_named_file(source_fd, destination_root.get(), name.c_str());
            if (!copied) {
                return unexpected(copied.error());
            }
            ++report.files_copied;
            report.bytes_copied += *copied;
        }

        auto manifest_copied = copy_named_file(source_fd, destination_root.get(), kManifestFilename);
        if (!manifest_copied) {
            return unexpected(manifest_copied.error());
        }
        ++report.files_copied;
        report.bytes_copied += *manifest_copied;

        if (auto synced = destination_root.sync(FileSyncMode::full); !synced) {
            return unexpected(synced.error());
        }
    } // release destination exclusive lock before verify re-opens the path

    auto destination_verification = verify_durable_store_path(destination, scan_records, limits);
    if (!destination_verification) {
        return unexpected(destination_verification.error());
    }
    report.destination_verification = std::move(*destination_verification);
    return report;
}

auto backup_durable_store(const std::filesystem::path& source, const std::filesystem::path& destination,
                          const bool scan_records, const DurableResourceLimits& limits)
    -> Result<DurableStoreBackupReport> {
    if (source.empty() || destination.empty()) {
        return fail(ErrorCode::invalid_argument, "backup source and destination paths are required");
    }
    if (source == destination) {
        return fail(ErrorCode::invalid_argument, "backup source and destination must differ");
    }

    auto source_locked = DataDirectory::open_and_lock(source, DataDirectoryOpenMode::existing);
    if (!source_locked) {
        return unexpected(source_locked.error());
    }
    auto source_verification = verify_durable_store(*source_locked, scan_records, limits);
    if (!source_verification) {
        return unexpected(source_verification.error());
    }
    source_verification->path = source;
    const Manifest catalog = source_verification->manifest;
    auto backed =
        backup_durable_store_from_open_directory(*source_locked, catalog, destination, scan_records, limits);
    if (!backed) {
        return unexpected(backed.error());
    }
    backed->source = source;
    backed->source_verification.path = source;
    return backed;
}

} // namespace glyphastore
