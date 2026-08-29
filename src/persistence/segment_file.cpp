#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/core/little_endian.hpp"

#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/segment/record.hpp"
#include "system_error.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

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

auto validate_private_segment(int descriptor) -> Status {
    struct stat status{};
    if (::fstat(descriptor, &status) != 0) {
        return persistence_system_error("fstat(segment)");
    }
    if (!S_ISREG(status.st_mode) || status.st_nlink != 1 || status.st_uid != ::geteuid() ||
        (status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
        return fail(ErrorCode::invalid_argument, "Segment must be a private, singly linked regular file");
    }
    return {};
}

auto truncate_exact(int descriptor, std::uint64_t size) -> Status {
    if (size > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        return fail(ErrorCode::invalid_argument, "Segment size exceeds off_t");
    }
    int result{};
    do {
        result = ::ftruncate(descriptor, static_cast<off_t>(size));
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        return persistence_system_error("ftruncate(segment)");
    }
    return {};
}

auto preallocate_segment(const FileDescriptor& file) -> Status {
    constexpr auto size = static_cast<std::uint64_t>(kSegmentSizeBytes);
#if defined(__linux__)
    int result{};
    do {
        result = ::fallocate(file.get(), 0, static_cast<off_t>(0), static_cast<off_t>(size));
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        return persistence_system_error("fallocate(segment)");
    }
#elif defined(__APPLE__)
    fstore_t store{};
    store.fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL;
    store.fst_posmode = F_PEOFPOSMODE;
    store.fst_offset = 0;
    store.fst_length = static_cast<off_t>(size);
    if (::fcntl(file.get(), F_PREALLOCATE, &store) != 0) {
        store.fst_flags = F_ALLOCATEALL;
        store.fst_bytesalloc = 0;
        if (::fcntl(file.get(), F_PREALLOCATE, &store) != 0) {
            return persistence_system_error("fcntl(F_PREALLOCATE)");
        }
    }
    if (store.fst_bytesalloc < static_cast<off_t>(size)) {
        return fail(ErrorCode::storage_exhausted, "F_PREALLOCATE did not reserve the complete Segment");
    }
#elif defined(__OpenBSD__)
    // OpenBSD has no documented allocation-reservation syscall. Eagerly writing
    // the complete new file avoids accepting a sparse ftruncate-only Segment.
    static constexpr std::array<std::byte, 256U * 1024U> zeros{};
    for (std::uint64_t offset = 0; offset < size; offset += zeros.size()) {
        if (auto written = file.write_all_at(zeros, offset); !written) {
            return unexpected(written.error());
        }
    }
#else
    int result{};
    do {
        result = ::posix_fallocate(file.get(), 0, static_cast<off_t>(size));
    } while (result == EINTR);
    if (result != 0) {
        return persistence_system_error("posix_fallocate(segment)", result);
    }
#endif
    return truncate_exact(file.get(), size);
}

auto unlink_named(int directory, const std::string& name, std::string_view description) -> Status {
    if (::unlinkat(directory, name.c_str(), 0) == 0 || errno == ENOENT) {
        return {};
    }
    return persistence_system_error(std::string{"unlinkat("} + std::string{description} + ')');
}

auto creation_failure(SegmentFileCreationOutcome outcome, Error error) -> SegmentFileCreationResult {
    return {.outcome = outcome, .file = std::nullopt, .error = std::move(error)};
}

auto commit_failure(SegmentCommitOutcome outcome, Error error) -> SegmentCommitResult {
    return {.outcome = outcome, .error = std::move(error)};
}

using le::get_u32;


auto fixed_hex(std::uint64_t value, std::size_t width) -> std::string {
    std::array<char, 16> digits{};
    const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), value, 16);
    const auto count = static_cast<std::size_t>(converted.ptr - digits.data());
    std::string result(width - count, '0');
    result.append(digits.data(), converted.ptr);
    return result;
}

} // namespace

auto segment_filename(const SegmentHeaderIdentity& identity) -> std::string {
    return "segment-" + fixed_hex(identity.segment_id.value, 16) + '-' +
           fixed_hex(identity.generation.value, 8) + ".glypha";
}

auto DurableSegmentFile::create(DataDirectory& directory, const SegmentHeaderIdentity& identity)
    -> SegmentFileCreationResult {
    if (!directory.healthy()) {
        return creation_failure(
            SegmentFileCreationOutcome::indeterminate,
            Error{ErrorCode::io_error, "data directory is poisoned by an indeterminate publication"});
    }

    const SegmentCommit initial_commit{
        .commit_generation = 1,
        .committed_end = static_cast<std::uint32_t>(kSegmentHeaderReservedBytes),
        .state = PersistedSegmentState::active,
        .record_count = 0,
        .first_sequence = SequenceNumber{0},
        .last_sequence = SequenceNumber{0},
    };
    std::array<std::byte, kSegmentHeaderReservedBytes> header_bytes{};
    const SegmentHeader header{.identity = identity, .commits = {initial_commit, std::nullopt}};
    if (auto encoded = encode_segment_header(header_bytes, header); !encoded) {
        return creation_failure(SegmentFileCreationOutcome::not_published, encoded.error());
    }

    const auto final_name = segment_filename(identity);
    const auto temporary_name = '.' + final_name + ".tmp";
    struct stat existing{};
    if (::fstatat(directory.directory_.get(), final_name.c_str(), &existing, AT_SYMLINK_NOFOLLOW) == 0) {
        return creation_failure(
            SegmentFileCreationOutcome::not_published,
            Error{ErrorCode::sequence_conflict, "Segment identity already exists in the data directory"});
    }
    if (errno != ENOENT) {
        return creation_failure(SegmentFileCreationOutcome::not_published,
                                persistence_system_error("fstatat(segment name)").error);
    }
    if (auto removed = unlink_named(directory.directory_.get(), temporary_name, "Segment temporary");
        !removed) {
        return creation_failure(SegmentFileCreationOutcome::not_published, removed.error());
    }

    FileDescriptor temporary{
        interrupted_open_at(directory.directory_.get(), temporary_name.c_str(),
                            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                            S_IRUSR | S_IWUSR),
        directory.hooks_.file_io};
    if (!temporary.valid()) {
        return creation_failure(SegmentFileCreationOutcome::not_published,
                                persistence_system_error("openat(Segment temporary)").error);
    }

    const auto cleanup = [&directory, &temporary_name]() {
        static_cast<void>(unlink_named(directory.directory_.get(), temporary_name, "Segment temporary"));
    };
    if (auto allowed = directory.before(FilesystemOperation::preallocate_segment); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (auto allocated = preallocate_segment(temporary); !allocated) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allocated.error());
    }
    directory.after(FilesystemOperation::preallocate_segment);
    if (auto allowed = directory.before(FilesystemOperation::write_segment_header); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (auto written = temporary.write_all_at(header_bytes, 0); !written) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, written.error());
    }
    directory.after(FilesystemOperation::write_segment_header);
    if (auto allowed = directory.before(FilesystemOperation::sync_segment_file); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (auto synced = temporary.sync(FileSyncMode::full); !synced) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, synced.error());
    }
    directory.after(FilesystemOperation::sync_segment_file);
    if (auto allowed = directory.before(FilesystemOperation::rename_segment); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (::renameat(directory.directory_.get(), temporary_name.c_str(), directory.directory_.get(),
                   final_name.c_str()) != 0) {
        directory.health_->store(false, std::memory_order_release);
        return creation_failure(SegmentFileCreationOutcome::indeterminate,
                                persistence_system_error("renameat(Segment)").error);
    }
    directory.after(FilesystemOperation::rename_segment);
    if (auto allowed = directory.before(FilesystemOperation::sync_directory); !allowed) {
        directory.health_->store(false, std::memory_order_release);
        return creation_failure(SegmentFileCreationOutcome::indeterminate, allowed.error());
    }
    if (auto synced = directory.sync_directory(); !synced) {
        directory.health_->store(false, std::memory_order_release);
        return creation_failure(SegmentFileCreationOutcome::indeterminate, synced.error());
    }
    directory.after(FilesystemOperation::sync_directory);

    DurableSegmentFile file{std::move(temporary),
                            identity,
                            SelectedSegmentCommit{.slot_index = 0, .commit = initial_commit},
                            directory.hooks_,
                            directory.health_,
                            true};
    return {.outcome = SegmentFileCreationOutcome::durable, .file = std::move(file), .error = std::nullopt};
}

auto DurableSegmentFile::create_staged(DataDirectory& directory, const SegmentHeaderIdentity& identity)
    -> Result<DurableSegmentFile> {
    if (!directory.healthy()) {
        return fail(ErrorCode::io_error, "cannot stage a Segment through a poisoned data directory");
    }

    const SegmentCommit initial_commit{
        .commit_generation = 1,
        .committed_end = static_cast<std::uint32_t>(kSegmentHeaderReservedBytes),
        .state = PersistedSegmentState::active,
        .record_count = 0,
        .first_sequence = SequenceNumber{0},
        .last_sequence = SequenceNumber{0},
    };
    std::array<std::byte, kSegmentHeaderReservedBytes> header_bytes{};
    const SegmentHeader header{.identity = identity, .commits = {initial_commit, std::nullopt}};
    if (auto encoded = encode_segment_header(header_bytes, header); !encoded) {
        return unexpected(encoded.error());
    }

    const auto final_name = segment_filename(identity);
    const auto temporary_name = '.' + final_name + ".tmp";
    struct stat existing{};
    if (::fstatat(directory.directory_.get(), final_name.c_str(), &existing, AT_SYMLINK_NOFOLLOW) == 0) {
        return fail(ErrorCode::sequence_conflict,
                    "staged Segment identity already exists in the data directory");
    }
    if (errno != ENOENT) {
        return persistence_system_error("fstatat(staged Segment final name)");
    }

    FileDescriptor temporary{
        interrupted_open_at(directory.directory_.get(), temporary_name.c_str(),
                            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
                            S_IRUSR | S_IWUSR),
        directory.hooks_.file_io};
    if (!temporary.valid()) {
        if (errno == EEXIST) {
            return fail(ErrorCode::sequence_conflict,
                        "staged Segment temporary identity is already owned by another build");
        }
        return persistence_system_error("openat(staged Segment)");
    }
    const auto cleanup = [&directory, &temporary_name]() noexcept {
        static_cast<void>(::unlinkat(directory.directory_.get(), temporary_name.c_str(), 0));
    };
    if (auto allowed = directory.before(FilesystemOperation::preallocate_segment); !allowed) {
        cleanup();
        return unexpected(allowed.error());
    }
    if (auto allocated = preallocate_segment(temporary); !allocated) {
        cleanup();
        return unexpected(allocated.error());
    }
    directory.after(FilesystemOperation::preallocate_segment);
    if (auto allowed = directory.before(FilesystemOperation::write_segment_header); !allowed) {
        cleanup();
        return unexpected(allowed.error());
    }
    if (auto written = temporary.write_all_at(header_bytes, 0); !written) {
        cleanup();
        return unexpected(written.error());
    }
    directory.after(FilesystemOperation::write_segment_header);

    // This handle intentionally owns private health until promotion. Any
    // staging I/O failure invalidates only disposable output, not Mold.
    auto staged_health = std::make_shared<std::atomic_bool>(true);
    return DurableSegmentFile{std::move(temporary),
                              identity,
                              SelectedSegmentCommit{.slot_index = 0, .commit = initial_commit},
                              directory.hooks_,
                              std::move(staged_health),
                              true};
}

auto DurableSegmentFile::open_staged(DataDirectory& directory, const SegmentHeaderIdentity& expected_identity)
    -> Result<DurableSegmentFile> {
    if (!directory.healthy()) {
        return fail(ErrorCode::io_error, "cannot open a staged Segment through a poisoned data directory");
    }
    const auto name = '.' + segment_filename(expected_identity) + ".tmp";
    FileDescriptor file{interrupted_open_at(directory.directory_.get(), name.c_str(),
                                            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK),
                        directory.hooks_.file_io};
    if (!file.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, "staged Segment does not exist");
        }
        return persistence_system_error("openat(staged Segment)");
    }
    if (auto valid = validate_private_segment(file.get()); !valid) {
        return unexpected(valid.error());
    }
    const auto size = file.size();
    if (!size) {
        return unexpected(size.error());
    }
    if (*size != kSegmentSizeBytes) {
        return fail(ErrorCode::invalid_record, "staged Segment file is not exactly 64 MiB");
    }
    std::array<std::byte, kSegmentHeaderReservedBytes> header_bytes{};
    if (auto read = file.read_exact_at(header_bytes, 0); !read) {
        return unexpected(read.error());
    }
    const auto header = decode_segment_header(header_bytes);
    if (!header) {
        return unexpected(header.error());
    }
    if (header->identity != expected_identity) {
        return fail(ErrorCode::corrupted_data,
                    "staged Segment header identity does not match the planned identity");
    }
    const auto selected = select_newest_segment_commit(*header);
    if (!selected) {
        return unexpected(selected.error());
    }
    auto staged_health = std::make_shared<std::atomic_bool>(true);
    return DurableSegmentFile{std::move(file),  header->identity,         *selected,
                              directory.hooks_, std::move(staged_health), false};
}

auto DurableSegmentFile::promote_staged(DataDirectory& directory,
                                        const std::span<const SegmentHeaderIdentity> identities) -> Status {
    if (!directory.healthy()) {
        return fail(ErrorCode::io_error, "cannot promote staged Segments through a poisoned data directory");
    }
    if (identities.empty()) {
        return {};
    }

    for (const auto& identity : identities) {
        auto staged = open_staged(directory, identity);
        if (!staged) {
            return unexpected(staged.error());
        }
        if (staged->selected_commit().commit.state != PersistedSegmentState::sealed) {
            return fail(ErrorCode::corrupted_data, "compaction staged Segment is not sealed");
        }
        const auto final_name = segment_filename(identity);
        struct stat existing{};
        if (::fstatat(directory.directory_.get(), final_name.c_str(), &existing, AT_SYMLINK_NOFOLLOW) == 0) {
            return fail(ErrorCode::sequence_conflict,
                        "compaction staged Segment final identity already exists");
        }
        if (errno != ENOENT) {
            return persistence_system_error("fstatat(compaction staged final name)");
        }
    }

    bool renamed_any{};
    for (const auto& identity : identities) {
        const auto final_name = segment_filename(identity);
        const auto temporary_name = '.' + final_name + ".tmp";
        if (auto allowed = directory.before(FilesystemOperation::rename_segment); !allowed) {
            if (renamed_any) {
                directory.health_->store(false, std::memory_order_release);
            }
            return allowed;
        }
        if (::renameat(directory.directory_.get(), temporary_name.c_str(), directory.directory_.get(),
                       final_name.c_str()) != 0) {
            directory.health_->store(false, std::memory_order_release);
            return persistence_system_error("renameat(staged Segment)");
        }
        renamed_any = true;
        directory.after(FilesystemOperation::rename_segment);
    }
    if (auto allowed = directory.before(FilesystemOperation::sync_directory); !allowed) {
        directory.health_->store(false, std::memory_order_release);
        return allowed;
    }
    if (auto synced = directory.sync_directory(); !synced) {
        directory.health_->store(false, std::memory_order_release);
        return synced;
    }
    directory.after(FilesystemOperation::sync_directory);
    return {};
}

void DurableSegmentFile::discard_staged(DataDirectory& directory,
                                        const std::span<const SegmentHeaderIdentity> identities) noexcept {
    for (const auto& identity : identities) {
        const auto name = '.' + segment_filename(identity) + ".tmp";
        static_cast<void>(::unlinkat(directory.directory_.get(), name.c_str(), 0));
    }
}

auto DurableSegmentFile::open(DataDirectory& directory, const SegmentHeaderIdentity& expected_identity,
                              const SegmentFileOpenMode mode) -> Result<DurableSegmentFile> {
    if (glyphastore::fault::consume_fail(glyphastore::fault::Site::segment_open)) {
        return fail(ErrorCode::descriptor_exhausted, "injected Segment open failure");
    }
    if (!directory.healthy()) {
        return fail(ErrorCode::io_error, "cannot open a Segment through a poisoned data directory");
    }
    const auto name = segment_filename(expected_identity);
    const auto access = mode == SegmentFileOpenMode::read_only ? O_RDONLY : O_RDWR;
    FileDescriptor file{interrupted_open_at(directory.directory_.get(), name.c_str(),
                                            access | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK),
                        directory.hooks_.file_io};
    if (!file.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, "Segment does not exist");
        }
        return persistence_system_error("openat(Segment)");
    }
    if (auto valid = validate_private_segment(file.get()); !valid) {
        return unexpected(valid.error());
    }
    const auto size = file.size();
    if (!size) {
        return unexpected(size.error());
    }
    if (*size != kSegmentSizeBytes) {
        return fail(ErrorCode::invalid_record, "Segment file is not exactly 64 MiB");
    }

    std::array<std::byte, kSegmentHeaderReservedBytes> header_bytes{};
    if (auto read = file.read_exact_at(header_bytes, 0); !read) {
        return unexpected(read.error());
    }
    const auto header = decode_segment_header(header_bytes);
    if (!header) {
        return unexpected(header.error());
    }
    if (header->identity != expected_identity) {
        return fail(ErrorCode::corrupted_data, "Segment header identity does not match its catalog identity");
    }
    const auto selected = select_newest_segment_commit(*header);
    if (!selected) {
        return unexpected(selected.error());
    }
    return DurableSegmentFile{std::move(file),  header->identity,  *selected,
                              directory.hooks_, directory.health_, mode == SegmentFileOpenMode::read_write};
}

auto DurableSegmentFile::open_path(const std::filesystem::path& path) -> Result<DurableSegmentFile> {
    if (path.empty()) {
        return fail(ErrorCode::invalid_argument, "Segment path cannot be empty");
    }
    FileDescriptor file{interrupted_open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)};
    if (!file.valid()) {
        if (errno == ENOENT) {
            return fail(ErrorCode::not_found, "Segment does not exist");
        }
        return persistence_system_error("open(Segment path)");
    }
    if (auto valid = validate_private_segment(file.get()); !valid) {
        return unexpected(valid.error());
    }
    const auto size = file.size();
    if (!size) {
        return unexpected(size.error());
    }
    if (*size != kSegmentSizeBytes) {
        return fail(ErrorCode::invalid_record, "Segment file is not exactly 64 MiB");
    }

    std::array<std::byte, kSegmentHeaderReservedBytes> header_bytes{};
    if (auto read = file.read_exact_at(header_bytes, 0); !read) {
        return unexpected(read.error());
    }
    const auto header = decode_segment_header(header_bytes);
    if (!header) {
        return unexpected(header.error());
    }
    const auto selected = select_newest_segment_commit(*header);
    if (!selected) {
        return unexpected(selected.error());
    }
    auto health = std::make_shared<std::atomic_bool>(true);
    return DurableSegmentFile{std::move(file), header->identity, *selected, {}, std::move(health), false};
}

auto DurableSegmentFile::before(const FilesystemOperation operation) const -> Status {
    if (!hooks_.before) {
        return {};
    }
    // before() runs ahead of the matching write/sync — known not committed whether the
    // hook returns Status or throws (see segment-filesystem.md / error-taxonomy-v1).
    try {
        return hooks_.before(hooks_.context, operation);
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "filesystem before-hook allocation failed");
    } catch (...) {
        return fail(ErrorCode::resource_exhausted, "filesystem before-hook failed");
    }
}

void DurableSegmentFile::after(const FilesystemOperation operation) const noexcept {
    detail::invoke_filesystem_after(hooks_, operation);
}

void DurableSegmentFile::poison() noexcept {
    directory_health_->store(false, std::memory_order_release);
}

void DurableSegmentFile::rollback_pending_metadata() noexcept {
    selected_.commit.committed_end = persisted_.commit.committed_end;
    selected_.commit.record_count = persisted_.commit.record_count;
    selected_.commit.first_sequence = persisted_.commit.first_sequence;
    selected_.commit.last_sequence = persisted_.commit.last_sequence;
    pending_record_count_ = 0;
    pending_bytes_ = 0;
}

auto DurableSegmentFile::publish_commit(const SegmentCommit& commit, const SegmentCommitSync sync)
    -> SegmentCommitResult {
    std::array<std::byte, kSegmentCommitSlotBytes> encoded{};
    if (auto valid = encode_segment_commit_slot(encoded, commit); !valid) {
        rollback_pending_metadata();
        return commit_failure(SegmentCommitOutcome::not_committed, valid.error());
    }
    if (commit.commit_generation != persisted_.commit.commit_generation + 1) {
        rollback_pending_metadata();
        return commit_failure(
            SegmentCommitOutcome::not_committed,
            Error{ErrorCode::sequence_conflict, "Segment commit generation must advance by exactly one"});
    }

    const auto next_slot = (persisted_.slot_index + 1) % kSegmentCommitSlotCount;
    const auto slot_offset = kSegmentCommitSlotsOffset + next_slot * kSegmentCommitSlotBytes;
    if (auto allowed = before(FilesystemOperation::write_commit_slot); !allowed) {
        rollback_pending_metadata();
        return commit_failure(SegmentCommitOutcome::not_committed, allowed.error());
    }
    if (auto written = file_.write_all_at(encoded, slot_offset); !written) {
        rollback_pending_metadata();
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, written.error());
    }
    after(FilesystemOperation::write_commit_slot);
    selected_ = {.slot_index = next_slot, .commit = commit};
    persisted_ = selected_;
    pending_record_count_ = 0;
    pending_bytes_ = 0;
    if (sync == SegmentCommitSync::deferred) {
        dirty_ = true;
        return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
    }
    if (auto allowed = before(FilesystemOperation::sync_commit_slot); !allowed) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, allowed.error());
    }
    if (auto synced = file_.sync(FileSyncMode::data); !synced) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, synced.error());
    }
    after(FilesystemOperation::sync_commit_slot);
    dirty_ = false;
    return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
}

auto DurableSegmentFile::sync_file() -> SegmentCommitResult {
    if (!healthy()) {
        return commit_failure(SegmentCommitOutcome::indeterminate,
                              Error{ErrorCode::io_error, "Segment file is poisoned"});
    }
    if (!dirty_) {
        return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
    }
    if (auto allowed = before(FilesystemOperation::sync_commit_slot); !allowed) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, allowed.error());
    }
    if (auto synced = file_.sync(FileSyncMode::data); !synced) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, synced.error());
    }
    after(FilesystemOperation::sync_commit_slot);
    dirty_ = false;
    return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
}

auto DurableSegmentFile::is_dirty() const noexcept -> bool {
    return dirty_;
}

auto DurableSegmentFile::has_pending_commit() const noexcept -> bool {
    return pending_record_count_ > 0;
}

auto DurableSegmentFile::pending_record_count() const noexcept -> std::uint64_t {
    return pending_record_count_;
}

auto DurableSegmentFile::pending_bytes() const noexcept -> std::uint64_t {
    return pending_bytes_;
}

auto DurableSegmentFile::append_record(const std::span<const std::byte> encoded_record,
                                       const SegmentRecordWritePacing pacing) -> SegmentCommitResult {
    if (!healthy()) {
        return commit_failure(SegmentCommitOutcome::indeterminate,
                              Error{ErrorCode::io_error, "Segment file is poisoned"});
    }
    if (!writable_) {
        return commit_failure(
            SegmentCommitOutcome::not_committed,
            Error{ErrorCode::invalid_argument, "cannot append through a read-only Segment handle"});
    }
    if (selected_.commit.state != PersistedSegmentState::active) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::segment_sealed, "cannot append to a sealed Segment"});
    }
    const auto record = decode_record(encoded_record);
    if (!record) {
        return commit_failure(SegmentCommitOutcome::not_committed, record.error());
    }
    if (record->sequence.value == 0 || (selected_.commit.record_count != 0 &&
                                        record->sequence.value <= selected_.commit.last_sequence.value)) {
        return commit_failure(
            SegmentCommitOutcome::not_committed,
            Error{ErrorCode::sequence_conflict, "Record sequence must increase strictly within a Segment"});
    }
    if (selected_.commit.record_count == std::numeric_limits<std::uint64_t>::max() ||
        persisted_.commit.commit_generation == std::numeric_limits<std::uint64_t>::max()) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::arithmetic_overflow, "Segment commit metadata overflow"});
    }
    const auto remaining = static_cast<std::uint64_t>(kSegmentSizeBytes) - selected_.commit.committed_end;
    if (encoded_record.size() > remaining) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::segment_full, "Record does not fit in the Segment"});
    }

    const auto record_offset = selected_.commit.committed_end;
    if (auto allowed = before(FilesystemOperation::write_record); !allowed) {
        return commit_failure(SegmentCommitOutcome::not_committed, allowed.error());
    }
    std::size_t written_bytes{};
    while (written_bytes < encoded_record.size()) {
        const auto remaining_record_bytes = encoded_record.size() - written_bytes;
        auto write_bytes = remaining_record_bytes;
        if (pacing.acquire != nullptr) {
            auto granted = pacing.acquire(pacing.context, remaining_record_bytes);
            if (!granted) {
                return commit_failure(SegmentCommitOutcome::not_committed, granted.error());
            }
            if (*granted == 0U || *granted > remaining_record_bytes) {
                return commit_failure(
                    SegmentCommitOutcome::not_committed,
                    Error{ErrorCode::internal_error, "Segment Record write pacer returned an invalid grant"});
            }
            write_bytes = *granted;
        }
        if (auto written = file_.write_all_at(encoded_record.subspan(written_bytes, write_bytes),
                                              record_offset + written_bytes);
            !written) {
            poison();
            return commit_failure(SegmentCommitOutcome::not_committed, written.error());
        }
        written_bytes += write_bytes;
    }
    after(FilesystemOperation::write_record);

    const auto first_sequence =
        selected_.commit.record_count == 0 ? record->sequence : selected_.commit.first_sequence;
    selected_.commit.committed_end = static_cast<std::uint32_t>(record_offset + encoded_record.size());
    selected_.commit.record_count += 1;
    selected_.commit.first_sequence = first_sequence;
    selected_.commit.last_sequence = record->sequence;
    ++pending_record_count_;
    pending_bytes_ += encoded_record.size();
    dirty_ = true;
    return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
}

auto DurableSegmentFile::flush_pending_commit(const SegmentCommitSync sync) -> SegmentCommitResult {
    if (!has_pending_commit()) {
        if (dirty_ && sync == SegmentCommitSync::immediate) {
            return sync_file();
        }
        return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
    }
    if (auto allowed = before(FilesystemOperation::sync_record); !allowed) {
        rollback_pending_metadata();
        return commit_failure(SegmentCommitOutcome::not_committed, allowed.error());
    }
    if (auto synced = file_.sync(FileSyncMode::ordered); !synced) {
        rollback_pending_metadata();
        poison();
        return commit_failure(SegmentCommitOutcome::not_committed, synced.error());
    }
    after(FilesystemOperation::sync_record);
    SegmentCommit next = selected_.commit;
    next.commit_generation = persisted_.commit.commit_generation + 1;
    return publish_commit(next, sync);
}

auto DurableSegmentFile::append(const std::span<const std::byte> encoded_record, const SegmentCommitSync sync)
    -> SegmentCommitResult {
    const auto appended = append_record(encoded_record);
    if (!appended.committed()) {
        return appended;
    }
    return flush_pending_commit(sync);
}

auto DurableSegmentFile::seal() -> SegmentCommitResult {
    if (!healthy()) {
        return commit_failure(SegmentCommitOutcome::indeterminate,
                              Error{ErrorCode::io_error, "Segment file is poisoned"});
    }
    if (!writable_) {
        return commit_failure(
            SegmentCommitOutcome::not_committed,
            Error{ErrorCode::invalid_argument, "cannot seal through a read-only Segment handle"});
    }
    if (selected_.commit.state == PersistedSegmentState::sealed) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::segment_sealed, "Segment is already sealed"});
    }
    if (selected_.commit.commit_generation == std::numeric_limits<std::uint64_t>::max()) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::arithmetic_overflow, "Segment commit generation overflow"});
    }
    if (has_pending_commit()) {
        const auto flushed = flush_pending_commit(SegmentCommitSync::immediate);
        if (!flushed.committed()) {
            return flushed;
        }
    }
    auto sealed = selected_.commit;
    ++sealed.commit_generation;
    sealed.state = PersistedSegmentState::sealed;
    return publish_commit(sealed, SegmentCommitSync::immediate);
}

auto DurableSegmentFile::visit_committed_records(void* context, const CommittedRecordVisitor visitor) const
    -> Status {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot scan a poisoned Segment file");
    }
    if (!visitor) {
        return fail(ErrorCode::invalid_argument, "committed Record visitor cannot be null");
    }
    const auto data_size =
        static_cast<std::size_t>(persisted_.commit.committed_end) - kSegmentHeaderReservedBytes;
    if (persisted_.commit.record_count == 0) {
        return {};
    }
    const auto maximum_records = data_size / kEncodedRecordHeaderSize;
    if (persisted_.commit.record_count > maximum_records) {
        return fail(ErrorCode::corrupted_data, "Segment commit Record count exceeds its extent");
    }

    std::vector<std::byte> bytes(data_size);
    if (auto read = file_.read_exact_at(bytes, kSegmentHeaderReservedBytes); !read) {
        return unexpected(read.error());
    }
    std::size_t cursor{};
    std::uint64_t record_count{};
    SequenceNumber first{};
    SequenceNumber previous{};
    while (cursor < bytes.size()) {
        const auto remaining = bytes.size() - cursor;
        if (remaining < kEncodedRecordHeaderSize) {
            return fail(ErrorCode::corrupted_data, "committed Segment extent ends in a truncated Record");
        }
        const auto encoded_size = get_u32(bytes, cursor + 8);
        if (encoded_size < kEncodedRecordHeaderSize || encoded_size > kMaxNormalRecordSize ||
            encoded_size % kRecordAlignment != 0 || encoded_size > remaining) {
            return fail(ErrorCode::corrupted_data, "committed Segment contains an invalid Record extent");
        }
        const auto record = decode_record(std::span<const std::byte>{bytes}.subspan(cursor, encoded_size));
        if (!record) {
            return unexpected(record.error());
        }
        if (record->sequence.value == 0 ||
            (previous.value != 0 && record->sequence.value <= previous.value)) {
            return fail(ErrorCode::corrupted_data,
                        "committed Segment Record sequences are not strictly increasing");
        }
        const RecordRef reference{
            .segment_id = identity_.segment_id,
            .offset = RecordOffset{static_cast<std::uint32_t>(kSegmentHeaderReservedBytes + cursor)},
            .size = RecordSize{encoded_size},
            .sequence = record->sequence,
            .generation = identity_.generation,
        };
        if (auto visited = visitor(context, reference, *record); !visited) {
            return unexpected(visited.error());
        }
        if (record_count == 0) {
            first = record->sequence;
        }
        ++record_count;
        previous = record->sequence;
        cursor += encoded_size;
    }
    if (record_count != persisted_.commit.record_count || first != persisted_.commit.first_sequence ||
        previous != persisted_.commit.last_sequence) {
        return fail(ErrorCode::corrupted_data, "Segment commit metadata does not match committed Records");
    }
    return {};
}

auto DurableSegmentFile::scan_committed() const -> Result<std::vector<RecordRef>> {
    std::vector<RecordRef> records;
    const auto data_size =
        static_cast<std::size_t>(persisted_.commit.committed_end) - kSegmentHeaderReservedBytes;
    if (persisted_.commit.record_count > data_size / kEncodedRecordHeaderSize) {
        return fail(ErrorCode::corrupted_data, "Segment commit Record count exceeds its extent");
    }
    records.reserve(static_cast<std::size_t>(persisted_.commit.record_count));
    const auto collect = [](void* context, const RecordRef& reference, const RecordView&) -> Status {
        static_cast<std::vector<RecordRef>*>(context)->push_back(reference);
        return {};
    };
    if (auto scanned = visit_committed_records(&records, collect); !scanned) {
        return unexpected(scanned.error());
    }
    return Result<std::vector<RecordRef>>{std::move(records)};
}

auto DurableSegmentFile::read_record_into(const RecordRef& reference, std::vector<std::byte>& bytes,
                                          RecordView& record) const -> Status {
    return read_record_into_extent(reference, persisted_.commit.committed_end, bytes, record);
}

auto DurableSegmentFile::read_record_into_extent(const RecordRef& reference, const std::uint64_t readable_end,
                                                 std::vector<std::byte>& bytes, RecordView& record) const
    -> Status {
    if (!healthy()) {
        return fail(ErrorCode::io_error, "cannot read a poisoned Segment file");
    }
    if (reference.segment_id != identity_.segment_id || reference.generation != identity_.generation) {
        return fail(ErrorCode::invalid_reference, "Record reference targets a different Segment generation");
    }
    const auto offset = static_cast<std::uint64_t>(reference.offset.value);
    const auto size = static_cast<std::uint64_t>(reference.size.value);
    if (offset < kSegmentHeaderReservedBytes || offset % kRecordAlignment != 0 ||
        size < kEncodedRecordHeaderSize || size > kMaxNormalRecordSize || size % kRecordAlignment != 0 ||
        readable_end > kSegmentSizeBytes || offset > readable_end || size > readable_end - offset) {
        return fail(ErrorCode::invalid_reference, "Record reference is outside the committed Segment extent");
    }
    bytes.resize(reference.size.value);
    if (auto read = file_.read_exact_at(bytes, offset); !read) {
        return unexpected(read.error());
    }
    const auto decoded = decode_record(bytes);
    if (!decoded) {
        return unexpected(decoded.error());
    }
    if (decoded->sequence != reference.sequence || decoded->encoded_size != reference.size.value) {
        return fail(ErrorCode::invalid_reference, "Record reference metadata does not match encoded data");
    }
    record = *decoded;
    return {};
}

auto DurableSegmentFile::read_record(const RecordRef& reference) const -> Result<std::vector<std::byte>> {
    std::vector<std::byte> bytes;
    RecordView record{};
    if (auto read = read_record_into(reference, bytes, record); !read) {
        return unexpected(read.error());
    }
    return Result<std::vector<std::byte>>{std::move(bytes)};
}

auto DurableSegmentFile::visit_record(const RecordRef& reference, void* context,
                                      const RecordVisitor visitor) const -> Status {
    std::vector<std::byte> scratch;
    return visit_record(reference, scratch, context, visitor);
}

auto DurableSegmentFile::visit_record(const RecordRef& reference, std::vector<std::byte>& scratch,
                                      void* context, const RecordVisitor visitor) const -> Status {
    if (!visitor) {
        return fail(ErrorCode::invalid_argument, "Record visitor cannot be null");
    }
    RecordView record{};
    if (auto read = read_record_into(reference, scratch, record); !read) {
        return unexpected(read.error());
    }
    return visitor(context, record);
}

auto DurableSegmentFile::visit_runtime_record(const RecordRef& reference, void* context,
                                              const RecordVisitor visitor) const -> Status {
    std::vector<std::byte> scratch;
    return visit_runtime_record(reference, scratch, context, visitor);
}

auto DurableSegmentFile::visit_runtime_record(const RecordRef& reference, std::vector<std::byte>& scratch,
                                              void* context, const RecordVisitor visitor) const -> Status {
    if (!visitor) {
        return fail(ErrorCode::invalid_argument, "Record visitor cannot be null");
    }
    RecordView record{};
    if (auto read = read_record_into_extent(reference, kSegmentSizeBytes, scratch, record); !read) {
        return unexpected(read.error());
    }
    return visitor(context, record);
}

auto inspect_durable_segment(const std::filesystem::path& path, const bool scan_records)
    -> Result<DurableSegmentInspectReport> {
    auto opened = DurableSegmentFile::open_path(path);
    if (!opened) {
        return unexpected(opened.error());
    }

    DurableSegmentInspectReport report{
        .path = path,
        .identity = opened->identity(),
        .selected = opened->selected_commit(),
    };

    const auto basename = path.filename().string();
    if (const auto parsed = parse_segment_filename(basename); parsed) {
        const bool matches = parsed->segment_id == report.identity.segment_id &&
                             parsed->generation == report.identity.generation && !parsed->temporary;
        if (!matches) {
            return fail(ErrorCode::corrupted_data,
                        "Segment filename identity disagrees with Segment header identity");
        }
        report.filename_matches_identity = true;
    }

    if (scan_records) {
        std::uint64_t counted{};
        const auto count = [](void* context, const RecordRef&, const RecordView&) -> Status {
            ++*static_cast<std::uint64_t*>(context);
            return {};
        };
        if (auto scanned = opened->visit_committed_records(&counted, count); !scanned) {
            return unexpected(scanned.error());
        }
        report.scanned_records = counted;
    } else {
        report.scanned_records = report.selected.commit.record_count;
    }
    return report;
}

} // namespace glyphastore
