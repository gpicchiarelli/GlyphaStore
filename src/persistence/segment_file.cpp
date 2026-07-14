#include "glyphastore/persistence/segment_file.hpp"

#include "glyphastore/segment/record.hpp"
#include "system_error.hpp"

#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace glyphastore {
namespace {

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
        return fail(ErrorCode::io_error, "F_PREALLOCATE did not reserve the complete Segment");
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

auto get_u32(const std::span<const std::byte> bytes, std::size_t at) -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[at + index])) << (index * 8U);
    }
    return value;
}

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

    FileDescriptor temporary{interrupted_open_at(
        directory.directory_.get(), temporary_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, S_IRUSR | S_IWUSR)};
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
    if (auto allowed = directory.before(FilesystemOperation::write_segment_header); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (auto written = temporary.write_all_at(header_bytes, 0); !written) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, written.error());
    }
    if (auto allowed = directory.before(FilesystemOperation::sync_segment_file); !allowed) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, allowed.error());
    }
    if (auto synced = temporary.sync(FileSyncMode::full); !synced) {
        cleanup();
        return creation_failure(SegmentFileCreationOutcome::not_published, synced.error());
    }
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
    if (auto allowed = directory.before(FilesystemOperation::sync_directory); !allowed) {
        directory.health_->store(false, std::memory_order_release);
        return creation_failure(SegmentFileCreationOutcome::indeterminate, allowed.error());
    }
    if (auto synced = directory.sync_directory(); !synced) {
        directory.health_->store(false, std::memory_order_release);
        return creation_failure(SegmentFileCreationOutcome::indeterminate, synced.error());
    }

    DurableSegmentFile file{std::move(temporary), identity,
                            SelectedSegmentCommit{.slot_index = 0, .commit = initial_commit},
                            directory.hooks_, directory.health_};
    return {.outcome = SegmentFileCreationOutcome::durable, .file = std::move(file), .error = std::nullopt};
}

auto DurableSegmentFile::open(DataDirectory& directory, const SegmentHeaderIdentity& expected_identity)
    -> Result<DurableSegmentFile> {
    if (!directory.healthy()) {
        return fail(ErrorCode::io_error, "cannot open a Segment through a poisoned data directory");
    }
    const auto name = segment_filename(expected_identity);
    FileDescriptor file{interrupted_open_at(directory.directory_.get(), name.c_str(),
                                            O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)};
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
    return DurableSegmentFile{std::move(file), header->identity, *selected, directory.hooks_,
                              directory.health_};
}

auto DurableSegmentFile::before(const FilesystemOperation operation) const -> Status {
    if (!hooks_.before) {
        return {};
    }
    return hooks_.before(hooks_.context, operation);
}

void DurableSegmentFile::poison() noexcept {
    directory_health_->store(false, std::memory_order_release);
}

auto DurableSegmentFile::publish_commit(const SegmentCommit& commit) -> SegmentCommitResult {
    std::array<std::byte, kSegmentCommitSlotBytes> encoded{};
    if (auto valid = encode_segment_commit_slot(encoded, commit); !valid) {
        return commit_failure(SegmentCommitOutcome::not_committed, valid.error());
    }
    if (commit.commit_generation != selected_.commit.commit_generation + 1) {
        return commit_failure(
            SegmentCommitOutcome::not_committed,
            Error{ErrorCode::sequence_conflict, "Segment commit generation must advance by exactly one"});
    }

    const auto next_slot = (selected_.slot_index + 1) % kSegmentCommitSlotCount;
    const auto slot_offset = kSegmentCommitSlotsOffset + next_slot * kSegmentCommitSlotBytes;
    if (auto allowed = before(FilesystemOperation::write_commit_slot); !allowed) {
        return commit_failure(SegmentCommitOutcome::not_committed, allowed.error());
    }
    if (auto written = file_.write_all_at(encoded, slot_offset); !written) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, written.error());
    }
    if (auto allowed = before(FilesystemOperation::sync_commit_slot); !allowed) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, allowed.error());
    }
    if (auto synced = file_.sync(FileSyncMode::data); !synced) {
        poison();
        return commit_failure(SegmentCommitOutcome::indeterminate, synced.error());
    }
    selected_ = {.slot_index = next_slot, .commit = commit};
    return {.outcome = SegmentCommitOutcome::committed, .error = std::nullopt};
}

auto DurableSegmentFile::append(const std::span<const std::byte> encoded_record) -> SegmentCommitResult {
    if (!healthy()) {
        return commit_failure(SegmentCommitOutcome::indeterminate,
                              Error{ErrorCode::io_error, "Segment file is poisoned"});
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
        selected_.commit.commit_generation == std::numeric_limits<std::uint64_t>::max()) {
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
    if (auto written = file_.write_all_at(encoded_record, record_offset); !written) {
        poison();
        return commit_failure(SegmentCommitOutcome::not_committed, written.error());
    }
    if (auto allowed = before(FilesystemOperation::sync_record); !allowed) {
        return commit_failure(SegmentCommitOutcome::not_committed, allowed.error());
    }
    if (auto synced = file_.sync(FileSyncMode::data); !synced) {
        poison();
        return commit_failure(SegmentCommitOutcome::not_committed, synced.error());
    }

    const auto first_sequence =
        selected_.commit.record_count == 0 ? record->sequence : selected_.commit.first_sequence;
    const SegmentCommit next{
        .commit_generation = selected_.commit.commit_generation + 1,
        .committed_end = static_cast<std::uint32_t>(record_offset + encoded_record.size()),
        .state = PersistedSegmentState::active,
        .record_count = selected_.commit.record_count + 1,
        .first_sequence = first_sequence,
        .last_sequence = record->sequence,
    };
    return publish_commit(next);
}

auto DurableSegmentFile::seal() -> SegmentCommitResult {
    if (!healthy()) {
        return commit_failure(SegmentCommitOutcome::indeterminate,
                              Error{ErrorCode::io_error, "Segment file is poisoned"});
    }
    if (selected_.commit.state == PersistedSegmentState::sealed) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::segment_sealed, "Segment is already sealed"});
    }
    if (selected_.commit.commit_generation == std::numeric_limits<std::uint64_t>::max()) {
        return commit_failure(SegmentCommitOutcome::not_committed,
                              Error{ErrorCode::arithmetic_overflow, "Segment commit generation overflow"});
    }
    auto sealed = selected_.commit;
    ++sealed.commit_generation;
    sealed.state = PersistedSegmentState::sealed;
    return publish_commit(sealed);
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
        static_cast<std::size_t>(selected_.commit.committed_end) - kSegmentHeaderReservedBytes;
    if (selected_.commit.record_count == 0) {
        return {};
    }
    const auto maximum_records = data_size / kEncodedRecordHeaderSize;
    if (selected_.commit.record_count > maximum_records) {
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
    if (record_count != selected_.commit.record_count || first != selected_.commit.first_sequence ||
        previous != selected_.commit.last_sequence) {
        return fail(ErrorCode::corrupted_data, "Segment commit metadata does not match committed Records");
    }
    return {};
}

auto DurableSegmentFile::scan_committed() const -> Result<std::vector<RecordRef>> {
    std::vector<RecordRef> records;
    const auto data_size =
        static_cast<std::size_t>(selected_.commit.committed_end) - kSegmentHeaderReservedBytes;
    if (selected_.commit.record_count > data_size / kEncodedRecordHeaderSize) {
        return fail(ErrorCode::corrupted_data, "Segment commit Record count exceeds its extent");
    }
    records.reserve(static_cast<std::size_t>(selected_.commit.record_count));
    const auto collect = [](void* context, const RecordRef& reference, const RecordView&) -> Status {
        static_cast<std::vector<RecordRef>*>(context)->push_back(reference);
        return {};
    };
    if (auto scanned = visit_committed_records(&records, collect); !scanned) {
        return unexpected(scanned.error());
    }
    return Result<std::vector<RecordRef>>{std::move(records)};
}

auto DurableSegmentFile::read_record(const RecordRef& reference) const -> Result<std::vector<std::byte>> {
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
        offset > selected_.commit.committed_end || size > selected_.commit.committed_end - offset) {
        return fail(ErrorCode::invalid_reference, "Record reference is outside the committed Segment extent");
    }
    std::vector<std::byte> bytes(reference.size.value);
    if (auto read = file_.read_exact_at(bytes, offset); !read) {
        return unexpected(read.error());
    }
    const auto record = decode_record(bytes);
    if (!record) {
        return unexpected(record.error());
    }
    if (record->sequence != reference.sequence) {
        return fail(ErrorCode::invalid_reference, "Record reference sequence does not match encoded data");
    }
    return Result<std::vector<std::byte>>{std::move(bytes)};
}

} // namespace glyphastore
