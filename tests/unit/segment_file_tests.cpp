#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class SegmentTemporaryDirectory final {
  public:
    SegmentTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-segment-file-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~SegmentTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto segment_identity(std::uint64_t segment = 0x12, std::uint32_t generation = 3)
    -> glyphastore::SegmentHeaderIdentity {
    return {
        .store_id = {std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14},
                     std::byte{0x15}, std::byte{0x16}, std::byte{0x17}, std::byte{0x18}, std::byte{0x19},
                     std::byte{0x1A}, std::byte{0x1B}, std::byte{0x1C}, std::byte{0x1D}, std::byte{0x1E},
                     std::byte{0x1F}},
        .segment_id = glyphastore::SegmentId{segment},
        .generation = glyphastore::GenerationId{generation},
        .owner_worker = glyphastore::WorkerId{2},
    };
}

auto encoded_record(std::uint64_t sequence, std::string key, std::string value)
    -> glyphastore::Result<std::vector<std::byte>> {
    const auto key_bytes = std::as_bytes(std::span{key});
    const auto value_bytes = std::as_bytes(std::span{value});
    return glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{sequence},
        .opcode = glyphastore::Opcode::put,
        .type = glyphastore::ValueType::bytes,
        .flags = 0,
        .key_hash = sequence * 17,
        .expire_at_ns = 0,
        .key = key_bytes,
        .value = value_bytes,
    });
}

struct SegmentInjectedFailure {
    glyphastore::FilesystemOperation operation{glyphastore::FilesystemOperation::preallocate_segment};
    bool enabled{};
};

auto fail_segment_operation(void* context, glyphastore::FilesystemOperation operation)
    -> glyphastore::Status {
    auto& failure = *static_cast<SegmentInjectedFailure*>(context);
    if (failure.enabled && failure.operation == operation) {
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected Segment failure");
    }
    return {};
}

} // namespace

GLYPHA_TEST("Segment filenames are fixed-width lowercase and generation-specific") {
    const auto identity = segment_identity();
    GLYPHA_REQUIRE(glyphastore::segment_filename(identity) == "segment-0000000000000012-00000003.glypha");
}

GLYPHA_TEST("durable Segment creation preallocates exact size and reopens verified identity") {
    SegmentTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();

    auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.durable());
    GLYPHA_REQUIRE(created.file.has_value());
    GLYPHA_REQUIRE(created.file->selected_commit().slot_index == 0);
    GLYPHA_REQUIRE(created.file->selected_commit().commit.commit_generation == 1);
    GLYPHA_REQUIRE(created.file->scan_committed().has_value());

    const auto path = temporary.path() / glyphastore::segment_filename(identity);
    GLYPHA_REQUIRE(std::filesystem::file_size(path) == glyphastore::kSegmentSizeBytes);
    created.file.reset();
    const auto reopened = glyphastore::DurableSegmentFile::open(*directory, identity);
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(reopened->identity() == identity);

    auto wrong_identity = identity;
    wrong_identity.store_id[0] = std::byte{0xFF};
    const auto mismatched = glyphastore::DurableSegmentFile::open(*directory, wrong_identity);
    GLYPHA_REQUIRE(!mismatched.has_value());
    GLYPHA_REQUIRE(mismatched.error().code == glyphastore::ErrorCode::corrupted_data);

    const auto duplicate = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(duplicate.outcome == glyphastore::SegmentFileCreationOutcome::not_published);
    GLYPHA_REQUIRE(duplicate.error.has_value());
    GLYPHA_REQUIRE(duplicate.error->code == glyphastore::ErrorCode::sequence_conflict);
}

GLYPHA_TEST("Segment append synchronizes data before alternating commit slots and scans Records") {
    SegmentTemporaryDirectory temporary;
    std::vector<glyphastore::FilesystemOperation> completed_operations;
    const auto hooks = glyphastore::FilesystemHooks{
        .context = &completed_operations,
        .after =
            [](void* context, const glyphastore::FilesystemOperation operation) {
                static_cast<std::vector<glyphastore::FilesystemOperation>*>(context)->push_back(operation);
            },
    };
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(), hooks);
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();
    auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.durable());
    auto& file = *created.file;

    const auto first = encoded_record(7, "alpha", "one");
    const auto second = encoded_record(11, "beta", "two");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    completed_operations.clear();
    GLYPHA_REQUIRE(file.append(*first).committed());
    const std::vector expected_operations{
        glyphastore::FilesystemOperation::write_record,
        glyphastore::FilesystemOperation::sync_record,
        glyphastore::FilesystemOperation::write_commit_slot,
        glyphastore::FilesystemOperation::sync_commit_slot,
    };
    GLYPHA_REQUIRE(completed_operations == expected_operations);
    GLYPHA_REQUIRE(file.selected_commit().slot_index == 1);
    GLYPHA_REQUIRE(file.append(*second).committed());
    GLYPHA_REQUIRE(file.selected_commit().slot_index == 0);
    GLYPHA_REQUIRE(file.selected_commit().commit.commit_generation == 3);
    GLYPHA_REQUIRE(file.selected_commit().commit.record_count == 2);

    const auto scanned = file.scan_committed();
    GLYPHA_REQUIRE(scanned.has_value());
    GLYPHA_REQUIRE(scanned->size() == 2);
    GLYPHA_REQUIRE((*scanned)[0].sequence.value == 7);
    GLYPHA_REQUIRE((*scanned)[1].sequence.value == 11);
    const auto bytes = file.read_record((*scanned)[1]);
    GLYPHA_REQUIRE(bytes.has_value());
    const auto decoded = glyphastore::decode_record(*bytes);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->key_string() == "beta");

    GLYPHA_REQUIRE(file.seal().committed());
    GLYPHA_REQUIRE(file.selected_commit().commit.state == glyphastore::PersistedSegmentState::sealed);
    GLYPHA_REQUIRE(file.append(*second).outcome == glyphastore::SegmentCommitOutcome::not_committed);

    created.file.reset();
    const auto reopened = glyphastore::DurableSegmentFile::open(*directory, identity);
    GLYPHA_REQUIRE(reopened.has_value());
    const auto recovered = reopened->scan_committed();
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(recovered->size() == 2);
    GLYPHA_REQUIRE(reopened->selected_commit().commit.state == glyphastore::PersistedSegmentState::sealed);

    auto read_only = glyphastore::DurableSegmentFile::open(*directory, identity,
                                                           glyphastore::SegmentFileOpenMode::read_only);
    GLYPHA_REQUIRE(read_only.has_value());
    const auto rejected_append = read_only->append(*second);
    GLYPHA_REQUIRE(rejected_append.outcome == glyphastore::SegmentCommitOutcome::not_committed);
    GLYPHA_REQUIRE(rejected_append.error.has_value());
    GLYPHA_REQUIRE(rejected_append.error->code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("preallocation failure publishes no Segment and keeps directory healthy") {
    SegmentTemporaryDirectory temporary;
    SegmentInjectedFailure failure{.operation = glyphastore::FilesystemOperation::preallocate_segment,
                                   .enabled = true};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &failure, .before = &fail_segment_operation});
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();
    const auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.outcome == glyphastore::SegmentFileCreationOutcome::not_published);
    GLYPHA_REQUIRE(directory->healthy());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::segment_filename(identity)));
    const auto temporary_name = '.' + glyphastore::segment_filename(identity) + ".tmp";
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / temporary_name));
}

GLYPHA_TEST("Segment creation fault matrix distinguishes pre and post rename failures") {
    static constexpr std::array prepublication_boundaries{
        glyphastore::FilesystemOperation::preallocate_segment,
        glyphastore::FilesystemOperation::write_segment_header,
        glyphastore::FilesystemOperation::sync_segment_file,
        glyphastore::FilesystemOperation::rename_segment,
    };
    for (const auto boundary : prepublication_boundaries) {
        SegmentTemporaryDirectory temporary;
        SegmentInjectedFailure failure{.operation = boundary, .enabled = true};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_segment_operation});
        GLYPHA_REQUIRE(directory.has_value());
        const auto identity = segment_identity();
        const auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
        GLYPHA_REQUIRE(created.outcome == glyphastore::SegmentFileCreationOutcome::not_published);
        GLYPHA_REQUIRE(created.error.has_value());
        GLYPHA_REQUIRE(directory->healthy());
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::segment_filename(identity)));
        GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() /
                                                ('.' + glyphastore::segment_filename(identity) + ".tmp")));
    }

    SegmentTemporaryDirectory temporary;
    SegmentInjectedFailure failure{.operation = glyphastore::FilesystemOperation::sync_directory,
                                   .enabled = true};
    const auto identity = segment_identity();
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_segment_operation});
        GLYPHA_REQUIRE(directory.has_value());
        const auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
        GLYPHA_REQUIRE(created.outcome == glyphastore::SegmentFileCreationOutcome::indeterminate);
        GLYPHA_REQUIRE(created.error.has_value());
        GLYPHA_REQUIRE(!directory->healthy());
    }
    auto reopened = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(glyphastore::DurableSegmentFile::open(*reopened, identity).has_value());
}

GLYPHA_TEST("fault boundaries distinguish uncommitted Record tails from indeterminate slots") {
    SegmentTemporaryDirectory temporary;
    SegmentInjectedFailure failure{};
    const auto identity = segment_identity();
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(), {.context = &failure, .before = &fail_segment_operation});
        GLYPHA_REQUIRE(directory.has_value());
        auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
        GLYPHA_REQUIRE(created.durable());
        auto& file = *created.file;
        const auto record = encoded_record(4, "key", "value");
        GLYPHA_REQUIRE(record.has_value());

        failure.operation = glyphastore::FilesystemOperation::write_commit_slot;
        failure.enabled = true;
        const auto before_slot = file.append(*record);
        GLYPHA_REQUIRE(before_slot.outcome == glyphastore::SegmentCommitOutcome::not_committed);
        GLYPHA_REQUIRE(file.healthy());
        GLYPHA_REQUIRE(file.selected_commit().commit.record_count == 0);

        failure.enabled = false;
        created.file.reset();
        auto reopened = glyphastore::DurableSegmentFile::open(*directory, identity);
        GLYPHA_REQUIRE(reopened.has_value());
        GLYPHA_REQUIRE(reopened->selected_commit().commit.record_count == 0);
        const auto empty_scan = reopened->scan_committed();
        GLYPHA_REQUIRE(empty_scan.has_value());
        GLYPHA_REQUIRE(empty_scan->empty());
        GLYPHA_REQUIRE(reopened->append(*record).committed());
        const auto next = encoded_record(5, "next", "record");
        GLYPHA_REQUIRE(next.has_value());
        failure.operation = glyphastore::FilesystemOperation::sync_commit_slot;
        failure.enabled = true;
        const auto after_slot = reopened->append(*next);
        GLYPHA_REQUIRE(after_slot.outcome == glyphastore::SegmentCommitOutcome::indeterminate);
        GLYPHA_REQUIRE(!reopened->healthy());
        GLYPHA_REQUIRE(!directory->healthy());
        GLYPHA_REQUIRE(!reopened->scan_committed().has_value());
        GLYPHA_REQUIRE(!glyphastore::DurableSegmentFile::open(*directory, identity).has_value());
        const auto blocked_create =
            glyphastore::DurableSegmentFile::create(*directory, segment_identity(0x13));
        GLYPHA_REQUIRE(blocked_create.outcome == glyphastore::SegmentFileCreationOutcome::indeterminate);
    }

    auto recovered_directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(recovered_directory.has_value());
    const auto recovered = glyphastore::DurableSegmentFile::open(*recovered_directory, identity);
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(recovered->selected_commit().commit.record_count >= 1);
    GLYPHA_REQUIRE(recovered->selected_commit().commit.record_count <= 2);
    GLYPHA_REQUIRE(recovered->scan_committed().has_value());
}

GLYPHA_TEST("committed Record corruption is detected by recovery scan") {
    SegmentTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();
    auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.durable());
    const auto record = encoded_record(9, "checksum", "protected");
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(created.file->append(*record).committed());

    const auto path = temporary.path() / glyphastore::segment_filename(identity);
    glyphastore::FileDescriptor raw{::open(path.c_str(), O_RDWR | O_CLOEXEC)};
    GLYPHA_REQUIRE(raw.valid());
    const std::byte corrupt{0x00};
    GLYPHA_REQUIRE(
        raw.write_all_at(std::span{&corrupt, 1}, glyphastore::kSegmentHeaderReservedBytes).has_value());
    GLYPHA_REQUIRE(raw.sync(glyphastore::FileSyncMode::data).has_value());

    const auto scan = created.file->scan_committed();
    GLYPHA_REQUIRE(!scan.has_value());
    GLYPHA_REQUIRE(scan.error().code == glyphastore::ErrorCode::invalid_record ||
                   scan.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("Segment handles fail closed when their data directory lifetime ends") {
    SegmentTemporaryDirectory temporary;
    std::optional<glyphastore::DurableSegmentFile> surviving;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto created = glyphastore::DurableSegmentFile::create(*directory, segment_identity());
        GLYPHA_REQUIRE(created.durable());
        surviving = std::move(*created.file);
        GLYPHA_REQUIRE(surviving->healthy());
    }

    GLYPHA_REQUIRE(!surviving->healthy());
    const auto record = encoded_record(1, "closed", "directory");
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(surviving->append(*record).outcome == glyphastore::SegmentCommitOutcome::indeterminate);
    GLYPHA_REQUIRE(glyphastore::DataDirectory::open_and_lock(temporary.path()).has_value());
}

GLYPHA_TEST("deferred append defers synchronization until sync_file") {
    SegmentTemporaryDirectory temporary;
    struct SyncCounter {
        std::size_t sync_record_calls{};
        std::size_t sync_commit_slot_calls{};
    } counter;
    const auto hooks = glyphastore::FilesystemHooks{
        .context = &counter,
        .before = [](void* context, const glyphastore::FilesystemOperation operation) -> glyphastore::Status {
            if (operation == glyphastore::FilesystemOperation::sync_record) {
                ++static_cast<SyncCounter*>(context)->sync_record_calls;
            }
            if (operation == glyphastore::FilesystemOperation::sync_commit_slot) {
                ++static_cast<SyncCounter*>(context)->sync_commit_slot_calls;
            }
            return {};
        },
    };
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(), hooks);
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();
    auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.durable());
    const auto record = encoded_record(1, "periodic", "value");
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(created.file->append(*record, glyphastore::SegmentCommitSync::deferred).committed());
    GLYPHA_REQUIRE(created.file->is_dirty());
    GLYPHA_REQUIRE(counter.sync_record_calls == 1);
    GLYPHA_REQUIRE(counter.sync_commit_slot_calls == 0);
    GLYPHA_REQUIRE(created.file->sync_file().committed());
    GLYPHA_REQUIRE(!created.file->is_dirty());
    GLYPHA_REQUIRE(counter.sync_commit_slot_calls == 1);

    created.file.reset();
    auto reopened = glyphastore::DurableSegmentFile::open(*directory, identity);
    GLYPHA_REQUIRE(reopened.has_value());
    const auto scan = reopened->scan_committed();
    GLYPHA_REQUIRE(scan.has_value());
    GLYPHA_REQUIRE(scan->size() == 1);
}

GLYPHA_TEST("batched append_record defers commit slot until flush_pending_commit") {
    SegmentTemporaryDirectory temporary;
    struct SlotCounter {
        std::size_t sync_record_calls{};
        std::size_t write_commit_slot_calls{};
        std::size_t sync_commit_slot_calls{};
    } counter;
    const auto hooks = glyphastore::FilesystemHooks{
        .context = &counter,
        .before = [](void* context, const glyphastore::FilesystemOperation operation) -> glyphastore::Status {
            auto& counts = *static_cast<SlotCounter*>(context);
            if (operation == glyphastore::FilesystemOperation::sync_record) {
                ++counts.sync_record_calls;
            }
            if (operation == glyphastore::FilesystemOperation::write_commit_slot) {
                ++counts.write_commit_slot_calls;
            }
            if (operation == glyphastore::FilesystemOperation::sync_commit_slot) {
                ++counts.sync_commit_slot_calls;
            }
            return {};
        },
    };
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path(), hooks);
    GLYPHA_REQUIRE(directory.has_value());
    const auto identity = segment_identity();
    auto created = glyphastore::DurableSegmentFile::create(*directory, identity);
    GLYPHA_REQUIRE(created.durable());
    auto& file = *created.file;

    const auto first = encoded_record(1, "one", "alpha");
    const auto second = encoded_record(2, "two", "beta");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(file.append_record(*first).committed());
    GLYPHA_REQUIRE(file.append_record(*second).committed());
    GLYPHA_REQUIRE(file.has_pending_commit());
    GLYPHA_REQUIRE(counter.sync_record_calls == 0);
    GLYPHA_REQUIRE(counter.write_commit_slot_calls == 0);
    GLYPHA_REQUIRE(counter.sync_commit_slot_calls == 0);

    GLYPHA_REQUIRE(file.flush_pending_commit(glyphastore::SegmentCommitSync::immediate).committed());
    GLYPHA_REQUIRE(counter.sync_record_calls == 1);
    GLYPHA_REQUIRE(counter.write_commit_slot_calls == 1);
    GLYPHA_REQUIRE(counter.sync_commit_slot_calls == 1);
    GLYPHA_REQUIRE(file.selected_commit().commit.record_count == 2);
    GLYPHA_REQUIRE(file.selected_commit().commit.commit_generation == 2);

    created.file.reset();
    const auto reopened = glyphastore::DurableSegmentFile::open(*directory, identity);
    GLYPHA_REQUIRE(reopened.has_value());
    const auto scan = reopened->scan_committed();
    GLYPHA_REQUIRE(scan.has_value());
    GLYPHA_REQUIRE(scan->size() == 2);
}
