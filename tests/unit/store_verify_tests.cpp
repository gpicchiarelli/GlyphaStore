#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_verify.hpp"
#include "glyphastore/segment/record.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class VerifyTemporaryDirectory final {
  public:
    VerifyTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-verify-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~VerifyTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto store_id() -> glyphastore::StoreId {
    return {std::byte{0x21}, std::byte{0x22}, std::byte{0x23}, std::byte{0x24}, std::byte{0x25},
            std::byte{0x26}, std::byte{0x27}, std::byte{0x28}, std::byte{0x29}, std::byte{0x2A},
            std::byte{0x2B}, std::byte{0x2C}, std::byte{0x2D}, std::byte{0x2E}, std::byte{0x2F},
            std::byte{0x30}};
}

auto identity_for(const glyphastore::Manifest& manifest, const glyphastore::ManifestSegmentEntry& entry)
    -> glyphastore::SegmentHeaderIdentity {
    return {.store_id = manifest.store_id,
            .segment_id = entry.segment_id,
            .generation = entry.generation,
            .owner_worker = entry.owner_worker};
}

auto single_active_manifest() -> glyphastore::Manifest {
    return {
        .store_id = store_id(),
        .manifest_generation = 1,
        .worker_count = 1,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{2},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = {{.segment_id = glyphastore::SegmentId{1},
                      .generation = glyphastore::GenerationId{1},
                      .owner_worker = glyphastore::WorkerId{0},
                      .role = glyphastore::ManifestSegmentRole::active}},
    };
}

auto encoded_record(std::uint64_t sequence, std::string key, std::string value)
    -> glyphastore::Result<std::vector<std::byte>> {
    return glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{sequence},
        .opcode = glyphastore::Opcode::put,
        .type = glyphastore::ValueType::bytes,
        .flags = 0,
        .key_hash = sequence * 17,
        .expire_at_ns = 0,
        .key = std::as_bytes(std::span{key}),
        .value = std::as_bytes(std::span{value}),
    });
}

} // namespace

GLYPHA_TEST("verify_durable_store accepts a locked catalog with a valid active Segment") {
    VerifyTemporaryDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto manifest = single_active_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, identity_for(manifest, manifest.segments[0]));
        GLYPHA_REQUIRE(created.durable());
        const auto record = encoded_record(1, "verify", "ok");
        GLYPHA_REQUIRE(record.has_value());
        GLYPHA_REQUIRE(created.file->append(*record).committed());
    }

    const auto report = glyphastore::verify_durable_store_path(temporary.path());
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->manifest.segments.size() == 1);
    GLYPHA_REQUIRE(report->segments.size() == 1);
    GLYPHA_REQUIRE(report->scanned_records == 1);
    GLYPHA_REQUIRE(report->namespace_audit.recovery_safe());
    GLYPHA_REQUIRE(report->active_requires_rotation_count == 0);
    GLYPHA_REQUIRE(report->segments[0].selected.commit.record_count == 1);
}

GLYPHA_TEST("verify_durable_store --no-scan skips committed Record corruption") {
    VerifyTemporaryDirectory temporary;
    glyphastore::Manifest manifest;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        manifest = single_active_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, identity_for(manifest, manifest.segments[0]));
        GLYPHA_REQUIRE(created.durable());
        const auto record = encoded_record(2, "corrupt", "later");
        GLYPHA_REQUIRE(record.has_value());
        GLYPHA_REQUIRE(created.file->append(*record).committed());
    }

    const auto path =
        temporary.path() / glyphastore::segment_filename(identity_for(manifest, manifest.segments[0]));
    glyphastore::FileDescriptor raw{::open(path.c_str(), O_RDWR | O_CLOEXEC)};
    GLYPHA_REQUIRE(raw.valid());
    const std::byte corrupt{0x00};
    GLYPHA_REQUIRE(
        raw.write_all_at(std::span{&corrupt, 1}, glyphastore::kSegmentHeaderReservedBytes).has_value());
    GLYPHA_REQUIRE(raw.sync(glyphastore::FileSyncMode::data).has_value());
    raw.reset();

    const auto header_only = glyphastore::verify_durable_store_path(temporary.path(), false);
    GLYPHA_REQUIRE(header_only.has_value());

    const auto scanned = glyphastore::verify_durable_store_path(temporary.path(), true);
    GLYPHA_REQUIRE(!scanned.has_value());
    GLYPHA_REQUIRE(scanned.error().code == glyphastore::ErrorCode::invalid_record ||
                   scanned.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("verify_durable_store fails on unlisted Segment files") {
    VerifyTemporaryDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto manifest = single_active_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, identity_for(manifest, manifest.segments[0]));
        GLYPHA_REQUIRE(created.durable());
        auto extra = glyphastore::DurableSegmentFile::create(
            *directory, {.store_id = manifest.store_id,
                         .segment_id = glyphastore::SegmentId{9},
                         .generation = glyphastore::GenerationId{1},
                         .owner_worker = glyphastore::WorkerId{0}});
        GLYPHA_REQUIRE(extra.durable());
    }

    const auto report = glyphastore::verify_durable_store_path(temporary.path());
    GLYPHA_REQUIRE(!report.has_value());
    GLYPHA_REQUIRE(report.error().code == glyphastore::ErrorCode::corrupted_data ||
                   report.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("verify_durable_store reports sealed-active Segments as active_requires_rotation") {
    VerifyTemporaryDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto manifest = single_active_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, identity_for(manifest, manifest.segments[0]));
        GLYPHA_REQUIRE(created.durable());
        GLYPHA_REQUIRE(created.file->seal().committed());
    }

    const auto report = glyphastore::verify_durable_store_path(temporary.path());
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->active_requires_rotation_count == 1);
    GLYPHA_REQUIRE(report->segments[0].active_requires_rotation);
}

GLYPHA_TEST("verify_durable_store fails when the data directory is already locked") {
    VerifyTemporaryDirectory temporary;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto manifest = single_active_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, identity_for(manifest, manifest.segments[0]));
        GLYPHA_REQUIRE(created.durable());

        const auto contested = glyphastore::verify_durable_store_path(temporary.path());
        GLYPHA_REQUIRE(!contested.has_value());
        GLYPHA_REQUIRE(contested.error().code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE(contested.error().message.find("already locked") != std::string::npos);
    }
}
