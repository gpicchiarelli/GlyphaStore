#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment_header.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/store.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

namespace {

[[nodiscard]] auto fixture_root() -> std::filesystem::path {
    return std::filesystem::path{GLYPHASTORE_SOURCE_DIR} / "tests/fixtures";
}

[[nodiscard]] auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

class CompatibilityTemporaryDirectory final {
  public:
    CompatibilityTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-compat-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        root_ = created;
    }

    ~CompatibilityTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] auto store_path() const -> std::filesystem::path {
        return root_ / "store";
    }

  private:
    std::filesystem::path root_;
};

} // namespace

GLYPHA_TEST("format fixtures decode independently without encoder round-trip") {
    const auto manifest_bytes = glyphastore::test::read_hex_fixture(fixture_root() / "manifest_v1.hex");
    const auto decoded_manifest = glyphastore::decode_manifest(manifest_bytes);
    GLYPHA_REQUIRE(decoded_manifest.has_value());
    GLYPHA_REQUIRE(decoded_manifest->manifest_generation == 0x0102030405060708ULL);
    GLYPHA_REQUIRE(decoded_manifest->segments.size() == 3);

    const auto intent_bytes =
        glyphastore::test::read_hex_fixture(fixture_root() / "compaction_intent_v1.hex");
    const auto decoded_intent = glyphastore::decode_compaction_intent(intent_bytes);
    GLYPHA_REQUIRE(decoded_intent.has_value());
    GLYPHA_REQUIRE(decoded_intent->worker_id == glyphastore::WorkerId{0});
    GLYPHA_REQUIRE(decoded_intent->old_manifest.manifest_generation == 9);
    GLYPHA_REQUIRE(decoded_intent->old_manifest.segments.size() == 4);
    GLYPHA_REQUIRE(decoded_intent->next_manifest.manifest_generation == 10);
    GLYPHA_REQUIRE(decoded_intent->next_manifest.segments.size() == 2);
    GLYPHA_REQUIRE(decoded_intent->next_manifest.segments.front().segment_id ==
                   glyphastore::SegmentId{1});
    GLYPHA_REQUIRE(decoded_intent->next_manifest.segments.front().generation ==
                   glyphastore::GenerationId{2});

    const auto header_bytes = glyphastore::test::read_hex_fixture(fixture_root() / "segment_header_v1.hex");
    GLYPHA_REQUIRE(header_bytes.size() ==
                   glyphastore::kSegmentCommitSlotsOffset +
                       glyphastore::kSegmentCommitSlotCount * glyphastore::kSegmentCommitSlotBytes);
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(header_bytes.size() <= encoded.size());
    std::copy(header_bytes.begin(), header_bytes.end(), encoded.begin());
    const auto decoded_header = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(decoded_header.has_value());
    const auto selected = glyphastore::select_newest_segment_commit(*decoded_header);
    GLYPHA_REQUIRE(selected.has_value());
    GLYPHA_REQUIRE(selected->commit.commit_generation == 2);

    const auto record_bytes = glyphastore::test::read_hex_fixture(fixture_root() / "record_v1.hex");
    const auto decoded_record = glyphastore::decode_record(record_bytes);
    GLYPHA_REQUIRE(decoded_record.has_value());
    GLYPHA_REQUIRE(decoded_record->sequence.value == 0x0102030405060708ULL);

    const auto segment_header_region =
        glyphastore::test::read_hex_fixture(fixture_root() / "segment_v1_header.hex");
    GLYPHA_REQUIRE(segment_header_region.size() ==
                   glyphastore::kSegmentCommitSlotsOffset +
                       glyphastore::kSegmentCommitSlotCount * glyphastore::kSegmentCommitSlotBytes);
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> segment_container{};
    std::copy(segment_header_region.begin(), segment_header_region.end(), segment_container.begin());
    const auto decoded_segment_container = glyphastore::decode_segment_header(segment_container);
    GLYPHA_REQUIRE(decoded_segment_container.has_value());
}

GLYPHA_TEST("durable store artifact survives simulated upgrade reopen") {
    CompatibilityTemporaryDirectory temporary;
    constexpr std::string_view kKey{"compat-key"};
    constexpr std::string_view kValue{"compat-value-v1"};
    {
        auto writer =
            glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                      .storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = temporary.store_path(),
                                      .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(writer.has_value());
        GLYPHA_REQUIRE((*writer)->put(kKey, bytes(kValue)).has_value());
        GLYPHA_REQUIRE((*writer)->verify_index().has_value());
    }

    {
        auto reader =
            glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                      .storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = temporary.store_path(),
                                      .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
        GLYPHA_REQUIRE(reader.has_value());
        const auto value = (*reader)->get(kKey);
        GLYPHA_REQUIRE(value.has_value());
        GLYPHA_REQUIRE(value->bytes.size() == kValue.size());
        GLYPHA_REQUIRE(
            std::equal(value->bytes.begin(), value->bytes.end(), bytes(kValue).begin(), bytes(kValue).end()));
        GLYPHA_REQUIRE((*reader)->verify_index().has_value());
    }

    const auto manifest = glyphastore::DataDirectory::open_and_lock(temporary.store_path());
    GLYPHA_REQUIRE(manifest.has_value());
    const auto on_disk = manifest->read_manifest();
    GLYPHA_REQUIRE(on_disk.has_value());
    GLYPHA_REQUIRE(on_disk->segments.size() == 1);
}

GLYPHA_TEST("segment container header fixture matches on-disk durable segment prefix") {
    CompatibilityTemporaryDirectory temporary;
    const glyphastore::StoreId store_id{std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13},
                                        std::byte{0x14}, std::byte{0x15}, std::byte{0x16}, std::byte{0x17},
                                        std::byte{0x18}, std::byte{0x19}, std::byte{0x1A}, std::byte{0x1B},
                                        std::byte{0x1C}, std::byte{0x1D}, std::byte{0x1E}, std::byte{0x1F}};
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.store_path(), glyphastore::DataDirectoryOpenMode::create_new);
        GLYPHA_REQUIRE(directory.has_value());
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, {.store_id = store_id,
                                                                 .segment_id = active.segment_id,
                                                                 .generation = active.generation,
                                                                 .owner_worker = active.owner_worker});
        GLYPHA_REQUIRE(created.durable());
        const glyphastore::Manifest manifest{
            .store_id = store_id,
            .manifest_generation = 1,
            .routing_algorithm = glyphastore::RoutingAlgorithm::fnv1a64_v1,
            .worker_count = 1,
            .routing_epoch = 1,
            .next_segment_id = glyphastore::SegmentId{2},
            .next_segment_generation = glyphastore::GenerationId{1},
            .segments = {active},
        };
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
    }

    const auto segment_name = glyphastore::segment_filename({.store_id = store_id,
                                                             .segment_id = active.segment_id,
                                                             .generation = active.generation,
                                                             .owner_worker = active.owner_worker});
    const auto segment_path = temporary.store_path() / segment_name;
    std::ifstream stream(segment_path, std::ios::binary);
    GLYPHA_REQUIRE(stream.is_open());
    std::vector<std::byte> prefix(glyphastore::kSegmentHeaderReservedBytes);
    stream.read(reinterpret_cast<char*>(prefix.data()), static_cast<std::streamsize>(prefix.size()));
    GLYPHA_REQUIRE(stream.gcount() == static_cast<std::streamsize>(prefix.size()));

    const auto decoded = glyphastore::decode_segment_header(prefix);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->identity.store_id == store_id);
    GLYPHA_REQUIRE(decoded->identity.segment_id == active.segment_id);
    GLYPHA_REQUIRE(std::filesystem::file_size(segment_path) == glyphastore::kSegmentSizeBytes);
}
