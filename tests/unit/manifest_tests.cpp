#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/segment/crc32c.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace {

inline constexpr std::size_t kManifestChecksumOffset = 80;

auto fixture_manifest() -> glyphastore::Manifest {
    return {
        .store_id = {std::byte{0x00}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44},
                     std::byte{0x55}, std::byte{0x66}, std::byte{0x77}, std::byte{0x88}, std::byte{0x99},
                     std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}, std::byte{0xDD}, std::byte{0xEE},
                     std::byte{0xFF}},
        .manifest_generation = 0x0102030405060708ULL,
        .routing_algorithm = glyphastore::RoutingAlgorithm::fnv1a64_v1,
        .worker_count = 2,
        .routing_epoch = 0x1112131415161718ULL,
        .next_segment_id = glyphastore::SegmentId{4},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments =
            {
                {.segment_id = glyphastore::SegmentId{1},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{2},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::active},
                {.segment_id = glyphastore::SegmentId{3},
                 .generation = glyphastore::GenerationId{2},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
}

void put_u32(std::span<std::byte> out, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        out[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

void refresh_checksum(std::span<std::byte> bytes) {
    put_u32(bytes, kManifestChecksumOffset, 0);
    put_u32(bytes, kManifestChecksumOffset, glyphastore::crc32c(bytes));
}

} // namespace

GLYPHA_TEST("manifest v1 matches its golden little-endian fixture") {
    const auto manifest = fixture_manifest();
    const auto encoded = glyphastore::encode_manifest(manifest);
    GLYPHA_REQUIRE(encoded.has_value());
    const auto fixture = glyphastore::test::read_hex_fixture(std::filesystem::path{GLYPHASTORE_SOURCE_DIR} /
                                                             "tests/fixtures/manifest_v1.hex");
    GLYPHA_REQUIRE(*encoded == fixture);

    const auto decoded = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(*decoded == manifest);
}

GLYPHA_TEST("manifest decoder rejects truncation trailing bytes and checksum corruption") {
    auto encoded = glyphastore::encode_manifest(fixture_manifest());
    GLYPHA_REQUIRE(encoded.has_value());

    const auto truncated = glyphastore::decode_manifest(
        std::span<const std::byte>{*encoded}.first(glyphastore::kManifestHeaderBytes - 1));
    GLYPHA_REQUIRE(!truncated.has_value());
    GLYPHA_REQUIRE(truncated.error().code == glyphastore::ErrorCode::invalid_record);

    encoded->push_back(std::byte{0});
    const auto trailing = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!trailing.has_value());
    GLYPHA_REQUIRE(trailing.error().code == glyphastore::ErrorCode::invalid_record);
    encoded->pop_back();

    (*encoded)[glyphastore::kManifestHeaderBytes] ^= std::byte{1};
    const auto corrupted = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("manifest encoder enforces canonical catalog and active ownership") {
    auto manifest = fixture_manifest();
    manifest.segments[1].segment_id = manifest.segments[0].segment_id;
    GLYPHA_REQUIRE(!glyphastore::encode_manifest(manifest).has_value());

    manifest = fixture_manifest();
    manifest.segments.back().role = glyphastore::ManifestSegmentRole::sealed;
    GLYPHA_REQUIRE(!glyphastore::encode_manifest(manifest).has_value());

    manifest = fixture_manifest();
    manifest.segments.back().owner_worker = glyphastore::WorkerId{2};
    GLYPHA_REQUIRE(!glyphastore::encode_manifest(manifest).has_value());

    manifest = fixture_manifest();
    manifest.next_segment_id = manifest.segments.back().segment_id;
    GLYPHA_REQUIRE(!glyphastore::encode_manifest(manifest).has_value());

    manifest = fixture_manifest();
    manifest.worker_count = 0;
    GLYPHA_REQUIRE(!glyphastore::encode_manifest(manifest).has_value());
}

GLYPHA_TEST("manifest decoder distinguishes structural and semantic corruption") {
    auto encoded = glyphastore::encode_manifest(fixture_manifest());
    GLYPHA_REQUIRE(encoded.has_value());

    const auto third_owner_offset =
        glyphastore::kManifestHeaderBytes + 2 * glyphastore::kManifestSegmentEntryBytes + 12;
    put_u32(*encoded, third_owner_offset, 2);
    refresh_checksum(*encoded);
    const auto invalid_owner = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!invalid_owner.has_value());
    GLYPHA_REQUIRE(invalid_owner.error().code == glyphastore::ErrorCode::corrupted_data);

    encoded = glyphastore::encode_manifest(fixture_manifest());
    GLYPHA_REQUIRE(encoded.has_value());
    (*encoded)[glyphastore::kManifestHeaderBytes + 20] = std::byte{1};
    refresh_checksum(*encoded);
    const auto reserved = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!reserved.has_value());
    GLYPHA_REQUIRE(reserved.error().code == glyphastore::ErrorCode::invalid_record);
}

GLYPHA_TEST("manifest decoder fails closed for supported-checksum unknown versions") {
    auto encoded = glyphastore::encode_manifest(fixture_manifest());
    GLYPHA_REQUIRE(encoded.has_value());
    (*encoded)[4] = std::byte{2};
    refresh_checksum(*encoded);
    const auto future = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!future.has_value());
    GLYPHA_REQUIRE(future.error().code == glyphastore::ErrorCode::invalid_record);

    encoded = glyphastore::encode_manifest(fixture_manifest());
    GLYPHA_REQUIRE(encoded.has_value());
    put_u32(*encoded, 56, static_cast<std::uint32_t>(glyphastore::kMaximumManifestSegmentCount + 1));
    const auto excessive = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(!excessive.has_value());
    GLYPHA_REQUIRE(excessive.error().code == glyphastore::ErrorCode::invalid_record);
}

GLYPHA_TEST("manifest publication selection uses generation and rejects ambiguity") {
    auto older = fixture_manifest();
    older.manifest_generation = 4;
    auto newest = fixture_manifest();
    newest.manifest_generation = 9;
    auto middle = fixture_manifest();
    middle.manifest_generation = 7;
    const std::array candidates{older, newest, middle};
    const auto selected = glyphastore::select_newest_manifest(candidates);
    GLYPHA_REQUIRE(selected.has_value());
    GLYPHA_REQUIRE(selected->candidate_index == 1);
    GLYPHA_REQUIRE(selected->manifest_generation == 9);

    auto conflict = newest;
    conflict.routing_epoch += 1;
    const std::array conflicting{newest, conflict};
    const auto ambiguous = glyphastore::select_newest_manifest(conflicting);
    GLYPHA_REQUIRE(!ambiguous.has_value());
    GLYPHA_REQUIRE(ambiguous.error().code == glyphastore::ErrorCode::corrupted_data);

    const std::array identical{newest, newest};
    const auto duplicate = glyphastore::select_newest_manifest(identical);
    GLYPHA_REQUIRE(duplicate.has_value());
    GLYPHA_REQUIRE(duplicate->candidate_index == 0);
}
