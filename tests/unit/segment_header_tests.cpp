#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/segment/segment_header.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>

namespace {

auto fixture_identity() -> glyphastore::SegmentHeaderIdentity {
    return {
        .store_id = {std::byte{0x00}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44},
                     std::byte{0x55}, std::byte{0x66}, std::byte{0x77}, std::byte{0x88}, std::byte{0x99},
                     std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}, std::byte{0xDD}, std::byte{0xEE},
                     std::byte{0xFF}},
        .segment_id = glyphastore::SegmentId{0x0102030405060708ULL},
        .generation = glyphastore::GenerationId{0x11223344U},
        .owner_worker = glyphastore::WorkerId{0x55667788U},
    };
}

auto empty_commit(std::uint64_t generation = 1) -> glyphastore::SegmentCommit {
    return {
        .commit_generation = generation,
        .committed_end = glyphastore::kSegmentHeaderReservedBytes,
        .state = glyphastore::PersistedSegmentState::active,
    };
}

auto populated_commit(std::uint64_t generation = 2) -> glyphastore::SegmentCommit {
    return {
        .commit_generation = generation,
        .committed_end = 0x1100,
        .state = glyphastore::PersistedSegmentState::sealed,
        .record_count = 2,
        .first_sequence = glyphastore::SequenceNumber{10},
        .last_sequence = glyphastore::SequenceNumber{11},
    };
}

} // namespace

GLYPHA_TEST("segment header v1 matches its golden little-endian fixture") {
    const glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {empty_commit(), populated_commit()},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(glyphastore::encode_segment_header(encoded, header).has_value());

    const auto fixture = glyphastore::test::read_hex_fixture(std::filesystem::path{GLYPHASTORE_SOURCE_DIR} /
                                                             "tests/fixtures/segment_header_v1.hex");
    GLYPHA_REQUIRE(fixture.size() ==
                   glyphastore::kSegmentCommitSlotsOffset +
                       glyphastore::kSegmentCommitSlotCount * glyphastore::kSegmentCommitSlotBytes);
    GLYPHA_REQUIRE(std::equal(fixture.begin(), fixture.end(), encoded.begin()));
    GLYPHA_REQUIRE(std::all_of(encoded.begin() + static_cast<std::ptrdiff_t>(fixture.size()), encoded.end(),
                               [](std::byte value) { return value == std::byte{0}; }));

    const auto decoded = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->identity == header.identity);
    const auto selected = glyphastore::select_newest_segment_commit(*decoded);
    GLYPHA_REQUIRE(selected.has_value());
    GLYPHA_REQUIRE(selected->slot_index == 1);
    GLYPHA_REQUIRE(selected->commit == populated_commit());
}

GLYPHA_TEST("segment header rejects truncation and immutable identity corruption") {
    glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {empty_commit(), std::nullopt},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(glyphastore::encode_segment_header(encoded, header).has_value());

    const auto truncated = glyphastore::decode_segment_header(
        std::span<const std::byte>{encoded}.first(glyphastore::kSegmentHeaderReservedBytes - 1));
    GLYPHA_REQUIRE(!truncated.has_value());
    GLYPHA_REQUIRE(truncated.error().code == glyphastore::ErrorCode::invalid_record);

    encoded[16] ^= std::byte{1};
    const auto corrupted = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("segment header recovers through the older slot when the newer slot is corrupt") {
    const glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {empty_commit(4), populated_commit(5)},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(glyphastore::encode_segment_header(encoded, header).has_value());
    encoded[glyphastore::kSegmentCommitSlotsOffset + glyphastore::kSegmentCommitSlotBytes + 24] ^=
        std::byte{1};

    const auto decoded = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->slots[1].validity == glyphastore::CommitSlotValidity::invalid);
    const auto selected = glyphastore::select_newest_segment_commit(*decoded);
    GLYPHA_REQUIRE(selected.has_value());
    GLYPHA_REQUIRE(selected->slot_index == 0);
    GLYPHA_REQUIRE(selected->commit == empty_commit(4));
}

GLYPHA_TEST("segment header selects by generation rather than physical slot order") {
    const glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {populated_commit(9), empty_commit(8)},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(glyphastore::encode_segment_header(encoded, header).has_value());
    const auto decoded = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    const auto selected = glyphastore::select_newest_segment_commit(*decoded);
    GLYPHA_REQUIRE(selected.has_value());
    GLYPHA_REQUIRE(selected->slot_index == 0);
    GLYPHA_REQUIRE(selected->commit.commit_generation == 9);
}

GLYPHA_TEST("segment header fails closed for conflicting equal commit generations") {
    const auto first = empty_commit(7);
    const auto second = populated_commit(7);
    glyphastore::DecodedSegmentHeader decoded{
        .identity = fixture_identity(),
        .slots = {{{.validity = glyphastore::CommitSlotValidity::valid, .commit = first},
                   {.validity = glyphastore::CommitSlotValidity::valid, .commit = second}}},
    };
    const auto selected = glyphastore::select_newest_segment_commit(decoded);
    GLYPHA_REQUIRE(!selected.has_value());
    GLYPHA_REQUIRE(selected.error().code == glyphastore::ErrorCode::corrupted_data);

    const glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {first, second},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    const auto status = glyphastore::encode_segment_header(encoded, header);
    GLYPHA_REQUIRE(!status.has_value());
    GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("segment commit codec rejects invalid metadata and isolates unknown slot versions") {
    auto invalid = populated_commit();
    invalid.committed_end += 1;
    std::array<std::byte, glyphastore::kSegmentCommitSlotBytes> slot{};
    GLYPHA_REQUIRE(!glyphastore::encode_segment_commit_slot(slot, invalid).has_value());

    GLYPHA_REQUIRE(glyphastore::encode_segment_commit_slot(slot, populated_commit()).has_value());
    slot[4] = std::byte{2};
    const auto decoded = glyphastore::decode_segment_commit_slot(slot);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->validity == glyphastore::CommitSlotValidity::invalid);

    slot[48] = std::byte{0};
    slot[49] = std::byte{0};
    slot[50] = std::byte{0};
    slot[51] = std::byte{0};
    const auto checksum = glyphastore::crc32c(slot);
    for (std::size_t index = 0; index < 4; ++index) {
        slot[48 + index] = static_cast<std::byte>((checksum >> (index * 8U)) & 0xFFU);
    }
    const auto future_slot = glyphastore::decode_segment_commit_slot(slot);
    GLYPHA_REQUIRE(!future_slot.has_value());
    GLYPHA_REQUIRE(future_slot.error().code == glyphastore::ErrorCode::invalid_record);
}

GLYPHA_TEST("segment header without a valid commit cannot define a recovery boundary") {
    const glyphastore::SegmentHeader header{
        .identity = fixture_identity(),
        .commits = {std::nullopt, std::nullopt},
    };
    std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
    GLYPHA_REQUIRE(glyphastore::encode_segment_header(encoded, header).has_value());
    const auto decoded = glyphastore::decode_segment_header(encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    const auto selected = glyphastore::select_newest_segment_commit(*decoded);
    GLYPHA_REQUIRE(!selected.has_value());
    GLYPHA_REQUIRE(selected.error().code == glyphastore::ErrorCode::corrupted_data);
}
