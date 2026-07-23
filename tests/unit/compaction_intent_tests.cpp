#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/segment/segment_header.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace {

inline constexpr std::size_t kChecksumOffset = 56;

void put_u32(const std::span<std::byte> out, const std::size_t offset, const std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        out[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

void refresh_checksum(const std::span<std::byte> bytes) {
    put_u32(bytes, kChecksumOffset, 0);
    put_u32(bytes, kChecksumOffset, glyphastore::crc32c(bytes));
}

auto intent_fixture() -> glyphastore::DurableCompactionIntent {
    glyphastore::Manifest old{
        .store_id = {std::byte{0x61}, std::byte{0x62}, std::byte{0x63}},
        .manifest_generation = 9,
        .worker_count = 1,
        .routing_epoch = 7,
        .next_segment_id = glyphastore::SegmentId{5},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments =
            {
                {.segment_id = glyphastore::SegmentId{1},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{2},
                 .generation = glyphastore::GenerationId{2},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{3},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{4},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_store_bytes = 10ULL * glyphastore::kSegmentSizeBytes;
    limits.max_segment_count = 10;
    limits.max_temporary_compaction_bytes = 4ULL * glyphastore::kSegmentSizeBytes;
    const auto plan = glyphastore::plan_durable_worker_compaction(old, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(plan.has_value());
    return {.worker_id = glyphastore::WorkerId{0},
            .old_manifest = std::move(old),
            .next_manifest = plan->next_manifest};
}

} // namespace

GLYPHA_TEST("compaction intent v1 matches its independent golden fixture") {
    const auto intent = intent_fixture();
    const auto encoded = glyphastore::encode_compaction_intent(intent);
    GLYPHA_REQUIRE(encoded.has_value());
    const auto old_size = glyphastore::encoded_manifest_size(intent.old_manifest);
    const auto next_size = glyphastore::encoded_manifest_size(intent.next_manifest);
    GLYPHA_REQUIRE(old_size.has_value());
    GLYPHA_REQUIRE(next_size.has_value());
    GLYPHA_REQUIRE(encoded->size() == glyphastore::kCompactionIntentHeaderBytes + *old_size + *next_size);

    const auto fixture = glyphastore::test::read_hex_fixture(
        std::filesystem::path{GLYPHASTORE_SOURCE_DIR} / "tests/fixtures/compaction_intent_v1.hex");
    GLYPHA_REQUIRE(*encoded == fixture);

    const auto decoded = glyphastore::decode_compaction_intent(fixture);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(*decoded == intent);
}

GLYPHA_TEST("compaction intent rejects truncation trailing bytes and checksum corruption") {
    auto encoded = glyphastore::encode_compaction_intent(intent_fixture());
    GLYPHA_REQUIRE(encoded.has_value());

    auto decoded = glyphastore::decode_compaction_intent(
        std::span<const std::byte>{*encoded}.first(glyphastore::kCompactionIntentHeaderBytes - 1U));
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::invalid_record);

    encoded->push_back(std::byte{0});
    decoded = glyphastore::decode_compaction_intent(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::invalid_record);
    encoded->pop_back();

    (*encoded)[glyphastore::kCompactionIntentHeaderBytes] ^= std::byte{1};
    decoded = glyphastore::decode_compaction_intent(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::checksum_mismatch);
}

GLYPHA_TEST("compaction intent binds its header to canonical old and next manifests") {
    auto intent = intent_fixture();
    intent.next_manifest.next_segment_id.value += 1;
    auto encoded = glyphastore::encode_compaction_intent(intent);
    GLYPHA_REQUIRE(!encoded.has_value());
    GLYPHA_REQUIRE(encoded.error().code == glyphastore::ErrorCode::invalid_argument);

    encoded = glyphastore::encode_compaction_intent(intent_fixture());
    GLYPHA_REQUIRE(encoded.has_value());
    (*encoded)[16] ^= std::byte{1};
    refresh_checksum(*encoded);
    const auto decoded = glyphastore::decode_compaction_intent(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::corrupted_data);
}

GLYPHA_TEST("compaction intent fails closed for unknown versions and nonzero reserved bytes") {
    auto encoded = glyphastore::encode_compaction_intent(intent_fixture());
    GLYPHA_REQUIRE(encoded.has_value());
    (*encoded)[4] = std::byte{2};
    refresh_checksum(*encoded);
    auto decoded = glyphastore::decode_compaction_intent(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::invalid_record);

    encoded = glyphastore::encode_compaction_intent(intent_fixture());
    GLYPHA_REQUIRE(encoded.has_value());
    (*encoded)[60] = std::byte{1};
    refresh_checksum(*encoded);
    decoded = glyphastore::decode_compaction_intent(*encoded);
    GLYPHA_REQUIRE(!decoded.has_value());
    GLYPHA_REQUIRE(decoded.error().code == glyphastore::ErrorCode::invalid_record);
}
