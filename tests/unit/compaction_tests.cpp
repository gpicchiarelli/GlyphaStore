#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/segment/segment_header.hpp"
#include "test.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>

namespace {

auto compaction_manifest() -> glyphastore::Manifest {
    return {
        .store_id = {std::byte{0x47}},
        .manifest_generation = 7,
        .worker_count = 2,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{7},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments =
            {
                {.segment_id = glyphastore::SegmentId{1},
                 .generation = glyphastore::GenerationId{2},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{2},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{3},
                 .generation = glyphastore::GenerationId{4},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{4},
                 .generation = glyphastore::GenerationId{3},
                 .owner_worker = glyphastore::WorkerId{1},
                 .role = glyphastore::ManifestSegmentRole::active},
                {.segment_id = glyphastore::SegmentId{5},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{6},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
}

auto compaction_limits() -> glyphastore::DurableResourceLimits {
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_store_bytes = 16ULL * glyphastore::kSegmentSizeBytes;
    limits.max_segment_count = 16;
    limits.max_temporary_compaction_bytes = 8ULL * glyphastore::kSegmentSizeBytes;
    return limits;
}

} // namespace

GLYPHA_TEST("durable compaction output sizing accounts for reserved Segment headers") {
    constexpr auto kPayload =
        static_cast<std::uint64_t>(glyphastore::kSegmentSizeBytes - glyphastore::kSegmentHeaderReservedBytes);
    GLYPHA_REQUIRE(*glyphastore::durable_compaction_output_segments(0) == 0);
    GLYPHA_REQUIRE(*glyphastore::durable_compaction_output_segments(1) == 1);
    GLYPHA_REQUIRE(*glyphastore::durable_compaction_output_segments(kPayload) == 1);
    GLYPHA_REQUIRE(*glyphastore::durable_compaction_output_segments(kPayload + 1) == 2);
}

GLYPHA_TEST("durable compaction exact layout accounts for Record boundary fragmentation") {
    constexpr auto kRecords = std::size_t{127};
    glyphastore::DurableCompactionLayout layout;
    for (std::size_t index = 0; index < kRecords; ++index) {
        const auto placement = layout.add_record(glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(placement.has_value());
    }
    const auto aggregate = static_cast<std::uint64_t>(kRecords) * glyphastore::kMaxNormalRecordSize;
    GLYPHA_REQUIRE(*glyphastore::durable_compaction_output_segments(aggregate) == 2);
    GLYPHA_REQUIRE(layout.segment_count() == 3);
    GLYPHA_REQUIRE(layout.encoded_bytes() == aggregate);
}

GLYPHA_TEST("durable compaction replaces a complete Worker sealed set in one manifest") {
    const auto current = compaction_manifest();
    const auto plan = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1,
                                                                  compaction_limits());
    GLYPHA_REQUIRE(plan.has_value());
    GLYPHA_REQUIRE(plan->sources.size() == 3);
    GLYPHA_REQUIRE(plan->replacements.size() == 1);
    GLYPHA_REQUIRE(plan->replacements[0].segment_id == glyphastore::SegmentId{1});
    GLYPHA_REQUIRE(plan->replacements[0].generation == glyphastore::GenerationId{3});
    GLYPHA_REQUIRE(plan->next_manifest.manifest_generation == current.manifest_generation + 1);
    GLYPHA_REQUIRE(plan->next_manifest.next_segment_id == current.next_segment_id);
    GLYPHA_REQUIRE(plan->next_manifest.segments.size() == 4);
    GLYPHA_REQUIRE(plan->next_manifest.segments[0] == plan->replacements[0]);
    GLYPHA_REQUIRE(plan->next_manifest.segments[1] == current.segments[1]);
    GLYPHA_REQUIRE(plan->next_manifest.segments[2] == current.segments[3]);
    GLYPHA_REQUIRE(plan->next_manifest.segments[3] == current.segments[5]);
    GLYPHA_REQUIRE(plan->temporary_bytes == glyphastore::kSegmentSizeBytes);
    GLYPHA_REQUIRE(plan->reclaimed_bytes == 2ULL * glyphastore::kSegmentSizeBytes);
    GLYPHA_REQUIRE(glyphastore::encode_manifest(plan->next_manifest).has_value());
}

GLYPHA_TEST("empty durable compaction can retire an entirely obsolete sealed history") {
    const auto current = compaction_manifest();
    const auto plan = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 0,
                                                                  compaction_limits());
    GLYPHA_REQUIRE(plan.has_value());
    GLYPHA_REQUIRE(plan->replacements.empty());
    GLYPHA_REQUIRE(plan->next_manifest.segments.size() == 3);
    GLYPHA_REQUIRE(plan->reclaimed_bytes == 3ULL * glyphastore::kSegmentSizeBytes);
    GLYPHA_REQUIRE(glyphastore::encode_manifest(plan->next_manifest).has_value());
}

GLYPHA_TEST("durable compaction rejects unsafe identity and non-reclaiming plans") {
    auto current = compaction_manifest();
    auto limits = compaction_limits();

    auto result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{2}, 0, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::invalid_argument);

    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{1}, 0, limits);
    GLYPHA_REQUIRE(result.has_value());

    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 4, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::invalid_argument);

    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 3, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);

    current.manifest_generation = std::numeric_limits<std::uint64_t>::max();
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::arithmetic_overflow);

    current = compaction_manifest();
    current.segments[0].generation = glyphastore::GenerationId{std::numeric_limits<std::uint32_t>::max()};
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::arithmetic_overflow);
}

GLYPHA_TEST("durable compaction enforces temporary peak and amplification budgets") {
    const auto current = compaction_manifest();
    auto limits = compaction_limits();
    limits.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    auto result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 2, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);

    limits = compaction_limits();
    limits.max_write_amplification = 1;
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 2, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);

    limits.max_write_amplification = 2;
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 2, limits);
    GLYPHA_REQUIRE(result.has_value());

    limits = compaction_limits();
    limits.max_store_bytes = 7ULL * glyphastore::kSegmentSizeBytes;
    limits.max_temporary_compaction_bytes = limits.max_store_bytes;
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);

    limits = compaction_limits();
    const auto current_manifest_bytes = glyphastore::encoded_manifest_size(current);
    GLYPHA_REQUIRE(current_manifest_bytes.has_value());
    const auto one_output =
        glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(one_output.has_value());
    const auto next_manifest_bytes = glyphastore::encoded_manifest_size(one_output->next_manifest);
    GLYPHA_REQUIRE(next_manifest_bytes.has_value());
    limits.max_store_bytes = current.segments.size() * glyphastore::kSegmentSizeBytes +
                             glyphastore::kSegmentSizeBytes + *current_manifest_bytes + *next_manifest_bytes;
    limits.max_temporary_compaction_bytes = limits.max_store_bytes;
    result = glyphastore::plan_durable_worker_compaction(current, glyphastore::WorkerId{0}, 1, limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);
}
