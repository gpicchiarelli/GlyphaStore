#include "glyphastore/core/types.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "persistence/system_error.hpp"
#include "test.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>

namespace {

auto manifest_with_segments(const std::size_t segment_count, const std::size_t worker_count = 1)
    -> glyphastore::Manifest {
    glyphastore::Manifest manifest{
        .store_id = {std::byte{0x31}},
        .manifest_generation = 1,
        .worker_count = static_cast<std::uint32_t>(worker_count),
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{segment_count + 1U},
        .next_segment_generation = glyphastore::GenerationId{1},
    };
    manifest.segments.reserve(segment_count);
    for (std::size_t index = 0; index < segment_count; ++index) {
        manifest.segments.push_back({
            .segment_id = glyphastore::SegmentId{index + 1U},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{static_cast<std::uint32_t>(index % worker_count)},
            .role = index < worker_count ? glyphastore::ManifestSegmentRole::active
                                         : glyphastore::ManifestSegmentRole::sealed,
        });
    }
    return manifest;
}

void require_invalid(const glyphastore::DurableResourceLimits& limits) {
    const auto result = glyphastore::validate_durable_resource_limits(limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::invalid_argument);
}

} // namespace

GLYPHA_TEST("durable resource limit configuration rejects every invalid boundary") {
    glyphastore::DurableResourceLimits limits{};
    limits.max_store_bytes = glyphastore::kSegmentSizeBytes - 1U;
    require_invalid(limits);

    limits = {};
    limits.max_segment_count = 0;
    require_invalid(limits);

    limits = {};
    limits.max_manifest_bytes = glyphastore::kManifestHeaderBytes;
    require_invalid(limits);

    limits = {};
    limits.max_open_files = 3;
    require_invalid(limits);

    limits = {};
    limits.max_recovery_memory_bytes = 0;
    require_invalid(limits);

    limits = {};
    limits.max_live_keys = 0;
    require_invalid(limits);

    limits = {};
    limits.max_temporary_compaction_bytes = 0;
    require_invalid(limits);

    limits = {};
    limits.max_temporary_compaction_bytes = limits.max_store_bytes + 1U;
    require_invalid(limits);

    limits = {};
    limits.max_write_amplification = 0;
    require_invalid(limits);
    limits.max_write_amplification = 65;
    require_invalid(limits);
}

GLYPHA_TEST("durable bootstrap and rotation budgets account for peak namespace bytes") {
    const glyphastore::DurableResourceLimits defaults{};
    const auto too_many_workers = glyphastore::validate_durable_bootstrap_resources(256, defaults);
    GLYPHA_REQUIRE(!too_many_workers.has_value());
    GLYPHA_REQUIRE(too_many_workers.error().code == glyphastore::ErrorCode::storage_exhausted);

    auto one_segment_only = defaults;
    one_segment_only.max_segment_count = 1;
    one_segment_only.max_store_bytes = 2ULL * glyphastore::kSegmentSizeBytes;
    one_segment_only.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    const auto rotation = glyphastore::validate_durable_rotation_resources(1, one_segment_only);
    GLYPHA_REQUIRE(!rotation.has_value());
    GLYPHA_REQUIRE(rotation.error().code == glyphastore::ErrorCode::storage_exhausted);

    auto exact_store = defaults;
    exact_store.max_segment_count = 1;
    exact_store.max_store_bytes = glyphastore::kSegmentSizeBytes;
    exact_store.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    const auto manifest =
        glyphastore::validate_durable_manifest_resources(manifest_with_segments(1), exact_store);
    GLYPHA_REQUIRE(!manifest.has_value());
    GLYPHA_REQUIRE(manifest.error().code == glyphastore::ErrorCode::storage_exhausted);
}

GLYPHA_TEST("manifest descriptor and live-key budgets fail with stable categories") {
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_segment_count = 2;
    limits.max_manifest_bytes = glyphastore::kManifestHeaderBytes + glyphastore::kManifestSegmentEntryBytes;
    auto result = glyphastore::validate_durable_manifest_resources(manifest_with_segments(2), limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::storage_exhausted);

    limits = {};
    limits.max_open_files = 4;
    result = glyphastore::validate_durable_manifest_resources(manifest_with_segments(1), limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::descriptor_exhausted);

    limits = {};
    limits.max_live_keys = 1;
    result = glyphastore::validate_durable_manifest_resources(manifest_with_segments(2, 2), limits);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().code == glyphastore::ErrorCode::resource_exhausted);
}

GLYPHA_TEST("live-key partitions exactly preserve the configured global limit") {
    constexpr std::size_t kWorkers = 7;
    constexpr std::size_t kKeys = 103;
    std::size_t sum{};
    for (std::size_t worker = 0; worker < kWorkers; ++worker) {
        const auto limit = glyphastore::durable_worker_live_key_limit(worker, kWorkers, kKeys);
        GLYPHA_REQUIRE(limit == 14 || limit == 15);
        sum += limit;
    }
    GLYPHA_REQUIRE(sum == kKeys);
    GLYPHA_REQUIRE(glyphastore::durable_worker_live_key_limit(kWorkers, kWorkers, kKeys) == 0);
}

GLYPHA_TEST("persistence errno mapping preserves resource failure categories") {
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", ENOSPC).error.code ==
                   glyphastore::ErrorCode::storage_exhausted);
#if defined(EDQUOT)
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", EDQUOT).error.code ==
                   glyphastore::ErrorCode::storage_exhausted);
#endif
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", EFBIG).error.code ==
                   glyphastore::ErrorCode::file_too_large);
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", EMFILE).error.code ==
                   glyphastore::ErrorCode::descriptor_exhausted);
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", ENFILE).error.code ==
                   glyphastore::ErrorCode::descriptor_exhausted);
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", EROFS).error.code ==
                   glyphastore::ErrorCode::read_only_filesystem);
    GLYPHA_REQUIRE(glyphastore::persistence_system_error("test", EIO).error.code ==
                   glyphastore::ErrorCode::io_error);
}
