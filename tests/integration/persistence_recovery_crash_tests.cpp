#include "persistence_recovery_test_support.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

GLYPHA_TEST("durable runtime detects post-recovery Record corruption and remains fail-closed") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    glyphastore::RecordRef reference{};
    glyphastore::SegmentHeaderIdentity identity{};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        append_record(segment, 1, "stable", "value");
        const auto records = segment.scan_committed();
        GLYPHA_REQUIRE(records.has_value());
        GLYPHA_REQUIRE(records->size() == 1);
        reference = records->front();
        identity = segment.identity();
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->get("stable").has_value());

    const auto path = temporary.path() / glyphastore::segment_filename(identity);
    const auto descriptor = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    GLYPHA_REQUIRE(descriptor >= 0);
    const std::byte corruption{0xFF};
    const auto value_offset =
        static_cast<off_t>(reference.offset.value + glyphastore::kEncodedRecordHeaderSize + 6U);
    GLYPHA_REQUIRE(::pwrite(descriptor, &corruption, 1, value_offset) == 1);
    GLYPHA_REQUIRE(::close(descriptor) == 0);

    const auto corrupted = (*runtime)->get("stable");
    GLYPHA_REQUIRE(!corrupted.has_value());
    GLYPHA_REQUIRE(corrupted.error().code == glyphastore::ErrorCode::checksum_mismatch);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
    const auto after_failure = (*runtime)->get("stable");
    GLYPHA_REQUIRE(!after_failure.has_value());
    GLYPHA_REQUIRE(after_failure.error().code == glyphastore::ErrorCode::unavailable);
}

GLYPHA_TEST("durable runtime completes a sealed-active interrupted rotation") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.seal().committed());
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->manifest().segments[0].role == glyphastore::ManifestSegmentRole::sealed);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE(std::filesystem::exists(
        temporary.path() / glyphastore::segment_filename(segment_identity(store_id, active))));
}

GLYPHA_TEST("durable runtime adopts only the exact pristine prepared rotation Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto old = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(old.seal().committed());
        auto manifest = recovery_manifest(store_id, 1, {active});
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        const glyphastore::SegmentHeaderIdentity prepared{
            .store_id = store_id,
            .segment_id = manifest.next_segment_id,
            .generation = manifest.next_segment_generation,
            .owner_worker = active.owner_worker,
        };
        const auto replacement = glyphastore::DurableSegmentFile::create(*directory, prepared);
        GLYPHA_REQUIRE(replacement.durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->manifest().manifest_generation == 2);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
}

GLYPHA_TEST("durable runtime commits puts replacements erases and recovers them") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const std::string long_key(96, 'L');
    const auto long_bytes = std::as_bytes(std::span{long_key});
    const std::string first{"first"};
    const std::string second{"second"};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(runtime.has_value());
        const auto inserted = (*runtime)->put(long_bytes, std::as_bytes(std::span{first}), 500);
        GLYPHA_REQUIRE(inserted.committed());
        GLYPHA_REQUIRE(inserted.sequence->value == 1);
        GLYPHA_REQUIRE(owned_text(*(*runtime)->get(long_key, 499)) == "first");

        const auto replaced = (*runtime)->put(long_bytes, std::as_bytes(std::span{second}));
        GLYPHA_REQUIRE(replaced.committed());
        GLYPHA_REQUIRE(replaced.sequence->value == 2);
        GLYPHA_REQUIRE(owned_text(*(*runtime)->get(long_key)) == "second");

        const auto erased = (*runtime)->erase(long_bytes);
        GLYPHA_REQUIRE(erased.committed());
        GLYPHA_REQUIRE(erased.sequence->value == 3);
        const auto missing = (*runtime)->get(long_key);
        GLYPHA_REQUIRE(!missing.has_value());
        GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
        GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 4);
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 4);
    const auto missing = (*reopened)->get(long_key);
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("zero hot-cache budget falls back to pinned active-Segment reads for all value sizes") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 0;
    limits.max_hot_cache_bytes_per_worker = 0;
    limits.max_hot_cache_staging_bytes_per_worker = 0;
    limits.max_hot_cache_entries_per_worker = 0;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::array sizes{std::size_t{0}, std::size_t{64}, std::size_t{4096},
                           glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - 32U};
    for (std::size_t index = 0; index < sizes.size(); ++index) {
        const auto key = std::string{"cold-active-"} + std::to_string(index);
        const auto value = std::vector<std::byte>(sizes[index], static_cast<std::byte>(0x30U + index));
        GLYPHA_REQUIRE((*runtime)->put(std::as_bytes(std::span{key}), value).committed());
        const auto visible = (*runtime)->get(key);
        GLYPHA_REQUIRE(visible.has_value());
        GLYPHA_REQUIRE(visible->bytes == value);
    }

    const std::string overwrite_key{"cold-overwrite"};
    const std::string first{"first"};
    const std::string second{"second"};
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{overwrite_key}), std::as_bytes(std::span{first}))
                       .committed());
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{overwrite_key}), std::as_bytes(std::span{second}))
                       .committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(overwrite_key)) == second);
    GLYPHA_REQUIRE((*runtime)->erase(std::as_bytes(std::span{overwrite_key})).committed());
    GLYPHA_REQUIRE(!(*runtime)->get(overwrite_key).has_value());

    const std::string ttl_key{"cold-ttl"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{ttl_key}), std::as_bytes(std::span{first}), 100).committed());
    GLYPHA_REQUIRE((*runtime)->get(ttl_key, 99).has_value());
    const auto expired = (*runtime)->get(ttl_key, 100);
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
    // Repeated cold GETs must not keep the expired Index entry; a second GET is Index-miss only.
    const auto expired_again = (*runtime)->get(ttl_key, 100);
    GLYPHA_REQUIRE(!expired_again.has_value());
    GLYPHA_REQUIRE(expired_again.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE(expired_again.error().message == "key is not present");

    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].resident_bytes == 0);
    GLYPHA_REQUIRE(stats[0].staged_entries == 0);
    GLYPHA_REQUIRE(stats[0].staged_bytes == 0);
    GLYPHA_REQUIRE(stats[0].bucket_bytes == 0);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes == 0);
    GLYPHA_REQUIRE(stats[0].byte_budget == 0);
    GLYPHA_REQUIRE(stats[0].admission_bypasses == sizes.size() + 3U);
    // First TTL GET is a miss that validates expiry; the second is an Index miss (no cold I/O).
    GLYPHA_REQUIRE(stats[0].misses == sizes.size() + 3U);
    GLYPHA_REQUIRE(stats[0].expired_gets >= 1);

    const auto path_stats = (*runtime)->get_path_stats();
    GLYPHA_REQUIRE(path_stats.size() == 1);
    GLYPHA_REQUIRE(path_stats[0].prepare_calls >= sizes.size() + 3U);
    GLYPHA_REQUIRE(path_stats[0].complete_calls >= 1);
    if constexpr (kExpectGetPathTiming) {
        GLYPHA_REQUIRE(path_stats[0].prepare_hold_ns > 0);
        GLYPHA_REQUIRE(path_stats[0].index_lookup_ns > 0);
        GLYPHA_REQUIRE(path_stats[0].generation_pin_lookup_ns > 0);
        GLYPHA_REQUIRE(path_stats[0].cold_read_ns > 0);
    } else {
        GLYPHA_REQUIRE(path_stats[0].prepare_hold_ns == 0);
        GLYPHA_REQUIRE(path_stats[0].index_lookup_ns == 0);
        GLYPHA_REQUIRE(path_stats[0].generation_pin_lookup_ns == 0);
        GLYPHA_REQUIRE(path_stats[0].cold_read_ns == 0);
    }
    GLYPHA_REQUIRE(path_stats[0].expired_ttl_gets >= 1);
    GLYPHA_REQUIRE(path_stats[0].hot_resident_entries == 0);
}

GLYPHA_TEST("deferred TTL reclaim allows reinsert after an expired GET") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_deferred_ttl_reclaims_per_worker = 64;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"ttl-reinsert"};
    const std::string first{"old"};
    const std::string second{"new"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{first}), 100).committed());
    const auto expired = (*runtime)->get(key, 100);
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
    // Mutate drains the deferred expired RecordRef before publishing the reinsert.
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{second}), 0).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == second);
    const auto again = (*runtime)->get(key, 100);
    GLYPHA_REQUIRE(again.has_value());
    GLYPHA_REQUIRE(owned_text(*again) == second);
}

GLYPHA_TEST("get path telemetry records hot hits and prepare hold without behavioral change") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"hot-telem"};
    const std::string value{"payload"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == value);
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == value);

    const auto cache = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(cache.size() == 1);
    GLYPHA_REQUIRE(cache[0].hits >= 2);
    GLYPHA_REQUIRE(cache[0].resident_entries == 1);
    GLYPHA_REQUIRE(cache[0].resident_bytes > 0);

    const auto path = (*runtime)->get_path_stats();
    GLYPHA_REQUIRE(path.size() == 1);
    GLYPHA_REQUIRE(path[0].prepare_calls >= 2);
    GLYPHA_REQUIRE(path[0].hot_hits >= 2);
    if constexpr (kExpectGetPathTiming) {
        GLYPHA_REQUIRE(path[0].prepare_hold_ns > 0);
        GLYPHA_REQUIRE(path[0].hot_cache_lookup_ns > 0);
    } else {
        GLYPHA_REQUIRE(path[0].prepare_hold_ns == 0);
        GLYPHA_REQUIRE(path[0].hot_cache_lookup_ns == 0);
    }
    GLYPHA_REQUIRE(path[0].hot_resident_entries == 1);
    GLYPHA_REQUIRE(path[0].hot_resident_bytes > 0);
}

GLYPHA_TEST("hot-cache rejects oversized values and can be disabled without correctness loss") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_value_bytes = 32;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string small_key{"small"};
    const std::string small_value(16, 's');
    const std::string large_key{"large"};
    const std::string large_value(64, 'L');
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{small_key}), std::as_bytes(std::span{small_value}))
                       .committed());
    GLYPHA_REQUIRE((*runtime)
                       ->put(std::as_bytes(std::span{large_key}), std::as_bytes(std::span{large_value}))
                       .committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(small_key)) == small_value);
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(large_key)) == large_value);
    auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].size_rejected >= 1);
    GLYPHA_REQUIRE(stats[0].resident_entries == 1);
    GLYPHA_REQUIRE(stats[0].max_value_bytes == 32);
    GLYPHA_REQUIRE(stats[0].enabled);

    limits.hot_cache_enabled = false;
    (*runtime).reset();
    auto directory2 = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory2.has_value());
    auto disabled =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory2), 0, {.limits = limits});
    GLYPHA_REQUIRE(disabled.has_value());
    GLYPHA_REQUIRE(owned_text(*(*disabled)->get(large_key)) == large_value);
    const auto disabled_stats = (*disabled)->hot_cache_stats();
    GLYPHA_REQUIRE(!disabled_stats[0].enabled);
    GLYPHA_REQUIRE(disabled_stats[0].bucket_bytes == 0);
    GLYPHA_REQUIRE(disabled_stats[0].total_accounted_bytes == 0);
}

GLYPHA_TEST("hot-cache accounting remains bounded across hit overwrite and erase") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 16U * 1024U;
    limits.max_hot_cache_bytes_per_worker = 16U * 1024U;
    limits.max_hot_cache_staging_bytes_per_worker = 8U * 1024U;
    limits.max_hot_cache_entries_per_worker = 1;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"bounded-hot"};
    const std::string first(64, 'a');
    const std::string second(64, 'b');
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{first})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == first);
    auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 1);
    GLYPHA_REQUIRE(stats[0].staged_entries == 0);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
    GLYPHA_REQUIRE(stats[0].hits == 1);

    // At the entry limit an overwrite may conservatively bypass admission; the
    // previous value must be evicted so the new authoritative Record is read cold.
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{second})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == second);
    stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].admission_bypasses == 1);
    GLYPHA_REQUIRE(stats[0].misses == 1);

    GLYPHA_REQUIRE((*runtime)->erase(std::as_bytes(std::span{key})).committed());
    stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 0);
    GLYPHA_REQUIRE(stats[0].resident_bytes == 0);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
}

GLYPHA_TEST("concurrent GET PUT DELETE on one Worker keep linearized values") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"race-key"};
    const std::string v0{"v0"};
    GLYPHA_REQUIRE((*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{v0})).committed());

    std::atomic_bool failed{};
    std::thread readers([&] {
        for (std::size_t i = 0; i < 200; ++i) {
            const auto got = (*runtime)->get(key);
            if (!got) {
                if (got.error().code != glyphastore::ErrorCode::not_found) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
                continue;
            }
            const auto text = owned_text(*got);
            if (text != "v0" && text != "v1" && text != "v2") {
                failed.store(true, std::memory_order_relaxed);
                return;
            }
        }
    });
    std::thread writer([&] {
        for (const auto* value : {"v1", "v2"}) {
            const std::string payload{value};
            if (!(*runtime)
                     ->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{payload}))
                     .committed()) {
                failed.store(true, std::memory_order_relaxed);
                return;
            }
        }
        if (!(*runtime)->erase(std::as_bytes(std::span{key})).committed()) {
            failed.store(true, std::memory_order_relaxed);
        }
    });
    readers.join();
    writer.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    const auto after = (*runtime)->get(key);
    GLYPHA_REQUIRE(!after.has_value());
    GLYPHA_REQUIRE(after.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("stale hot entry is dropped after overwrite bypass and cold GET sees new value") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_entries_per_worker = 1;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    const std::string key{"stale-hot"};
    const std::string first(48, 'a');
    const std::string second(48, 'b');
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{first})).committed());
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == first);
    GLYPHA_REQUIRE((*runtime)->hot_cache_stats()[0].hits >= 1);

    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{second})).committed());
    // Entry limit forces admission bypass + eviction; next GET must not return first.
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == second);
    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].admission_bypasses >= 1);
    GLYPHA_REQUIRE(stats[0].stale_hits + stats[0].misses >= 1);
}

GLYPHA_TEST("flat hot-cache full-key compare rejects colliding probe neighbors") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    // Many short keys force open-addressing probe chains; identity remains full key bytes.
    for (std::size_t i = 0; i < 64; ++i) {
        const auto key = std::string{"k"} + std::to_string(i);
        const auto value = std::string{"v"} + std::to_string(i);
        GLYPHA_REQUIRE(
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed());
    }
    for (std::size_t i = 0; i < 64; ++i) {
        const auto key = std::string{"k"} + std::to_string(i);
        const auto value = std::string{"v"} + std::to_string(i);
        GLYPHA_REQUIRE(owned_text(*(*runtime)->get(key)) == value);
    }
    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].hits >= 64);
    GLYPHA_REQUIRE(stats[0].resident_entries == 64);
}

GLYPHA_TEST("durable runtime commits different Worker mutations concurrently") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        auto second = create_segment(*directory, store_id, entries[1]);
        GLYPHA_REQUIRE(first.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(second.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    const auto first_key = key_for_worker(0, 2, "parallel-a");
    const auto second_key = key_for_worker(1, 2, "parallel-b");
    std::atomic_bool failed{};
    std::thread first([&] {
        for (std::size_t iteration = 0; iteration < 8; ++iteration) {
            const auto value = std::string{"a-"} + std::to_string(iteration);
            if (!(*runtime)
                     ->put(std::as_bytes(std::span{first_key}), std::as_bytes(std::span{value}))
                     .committed()) {
                failed.store(true, std::memory_order_relaxed);
            }
        }
    });
    std::thread second([&] {
        for (std::size_t iteration = 0; iteration < 8; ++iteration) {
            const auto value = std::string{"b-"} + std::to_string(iteration);
            if (!(*runtime)
                     ->put(std::as_bytes(std::span{second_key}), std::as_bytes(std::span{value}))
                     .committed()) {
                failed.store(true, std::memory_order_relaxed);
            }
        }
    });
    first.join();
    second.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(first_key)) == "a-7");
    GLYPHA_REQUIRE(owned_text(*(*runtime)->get(second_key)) == "b-7");
    GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 9);
    GLYPHA_REQUIRE((*runtime)->next_sequence(1)->value == 9);
}

GLYPHA_TEST("rotation space preflight fails before sealing the active Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    RotationBudgetObserver observer{};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &observer,
                           .before = &RotationBudgetObserver::before,
                           .available_space_bytes = &RotationBudgetObserver::available});
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.reserved_free_bytes = 0;
    auto runtime =
        glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, {.limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string key{"rotation-budget"};
    const std::string value{"value"};
    const auto rejected = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
    GLYPHA_REQUIRE(!rejected.committed());
    GLYPHA_REQUIRE(rejected.error.has_value());
    GLYPHA_REQUIRE(rejected.error->code == glyphastore::ErrorCode::storage_exhausted);
    const auto rotation_stats = (*runtime)->rotation_stats();
    GLYPHA_REQUIRE(rotation_stats.attempts == 1);
    GLYPHA_REQUIRE(rotation_stats.committed == 0);
    GLYPHA_REQUIRE(rotation_stats.compaction_waits == 0);
    GLYPHA_REQUIRE(rotation_stats.final_record_commit_attempts == 0);
    GLYPHA_REQUIRE(rotation_stats.final_record_commits == 0);
    GLYPHA_REQUIRE(rotation_stats.last_seal_duration_ns == 0);
    GLYPHA_REQUIRE(rotation_stats.last_create_duration_ns == 0);
    GLYPHA_REQUIRE(rotation_stats.last_manifest_publication_duration_ns == 0);
    GLYPHA_REQUIRE(rotation_stats.last_total_duration_ns > 0);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 1);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.front().role == glyphastore::ManifestSegmentRole::active);
    runtime->reset();

    auto inspection = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(inspection.has_value());
    auto segment = glyphastore::DurableSegmentFile::open(*inspection, segment_identity(store_id, active),
                                                         glyphastore::SegmentFileOpenMode::read_only);
    GLYPHA_REQUIRE(segment.has_value());
    GLYPHA_REQUIRE(segment->selected_commit().commit.state == glyphastore::PersistedSegmentState::active);
}

GLYPHA_TEST("blocked durable append holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingFilesystemOperation blocker{glyphastore::FilesystemOperation::sync_record, false};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options;
    options.limits.hot_cache_enabled = false;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string old_key{"append-readable"};
    const std::string new_key{"append-writer"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{old_key}), std::as_bytes(std::span{value})).committed());

    blocker.arm();
    glyphastore::DurableMutationResult appended;
    std::thread writer{[&] {
        appended = (*runtime)->put(std::as_bytes(std::span{new_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    const auto visible = (*runtime)->get(old_key);
    blocker.release();
    writer.join();
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(owned_text(*visible) == value);
    GLYPHA_REQUIRE(appended.committed());
    GLYPHA_REQUIRE((*runtime)->get(new_key).has_value());
}

GLYPHA_TEST("blocked group commit holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingFilesystemOperation blocker{glyphastore::FilesystemOperation::sync_record, false};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options;
    options.limits.hot_cache_enabled = false;
    options.batch =
        glyphastore::DurableGroupConfig{.max_records = 1, .max_bytes = 1U << 20U, .max_wait_ms = 1000};
    options.strict_ack = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string old_key{"batch-readable"};
    const std::string new_key{"batch-writer"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{old_key}), std::as_bytes(std::span{value})).committed());

    blocker.arm();
    glyphastore::DurableMutationResult committed;
    std::thread writer{[&] {
        committed = (*runtime)->put(std::as_bytes(std::span{new_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    const auto visible = (*runtime)->get(old_key);
    blocker.release();
    writer.join();
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(owned_text(*visible) == value);
    GLYPHA_REQUIRE(committed.committed());
}

GLYPHA_TEST("blocked deferred dirty sync holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingFilesystemOperation blocker{glyphastore::FilesystemOperation::sync_commit_slot, false};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options;
    options.limits.hot_cache_enabled = false;
    options.commit_sync = glyphastore::SegmentCommitSync::deferred;
    options.sync_interval_ms = 60'000;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string key{"dirty-sync-readable"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed());

    blocker.arm();
    std::optional<glyphastore::Status> flushed;
    std::thread flusher{[&] { flushed.emplace((*runtime)->flush()); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    const auto visible = (*runtime)->get(key);
    blocker.release();
    flusher.join();
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(owned_text(*visible) == value);
    GLYPHA_REQUIRE(flushed.has_value());
    GLYPHA_REQUIRE(flushed->has_value());
}

GLYPHA_TEST("blocked rotation seal holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingRotationSeal blocker;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingRotationSeal::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options;
    options.limits.hot_cache_enabled = false;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string old_key{"rotation-readable"};
    const std::string new_key{"rotation-writer"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{old_key}), std::as_bytes(std::span{value})).committed());

    blocker.arm();
    glyphastore::DurableMutationResult rotated;
    std::thread writer{[&] {
        rotated = (*runtime)->put(std::as_bytes(std::span{new_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    std::mutex completion_mutex;
    std::condition_variable completion;
    bool read_finished{};
    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read;
    std::thread reader{[&] {
        read.emplace((*runtime)->get(old_key));
        {
            const std::lock_guard lock{completion_mutex};
            read_finished = true;
        }
        completion.notify_one();
    }};
    bool completed_while_seal_blocked{};
    {
        std::unique_lock lock{completion_mutex};
        completed_while_seal_blocked =
            completion.wait_for(lock, std::chrono::seconds{1}, [&] { return read_finished; });
    }

    blocker.release();
    reader.join();
    writer.join();
    GLYPHA_REQUIRE(completed_while_seal_blocked);
    GLYPHA_REQUIRE(read.has_value());
    GLYPHA_REQUIRE(read->has_value());
    GLYPHA_REQUIRE(owned_text(**read) == value);
    GLYPHA_REQUIRE(rotated.committed());
    GLYPHA_REQUIRE((*runtime)->get(new_key).has_value());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
}

GLYPHA_TEST("active rotation retires old-generation hot-cache accounting") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingRecordRead observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &observer, .before = &BlockingRecordRead::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string old_key{"old-hot"};
    const std::string new_key{"new-hot"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{old_key}), std::as_bytes(std::span{value})).committed());
    GLYPHA_REQUIRE((*runtime)->hot_cache_stats()[0].resident_entries == 1);

    observer.force_next_record_write_full();
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{new_key}), std::as_bytes(std::span{value})).committed());
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
    const auto stats = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(stats[0].resident_entries == 1);
    GLYPHA_REQUIRE(stats[0].total_accounted_bytes <= stats[0].byte_budget);
    GLYPHA_REQUIRE((*runtime)->get(old_key).has_value());
    GLYPHA_REQUIRE((*runtime)->get(new_key).has_value());
}

GLYPHA_TEST("durable runtime rotates a full active Segment before committing the Record") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const std::string fill_key{"fill"};
    const std::string maximum_value(
        glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - fill_key.size(), 'x');
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        for (std::uint64_t sequence = 1; sequence <= 63; ++sequence) {
            append_record(segment, sequence, fill_key, maximum_value);
        }
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    const std::string next_key{"next"};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(runtime.has_value());
        const auto committed =
            (*runtime)->put(std::as_bytes(std::span{next_key}), std::as_bytes(std::span{maximum_value}));
        GLYPHA_REQUIRE(committed.committed());
        GLYPHA_REQUIRE(committed.sequence->value == 64);
        GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
        GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
        const auto visible = (*runtime)->get(next_key);
        GLYPHA_REQUIRE(visible.has_value());
        GLYPHA_REQUIRE(visible->bytes.size() == maximum_value.size());

        const auto observation = (*runtime)->maintenance_observation(0);
        GLYPHA_REQUIRE(observation.has_value());
        GLYPHA_REQUIRE(observation->compaction_candidate_worker == 0);
        GLYPHA_REQUIRE(observation->candidate_sealed_record_bytes ==
                       63ULL * glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_live_record_bytes == glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_dead_record_bytes == 62ULL * glyphastore::kMaxNormalRecordSize);
        GLYPHA_REQUIRE(observation->candidate_dead_byte_ratio_bp ==
                       static_cast<std::uint32_t>(62ULL * 10'000ULL / 63ULL));

        GLYPHA_REQUIRE((*runtime)
                           ->put(std::as_bytes(std::span{fill_key}), std::as_bytes(std::span{maximum_value}))
                           .committed());
        const auto overwritten = (*runtime)->maintenance_observation(0);
        GLYPHA_REQUIRE(overwritten.has_value());
        GLYPHA_REQUIRE(overwritten->candidate_live_record_bytes == 0);
        GLYPHA_REQUIRE(overwritten->candidate_dead_record_bytes ==
                       overwritten->candidate_sealed_record_bytes);
        GLYPHA_REQUIRE(overwritten->candidate_dead_byte_ratio_bp == 10'000);
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 66);
    GLYPHA_REQUIRE((*reopened)->get(next_key).has_value());
    GLYPHA_REQUIRE((*reopened)->get(fill_key).has_value());
}

GLYPHA_TEST("durable group closes a pending batch before rotating a full Segment") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const std::string first_key{"one!"};
    const std::string second_key{"two!"};
    const std::string maximum_value(
        glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - first_key.size(), 'x');
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        for (std::uint64_t sequence = 1; sequence <= 62; ++sequence) {
            append_record(segment, sequence, "fill", maximum_value);
        }
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    SyncThreadObserver observer;
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(),
            glyphastore::FilesystemHooks{.context = &observer, .before = &SyncThreadObserver::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
            std::move(*directory), 0,
            {.commit_sync = glyphastore::SegmentCommitSync::immediate,
             .sync_interval_ms = 50,
             .batch = glyphastore::DurableGroupConfig{.max_records = 2,
                                                      .max_bytes = 2U * glyphastore::kMaxNormalRecordSize,
                                                      .max_wait_ms = 50},
             .strict_ack = true});
        GLYPHA_REQUIRE(runtime.has_value());

        std::atomic committed{0};
        std::array<std::thread::id, 2> producer_threads{};
        const auto put = [&](const std::size_t producer, const std::string& key) {
            producer_threads[producer] = std::this_thread::get_id();
            const auto result =
                (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{maximum_value}));
            if (result.committed()) {
                committed.fetch_add(1);
            }
        };
        std::thread first{[&] { put(0, first_key); }};
        std::thread second{[&] { put(1, second_key); }};
        first.join();
        second.join();

        GLYPHA_REQUIRE(committed.load() == 2);
        GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);
        GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 65);
        GLYPHA_REQUIRE((*runtime)->get(first_key).has_value());
        GLYPHA_REQUIRE((*runtime)->get(second_key).has_value());
        const std::lock_guard lock{observer.mutex};
        GLYPHA_REQUIRE(!observer.sync_threads.empty());
        GLYPHA_REQUIRE(std::ranges::none_of(observer.sync_threads, [&](const std::thread::id thread) {
            return thread == producer_threads[0] || thread == producer_threads[1];
        }));
    }

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->active_segment(0)->value == 2);
    GLYPHA_REQUIRE((*reopened)->next_sequence(0)->value == 65);
    GLYPHA_REQUIRE((*reopened)->get(first_key).has_value());
    GLYPHA_REQUIRE((*reopened)->get(second_key).has_value());
}

GLYPHA_TEST("one-Worker durable group commits on the dedicated commit executor") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    SyncThreadObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &SyncThreadObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_hot_cache_bytes = 0;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true,
         .limits = limits});
    GLYPHA_REQUIRE(runtime.has_value());

    std::array<std::thread::id, 2> producer_threads{};
    std::atomic committed{0};
    const auto put = [&](const std::size_t producer, const std::string key) {
        producer_threads[producer] = std::this_thread::get_id();
        const std::string value{"value"};
        if ((*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed()) {
            committed.fetch_add(1, std::memory_order_relaxed);
        }
    };
    std::thread first{put, 0, "first"};
    std::thread second{put, 1, "second"};
    first.join();
    second.join();

    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed) == 2);
    std::thread::id sync_thread;
    {
        const std::lock_guard lock{observer.mutex};
        sync_thread = observer.sync_thread;
    }
    GLYPHA_REQUIRE(sync_thread != std::thread::id{});
    GLYPHA_REQUIRE(sync_thread != producer_threads[0]);
    GLYPHA_REQUIRE(sync_thread != producer_threads[1]);
    GLYPHA_REQUIRE((*runtime)->get("first").has_value());
    GLYPHA_REQUIRE((*runtime)->get("second").has_value());
    const auto cache = (*runtime)->hot_cache_stats();
    GLYPHA_REQUIRE(cache[0].resident_entries == 0);
    GLYPHA_REQUIRE(cache[0].bucket_bytes == 0);
    GLYPHA_REQUIRE(cache[0].total_accounted_bytes == 0);
    GLYPHA_REQUIRE(cache[0].admission_bypasses == 2);
    GLYPHA_REQUIRE(cache[0].misses == 2);
}

GLYPHA_TEST("one-Worker commit executor bounds admission at the batch record limit") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BatchBoundaryObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &BatchBoundaryObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    static constexpr std::size_t kProducerCount = 32;
    std::barrier start{static_cast<std::ptrdiff_t>(kProducerCount + 1)};
    std::atomic committed{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (std::size_t producer = 0; producer < kProducerCount; ++producer) {
        producers.emplace_back([&, producer] {
            start.arrive_and_wait();
            const auto key = std::string{"key-"} + std::to_string(producer);
            const std::string value{"value"};
            if ((*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed()) {
                committed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    start.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }

    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed) == kProducerCount);
    const std::lock_guard lock{observer.mutex};
    GLYPHA_REQUIRE(observer.maximum_writes_before_sync == 2);
    GLYPHA_REQUIRE(observer.writes_since_sync == 0);
    GLYPHA_REQUIRE(observer.sync_count == kProducerCount / 2);
}

GLYPHA_TEST("explicit flush completes a partial sequenced durable group") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    RecordWriteObserver observer;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &observer, .before = &RecordWriteObserver::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 32, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    std::atomic committed{false};
    std::thread producer{[&] {
        const std::string key{"partial"};
        const std::string value{"value"};
        committed.store(
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed(),
            std::memory_order_relaxed);
    }};
    {
        std::unique_lock lock{observer.mutex};
        GLYPHA_REQUIRE(observer.written.wait_for(lock, std::chrono::seconds{5},
                                                 [&] { return observer.record_written; }));
    }
    GLYPHA_REQUIRE((*runtime)->flush().has_value());
    producer.join();
    GLYPHA_REQUIRE(committed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE((*runtime)->get("partial").has_value());
}

GLYPHA_TEST("durable runtime reports an indeterminate slot sync and recovery resolves one boundary") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_commit_slot};
    {
        auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(
            temporary.path(), 0,
            glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
        GLYPHA_REQUIRE(runtime.has_value());
        const std::string key{"uncertain"};
        const std::string value{"value"};
        const auto result = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableMutationOutcome::indeterminate);
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(!(*runtime)->healthy());
    }
    GLYPHA_REQUIRE(failure.fired);

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    const auto next = (*recovered)->next_sequence(0)->value;
    GLYPHA_REQUIRE(next == 1 || next == 2);
    const auto resolved = (*recovered)->get("uncertain");
    if (next == 1) {
        GLYPHA_REQUIRE(!resolved.has_value());
        GLYPHA_REQUIRE(resolved.error().code == glyphastore::ErrorCode::not_found);
    } else {
        GLYPHA_REQUIRE(resolved.has_value());
        GLYPHA_REQUIRE(owned_text(*resolved) == "value");
    }
}

GLYPHA_TEST("durable mutation fault matrix preserves pre and post commit recovery oracles") {
    static constexpr std::array boundaries{
        glyphastore::FilesystemOperation::write_record,
        glyphastore::FilesystemOperation::sync_record,
        glyphastore::FilesystemOperation::write_commit_slot,
        glyphastore::FilesystemOperation::sync_commit_slot,
    };
    static constexpr std::array failures{
        glyphastore::ErrorCode::io_error,
        glyphastore::ErrorCode::storage_exhausted,
        glyphastore::ErrorCode::read_only_filesystem,
    };

    for (const auto boundary : boundaries) {
        for (const auto failure_code : failures) {
            RecoveryTemporaryDirectory temporary;
            const auto store_id = recovery_store_id();
            const glyphastore::ManifestSegmentEntry active{
                .segment_id = glyphastore::SegmentId{1},
                .generation = glyphastore::GenerationId{1},
                .owner_worker = glyphastore::WorkerId{0},
                .role = glyphastore::ManifestSegmentRole::active,
            };
            {
                auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
                GLYPHA_REQUIRE(directory.has_value());
                static_cast<void>(create_segment(*directory, store_id, active));
                GLYPHA_REQUIRE(
                    directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
            }

            OneShotFilesystemFailure failure{.target = boundary, .code = failure_code};
            {
                auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(
                    temporary.path(), 0,
                    glyphastore::FilesystemHooks{.context = &failure,
                                                 .before = &OneShotFilesystemFailure::before});
                GLYPHA_REQUIRE(runtime.has_value());
                const std::string key{"fault-matrix"};
                const std::string value{"value"};
                const auto result =
                    (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
                GLYPHA_REQUIRE(!result.committed());
                GLYPHA_REQUIRE(result.error.has_value());
                GLYPHA_REQUIRE(result.error->code == failure_code);
                GLYPHA_REQUIRE(result.outcome ==
                               (boundary == glyphastore::FilesystemOperation::sync_commit_slot
                                    ? glyphastore::DurableMutationOutcome::indeterminate
                                    : glyphastore::DurableMutationOutcome::not_committed));
            }
            GLYPHA_REQUIRE(failure.fired);

            auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
            GLYPHA_REQUIRE(recovered.has_value());
            const auto visible = (*recovered)->get("fault-matrix");
            if (boundary == glyphastore::FilesystemOperation::sync_commit_slot) {
                GLYPHA_REQUIRE(visible.has_value() ||
                               visible.error().code == glyphastore::ErrorCode::not_found);
                if (visible) {
                    GLYPHA_REQUIRE(owned_text(*visible) == "value");
                }
            } else {
                GLYPHA_REQUIRE(!visible.has_value());
                GLYPHA_REQUIRE(visible.error().code == glyphastore::ErrorCode::not_found);
            }
            GLYPHA_REQUIRE((*recovered)->verify_index().has_value());
        }
    }
}

GLYPHA_TEST("durable group flush failure wakes every batch waiter fail-closed") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.selected_commit().commit.record_count == 0);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000},
         .strict_ack = true});
    GLYPHA_REQUIRE(runtime.has_value());

    std::atomic outcomes{0};
    const auto put = [&](std::string key) {
        const std::string value{"value"};
        const auto result = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (result.outcome == glyphastore::DurableMutationOutcome::indeterminate) {
            outcomes.fetch_add(1);
        }
    };
    std::thread first{put, "first"};
    std::thread second{put, "second"};
    first.join();
    second.join();

    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(outcomes.load() == 2);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
}

GLYPHA_TEST("durable runtime close returns a sticky final flush failure") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, active));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    OneShotFilesystemFailure failure{.target = glyphastore::FilesystemOperation::sync_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::deferred, .sync_interval_ms = 60'000});
    GLYPHA_REQUIRE(runtime.has_value());
    const std::string key{"close-failure"};
    const std::string value{"value"};
    GLYPHA_REQUIRE(
        (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value})).committed());

    const auto first = (*runtime)->close();
    GLYPHA_REQUIRE(!first.has_value());
    GLYPHA_REQUIRE(first.error().code == glyphastore::ErrorCode::io_error);
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(!(*runtime)->healthy());
    const auto repeated = (*runtime)->close();
    GLYPHA_REQUIRE(!repeated.has_value());
    GLYPHA_REQUIRE(repeated.error().code == glyphastore::ErrorCode::io_error);
    const auto blocked = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
    // Post-close sticky admission never enters Store — known not committed (not
    // indeterminate). Matches Writer healthy_ reject → wire OVERLOADED polarity.
    GLYPHA_REQUIRE(blocked.outcome == glyphastore::DurableMutationOutcome::not_committed);
    GLYPHA_REQUIRE(blocked.error.has_value());
    GLYPHA_REQUIRE(blocked.error->code == glyphastore::ErrorCode::unavailable);
}
