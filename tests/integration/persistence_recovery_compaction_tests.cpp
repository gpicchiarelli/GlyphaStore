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

GLYPHA_TEST("blocked durable compaction build permits same-Worker reads and mutations") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, "changing", "old");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "stable", "visible");
        append_record(second, 3, "erase-me", "present");
        append_record(second, 4, "ttl-key", "old-ttl");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());

    const std::string key{"changing"};
    const std::string replacement{"new"};
    std::optional<glyphastore::Result<glyphastore::OwnedValue>> stable;
    glyphastore::DurableMutationResult mutation;
    glyphastore::DurableMutationResult erased;
    glyphastore::DurableMutationResult ttl_updated;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool operations_finished{};
    std::thread operations{[&] {
        stable.emplace((*runtime)->get("stable"));
        mutation = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{replacement}));
        const std::string erased_key{"erase-me"};
        erased = (*runtime)->erase(std::as_bytes(std::span{erased_key}));
        const std::string ttl_key{"ttl-key"};
        const std::string ttl_value{"new-ttl"};
        ttl_updated =
            (*runtime)->put(std::as_bytes(std::span{ttl_key}), std::as_bytes(std::span{ttl_value}), 100);
        {
            const std::lock_guard lock{completion_mutex};
            operations_finished = true;
        }
        completion.notify_one();
    }};
    bool completed_during_build{};
    {
        std::unique_lock lock{completion_mutex};
        completed_during_build =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return operations_finished; });
    }

    blocked_build.release();
    operations.join();
    compactor.join();

    GLYPHA_REQUIRE(completed_during_build);
    GLYPHA_REQUIRE(stable.has_value());
    GLYPHA_REQUIRE(stable->has_value());
    GLYPHA_REQUIRE(owned_text(**stable) == "visible");
    GLYPHA_REQUIRE(mutation.committed());
    GLYPHA_REQUIRE(erased.committed());
    GLYPHA_REQUIRE(ttl_updated.committed());
    GLYPHA_REQUIRE(compaction.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto current = (*runtime)->get(key);
    GLYPHA_REQUIRE(current.has_value());
    GLYPHA_REQUIRE(owned_text(*current) == replacement);
    const auto erased_value = (*runtime)->get("erase-me");
    GLYPHA_REQUIRE(!erased_value.has_value());
    GLYPHA_REQUIRE(erased_value.error().code == glyphastore::ErrorCode::not_found);
    const auto ttl_visible = (*runtime)->get("ttl-key", 99);
    GLYPHA_REQUIRE(ttl_visible.has_value());
    GLYPHA_REQUIRE(owned_text(*ttl_visible) == "new-ttl");
    const auto ttl_expired = (*runtime)->get("ttl-key", 100);
    GLYPHA_REQUIRE(!ttl_expired.has_value());
    GLYPHA_REQUIRE(ttl_expired.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*runtime)->manifest() == recovery_manifest(store_id, 1, entries));
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
}

GLYPHA_TEST("blocked read-only compaction scan lets an unrelated rotation commit") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{4},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto compacted_key = key_for_worker(0, 2, "compact-");
    const auto second_compacted_key = key_for_worker(0, 2, "compact-second-");
    const auto rotating_key = key_for_worker(1, 2, "rotate-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, compacted_key, "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, second_compacted_key, "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .before = &BlockingRecordRead::before,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.force_next_record_write_full();
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());

    glyphastore::DurableMutationResult rotation;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool rotation_finished{};
    std::thread writer{[&] {
        const std::string value{"value"};
        rotation = (*runtime)->put(std::as_bytes(std::span{rotating_key}), std::as_bytes(std::span{value}));
        {
            const std::lock_guard lock{completion_mutex};
            rotation_finished = true;
        }
        completion.notify_one();
    }};
    bool rotation_completed_during_scan{};
    {
        std::unique_lock lock{completion_mutex};
        rotation_completed_during_scan =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return rotation_finished; });
    }

    auto in_flight_rotation_stats = (*runtime)->rotation_stats();
    const auto stats_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (in_flight_rotation_stats.attempts == 0 && std::chrono::steady_clock::now() < stats_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        in_flight_rotation_stats = (*runtime)->rotation_stats();
    }
    GLYPHA_REQUIRE(in_flight_rotation_stats.attempts == 1);
    GLYPHA_REQUIRE(in_flight_rotation_stats.committed == 1);
    GLYPHA_REQUIRE(in_flight_rotation_stats.compaction_waits == 0);
    GLYPHA_REQUIRE(in_flight_rotation_stats.final_record_commit_attempts == 1);
    GLYPHA_REQUIRE(in_flight_rotation_stats.last_total_duration_ns > 0);

    blocked_build.release();
    writer.join();
    compactor.join();

    GLYPHA_REQUIRE(rotation_completed_during_scan);
    GLYPHA_REQUIRE(rotation.committed());
    GLYPHA_REQUIRE(compaction.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    const auto rotation_stats = (*runtime)->rotation_stats();
    GLYPHA_REQUIRE(rotation_stats.attempts == 1);
    GLYPHA_REQUIRE(rotation_stats.committed == 1);
    GLYPHA_REQUIRE(rotation_stats.compaction_waits == 0);
    GLYPHA_REQUIRE(rotation_stats.final_record_commit_attempts == 1);
    GLYPHA_REQUIRE(rotation_stats.final_record_commits == 1);
    GLYPHA_REQUIRE(rotation_stats.last_seal_duration_ns > 0);
    GLYPHA_REQUIRE(rotation_stats.last_create_duration_ns > 0);
    GLYPHA_REQUIRE(rotation_stats.last_manifest_publication_duration_ns > 0);
    GLYPHA_REQUIRE(rotation_stats.last_execution_duration_ns > 0);
    GLYPHA_REQUIRE(rotation_stats.last_execution_duration_ns >=
                   rotation_stats.last_seal_duration_ns + rotation_stats.last_create_duration_ns +
                       rotation_stats.last_manifest_publication_duration_ns);
    GLYPHA_REQUIRE(rotation_stats.last_total_duration_ns >= rotation_stats.last_publication_wait_duration_ns);
    GLYPHA_REQUIRE(rotation_stats.last_total_duration_ns >= rotation_stats.last_execution_duration_ns);
    GLYPHA_REQUIRE(rotation_stats.total_duration_ns == rotation_stats.last_total_duration_ns);
    GLYPHA_REQUIRE(rotation_stats.maximum_total_duration_ns == rotation_stats.last_total_duration_ns);
    GLYPHA_REQUIRE(rotation_stats.last_final_record_commit_duration_ns > 0);
    GLYPHA_REQUIRE(rotation_stats.total_final_record_commit_duration_ns ==
                   rotation_stats.last_final_record_commit_duration_ns);
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 5);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
    const auto visible = (*runtime)->get(rotating_key);
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(owned_text(*visible) == "value");
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto durable = (*reopened)->get(rotating_key);
    GLYPHA_REQUIRE(durable.has_value());
    GLYPHA_REQUIRE(owned_text(*durable) == "value");
    GLYPHA_REQUIRE((*reopened)->manifest().segments.size() == 5);
    GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
}

GLYPHA_TEST("rotation waiting on compaction intent does not block its Worker queue") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{4},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto first_key = key_for_worker(1, 2, "waiting-rotation-");
    const auto queued_key = key_for_worker(1, 2, "queue-progress-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, key_for_worker(0, 2, "compact-a-"), "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "compact-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingFilesystemOperation blocker{glyphastore::FilesystemOperation::write_compaction_intent};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    blocker.force_next_record_write_full();
    glyphastore::DurableMutationResult rotating;
    std::thread rotation{[&] {
        const std::string value{"rotation"};
        rotating = (*runtime)->put(std::as_bytes(std::span{first_key}), std::as_bytes(std::span{value}));
    }};
    auto rotation_stats = (*runtime)->rotation_stats();
    const auto rotation_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (rotation_stats.attempts == 0 && std::chrono::steady_clock::now() < rotation_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        rotation_stats = (*runtime)->rotation_stats();
    }

    glyphastore::DurableMutationResult queued;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool queued_finished{};
    std::thread queued_writer{[&] {
        const std::string value{"queued"};
        queued = (*runtime)->put(std::as_bytes(std::span{queued_key}), std::as_bytes(std::span{value}));
        {
            const std::lock_guard lock{completion_mutex};
            queued_finished = true;
        }
        completion.notify_one();
    }};
    bool queue_progressed{};
    {
        std::unique_lock lock{completion_mutex};
        queue_progressed =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return queued_finished; });
    }

    blocker.release();
    queued_writer.join();
    rotation.join();
    compactor.join();

    GLYPHA_REQUIRE(rotation_stats.attempts == 1);
    GLYPHA_REQUIRE(queue_progressed);
    GLYPHA_REQUIRE(queued.committed());
    GLYPHA_REQUIRE(!rotating.committed());
    GLYPHA_REQUIRE(rotating.error.has_value());
    GLYPHA_REQUIRE(rotating.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto rejected = (*runtime)->get(first_key);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*runtime)->get(queued_key).has_value());
}

GLYPHA_TEST("compaction manifest sync holds no Worker or catalog mutex") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{4},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto stable_key = key_for_worker(0, 2, "stable-");
    const auto second_key = key_for_worker(0, 2, "second-");
    const auto rejected_key = key_for_worker(0, 2, "rejected-");
    const auto other_worker_key = key_for_worker(1, 2, "other-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, stable_key, "stable-value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, second_key, "second-value");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingFilesystemOperation blocked_sync{glyphastore::FilesystemOperation::sync_manifest};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), glyphastore::FilesystemHooks{.context = &blocked_sync,
                                                       .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_sync.wait_until_blocked());

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read;
    glyphastore::DurableMutationResult rejected;
    glyphastore::DurableMutationResult other_worker;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool operations_finished{};
    std::thread operations{[&] {
        read.emplace((*runtime)->get(stable_key));
        const std::string rejected_value{"rejected"};
        rejected =
            (*runtime)->put(std::as_bytes(std::span{rejected_key}), std::as_bytes(std::span{rejected_value}));
        const std::string other_value{"other-value"};
        other_worker = (*runtime)->put(std::as_bytes(std::span{other_worker_key}),
                                       std::as_bytes(std::span{other_value}));
        {
            const std::lock_guard lock{completion_mutex};
            operations_finished = true;
        }
        completion.notify_one();
    }};
    bool completed_during_manifest_sync{};
    {
        std::unique_lock lock{completion_mutex};
        completed_during_manifest_sync =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return operations_finished; });
    }

    blocked_sync.release();
    operations.join();
    compactor.join();

    GLYPHA_REQUIRE(completed_during_manifest_sync);
    GLYPHA_REQUIRE(read.has_value());
    GLYPHA_REQUIRE(read->has_value());
    GLYPHA_REQUIRE(owned_text(**read) == "stable-value");
    GLYPHA_REQUIRE(rejected.outcome == glyphastore::DurableMutationOutcome::not_committed);
    GLYPHA_REQUIRE(rejected.error.has_value());
    GLYPHA_REQUIRE(rejected.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE(other_worker.committed());
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    const auto durable_other = (*reopened)->get(other_worker_key);
    GLYPHA_REQUIRE(durable_other.has_value());
    GLYPHA_REQUIRE(owned_text(*durable_other) == "other-value");
}

GLYPHA_TEST("close during a blocked compaction build rolls back the old authority") {
    RecoveryTemporaryDirectory temporary;
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, "first", "value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "second", "value");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_build;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_build,
            .file_io = {.context = &blocked_build, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_build.arm();

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocked_build.wait_until_blocked());
    const auto closed = (*runtime)->close();
    GLYPHA_REQUIRE(closed.has_value());

    blocked_build.release();
    compactor.join();
    GLYPHA_REQUIRE(compaction.outcome == glyphastore::DurableCompactionOutcome::not_compacted);
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    runtime->reset();

    auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->manifest() == recovery_manifest(store_id, 1, entries));
    GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
    GLYPHA_REQUIRE((*reopened)->get("first").has_value());
    GLYPHA_REQUIRE((*reopened)->get("second").has_value());
}

GLYPHA_TEST("online compaction filesystem fault matrix reopens one clean authority") {
    struct FaultCase {
        glyphastore::FilesystemOperation operation;
        std::size_t occurrence{1};
    };
    const std::vector<FaultCase> faults{
        {glyphastore::FilesystemOperation::write_compaction_intent},
        {glyphastore::FilesystemOperation::sync_compaction_intent},
        {glyphastore::FilesystemOperation::rename_compaction_intent},
        {glyphastore::FilesystemOperation::sync_directory, 1},
        {glyphastore::FilesystemOperation::preallocate_segment},
        {glyphastore::FilesystemOperation::write_segment_header},
        {glyphastore::FilesystemOperation::sync_segment_file},
        {glyphastore::FilesystemOperation::rename_segment},
        {glyphastore::FilesystemOperation::sync_directory, 2},
        {glyphastore::FilesystemOperation::write_record, 1},
        {glyphastore::FilesystemOperation::write_record, 2},
        {glyphastore::FilesystemOperation::sync_record},
        {glyphastore::FilesystemOperation::write_commit_slot, 1},
        {glyphastore::FilesystemOperation::sync_commit_slot, 1},
        {glyphastore::FilesystemOperation::write_commit_slot, 2},
        {glyphastore::FilesystemOperation::sync_commit_slot, 2},
        {glyphastore::FilesystemOperation::write_manifest},
        {glyphastore::FilesystemOperation::sync_manifest},
        {glyphastore::FilesystemOperation::rename_manifest},
        {glyphastore::FilesystemOperation::sync_directory, 3},
        {glyphastore::FilesystemOperation::remove_compaction_segment, 1},
        {glyphastore::FilesystemOperation::remove_compaction_segment, 2},
        {glyphastore::FilesystemOperation::sync_directory, 4},
        {glyphastore::FilesystemOperation::remove_compaction_intent},
        {glyphastore::FilesystemOperation::sync_directory, 5},
    };
    for (const auto& fault : faults) {
        RecoveryTemporaryDirectory temporary;
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        {
            auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
            GLYPHA_REQUIRE(directory.has_value());
            auto first = create_segment(*directory, store_id, entries[0]);
            append_record(first, 1, "fault-first", "first-value");
            GLYPHA_REQUIRE(first.seal().committed());
            auto second = create_segment(*directory, store_id, entries[1]);
            append_record(second, 2, "fault-second", "second-value");
            GLYPHA_REQUIRE(second.seal().committed());
            static_cast<void>(create_segment(*directory, store_id, entries[2]));
            GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        }

        OneShotFilesystemFailure failure{.target = fault.operation, .target_occurrence = fault.occurrence};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(),
            glyphastore::FilesystemHooks{.context = &failure, .before = &OneShotFilesystemFailure::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
        GLYPHA_REQUIRE(runtime.has_value());
        const auto result = (*runtime)->compact_worker(0, 0);
        GLYPHA_REQUIRE(failure.fired);
        GLYPHA_REQUIRE(!result.compacted());
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(result.error->code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableCompactionOutcome::not_compacted ||
                       result.outcome == glyphastore::DurableCompactionOutcome::recovery_required);
        GLYPHA_REQUIRE((*runtime)->healthy() ==
                       (result.outcome == glyphastore::DurableCompactionOutcome::not_compacted));
        runtime->reset();

        auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(reopened.has_value());
        GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
        const auto first = (*reopened)->get("fault-first");
        const auto second = (*reopened)->get("fault-second");
        GLYPHA_REQUIRE(first.has_value());
        GLYPHA_REQUIRE(second.has_value());
        GLYPHA_REQUIRE(owned_text(*first) == "first-value");
        GLYPHA_REQUIRE(owned_text(*second) == "second-value");
    }
}

