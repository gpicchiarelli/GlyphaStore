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

GLYPHA_TEST("exclusive Writer with flusher does not deadlock rotation on compaction depth") {
    // Paired durable-group/periodic: exclusive_writer=true but flusher shares the
    // Worker mutex (depth never incremented). ExclusiveHotPathPause must not
    // underflow hot_path_depth while waiting on the compaction publication lease,
    // or compaction's depth wait deadlocks against that wait.
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
    const auto first_key = key_for_worker(1, 2, "flusher-rotation-");
    const auto queued_key = key_for_worker(1, 2, "flusher-queue-");
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
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    options.batch = glyphastore::DurableGroupConfig{.max_records = 32,
                                                    .max_bytes = 65'536,
                                                    .max_wait_ms = 50,
                                                    .min_records = 1};
    options.strict_ack = true;
    options.sync_interval_ms = 50;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
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
    GLYPHA_REQUIRE(rotation_stats.attempts == 1);

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
    // Without the pause predicate fix, compaction's depth wait hangs forever on join.
    queued_writer.join();
    rotation.join();
    compactor.join();

    GLYPHA_REQUIRE(queue_progressed);
    GLYPHA_REQUIRE(queued.committed() ||
                   (queued.error.has_value() &&
                    (queued.error->code == glyphastore::ErrorCode::sequence_conflict ||
                     queued.error->code == glyphastore::ErrorCode::resource_exhausted)));
    GLYPHA_REQUIRE(!rotating.committed());
    GLYPHA_REQUIRE(rotating.error.has_value());
    GLYPHA_REQUIRE(rotating.error->code == glyphastore::ErrorCode::sequence_conflict ||
                   rotating.error->code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(compaction.compacted() ||
                   compaction.outcome == glyphastore::DurableCompactionOutcome::not_beneficial ||
                   (compaction.error.has_value() &&
                    compaction.error->code == glyphastore::ErrorCode::sequence_conflict));
    GLYPHA_REQUIRE((*runtime)->healthy());
}

GLYPHA_TEST("exclusive Writer compact unlocks before hot_path_depth wait") {
    // Phase C must drop catalog/worker before waiting on hot_path_depth. Otherwise an
    // exclusive durable_sync mutate that already holds depth and needs shared catalog
    // after unlocked append deadlocks against compaction's depth wait.
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
    const auto sealed_key = key_for_worker(0, 2, "depth-sealed-");
    const auto mutate_key = key_for_worker(0, 2, "depth-mutate-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, sealed_key, "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "depth-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    struct DualBlocker final {
        BlockingFilesystemOperation intent{glyphastore::FilesystemOperation::write_compaction_intent};
        BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record, false};

        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<DualBlocker*>(opaque);
            if (auto status = BlockingFilesystemOperation::before(&state.append, operation); !status) {
                return status;
            }
            return BlockingFilesystemOperation::before(&state.intent, operation);
        }
    } blocker;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &DualBlocker::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    GLYPHA_REQUIRE(blocker.intent.wait_until_blocked());

    blocker.append.arm();
    glyphastore::DurableMutationResult mutating;
    std::thread writer{[&] {
        const std::string value{"concurrent"};
        mutating =
            (*runtime)->put(std::as_bytes(std::span{mutate_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.append.wait_until_blocked());

    blocker.intent.release();
    // Without unlock-before-depth-wait, compaction holds catalog here and join hangs.
    blocker.append.release();
    writer.join();
    compactor.join();

    GLYPHA_REQUIRE(mutating.committed());
    GLYPHA_REQUIRE(compaction.error.has_value());
    GLYPHA_REQUIRE(compaction.error->code == glyphastore::ErrorCode::sequence_conflict);
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto sealed = (*runtime)->get(sealed_key);
    GLYPHA_REQUIRE(sealed.has_value());
    GLYPHA_REQUIRE(owned_text(*sealed) == "first");
    const auto written = (*runtime)->get(mutate_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == "concurrent");
}

GLYPHA_TEST("exclusive Writer compact Phase A drains hot_path_depth before Index snapshot") {
    // Phase A must arm compaction_commit_active and wait for hot_path_depth==0 before
    // index.entries(): exclusive durable_sync mutates elide worker.mutex and share only
    // the catalog lock, so a mutex-only snapshot races Index find/prepare/publish.
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
    const auto in_flight_key = key_for_worker(0, 2, "phase-a-inflight-");
    const auto gated_key = key_for_worker(0, 2, "phase-a-gated-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, key_for_worker(0, 2, "phase-a-sealed-a-"), "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "phase-a-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    struct DualBlocker final {
        BlockingFilesystemOperation intent{glyphastore::FilesystemOperation::write_compaction_intent};
        BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record, false};

        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<DualBlocker*>(opaque);
            if (auto status = BlockingFilesystemOperation::before(&state.append, operation); !status) {
                return status;
            }
            return BlockingFilesystemOperation::before(&state.intent, operation);
        }
    } blocker;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &DualBlocker::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    blocker.append.arm();
    glyphastore::DurableMutationResult in_flight;
    std::thread writer{[&] {
        const std::string value{"in-flight"};
        in_flight =
            (*runtime)->put(std::as_bytes(std::span{in_flight_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.append.wait_until_blocked());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    // Phase A arms the gate before the depth wait. Give the compact thread a
    // timeslice so a sibling exclusive put cannot sneak in before arm().
    std::this_thread::sleep_for(std::chrono::milliseconds{20});

    glyphastore::DurableMutationResult gated;
    bool saw_snapshot_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = gated_key + std::to_string(attempt);
        const std::string value{"gated"};
        gated = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (!gated.committed() && gated.error.has_value() &&
            gated.error->code == glyphastore::ErrorCode::sequence_conflict) {
            saw_snapshot_gate = true;
            break;
        }
        // A commit before the gate arms would mean a second exclusive Writer raced
        // the in-flight append — do not keep poking the Index.
        GLYPHA_REQUIRE(!gated.committed());
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(saw_snapshot_gate);

    blocker.append.release();
    writer.join();
    blocker.intent.release();
    compactor.join();

    GLYPHA_REQUIRE(in_flight.committed());
    GLYPHA_REQUIRE(compaction.compacted() ||
                   (compaction.error.has_value() &&
                    compaction.error->code == glyphastore::ErrorCode::sequence_conflict));
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto written = (*runtime)->get(in_flight_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == "in-flight");
}

GLYPHA_TEST("exclusive Writer unread TTL probe drains hot_path_depth before Index walk") {
    // maintenance_observation's unread TTL probe must use the same Index ownership
    // protocol as compaction Phase A: arm compaction_commit_active and drain
    // hot_path_depth before index.entries(). Mutex-only walk races exclusive
    // durable_sync mutates that elide worker.mutex.
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
    const auto in_flight_key = key_for_worker(0, 2, "ttl-probe-inflight-");
    const auto gated_key = key_for_worker(0, 2, "ttl-probe-gated-");
    const auto expired_key = key_for_worker(0, 2, "ttl-probe-expired-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, expired_key, "expired", glyphastore::Opcode::put, 1);
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "ttl-probe-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &append, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableMutationResult in_flight;
    std::thread writer{[&] {
        const std::string value{"in-flight"};
        in_flight =
            (*runtime)->put(std::as_bytes(std::span{in_flight_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(append.wait_until_blocked());

    glyphastore::Result<glyphastore::MaintenanceObservation> observation{
        glyphastore::fail(glyphastore::ErrorCode::internal_error, "unset")};
    std::thread prober{[&] {
        observation = (*runtime)->maintenance_observation(0, /*now_ns=*/100, /*probe=*/true);
    }};
    std::this_thread::sleep_for(std::chrono::milliseconds{20});

    glyphastore::DurableMutationResult gated;
    bool saw_probe_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = gated_key + std::to_string(attempt);
        const std::string value{"gated"};
        gated = (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (!gated.committed() && gated.error.has_value() &&
            gated.error->code == glyphastore::ErrorCode::sequence_conflict) {
            saw_probe_gate = true;
            break;
        }
        GLYPHA_REQUIRE(!gated.committed());
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(saw_probe_gate);

    append.release();
    writer.join();
    prober.join();

    GLYPHA_REQUIRE(in_flight.committed());
    GLYPHA_REQUIRE(observation.has_value());
    GLYPHA_REQUIRE(observation->unread_ttl_probe_performed);
    GLYPHA_REQUIRE(observation->candidate_unread_expired_sealed_record_count >= 1);
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto written = (*runtime)->get(in_flight_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == "in-flight");
}

GLYPHA_TEST("exclusive Writer capture_published_read takes Worker mutex under compaction") {
    // capture_published_read must always take worker.mutex (like snapshot_published_reads).
    // Eliding on exclusive durable_sync raced compaction Index enumerate/swap and could
    // fail_closed after a committed PUT when pin lookup saw a torn catalog view.
    // Exercise the same Index ownership under the Phase A gate via catalog put/get;
    // StoreAccess::capture_durable_read shares this lock (paired Writer publish path).
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
    const auto stable_key = key_for_worker(0, 2, "capture-stable-");
    const auto in_flight_key = key_for_worker(0, 2, "capture-inflight-");
    const auto gated_key = key_for_worker(0, 2, "capture-gated-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, key_for_worker(0, 2, "capture-sealed-a-"), "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "capture-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    struct DualBlocker final {
        BlockingFilesystemOperation intent{glyphastore::FilesystemOperation::write_compaction_intent};
        BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record, false};

        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<DualBlocker*>(opaque);
            if (auto status = BlockingFilesystemOperation::before(&state.append, operation); !status) {
                return status;
            }
            return BlockingFilesystemOperation::before(&state.intent, operation);
        }
    } blocker;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &DualBlocker::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    {
        const std::string value{"stable"};
        GLYPHA_REQUIRE(
            (*runtime)
                ->put(std::as_bytes(std::span{stable_key}), std::as_bytes(std::span{value}))
                .committed());
    }

    blocker.append.arm();
    glyphastore::DurableMutationResult in_flight;
    std::thread writer{[&] {
        const std::string value{"in-flight"};
        in_flight =
            (*runtime)->put(std::as_bytes(std::span{in_flight_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.append.wait_until_blocked());

    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] { compaction = (*runtime)->compact_worker(0, 0); }};
    // Phase A arms the Index quiesce gate before the depth wait.
    std::this_thread::sleep_for(std::chrono::milliseconds{20});

    bool saw_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = gated_key + std::to_string(attempt);
        const std::string value{"gated"};
        const auto gated =
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (!gated.committed() && gated.error.has_value() &&
            gated.error->code == glyphastore::ErrorCode::sequence_conflict) {
            saw_gate = true;
            break;
        }
        GLYPHA_REQUIRE(!gated.committed());
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(saw_gate);

    // Release mid-append before GET: prepare_get also drains hot_path_depth, so a
    // GET on this thread while depth>0 would deadlock against append.release().
    blocker.append.release();
    writer.join();

    // Under / after Phase A gate: warm key stays readable — never sticky
    // corrupted_data from a torn Index/catalog view (capture shares this mutex).
    const auto visible = (*runtime)->get(stable_key);
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(owned_text(*visible) == "stable");

    blocker.intent.release();
    compactor.join();

    GLYPHA_REQUIRE(in_flight.committed());
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto again = (*runtime)->get(stable_key);
    GLYPHA_REQUIRE(again.has_value());
    GLYPHA_REQUIRE(owned_text(*again) == "stable");
}

GLYPHA_TEST("exclusive Writer prepare_get drains hot_path_depth before Index find") {
    // prepare_get (catalog GET / verify_index) must ExclusiveIndexQuiesce before
    // index.find: exclusive durable_sync mutate elides worker.mutex, so mutex-only
    // GET races Index publish and can sticky fail_closed on a torn pin view.
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
    const auto stable_key = key_for_worker(0, 2, "prepare-get-stable-");
    const auto in_flight_key = key_for_worker(0, 2, "prepare-get-inflight-");
    const auto gated_key = key_for_worker(0, 2, "prepare-get-gated-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, key_for_worker(0, 2, "prepare-get-sealed-a-"), "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "prepare-get-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    // Disarmed until after the stable seed PUT — default-armed would hang that write.
    BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record, false};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &append, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    {
        const std::string value{"stable"};
        GLYPHA_REQUIRE(
            (*runtime)
                ->put(std::as_bytes(std::span{stable_key}), std::as_bytes(std::span{value}))
                .committed());
    }

    append.arm();
    glyphastore::DurableMutationResult in_flight;
    std::thread writer{[&] {
        const std::string value{"in-flight"};
        in_flight =
            (*runtime)->put(std::as_bytes(std::span{in_flight_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(append.wait_until_blocked());

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> visible;
    std::thread reader{[&] { visible.emplace((*runtime)->get(stable_key)); }};
    // prepare_get arms the Index quiesce gate before waiting on depth.
    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    bool saw_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = gated_key + std::to_string(attempt);
        const std::string value{"gated"};
        const auto gated =
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (!gated.committed() && gated.error.has_value() &&
            gated.error->code == glyphastore::ErrorCode::sequence_conflict) {
            saw_gate = true;
            break;
        }
        if (gated.committed()) {
            // Gate not armed yet — keep trying with a fresh key.
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }

    append.release();
    writer.join();
    reader.join();

    GLYPHA_REQUIRE(saw_gate);
    GLYPHA_REQUIRE(in_flight.committed());
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(visible->has_value());
    GLYPHA_REQUIRE(owned_text(**visible) == "stable");
    GLYPHA_REQUIRE((*runtime)->healthy());
}

GLYPHA_TEST("exclusive Writer flush drains hot_path_depth before dirty sync") {
    // flush_dirty_segments must ExclusiveIndexQuiesce before touching cached_file /
    // mutation_io_active: exclusive durable_sync mutate elides worker.mutex, so a
    // mutex+CV-only flush can lose the wakeup or race the stolen Segment handle.
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
    const auto in_flight_key = key_for_worker(0, 2, "flush-inflight-");
    const auto gated_key = key_for_worker(0, 2, "flush-gated-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 1, key_for_worker(0, 2, "flush-sealed-a-"), "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, key_for_worker(0, 2, "flush-sealed-b-"), "second");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        static_cast<void>(create_segment(*directory, store_id, entries[3]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingFilesystemOperation append{glyphastore::FilesystemOperation::write_record};
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &append, .before = &BlockingFilesystemOperation::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableMutationResult in_flight;
    std::thread writer{[&] {
        const std::string value{"in-flight"};
        in_flight =
            (*runtime)->put(std::as_bytes(std::span{in_flight_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(append.wait_until_blocked());

    glyphastore::Status flushed{glyphastore::fail(glyphastore::ErrorCode::internal_error, "unset")};
    std::thread flusher{[&] { flushed = (*runtime)->flush(); }};
    // Flush arms ExclusiveIndexQuiesce before the depth wait.
    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    bool saw_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = gated_key + std::to_string(attempt);
        const std::string value{"gated"};
        const auto gated =
            (*runtime)->put(std::as_bytes(std::span{key}), std::as_bytes(std::span{value}));
        if (!gated.committed() && gated.error.has_value() &&
            gated.error->code == glyphastore::ErrorCode::sequence_conflict) {
            saw_gate = true;
            break;
        }
        if (gated.committed()) {
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }

    append.release();
    writer.join();
    flusher.join();

    GLYPHA_REQUIRE(saw_gate);
    GLYPHA_REQUIRE(in_flight.committed());
    GLYPHA_REQUIRE(flushed.has_value());
    GLYPHA_REQUIRE((*runtime)->healthy());
    const auto written = (*runtime)->get(in_flight_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == "in-flight");
}

GLYPHA_TEST("exclusive Writer with flusher re-locks Worker after rotation I/O") {
    // exclusive_writer + background flusher must re-take worker.mutex after
    // seal/create/publish before clearing mutation_io_active (same predicate as
    // mutate post-append). Skipping the lock races the flusher / a sibling mutate
    // waiting on mutation_io_finished against non-atomic Worker fields.
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
    const auto rotating_key = key_for_worker(0, 2, "relock-rotate-");
    const auto sibling_key = key_for_worker(0, 2, "relock-sibling-");
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        static_cast<void>(create_segment(*directory, store_id, entries[0]));
        static_cast<void>(create_segment(*directory, store_id, entries[1]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
    }

    BlockingRotationSeal blocker;
    blocker.arm();
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(), {.context = &blocker, .before = &BlockingRotationSeal::before});
    GLYPHA_REQUIRE(directory.has_value());
    glyphastore::DurableRuntimeOptions options{};
    options.exclusive_writer = true;
    options.batch = glyphastore::DurableGroupConfig{.max_records = 32,
                                                    .max_bytes = 65'536,
                                                    .max_wait_ms = 50,
                                                    .min_records = 1};
    options.strict_ack = true;
    options.sync_interval_ms = 50;
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    GLYPHA_REQUIRE(runtime.has_value());

    glyphastore::DurableMutationResult rotating;
    std::thread rotator{[&] {
        const std::string value{"rotating"};
        rotating =
            (*runtime)->put(std::as_bytes(std::span{rotating_key}), std::as_bytes(std::span{value}));
    }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    glyphastore::DurableMutationResult sibling;
    std::mutex completion_mutex;
    std::condition_variable completion;
    bool sibling_finished{};
    std::thread sibling_writer{[&] {
        const std::string value{"sibling"};
        sibling =
            (*runtime)->put(std::as_bytes(std::span{sibling_key}), std::as_bytes(std::span{value}));
        {
            const std::lock_guard lock{completion_mutex};
            sibling_finished = true;
        }
        completion.notify_one();
    }};

    blocker.release();
    rotator.join();
    bool sibling_progressed{};
    {
        std::unique_lock lock{completion_mutex};
        sibling_progressed =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return sibling_finished; });
    }
    sibling_writer.join();

    GLYPHA_REQUIRE(sibling_progressed);
    GLYPHA_REQUIRE(rotating.committed() ||
                   (rotating.error.has_value() &&
                    (rotating.error->code == glyphastore::ErrorCode::sequence_conflict ||
                     rotating.error->code == glyphastore::ErrorCode::resource_exhausted)));
    GLYPHA_REQUIRE(sibling.committed() ||
                   (sibling.error.has_value() &&
                    (sibling.error->code == glyphastore::ErrorCode::sequence_conflict ||
                     sibling.error->code == glyphastore::ErrorCode::resource_exhausted)));
    GLYPHA_REQUIRE(rotating.committed() || sibling.committed());
    GLYPHA_REQUIRE((*runtime)->healthy());
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
