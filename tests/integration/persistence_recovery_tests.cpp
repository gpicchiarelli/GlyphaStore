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

GLYPHA_TEST("durable recovery rebuilds partitioned visibility and Worker sequences") {
    RecoveryTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{3},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const auto alpha = key_for_worker(0, 2, "alpha");
    const auto gone = key_for_worker(0, 2, "gone");
    const auto expired = key_for_worker(0, 2, "expired");
    const std::string binary_prefix{"binary\0key", 10};
    const auto binary = key_for_worker(0, 2, binary_prefix);
    const auto beta = key_for_worker(1, 2, "beta");

    auto first = create_segment(*directory, store_id, entries[0]);
    append_record(first, 1, expired, "older-visible");
    append_record(first, 2, gone, "present");
    append_record(first, 3, gone, {}, glyphastore::Opcode::erase);
    append_record(first, 4, alpha, "old");
    append_record(first, 5, binary, "binary-value");
    GLYPHA_REQUIRE(first.seal().committed());

    auto second = create_segment(*directory, store_id, entries[1]);
    append_record(second, 6, alpha, "new");
    append_record(second, 7, expired, "stale", glyphastore::Opcode::put, 100);

    auto third = create_segment(*directory, store_id, entries[2]);
    append_record(third, 9, beta, "visible");
    GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());

    const auto recovered = glyphastore::recover_durable_state(*directory, 101);
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE(recovered->segments.size() == 3);
    GLYPHA_REQUIRE(recovered->workers.size() == 2);
    GLYPHA_REQUIRE(recovered->stats.segments_scanned == 3);
    GLYPHA_REQUIRE(recovered->stats.rebuild.records_scanned == 8);
    GLYPHA_REQUIRE(recovered->stats.rebuild.records_visible == 3);
    GLYPHA_REQUIRE(recovered->stats.rebuild.tombstones == 1);
    GLYPHA_REQUIRE(recovered->stats.rebuild.expired == 1);
    GLYPHA_REQUIRE(recovered->stats.workers_requiring_rotation == 0);

    const auto alpha_ref = recovered->workers[0].index.find(alpha);
    GLYPHA_REQUIRE(alpha_ref.has_value());
    GLYPHA_REQUIRE(alpha_ref->segment_id.value == 2);
    GLYPHA_REQUIRE(alpha_ref->sequence.value == 6);
    GLYPHA_REQUIRE(!recovered->workers[0].index.find(gone).has_value());
    GLYPHA_REQUIRE(!recovered->workers[0].index.find(expired).has_value());
    GLYPHA_REQUIRE(recovered->workers[0].index.find(binary).has_value());
    GLYPHA_REQUIRE(recovered->workers[0].next_sequence.value == 8);
    GLYPHA_REQUIRE(recovered->workers[0].active_segment.value == 2);

    const auto beta_ref = recovered->workers[1].index.find(beta);
    GLYPHA_REQUIRE(beta_ref.has_value());
    GLYPHA_REQUIRE(beta_ref->sequence.value == 9);
    GLYPHA_REQUIRE(recovered->workers[1].next_sequence.value == 10);
    GLYPHA_REQUIRE(recovered->workers[1].active_segment.value == 3);
}

GLYPHA_TEST("recovery reports crash temporaries but rejects unlisted Segments without adoption") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.identity().segment_id == active.segment_id);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        create_private_file(temporary.path() / glyphastore::kManifestTemporaryFilename);
        create_private_file(temporary.path() /
                            ('.' + glyphastore::segment_filename(segment.identity()) + ".tmp"));

        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(recovered.has_value());
        GLYPHA_REQUIRE(recovered->namespace_audit.issues.size() == 2);
        GLYPHA_REQUIRE(recovered->namespace_audit.recovery_safe());
        GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kManifestTemporaryFilename));
    }
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto segment = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(segment.identity().generation == active.generation);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const glyphastore::SegmentHeaderIdentity orphan{
            .store_id = store_id,
            .segment_id = glyphastore::SegmentId{2},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
        };
        create_private_file(temporary.path() / glyphastore::segment_filename(orphan));

        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
        GLYPHA_REQUIRE(recovered.error().message.find("unlisted Segment") != std::string::npos);
        GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::segment_filename(orphan)));
    }
}

GLYPHA_TEST("recovery accepts only the documented sealed-active rotation transition") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        GLYPHA_REQUIRE(file.seal().committed());
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(recovered.has_value());
        GLYPHA_REQUIRE(recovered->workers[0].active_requires_rotation);
        GLYPHA_REQUIRE(recovered->stats.workers_requiring_rotation == 1);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        auto incorrectly_active = create_segment(*directory, store_id, entries[0]);
        auto active = create_segment(*directory, store_id, entries[1]);
        static_cast<void>(incorrectly_active);
        static_cast<void>(active);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery rejects missing and identity-mismatched manifest Segments") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto file_store_id = recovery_store_id();
        const auto manifest_store_id = recovery_store_id(std::byte{0x99});
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, file_store_id, active);
        static_cast<void>(file);
        GLYPHA_REQUIRE(
            directory->publish_manifest(recovery_manifest(manifest_store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery validates persisted key hashes and Worker routing") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        const auto key = key_for_worker(0, 1, "hash");
        append_record(file, 1, key, "value", glyphastore::Opcode::put, 0, glyphastore::hash_key(key) ^ 1U);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
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
        auto wrong_owner = create_segment(*directory, store_id, entries[0]);
        auto other_active = create_segment(*directory, store_id, entries[1]);
        const auto key = key_for_worker(1, 2, "wrong-owner");
        append_record(wrong_owner, 1, key, "value");
        static_cast<void>(other_active);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 2, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}

GLYPHA_TEST("recovery rejects equal winning sequences and exhausted Worker sequence space") {
    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const std::vector entries{
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::sealed},
            glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                              .generation = glyphastore::GenerationId{1},
                                              .owner_worker = glyphastore::WorkerId{0},
                                              .role = glyphastore::ManifestSegmentRole::active},
        };
        const auto key = key_for_worker(0, 1, "duplicate");
        auto first = create_segment(*directory, store_id, entries[0]);
        append_record(first, 5, key, "first");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 5, key, "second");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    }

    {
        RecoveryTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto store_id = recovery_store_id();
        const glyphastore::ManifestSegmentEntry active{
            .segment_id = glyphastore::SegmentId{1},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::active,
        };
        auto file = create_segment(*directory, store_id, active);
        append_record(file, std::numeric_limits<std::uint64_t>::max(), "last", "value");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
        const auto recovered = glyphastore::recover_durable_state(*directory);
        GLYPHA_REQUIRE(!recovered.has_value());
        GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::arithmetic_overflow);
    }
}

GLYPHA_TEST("recovery rejects overlapping sequence ranges across Worker Segments") {
    RecoveryTemporaryDirectory temporary;
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    const auto store_id = recovery_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::sealed},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    auto first = create_segment(*directory, store_id, entries[0]);
    append_record(first, 10, "first", "value");
    GLYPHA_REQUIRE(first.seal().committed());
    auto second = create_segment(*directory, store_id, entries[1]);
    append_record(second, 9, "different-key", "value");
    GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());

    const auto recovered = glyphastore::recover_durable_state(*directory);
    GLYPHA_REQUIRE(!recovered.has_value());
    GLYPHA_REQUIRE(recovered.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(recovered.error().message.find("overlaps or reverses") != std::string::npos);
}

GLYPHA_TEST("durable runtime materializes recovered Indexes with bounded concurrent reads") {
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
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const std::string binary_key{"bin\0key", 7};
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        auto sealed = create_segment(*directory, store_id, entries[0]);
        append_record(sealed, 1, "alpha", "one");
        append_record(sealed, 2, binary_key, "binary");
        GLYPHA_REQUIRE(sealed.seal().committed());
        auto active = create_segment(*directory, store_id, entries[1]);
        append_record(active, 3, "beta", "two");
        append_record(active, 4, "expired", "old", glyphastore::Opcode::put, 100);
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->healthy());
    GLYPHA_REQUIRE((*runtime)->worker_count() == 1);
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().clean());
    GLYPHA_REQUIRE((*runtime)->next_sequence(0).has_value());
    GLYPHA_REQUIRE((*runtime)->next_sequence(0)->value == 5);
    GLYPHA_REQUIRE((*runtime)->active_segment(0)->value == 2);

    const auto alpha = (*runtime)->get("alpha");
    const auto beta = (*runtime)->get("beta");
    const auto binary = (*runtime)->get(binary_key);
    GLYPHA_REQUIRE(alpha.has_value());
    GLYPHA_REQUIRE(beta.has_value());
    GLYPHA_REQUIRE(binary.has_value());
    GLYPHA_REQUIRE(owned_text(*alpha) == "one");
    GLYPHA_REQUIRE(owned_text(*beta) == "two");
    GLYPHA_REQUIRE(owned_text(*binary) == "binary");
    const auto expired = (*runtime)->get("expired", 100);
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);

    std::atomic_bool failed{};
    std::vector<std::thread> readers;
    for (std::size_t thread = 0; thread < 8; ++thread) {
        readers.emplace_back([&, thread] {
            for (std::size_t iteration = 0; iteration < 32; ++iteration) {
                const bool choose_alpha = (thread + iteration) % 2 == 0;
                const auto value = (*runtime)->get(choose_alpha ? "alpha" : "beta");
                if (!value || owned_text(*value) != (choose_alpha ? "one" : "two")) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE((*runtime)->healthy());

    const auto locked_again = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(!locked_again.has_value());
}

GLYPHA_TEST("blocked durable cold read does not block a mutation on the same Worker") {
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
        append_record(segment, 1, "cold", "value");
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, {active})).durable());
    }

    BlockingRecordRead blocked_read;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_read,
            .file_io = {.context = &blocked_read, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_read.arm();

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read_result;
    std::thread reader{[&] { read_result.emplace((*runtime)->get("cold")); }};
    GLYPHA_REQUIRE(blocked_read.wait_until_blocked());

    std::mutex completion_mutex;
    std::condition_variable completion;
    bool mutation_finished{};
    glyphastore::DurableMutationResult mutation;
    const std::string other_key{"other"};
    const std::string other_value{"new-value"};
    std::thread writer{[&] {
        mutation =
            (*runtime)->put(std::as_bytes(std::span{other_key}), std::as_bytes(std::span{other_value}));
        {
            const std::lock_guard lock{completion_mutex};
            mutation_finished = true;
        }
        completion.notify_one();
    }};

    bool mutation_completed_while_read_blocked{};
    {
        std::unique_lock lock{completion_mutex};
        mutation_completed_while_read_blocked =
            completion.wait_for(lock, std::chrono::seconds{2}, [&] { return mutation_finished; });
    }
    blocked_read.release();
    writer.join();
    reader.join();

    GLYPHA_REQUIRE(mutation_completed_while_read_blocked);
    GLYPHA_REQUIRE(mutation.committed());
    GLYPHA_REQUIRE(read_result.has_value());
    GLYPHA_REQUIRE(read_result->has_value());
    GLYPHA_REQUIRE(owned_text(**read_result) == "value");
    const auto written = (*runtime)->get(other_key);
    GLYPHA_REQUIRE(written.has_value());
    GLYPHA_REQUIRE(owned_text(*written) == other_value);
}

GLYPHA_TEST("durable cold read pin survives concurrent source retirement and relinearizes") {
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
        append_record(first, 1, "cold", "value");
        GLYPHA_REQUIRE(first.seal().committed());
        auto second = create_segment(*directory, store_id, entries[1]);
        append_record(second, 2, "second", "record");
        GLYPHA_REQUIRE(second.seal().committed());
        static_cast<void>(create_segment(*directory, store_id, entries[2]));
        GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
    }

    BlockingRecordRead blocked_read;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{
            .context = &blocked_read,
            .file_io = {.context = &blocked_read, .read_some_at = &BlockingRecordRead::read_some_at}});
    GLYPHA_REQUIRE(directory.has_value());
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
    GLYPHA_REQUIRE(runtime.has_value());
    blocked_read.arm();

    std::optional<glyphastore::Result<glyphastore::OwnedValue>> read_result;
    std::thread reader{[&] { read_result.emplace((*runtime)->get("cold")); }};
    GLYPHA_REQUIRE(blocked_read.wait_until_blocked());

    std::mutex completion_mutex;
    std::condition_variable completion;
    bool compaction_finished{};
    glyphastore::DurableCompactionResult compaction;
    std::thread compactor{[&] {
        compaction = (*runtime)->compact_worker(0, 0);
        {
            const std::lock_guard lock{completion_mutex};
            compaction_finished = true;
        }
        completion.notify_one();
    }};
    bool retired_while_read_blocked{};
    {
        std::unique_lock lock{completion_mutex};
        retired_while_read_blocked =
            completion.wait_for(lock, std::chrono::seconds{5}, [&] { return compaction_finished; });
    }
    blocked_read.release();
    compactor.join();
    reader.join();

    GLYPHA_REQUIRE(retired_while_read_blocked);
    GLYPHA_REQUIRE(compaction.compacted());
    GLYPHA_REQUIRE(read_result.has_value());
    GLYPHA_REQUIRE(read_result->has_value());
    GLYPHA_REQUIRE(owned_text(**read_result) == "value");
    GLYPHA_REQUIRE((*runtime)->manifest().segments.size() == 2);
}
