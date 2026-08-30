#include "persistence_recovery_test_support.hpp"

#include <cstddef>
#include <utility>
#include <vector>

GLYPHA_TEST("online compaction enumerates every reached filesystem failure point") {
    constexpr std::size_t kMaximumEnumeratedFailurePoints = 64U;
    bool reached_successful_terminal_iteration{};
    std::size_t exercised_failure_points{};

    for (std::size_t failure_index = 1U; failure_index <= kMaximumEnumeratedFailurePoints; ++failure_index) {
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
            append_record(first, 1, "enumerated-a", "alpha");
            GLYPHA_REQUIRE(first.seal().committed());
            auto second = create_segment(*directory, store_id, entries[1]);
            append_record(second, 2, "enumerated-b", "beta");
            GLYPHA_REQUIRE(second.seal().committed());
            static_cast<void>(create_segment(*directory, store_id, entries[2]));
            GLYPHA_REQUIRE(directory->publish_manifest(recovery_manifest(store_id, 1, entries)).durable());
        }

        NthFilesystemFailure failure{.target_occurrence = failure_index};
        auto directory = glyphastore::DataDirectory::open_and_lock(
            temporary.path(),
            glyphastore::FilesystemHooks{.context = &failure, .before = &NthFilesystemFailure::before});
        GLYPHA_REQUIRE(directory.has_value());
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory));
        GLYPHA_REQUIRE(runtime.has_value());
        const auto result = (*runtime)->compact_worker(0, 0);

        if (!failure.fired_operation) {
            GLYPHA_REQUIRE(result.compacted());
            GLYPHA_REQUIRE(failure_index == failure.occurrences + 1U);
            reached_successful_terminal_iteration = true;
            runtime->reset();
            break;
        }

        ++exercised_failure_points;
        GLYPHA_REQUIRE(failure.occurrences == failure_index);
        GLYPHA_REQUIRE(!result.compacted());
        GLYPHA_REQUIRE(result.error.has_value());
        GLYPHA_REQUIRE(result.error->code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE(result.outcome == glyphastore::DurableCompactionOutcome::not_compacted ||
                       result.outcome == glyphastore::DurableCompactionOutcome::recovery_required);
        runtime->reset();

        // Reopen is the persisted-state oracle: it must select one clean
        // authority, release every temporary authority/descriptor, and retain
        // both logical records regardless of the failed boundary.
        auto reopened = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
        GLYPHA_REQUIRE(reopened.has_value());
        GLYPHA_REQUIRE((*reopened)->healthy());
        GLYPHA_REQUIRE((*reopened)->namespace_audit().clean());
        const auto first = (*reopened)->get("enumerated-a");
        const auto second = (*reopened)->get("enumerated-b");
        GLYPHA_REQUIRE(first.has_value());
        GLYPHA_REQUIRE(second.has_value());
        GLYPHA_REQUIRE(owned_text(*first) == "alpha");
        GLYPHA_REQUIRE(owned_text(*second) == "beta");
    }

    GLYPHA_REQUIRE(reached_successful_terminal_iteration);
    GLYPHA_REQUIRE(exercised_failure_points > 0U);
}
