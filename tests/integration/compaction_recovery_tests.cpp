#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "test.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class CompactionRecoveryDirectory final {
  public:
    CompactionRecoveryDirectory() {
        auto pattern =
            (std::filesystem::temp_directory_path() / "glyphastore-compaction-recovery-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~CompactionRecoveryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

struct OneShotRecoveryFailure {
    glyphastore::FilesystemOperation target{};
    std::size_t fail_on_matching_call{1};
    std::size_t matching_calls{};
    bool fired{};

    static auto before(void* context, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& failure = *static_cast<OneShotRecoveryFailure*>(context);
        if (operation != failure.target) {
            return {};
        }
        ++failure.matching_calls;
        if (failure.matching_calls != failure.fail_on_matching_call) {
            return {};
        }
        failure.fired = true;
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected compaction recovery failure");
    }
};

auto compaction_manifests() -> std::pair<glyphastore::Manifest, glyphastore::Manifest> {
    glyphastore::Manifest old{
        .store_id = {std::byte{0x71}, std::byte{0x72}, std::byte{0x73}},
        .manifest_generation = 11,
        .worker_count = 1,
        .routing_epoch = 1,
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
                 .role = glyphastore::ManifestSegmentRole::sealed},
                {.segment_id = glyphastore::SegmentId{3},
                 .generation = glyphastore::GenerationId{1},
                 .owner_worker = glyphastore::WorkerId{0},
                 .role = glyphastore::ManifestSegmentRole::active},
            },
    };
    auto next = old;
    ++next.manifest_generation;
    next.segments = {
        {.segment_id = glyphastore::SegmentId{1},
         .generation = glyphastore::GenerationId{2},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        old.segments.back(),
    };
    return {std::move(old), std::move(next)};
}

auto multi_output_compaction_manifests() -> std::pair<glyphastore::Manifest, glyphastore::Manifest> {
    glyphastore::Manifest old{
        .store_id = {std::byte{0x81}, std::byte{0x82}, std::byte{0x83}},
        .manifest_generation = 31,
        .worker_count = 1,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{5},
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
    auto next = old;
    ++next.manifest_generation;
    next.segments = {
        {.segment_id = glyphastore::SegmentId{1},
         .generation = glyphastore::GenerationId{2},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        {.segment_id = glyphastore::SegmentId{2},
         .generation = glyphastore::GenerationId{2},
         .owner_worker = glyphastore::WorkerId{0},
         .role = glyphastore::ManifestSegmentRole::sealed},
        old.segments.back(),
    };
    return {std::move(old), std::move(next)};
}

auto identity(const glyphastore::Manifest& manifest, const glyphastore::ManifestSegmentEntry& entry)
    -> glyphastore::SegmentHeaderIdentity {
    return {.store_id = manifest.store_id,
            .segment_id = entry.segment_id,
            .generation = entry.generation,
            .owner_worker = entry.owner_worker};
}

void create_segment(glyphastore::DataDirectory& directory, const glyphastore::Manifest& manifest,
                    const glyphastore::ManifestSegmentEntry& entry) {
    auto created = glyphastore::DurableSegmentFile::create(directory, identity(manifest, entry));
    GLYPHA_REQUIRE(created.durable());
    if (entry.role == glyphastore::ManifestSegmentRole::sealed) {
        const auto sealed = created.file->seal();
        GLYPHA_REQUIRE(sealed.committed());
    }
}

void prepare_interrupted_compaction(const std::filesystem::path& path, const glyphastore::Manifest& old,
                                    const glyphastore::Manifest& next, const bool publish_next) {
    auto directory = glyphastore::DataDirectory::open_and_lock(path);
    GLYPHA_REQUIRE(directory.has_value());
    GLYPHA_REQUIRE(directory->publish_manifest(old).durable());
    for (const auto& entry : old.segments) {
        create_segment(*directory, old, entry);
    }
    const glyphastore::DurableCompactionIntent intent{
        .worker_id = glyphastore::WorkerId{0}, .old_manifest = old, .next_manifest = next};
    GLYPHA_REQUIRE(directory->publish_compaction_intent(intent).durable());
    for (const auto& entry : next.segments) {
        if (std::find(old.segments.begin(), old.segments.end(), entry) == old.segments.end()) {
            create_segment(*directory, next, entry);
        }
    }
    if (publish_next) {
        GLYPHA_REQUIRE(directory->publish_manifest(next).durable());
    }
}

void prepare_interrupted_compaction(const std::filesystem::path& path, const bool publish_next) {
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(path, old, next, publish_next);
}

auto segment_path(const std::filesystem::path& directory, const glyphastore::Manifest& manifest,
                  const glyphastore::ManifestSegmentEntry& entry) -> std::filesystem::path {
    return directory / glyphastore::segment_filename(identity(manifest, entry));
}

auto segment_temporary_path(const std::filesystem::path& directory, const glyphastore::Manifest& manifest,
                            const glyphastore::ManifestSegmentEntry& entry) -> std::filesystem::path {
    return directory / ('.' + glyphastore::segment_filename(identity(manifest, entry)) + ".tmp");
}

} // namespace

GLYPHA_TEST("compaction recovery rolls back exact replacements when the old manifest wins") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), false);

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest() == old);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().recovery_safe());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));
}

GLYPHA_TEST("compaction recovery retires exact sources when the next manifest wins") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), true);

    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(runtime.has_value());
    GLYPHA_REQUIRE((*runtime)->manifest() == next);
    GLYPHA_REQUIRE((*runtime)->namespace_audit().recovery_safe());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
}

GLYPHA_TEST("multi-output compaction rollback resumes after a partial replacement unlink") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = multi_output_compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), old, next, false);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::remove_compaction_segment,
                                   .fail_on_matching_call = 2};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments[1])));
    for (std::size_t index = 0; index < 3; ++index) {
        GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[index])));
    }

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == old);
    GLYPHA_REQUIRE((*recovered)->namespace_audit().clean());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments[1])));
}

GLYPHA_TEST("multi-output compaction rollback removes a partially created second replacement") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = multi_output_compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), old, next, false);
    const auto second_final = segment_path(temporary.path(), next, next.segments[1]);
    const auto second_temporary = segment_temporary_path(temporary.path(), next, next.segments[1]);
    std::filesystem::rename(second_final, second_temporary);

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == old);
    GLYPHA_REQUIRE((*recovered)->namespace_audit().clean());
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(second_final));
    GLYPHA_REQUIRE(!std::filesystem::exists(second_temporary));
}

GLYPHA_TEST("multi-output compaction retirement resumes after a partial source unlink") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = multi_output_compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), old, next, true);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::remove_compaction_segment,
                                   .fail_on_matching_call = 3};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[2])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == next);
    GLYPHA_REQUIRE((*recovered)->namespace_audit().clean());
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    for (std::size_t index = 0; index < 3; ++index) {
        GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[index])));
    }
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments[1])));
}

GLYPHA_TEST("compaction recovery rejects a manifest outside both intent authorities") {
    CompactionRecoveryDirectory temporary;
    auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), false);
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        old.manifest_generation = next.manifest_generation + 1U;
        GLYPHA_REQUIRE(directory->publish_manifest(old).durable());
    }
    const auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(!runtime.has_value());
    GLYPHA_REQUIRE(runtime.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("compaction recovery rejects an unrelated unlisted Segment") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), false);
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const glyphastore::ManifestSegmentEntry unrelated{
            .segment_id = glyphastore::SegmentId{4},
            .generation = glyphastore::GenerationId{1},
            .owner_worker = glyphastore::WorkerId{0},
            .role = glyphastore::ManifestSegmentRole::sealed,
        };
        create_segment(*directory, old, unrelated);
    }

    const auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(!runtime.has_value());
    GLYPHA_REQUIRE(runtime.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
}

GLYPHA_TEST("compaction recovery preserves all files when retirement fails before its first unlink") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), false);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::remove_compaction_segment};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == old);
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("compaction recovery resumes an indeterminate partially retired source set") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), true);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::remove_compaction_segment,
                                   .fail_on_matching_call = 2};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == next);
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("compaction recovery resumes after retirement directory sync is indeterminate") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), true);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::sync_directory};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == next);
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("compaction recovery resumes when intent removal fails before unlink") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), true);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::remove_compaction_intent};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == next);
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
}

GLYPHA_TEST("compaction recovery reopens ordinarily after intent removal sync is indeterminate") {
    CompactionRecoveryDirectory temporary;
    const auto [old, next] = compaction_manifests();
    prepare_interrupted_compaction(temporary.path(), true);
    OneShotRecoveryFailure failure{.target = glyphastore::FilesystemOperation::sync_directory,
                                   .fail_on_matching_call = 2};

    const auto interrupted = glyphastore::DurableRuntimeCatalog::open_existing(
        temporary.path(), 0, {.context = &failure, .before = &OneShotRecoveryFailure::before});
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(!std::filesystem::exists(temporary.path() / glyphastore::kCompactionIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[0])));
    GLYPHA_REQUIRE(!std::filesystem::exists(segment_path(temporary.path(), old, old.segments[1])));

    auto recovered = glyphastore::DurableRuntimeCatalog::open_existing(temporary.path());
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->manifest() == next);
    GLYPHA_REQUIRE((*recovered)->namespace_audit().recovery_safe());
    GLYPHA_REQUIRE(std::filesystem::exists(segment_path(temporary.path(), next, next.segments.front())));
}
