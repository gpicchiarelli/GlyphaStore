#include "glyphastore/store/paired/read_generation.hpp"
#include "store/paired/read_generation_impl.hpp"

namespace glyphastore::store::paired {

auto PairReadGeneration::publish_incremental_direct(const PairReadGeneration& previous,
                                                    const std::span<const ReadMutation> mutations,
                                                    GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> {
    const PairReadGeneration* direct_result{};
    auto built = publish_incremental_construct(previous, {}, mutations, nullptr, {}, nullptr, &storage,
                                               &direct_result);
    if (!built) {
        return unexpected(std::move(built.error()));
    }
    if (direct_result == nullptr) {
        return fail(ErrorCode::internal_error, "direct generation construction returned no object");
    }
    return direct_result;
}

auto PairReadGeneration::empty_direct(const WorkerRoutingState routing, GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> try {
    static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
    static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
    auto base = std::make_shared<const ImmutableReadIndex>();
    auto delta = make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries);
    auto* location =
        storage.claim(sizeof(PairReadGenerationEnableShared), alignof(PairReadGenerationEnableShared));
    try {
        return std::construct_at(static_cast<PairReadGenerationEnableShared*>(location), routing,
                                 std::move(base), std::move(delta), 0, 0);
    } catch (...) {
        storage.release(location);
        throw;
    }
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "direct empty generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "direct empty generation construction failed");
}

auto PairReadGeneration::from_durable_snapshot_direct(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records,
    GenerationDirectStorage& storage) -> Result<const PairReadGeneration*> {
    return build_durable_snapshot_direct(routing, records, 0, 0, storage);
}

auto PairReadGeneration::replace_durable_snapshot_direct(
    const PairReadGeneration& previous,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records,
    GenerationDirectStorage& storage) -> Result<const PairReadGeneration*> {
    if (previous.epoch_ >= GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    return build_durable_snapshot_direct(previous.routing_, records, previous.epoch_ + 1U,
                                         previous.visible_through_, storage);
}

auto PairReadGeneration::build_durable_snapshot_direct(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records, const std::uint64_t epoch,
    const std::uint64_t visible_floor, GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> try {
    if (epoch > GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
    static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
    auto visible_through = visible_floor;
    for (const auto& record : records) {
        visible_through = std::max(visible_through, record.reference().sequence.value);
    }
    auto base = std::make_shared<const ImmutableReadIndex>(records);
    auto delta = make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries);
    auto* location =
        storage.claim(sizeof(PairReadGenerationEnableShared), alignof(PairReadGenerationEnableShared));
    try {
        return std::construct_at(static_cast<PairReadGenerationEnableShared*>(location), routing,
                                 std::move(base), std::move(delta), epoch, visible_through);
    } catch (...) {
        storage.release(location);
        throw;
    }
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "direct durable read generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "direct durable read generation construction failed");
}

void PairReadGeneration::destroy_direct(const PairReadGeneration* generation,
                                        GenerationDirectStorage& storage) noexcept {
    if (generation == nullptr) {
        return;
    }
    auto* concrete = const_cast<PairReadGenerationEnableShared*>(
        static_cast<const PairReadGenerationEnableShared*>(generation));
    std::destroy_at(concrete);
    storage.release(concrete);
}

} // namespace glyphastore::store::paired

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental(
    std::shared_ptr<const store::paired::PairReadGeneration> previous,
    const std::span<const store::paired::ReadMutation> mutations,
    std::shared_ptr<PairReadGenerationShellStorage> storage, store::paired::PairReadMerge* merge)
    -> Result<std::shared_ptr<const store::paired::PairReadGeneration>> {
    return store::paired::PairReadGeneration::publish_incremental_in_shell(std::move(previous), mutations,
                                                                           merge, std::move(storage));
}

auto glyphastore::experimental::PairReadGenerationShellAccess::empty_direct(
    const WorkerRoutingState routing, PairReadGenerationDirectStorage& storage)
    -> Result<const store::paired::PairReadGeneration*> {
    return store::paired::PairReadGeneration::empty_direct(routing, storage);
}

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental_borrowed(
    std::shared_ptr<const store::paired::PairReadGeneration> previous,
    const std::span<const store::paired::ReadMutation> mutations,
    PairReadGenerationInlineShellStorage& storage)
    -> Result<std::shared_ptr<const store::paired::PairReadGeneration>> {
    return store::paired::PairReadGeneration::publish_incremental_in_borrowed_shell(std::move(previous),
                                                                                    mutations, storage);
}

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental_direct(
    const store::paired::PairReadGeneration& previous,
    const std::span<const store::paired::ReadMutation> mutations, PairReadGenerationDirectStorage& storage)
    -> Result<const store::paired::PairReadGeneration*> {
    return store::paired::PairReadGeneration::publish_incremental_direct(previous, mutations, storage);
}

void glyphastore::experimental::PairReadGenerationShellAccess::destroy_direct(
    const store::paired::PairReadGeneration* generation, PairReadGenerationDirectStorage& storage) noexcept {
    store::paired::PairReadGeneration::destroy_direct(generation, storage);
}
