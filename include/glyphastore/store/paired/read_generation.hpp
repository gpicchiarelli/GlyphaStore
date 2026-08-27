#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment.hpp"
#include "glyphastore/store/value.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace glyphastore::experimental {
struct PairReadGenerationShellAccess;
class PairReadGenerationShellStorage;
class PairReadGenerationInlineShellStorage;
class PairReadGenerationDirectStorage;
} // namespace glyphastore::experimental

namespace glyphastore::store::paired {

// A Writer-owned mutation which has already been linearized in the Store.
// The SegmentPtr is the generation pin: RecordRef must never escape alone.
struct ReadMutation final {
    HashedKey key{};
    RecordRef record{};
    SegmentPtr segment{};
    std::optional<DurableRuntimeCatalog::PublishedReadRecord> durable{};
    Opcode opcode{Opcode::put};
};

class ImmutableReadIndex;
class DeltaState;
class PairReadMerge;

// Immutable two-level read view published by one paired Writer and adopted by
// its Reader. GET performs at most one delta lookup and one base lookup.
// Not marked final: a private EnableShared derived type exists solely so
// make_shared can co-allocate the object and control block.
class PairReadGeneration {
  public:
    static constexpr std::size_t kMaximumIncrementalDeltaEntries = 40'960;

    [[nodiscard]] static auto empty(WorkerRoutingState routing)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    [[nodiscard]] static auto
    from_durable_snapshot(WorkerRoutingState routing,
                          std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    // Rebuilds the durable base from one catalog-consistent snapshot while
    // preserving the logical visibility watermark of erased records.
    [[nodiscard]] static auto
    replace_durable_snapshot(std::shared_ptr<const PairReadGeneration> previous,
                             std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    [[nodiscard]] static auto publish(std::shared_ptr<const PairReadGeneration> previous,
                                      std::span<const ReadMutation> mutations,
                                      std::size_t merge_delta_entries)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    // Production publication path. While merge is non-null the same immutable
    // mutation handles are also appended to its bounded post-cut delta.
    [[nodiscard]] static auto publish_incremental(std::shared_ptr<const PairReadGeneration> previous,
                                                  std::span<const ReadMutation> mutations,
                                                  PairReadMerge* merge = nullptr)
        -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto start_incremental_merge(std::shared_ptr<const PairReadGeneration> cut,
                                                      std::size_t maximum_post_entries)
        -> Result<std::unique_ptr<PairReadMerge>>;
    [[nodiscard]] static auto advance_incremental_merge(PairReadMerge& merge, std::size_t maximum_slots)
        -> Result<std::size_t>;
    [[nodiscard]] static auto finish_incremental_merge(std::shared_ptr<const PairReadGeneration> current,
                                                       PairReadMerge& merge)
        -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto merge_ready(const PairReadMerge& merge) noexcept -> bool;
    [[nodiscard]] static auto merge_post_entries(const PairReadMerge& merge) noexcept -> std::size_t;
    [[nodiscard]] static auto can_publish_incremental(const PairReadGeneration& current,
                                                      const PairReadMerge* merge,
                                                      std::size_t maximum_new_entries) noexcept -> bool;

    [[nodiscard]] auto get(const HashedKey& key, std::uint64_t now_ns) const -> Result<OwnedValue>;
    [[nodiscard]] auto prepare_durable(const HashedKey& key) const
        -> Result<DurableRuntimeCatalog::PublishedReadView>;

    [[nodiscard]] auto epoch() const noexcept -> std::uint64_t {
        return epoch_;
    }
    [[nodiscard]] auto visible_through() const noexcept -> std::uint64_t {
        return visible_through_;
    }
    [[nodiscard]] auto delta_entries() const noexcept -> std::size_t;
    [[nodiscard]] auto delta_record_versions() const noexcept -> std::size_t;
    [[nodiscard]] auto delta_arena_record_bytes() const noexcept -> std::size_t;
    [[nodiscard]] auto delta_arena_key_bytes() const noexcept -> std::size_t;
    [[nodiscard]] auto delta_arena_key_storage_bytes() const noexcept -> std::size_t;
    [[nodiscard]] auto base_entries() const noexcept -> std::size_t;

  private:
    [[nodiscard]] static auto
    build_durable_snapshot(WorkerRoutingState routing,
                           std::span<const DurableRuntimeCatalog::PublishedReadRecord> records,
                           std::uint64_t epoch, std::uint64_t visible_floor)
        -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto
    publish_incremental_in_shell(std::shared_ptr<const PairReadGeneration> previous,
                                 std::span<const ReadMutation> mutations, PairReadMerge* merge,
                                 std::shared_ptr<experimental::PairReadGenerationShellStorage> storage)
        -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto
    publish_incremental_in_borrowed_shell(std::shared_ptr<const PairReadGeneration> previous,
                                          std::span<const ReadMutation> mutations,
                                          experimental::PairReadGenerationInlineShellStorage& storage)
        -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto publish_incremental_construct(
        const PairReadGeneration& previous, std::shared_ptr<const PairReadGeneration> previous_owner,
        std::span<const ReadMutation> mutations, PairReadMerge* merge,
        std::shared_ptr<experimental::PairReadGenerationShellStorage> owned_storage,
        experimental::PairReadGenerationInlineShellStorage* borrowed_storage,
        experimental::PairReadGenerationDirectStorage* direct_storage,
        const PairReadGeneration** direct_result) -> Result<std::shared_ptr<const PairReadGeneration>>;
    [[nodiscard]] static auto
    publish_incremental_direct(const PairReadGeneration& previous, std::span<const ReadMutation> mutations,
                               experimental::PairReadGenerationDirectStorage& storage)
        -> Result<const PairReadGeneration*>;
    [[nodiscard]] static auto empty_direct(WorkerRoutingState routing,
                                           experimental::PairReadGenerationDirectStorage& storage)
        -> Result<const PairReadGeneration*>;
    // Generation + embedded DeltaState are co-allocated in the .cpp via a private
    // derived helper. delta_ points into that storage for the object's lifetime.
    PairReadGeneration(WorkerRoutingState routing, std::shared_ptr<const ImmutableReadIndex> base,
                       const DeltaState* delta, std::uint64_t epoch, std::uint64_t visible_through) noexcept;
    void bind_delta(const DeltaState* delta) noexcept {
        delta_ = delta;
    }

    WorkerRoutingState routing_{};
    std::shared_ptr<const ImmutableReadIndex> base_;
    const DeltaState* delta_{};
    std::uint64_t epoch_{};
    std::uint64_t visible_through_{};

    // Allow the .cpp co-allocation helper to construct and bind embedded delta storage.
    friend struct PairReadGenerationEnableShared;
    friend struct experimental::PairReadGenerationShellAccess;
};

// Opaque Writer-owned state. It is never published to or touched by Reader.
class PairReadMerge final {
  public:
    ~PairReadMerge();
    PairReadMerge(const PairReadMerge&) = delete;
    auto operator=(const PairReadMerge&) -> PairReadMerge& = delete;
    PairReadMerge(PairReadMerge&&) noexcept;
    auto operator=(PairReadMerge&&) noexcept -> PairReadMerge&;

  private:
    struct State;
    explicit PairReadMerge(std::unique_ptr<State> state) noexcept;
    std::unique_ptr<State> state_;

    friend class PairReadGeneration;
};

} // namespace glyphastore::store::paired
