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

namespace glyphastore::server {

// A Writer-owned mutation which has already been linearized in the Store.
// The SegmentPtr is the generation pin: RecordRef must never escape alone.
struct ReadMutation final {
    HashedKey key;
    RecordRef record;
    SegmentPtr segment;
    std::optional<DurableRuntimeCatalog::PublishedReadRecord> durable;
    Opcode opcode{Opcode::put};
};

class ImmutableReadIndex;
class DeltaState;

// Immutable two-level read view published by one paired Writer and adopted by
// its Reader. GET performs at most one delta lookup and one base lookup.
class PairReadGeneration final {
  public:
    [[nodiscard]] static auto empty(WorkerRoutingState routing)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    [[nodiscard]] static auto
    from_durable_snapshot(WorkerRoutingState routing,
                          std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    [[nodiscard]] static auto publish(std::shared_ptr<const PairReadGeneration> previous,
                                      std::span<const ReadMutation> mutations,
                                      std::size_t merge_delta_entries)
        -> Result<std::shared_ptr<const PairReadGeneration>>;

    [[nodiscard]] auto get(const HashedKey& key, std::uint64_t now_ns) const -> Result<OwnedValue>;
    [[nodiscard]] auto prepare_durable(const HashedKey& key) const
        -> Result<DurableRuntimeCatalog::PublishedReadRecord>;

    [[nodiscard]] auto epoch() const noexcept -> std::uint64_t {
        return epoch_;
    }
    [[nodiscard]] auto visible_through() const noexcept -> std::uint64_t {
        return visible_through_;
    }
    [[nodiscard]] auto delta_entries() const noexcept -> std::size_t;
    [[nodiscard]] auto base_entries() const noexcept -> std::size_t;

  private:
    PairReadGeneration(WorkerRoutingState routing, std::shared_ptr<const ImmutableReadIndex> base,
                       std::shared_ptr<const DeltaState> delta, std::uint64_t epoch,
                       std::uint64_t visible_through) noexcept;

    WorkerRoutingState routing_{};
    std::shared_ptr<const ImmutableReadIndex> base_;
    std::shared_ptr<const DeltaState> delta_;
    std::uint64_t epoch_{};
    std::uint64_t visible_through_{};
};

} // namespace glyphastore::server
