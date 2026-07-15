#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index_types.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore {

// Logical Index partition owned by one Worker. Backed by SwissTableIndex.
class Index final {
  public:
    Index();
    ~Index();
    Index(Index&&) noexcept;
    auto operator=(Index&&) noexcept -> Index&;
    Index(const Index&) = delete;
    auto operator=(const Index&) -> Index& = delete;

    [[nodiscard]] auto find(std::string_view key) const -> std::optional<RecordRef>;
    [[nodiscard]] auto find(const HashedKey& key) const -> std::optional<RecordRef>;
    [[nodiscard]] auto insert_or_assign(std::string_view key, RecordRef ref) -> Result<IndexMutationResult>;
    [[nodiscard]] auto insert_or_assign(const HashedKey& key, RecordRef ref) -> Result<IndexMutationResult>;
    auto erase(std::string_view key) -> IndexMutationResult;
    auto erase(const HashedKey& key) -> IndexMutationResult;
    auto erase_no_compact(const HashedKey& key) -> IndexMutationResult;
    [[nodiscard]] auto reserve(std::size_t count) -> Status;
    [[nodiscard]] auto prepare_insert(const HashedKey& key) -> Status;
    [[nodiscard]] auto prepare_batch_insert(std::size_t additional_entries,
                                            std::size_t additional_heap_key_bytes) -> Status;
    [[nodiscard]] auto entries() const -> std::vector<IndexEntry>;
    [[nodiscard]] auto stats() const noexcept -> IndexStats;
    [[nodiscard]] auto make_empty() const -> Index;

  private:
    explicit Index(SwissTableIndex table);

    SwissTableIndex table_;
};

struct RebuildResult {
    Index index;
    RebuildStats stats;
};

[[nodiscard]] auto rebuild_index_from_segments(std::span<const SegmentPtr> segments, std::uint64_t now_ns = 0)
    -> Result<RebuildResult>;

} // namespace glyphastore
