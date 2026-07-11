#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore {

struct IndexEntry {
    std::string key;
    RecordRef record;
};

struct IndexMutationResult {
    bool inserted{};
    std::optional<RecordRef> previous;
};

struct IndexStats {
    std::size_t size{};
    std::size_t bucket_count{};
    float load_factor{};
};

class IndexBackend {
  public:
    virtual ~IndexBackend() = default;
    [[nodiscard]] virtual auto find(std::string_view key) const -> std::optional<RecordRef> = 0;
    virtual auto insert_or_assign(std::string_view key, RecordRef ref) -> IndexMutationResult = 0;
    virtual auto erase(std::string_view key) -> IndexMutationResult = 0;
    virtual void reserve(std::size_t count) = 0;
    [[nodiscard]] virtual auto entries() const -> std::vector<IndexEntry> = 0;
    [[nodiscard]] virtual auto stats() const noexcept -> IndexStats = 0;
    [[nodiscard]] virtual auto clone_empty() const -> std::unique_ptr<IndexBackend> = 0;
};

class Index final {
  public:
    Index();
    explicit Index(std::unique_ptr<IndexBackend> backend);
    ~Index();
    Index(Index&&) noexcept;
    auto operator=(Index&&) noexcept -> Index&;
    Index(const Index&) = delete;
    auto operator=(const Index&) -> Index& = delete;

    [[nodiscard]] auto find(std::string_view key) const -> std::optional<RecordRef>;
    auto insert_or_assign(std::string_view key, RecordRef ref) -> IndexMutationResult;
    auto erase(std::string_view key) -> IndexMutationResult;
    void reserve(std::size_t count);
    [[nodiscard]] auto entries() const -> std::vector<IndexEntry>;
    [[nodiscard]] auto stats() const noexcept -> IndexStats;
    [[nodiscard]] auto make_empty() const -> Index;

  private:
    std::unique_ptr<IndexBackend> backend_;
};

struct RebuildStats {
    std::uint64_t records_scanned{};
    std::uint64_t records_visible{};
    std::uint64_t tombstones{};
    std::uint64_t expired{};
};

struct RebuildResult {
    Index index;
    RebuildStats stats;
};

[[nodiscard]] auto rebuild_index_from_segments(std::span<const SegmentPtr> segments, std::uint64_t now_ns = 0)
    -> Result<RebuildResult>;

} // namespace glyphastore
