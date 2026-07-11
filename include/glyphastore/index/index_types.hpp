#pragma once

#include "glyphastore/core/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
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
    std::size_t arena_allocated_bytes{};
    std::size_t arena_live_bytes{};
};

struct RebuildStats {
    std::uint64_t records_scanned{};
    std::uint64_t records_visible{};
    std::uint64_t tombstones{};
    std::uint64_t expired{};
};

} // namespace glyphastore
