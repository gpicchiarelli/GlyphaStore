#pragma once

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment.hpp"
#include "glyphastore/store/store.hpp"
#include "glyphastore/worker/worker.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore::detail {

// Internal bridge for the native server and white-box tests. This header is
// never installed and is not part of the supported C++ API.
class StoreAccess final {
  public:
    [[nodiscard]] static auto get_view(Store& store, std::size_t worker_index, const HashedKey& key,
                                       std::uint64_t now_ns) -> Result<RecordView>;
    [[nodiscard]] static auto put(Store& store, std::size_t worker_index, const HashedKey& key,
                                  std::span<const std::byte> value, std::uint64_t expire_at_ns) -> Status;
    [[nodiscard]] static auto erase(Store& store, std::size_t worker_index, const HashedKey& key) -> Status;

    [[nodiscard]] static auto worker(const Store& store, std::size_t index) noexcept -> const Worker&;
    [[nodiscard]] static auto segments(const Store& store) -> std::vector<SegmentPtr>;
};

} // namespace glyphastore::detail
