#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/segment/record.hpp"

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace glyphastore {

class Worker final {
  public:
    Worker(WorkerId id, GlobalSegmentManager& manager);

    [[nodiscard]] auto id() const noexcept -> WorkerId {
        return id_;
    }
    [[nodiscard]] auto index() const noexcept -> const Index& {
        return index_;
    }
    [[nodiscard]] auto owned_segments() const noexcept -> const std::vector<SegmentPtr>& {
        return owned_;
    }

    // Expired keys are tombstoned and removed from the Index on read.
    [[nodiscard]] auto get(const HashedKey& key, std::uint64_t now_ns = 0) -> Result<RecordView>;
    [[nodiscard]] auto put(const HashedKey& key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto erase(const HashedKey& key) -> Status;

  private:
    [[nodiscard]] auto next_sequence() -> SequenceNumber;
    [[nodiscard]] auto append_record(const RecordInput& input) -> Result<RecordRef>;
    [[nodiscard]] auto read_ref(const RecordRef& ref) const -> Result<RecordView>;
    [[nodiscard]] auto publish(const HashedKey& key, const RecordRef& ref) -> Status;
    [[nodiscard]] auto unpublish(const HashedKey& key) -> Status;

    WorkerId id_;
    GlobalSegmentManager& manager_;
    Index index_;
    SegmentPtr active_;
    SequenceNumber next_sequence_{SequenceNumber{1}};
    std::vector<SegmentPtr> owned_;
};

} // namespace glyphastore
