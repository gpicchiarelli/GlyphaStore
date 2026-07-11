#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/manager.hpp"
#include "glyphastore/segment/record.hpp"

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace glyphastore {

class Worker final {
  public:
    Worker(WorkerId id, SegmentManager& manager);

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
    [[nodiscard]] auto get(std::string_view key, std::uint64_t now_ns = 0) -> Result<RecordView>;
    [[nodiscard]] auto put(std::string_view key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto erase(std::string_view key) -> Status;

  private:
    [[nodiscard]] auto next_sequence() -> SequenceNumber;
    [[nodiscard]] auto append_record(const RecordInput& input) -> Result<RecordRef>;
    [[nodiscard]] auto read_ref(const RecordRef& ref) const -> Result<RecordView>;
    [[nodiscard]] auto publish(std::string_view key, const RecordRef& ref) -> Status;
    [[nodiscard]] auto unpublish(std::string_view key) -> Status;

    WorkerId id_;
    SegmentManager& manager_;
    Index index_;
    SegmentPtr active_;
    SequenceNumber next_sequence_{SequenceNumber{1}};
    std::vector<SegmentPtr> owned_;
};

} // namespace glyphastore
