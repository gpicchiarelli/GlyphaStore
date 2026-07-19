#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/vacuum/vacuum.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace glyphastore {

class Store;
namespace detail {
class StoreAccess;
}

class Worker final {
  public:
    Worker(WorkerId id, GlobalSegmentManager& manager);
    Worker(const Worker&) = delete;
    auto operator=(const Worker&) -> Worker& = delete;
    Worker(Worker&&) = delete;
    auto operator=(Worker&&) -> Worker& = delete;

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
    [[nodiscard]] auto compact(std::uint64_t now_ns, VacuumPolicy policy = {})
        -> Result<std::optional<VacuumStats>>;

  private:
    [[nodiscard]] auto get_locked(const HashedKey& key, std::uint64_t now_ns) -> Result<RecordView>;
    [[nodiscard]] auto put_locked(const HashedKey& key, std::span<const std::byte> value,
                                  std::uint64_t expire_at_ns) -> Status;
    [[nodiscard]] auto erase_locked(const HashedKey& key) -> Status;
    [[nodiscard]] auto next_sequence() -> SequenceNumber;
    void register_owned_segment(SegmentPtr segment);
    void unregister_owned_segment(SegmentId id) noexcept;
    [[nodiscard]] auto find_owned_segment(SegmentId id) noexcept -> Segment*;
    [[nodiscard]] auto find_owned_segment(SegmentId id) const noexcept -> const Segment*;
    void maybe_retire(Segment& segment);
    [[nodiscard]] auto append_record(const RecordInput& input) -> Result<RecordRef>;
    [[nodiscard]] auto read_ref(const RecordRef& ref) const -> Result<RecordView>;
    [[nodiscard]] auto publish(const HashedKey& key, const RecordRef& ref, Segment& segment) -> Status;

    WorkerId id_;
    GlobalSegmentManager& manager_;
    Index index_;
    SegmentPtr active_;
    SequenceNumber next_sequence_{SequenceNumber{1}};
    std::vector<SegmentPtr> owned_;
    std::unordered_map<SegmentId, Segment*> owned_by_id_;
    mutable std::mutex mutex_;

    friend class Store;
    friend class detail::StoreAccess;
};

} // namespace glyphastore
