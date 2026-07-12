#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment.hpp"
#include "glyphastore/worker/pool.hpp"
#include "glyphastore/worker/topology.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace glyphastore {

namespace server {
class Reactor;
}

struct StoreConfig {
    WorkerCountConfig worker_config{};
    SegmentId first_segment_id{1};
};

class Store final {
  public:
    [[nodiscard]] static auto open(const StoreConfig& config = {}) -> Result<std::unique_ptr<Store>>;

    Store(const Store&) = delete;
    auto operator=(const Store&) -> Store& = delete;
    Store(Store&&) = delete;
    auto operator=(Store&&) -> Store& = delete;

    [[nodiscard]] auto worker_count() const noexcept -> std::size_t {
        return workers_.size();
    }
    [[nodiscard]] auto worker(std::size_t index) const noexcept -> const Worker& {
        return workers_.worker(index);
    }
    [[nodiscard]] auto segment_manager() const noexcept -> const GlobalSegmentManager& {
        return segment_manager_;
    }
    [[nodiscard]] auto segments() const -> std::vector<SegmentPtr> {
        return segment_manager_.segments();
    }

    // Thread-safe Store API: each key routes to one Worker partition and acquires that
    // Worker's mutex for the duration of the operation. Different keys on different
    // Workers run concurrently; the same Worker serializes callers.
    //
    // get() returns segment-backed spans; copy value bytes before calling Store again
    // on the same key from another thread if the view must remain stable.
    [[nodiscard]] auto get(std::string_view key, std::uint64_t now_ns = 0) -> Result<RecordView>;
    [[nodiscard]] auto get(const HashedKey& key, std::uint64_t now_ns = 0) -> Result<RecordView>;
    [[nodiscard]] auto put(std::string_view key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto put(const HashedKey& key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto erase(std::string_view key) -> Status;
    [[nodiscard]] auto erase(const HashedKey& key) -> Status;

    [[nodiscard]] auto verify_index() const -> Status;

  private:
    Store(SegmentId first_segment_id, std::size_t worker_count);
    [[nodiscard]] auto get_owned(std::size_t worker_index, const HashedKey& key, std::uint64_t now_ns)
        -> Result<RecordView>;
    [[nodiscard]] auto put_owned(std::size_t worker_index, const HashedKey& key,
                                 std::span<const std::byte> value, std::uint64_t expire_at_ns) -> Status;
    [[nodiscard]] auto erase_owned(std::size_t worker_index, const HashedKey& key) -> Status;

    GlobalSegmentManager segment_manager_;
    WorkerPool workers_;

    friend class server::Reactor;
};

} // namespace glyphastore
