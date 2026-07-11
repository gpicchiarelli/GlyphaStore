#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/manager.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment.hpp"
#include "glyphastore/worker/topology.hpp"
#include "glyphastore/worker/worker.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace glyphastore {

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
    [[nodiscard]] auto segments() const noexcept -> const std::vector<SegmentPtr>& {
        return manager_.segments();
    }

    [[nodiscard]] auto get(std::string_view key, std::uint64_t now_ns = 0) const -> Result<RecordView>;
    [[nodiscard]] auto put(std::string_view key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto erase(std::string_view key) -> Status;

    [[nodiscard]] auto verify_index() const -> Status;

  private:
    Store(SegmentManager manager, std::size_t worker_count);

    [[nodiscard]] auto worker_for(std::string_view key) const -> const Worker&;
    [[nodiscard]] auto worker_for(std::string_view key) -> Worker&;

    SegmentManager manager_;
    std::vector<Worker> workers_;
};

} // namespace glyphastore
