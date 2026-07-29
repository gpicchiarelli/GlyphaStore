#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/bounded_spsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/prepared_read.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

namespace glyphastore::server {

struct DiskReadCompletion final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::optional<OwnedValue> value;
    std::optional<Error> error;
};

struct DiskReadTask final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t worker_index{};
    detail::PreparedColdRead read;
    detail::ColdReadCancellation cancellation;
    std::size_t maximum_value_bytes{};
    BoundedSpscQueue<DiskReadCompletion>* completions{};
    Wakeup* wakeup{};
};

// One bounded cold-read lane per ShardPair. Each Reader is the sole producer
// of its SPSC lane and one persistent I/O worker is the sole consumer; durable
// GET submission therefore has no process-wide mutex or condition variable.
class DiskReadExecutor final {
  public:
    [[nodiscard]] static auto create(Store& store, std::size_t worker_count, std::size_t capacity_per_worker)
        -> Result<std::unique_ptr<DiskReadExecutor>>;
    ~DiskReadExecutor();

    DiskReadExecutor(const DiskReadExecutor&) = delete;
    auto operator=(const DiskReadExecutor&) -> DiskReadExecutor& = delete;
    DiskReadExecutor(DiskReadExecutor&&) = delete;
    auto operator=(DiskReadExecutor&&) -> DiskReadExecutor& = delete;

    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto try_submit(DiskReadTask task) -> bool;
    void stop() noexcept;

  private:
    struct Lane;

    DiskReadExecutor(Store& store, std::size_t worker_count, std::size_t capacity_per_worker);
    void run(std::size_t worker_index) noexcept;
    [[nodiscard]] auto begin_submission() noexcept -> bool;
    void finish_submission() noexcept;

    static constexpr auto kAdmissionClosed = std::size_t{1}
                                             << (std::numeric_limits<std::size_t>::digits - 1U);
    static constexpr auto kAdmissionCountMask = kAdmissionClosed - 1U;

    Store& store_;
    std::vector<std::unique_ptr<Lane>> lanes_;
    std::atomic_size_t admission_state_{};
    std::atomic_bool started_{};
    std::atomic_bool stopping_{};
};

} // namespace glyphastore::server
