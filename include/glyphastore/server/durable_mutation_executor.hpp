#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/store.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace glyphastore::server {

enum class DurableMutationKind : std::uint8_t { put, erase };

struct DurableMutationCompletion final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t admission_bytes{};
    std::optional<Error> error;
};

struct DurableMutationTask final {
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::size_t worker_index{};
    DurableMutationKind kind{};
    std::string key;
    std::uint64_t key_hash{};
    std::vector<std::byte> value;
    std::uint64_t expire_at_ns{};
    std::size_t admission_bytes{};
    BoundedMpscQueue<DurableMutationCompletion>* completions{};
    Wakeup* wakeup{};
};

// One bounded FIFO lane per Store Worker. A lane mutex protects only queue
// admission/removal; durable Store work always runs after that mutex is
// released. Separate lanes prevent a slow Worker from serializing admission or
// execution for unrelated Workers.
class DurableMutationExecutor final {
  public:
    [[nodiscard]] static auto create(Store& store, std::size_t worker_count, std::size_t capacity_per_worker)
        -> Result<std::unique_ptr<DurableMutationExecutor>>;
    ~DurableMutationExecutor();

    DurableMutationExecutor(const DurableMutationExecutor&) = delete;
    auto operator=(const DurableMutationExecutor&) -> DurableMutationExecutor& = delete;
    DurableMutationExecutor(DurableMutationExecutor&&) = delete;
    auto operator=(DurableMutationExecutor&&) -> DurableMutationExecutor& = delete;

    [[nodiscard]] auto start() -> Status;
    [[nodiscard]] auto try_submit(DurableMutationTask task) -> bool;
    // Stops admission and drains every admitted mutation before returning.
    // This is required because an admitted mutation may already have crossed
    // its durable commit point even if its client disconnects.
    void stop_and_drain() noexcept;

  private:
    struct Lane;

    DurableMutationExecutor(Store& store, std::size_t worker_count, std::size_t capacity_per_worker);
    void run(std::size_t worker_index) noexcept;

    Store& store_;
    std::vector<std::unique_ptr<Lane>> lanes_;
    std::mutex lifecycle_mutex_;
    std::atomic_bool started_{};
    std::atomic_bool stopping_{};
};

} // namespace glyphastore::server
