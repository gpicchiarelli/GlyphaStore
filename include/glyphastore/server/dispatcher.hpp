#pragma once

#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/wakeup.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace glyphastore::server {

struct DispatchTask {
    RequestOpcode opcode{RequestOpcode::get};
    std::size_t origin_executor{};
    ConnectionToken connection;
    std::uint64_t request_id{};
    std::uint64_t key_hash{};
    std::uint64_t expire_at_ns{};
    std::string key;
    std::vector<std::byte> value;
};

struct DispatchCompletion {
    ConnectionToken connection;
    ResponseStatus status{ResponseStatus::ok};
    std::uint64_t request_id{};
    std::vector<std::byte> value;
};

// Cross-executor transport only. Every endpoint has one MPSC Worker inbox and
// one MPSC completion queue, both consumed solely by that endpoint's Reactor.
class DispatchMesh final {
  public:
    DispatchMesh(std::size_t executor_count, std::size_t inbox_capacity, std::size_t completion_capacity);

    DispatchMesh(const DispatchMesh&) = delete;
    auto operator=(const DispatchMesh&) -> DispatchMesh& = delete;
    DispatchMesh(DispatchMesh&&) = delete;
    auto operator=(DispatchMesh&&) -> DispatchMesh& = delete;

    void register_wakeup(std::size_t executor, Wakeup& wakeup) noexcept;
    [[nodiscard]] auto try_submit(std::size_t target_executor, DispatchTask&& task) -> bool;
    [[nodiscard]] auto try_pop_task(std::size_t executor) -> std::optional<DispatchTask>;
    [[nodiscard]] auto try_complete(std::size_t target_executor, DispatchCompletion&& completion) -> bool;
    [[nodiscard]] auto try_pop_completion(std::size_t executor) -> std::optional<DispatchCompletion>;
    [[nodiscard]] auto try_handoff(std::size_t target_executor, SocketHandle&& socket) -> bool;
    [[nodiscard]] auto try_pop_handoff(std::size_t executor) -> std::optional<SocketHandle>;
    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return endpoints_.size();
    }

  private:
    struct Endpoint {
        Endpoint(std::size_t inbox_capacity, std::size_t completion_capacity)
            : inbox(inbox_capacity), completions(completion_capacity), accepted(inbox_capacity) {}

        BoundedMpscQueue<DispatchTask> inbox;
        BoundedMpscQueue<DispatchCompletion> completions;
        BoundedMpscQueue<SocketHandle> accepted;
        Wakeup* wakeup{};
    };

    std::vector<std::unique_ptr<Endpoint>> endpoints_;
};

} // namespace glyphastore::server
