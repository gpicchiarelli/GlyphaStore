#pragma once

#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "glyphastore/server/authz.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/tls.hpp"
#include "glyphastore/server/wakeup.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace glyphastore::server {

struct ConnectionHandoff {
    SocketHandle socket;
    std::unique_ptr<TlsSession> tls;
    std::string principal{};
    Capability capabilities{Capability::none};
    std::vector<std::byte> input;
    std::vector<std::byte> output;
    std::optional<std::uint32_t> bound_worker;
    bool initialized{};
    bool peer_read_closed{};
};

// The only cross-executor data structure in the server data path. It transfers
// ownership of a connection reference and its buffered state exactly once when
// BIND_WORKER selects the final Reactor.
class ConnectionHandoffMesh final {
  public:
    ConnectionHandoffMesh(std::size_t executor_count, std::size_t queue_capacity);

    ConnectionHandoffMesh(const ConnectionHandoffMesh&) = delete;
    auto operator=(const ConnectionHandoffMesh&) -> ConnectionHandoffMesh& = delete;
    ConnectionHandoffMesh(ConnectionHandoffMesh&&) noexcept = default;
    auto operator=(ConnectionHandoffMesh&&) noexcept -> ConnectionHandoffMesh& = default;

    void register_wakeup(std::size_t executor, Wakeup& wakeup) noexcept;
    [[nodiscard]] auto try_handoff(std::size_t target_executor, ConnectionHandoff&& connection) -> bool;
    [[nodiscard]] auto try_pop(std::size_t executor) -> std::optional<ConnectionHandoff>;
    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return endpoints_.size();
    }

  private:
    struct Endpoint {
        explicit Endpoint(std::size_t capacity) : connections(capacity) {}

        BoundedMpscQueue<ConnectionHandoff> connections;
        Wakeup* wakeup{};
    };

    std::vector<std::unique_ptr<Endpoint>> endpoints_;
};

} // namespace glyphastore::server
