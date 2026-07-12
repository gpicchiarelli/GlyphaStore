#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/connection_token.hpp"
#include "glyphastore/server/poller.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/server/wakeup.hpp"
#include "glyphastore/store/store.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace glyphastore::server {

struct ReactorConfig {
    std::string bind_address{"127.0.0.1"};
    std::uint16_t port{7379};
    std::size_t maximum_connections{4096};
    std::size_t worker_count{1};
    std::size_t event_batch_size{256};
    std::size_t maximum_input_bytes{4U * 1024U * 1024U};
    std::size_t maximum_output_bytes{4U * 1024U * 1024U};
    std::size_t connection_handoff_capacity{4096};
    bool reuse_port{true};
    bool executor_affinity{};
};

class Reactor final {
  public:
    [[nodiscard]] static auto create(const ReactorConfig& config, std::size_t executor_id,
                                     TcpListener listener, Store& store, ConnectionHandoffMesh& mesh)
        -> Result<std::unique_ptr<Reactor>>;

    Reactor(const Reactor&) = delete;
    auto operator=(const Reactor&) -> Reactor& = delete;
    Reactor(Reactor&&) = delete;
    auto operator=(Reactor&&) -> Reactor& = delete;

    [[nodiscard]] auto run_once(int timeout_ms) -> Status;
    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return listener_.port();
    }
    [[nodiscard]] auto executor_id() const noexcept -> std::size_t {
        return executor_id_;
    }
    [[nodiscard]] auto active_connections() const noexcept -> std::size_t {
        return active_connections_;
    }
    [[nodiscard]] auto adopted_connections() const noexcept -> std::size_t {
        return adopted_connections_;
    }

  private:
    struct Connection {
        SocketHandle socket;
        std::uint32_t generation{1};
        std::vector<std::byte> input;
        std::size_t input_offset{};
        std::vector<std::byte> output;
        std::size_t output_offset{};
        std::optional<std::uint32_t> bound_worker;
        bool initialized{};
        bool peer_read_closed{};
    };

    Reactor(ReactorConfig config, std::size_t executor_id, TcpListener listener, Poller poller, Wakeup wakeup,
            Store& store, ConnectionHandoffMesh& mesh);

    [[nodiscard]] auto accept_ready() -> Status;
    [[nodiscard]] auto adopt_connection(ConnectionHandoff handoff) -> Status;
    [[nodiscard]] auto read_ready(ConnectionToken token) -> Status;
    [[nodiscard]] auto write_ready(ConnectionToken token) -> Status;
    [[nodiscard]] auto process_frames(ConnectionToken token) -> Status;
    [[nodiscard]] auto bind_connection(ConnectionToken token, const RequestView& request) -> Status;
    [[nodiscard]] auto transfer_connection(ConnectionToken token, std::size_t target_worker) -> Status;
    [[nodiscard]] auto dispatch_request(ConnectionToken token, const RequestView& request) -> Status;
    [[nodiscard]] auto execute_local(ConnectionToken token, const RequestView& request,
                                     std::uint64_t key_hash) -> Status;
    [[nodiscard]] auto process_messages() -> Status;
    [[nodiscard]] auto process_handoffs() -> Status;
    [[nodiscard]] auto queue_response(ConnectionToken token, const ResponseView& response) -> Status;
    [[nodiscard]] auto connection(ConnectionToken token) noexcept -> Connection*;
    void close_connection(ConnectionToken token) noexcept;

    static constexpr std::uint64_t kListenerToken = std::numeric_limits<std::uint64_t>::max();
    static constexpr std::uint64_t kMessageToken = kListenerToken - 1U;
    static constexpr std::uint64_t kRoutingEpoch = 1;

    ReactorConfig config_;
    std::size_t executor_id_{};
    TcpListener listener_;
    Poller poller_;
    Wakeup wakeup_;
    Store& store_;
    ConnectionHandoffMesh& mesh_;
    std::vector<Connection> connections_;
    std::vector<std::uint32_t> free_slots_;
    std::vector<IoEvent> events_;
    std::size_t active_connections_{};
    std::size_t adopted_connections_{};
};

} // namespace glyphastore::server
