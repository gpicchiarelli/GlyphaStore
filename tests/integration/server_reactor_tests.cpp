#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <netinet/in.h>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto text(const std::span<const std::byte> value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

auto load_u32(const std::span<const std::byte> input) -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t byte = 0; byte < 4; ++byte) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[byte])) << (byte * 8U);
    }
    return value;
}

auto send_all(const int socket, const std::span<const std::byte> data) -> bool {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto written = ::send(socket, data.data() + sent, data.size() - sent, 0);
        if (written <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(written);
    }
    return true;
}

auto receive_exact(const int socket, const std::span<std::byte> output) -> bool {
    std::size_t received = 0;
    while (received < output.size()) {
        const auto count = ::recv(socket, output.data() + received, output.size() - received, 0);
        if (count <= 0) {
            return false;
        }
        received += static_cast<std::size_t>(count);
    }
    return true;
}

auto receive_response(const int socket) -> std::vector<std::byte> {
    std::array<std::byte, glyphastore::server::kResponseHeaderBytes> header{};
    if (!receive_exact(socket, header)) {
        return {};
    }
    const auto frame_size = static_cast<std::size_t>(load_u32(header));
    if (frame_size < header.size() || frame_size > glyphastore::server::kMaxFrameBytes) {
        return {};
    }
    std::vector<std::byte> frame(frame_size);
    std::ranges::copy(header, frame.begin());
    if (!receive_exact(socket, std::span<std::byte>{frame}.subspan(header.size()))) {
        return {};
    }
    return frame;
}

auto connect_to(const std::uint16_t port) -> int {
    const auto socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket < 0) {
        return -1;
    }
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    static_cast<void>(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    static_cast<void>(::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr));
    if (::connect(socket, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        static_cast<void>(::close(socket));
        return -1;
    }
    return socket;
}

auto initialize_and_bind(const int socket, const std::uint32_t worker, const std::uint32_t worker_count)
    -> bool {
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    if (!init || !send_all(socket, *init)) {
        return false;
    }
    const auto init_frame = receive_response(socket);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    if (!initialized || initialized->frame.status != glyphastore::server::ResponseStatus::ok ||
        initialized->frame.worker_count != worker_count || initialized->frame.routing_epoch == 0) {
        return false;
    }
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = worker,
    });
    if (!bind || !send_all(socket, *bind)) {
        return false;
    }
    const auto bind_frame = receive_response(socket);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    return bound && bound->frame.status == glyphastore::server::ResponseStatus::ok &&
           bound->frame.owner_worker == worker && bound->frame.worker_count == worker_count;
}

class ServerTemporaryDirectory final {
  public:
    ServerTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-server-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        root_ = created;
    }

    ~ServerTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] auto store_path() const -> std::filesystem::path {
        return root_ / "store";
    }

  private:
    std::filesystem::path root_;
};

} // namespace

GLYPHA_TEST("server rejects unsupported worker counts and undersized protocol buffers") {
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .worker_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_input_bytes = glyphastore::server::kRequestHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_output_bytes = glyphastore::server::kResponseHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create({.port = 0, .worker_count = 2},
                                                        {.worker_config = {.explicit_count = 1}})
                        .has_value());
}

GLYPHA_TEST("server StoreConfig persists acknowledged wire writes across restart") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 4},
            {.storage_mode = glyphastore::StorageMode::durable_sync,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(opened.has_value());
        auto& server = **opened;
        GLYPHA_REQUIRE(server.start().has_value());

        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 3,
            .key = bytes("durable-wire-key"),
            .value = bytes("durable-wire-value"),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto put_frame = receive_response(socket);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(socket));
        server.request_stop();
        GLYPHA_REQUIRE(server.join().has_value());
    }

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    auto& server = **reopened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 4,
        .key = bytes("durable-wire-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    const auto get_frame = receive_response(socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "durable-wire-value");
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("TCP reactor handles partial and pipelined protocol frames") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 16});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 10,
        .value = bytes("first"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data(), 3}));
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data() + 3, ping->size() - 3}));

    const auto first_frame = receive_response(socket);
    GLYPHA_REQUIRE(!first_frame.empty());
    const auto first = glyphastore::server::decode_response(first_frame);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(first->frame.request_id == 10);
    GLYPHA_REQUIRE(text(first->frame.value) == "first");

    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 11,
    });
    const auto second_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 12,
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(second_ping.has_value());
    std::vector<std::byte> pipelined;
    pipelined.insert(pipelined.end(), init->begin(), init->end());
    pipelined.insert(pipelined.end(), second_ping->begin(), second_ping->end());
    GLYPHA_REQUIRE(send_all(socket, pipelined));

    const auto init_frame = receive_response(socket);
    const auto second_ping_frame = receive_response(socket);
    const auto init_response = glyphastore::server::decode_response(init_frame);
    const auto ping_response = glyphastore::server::decode_response(second_ping_frame);
    GLYPHA_REQUIRE(init_response.has_value());
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(init_response->frame.request_id == 11);
    GLYPHA_REQUIRE(text(init_response->frame.value) == "GlyphaStore/2");
    GLYPHA_REQUIRE(init_response->frame.worker_count == 1);
    GLYPHA_REQUIRE(ping_response->frame.request_id == 12);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "second");
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 14,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(socket, *bind));
    const auto bind_frame = receive_response(socket);
    const auto bind_response = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bind_response.has_value());
    GLYPHA_REQUIRE(bind_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(bind_response->frame.owner_worker == 0);

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 20,
        .key = bytes("network-key"),
        .value = bytes("network-value"),
    });
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 21,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(get.has_value());
    std::vector<std::byte> store_pipeline;
    store_pipeline.insert(store_pipeline.end(), put->begin(), put->end());
    store_pipeline.insert(store_pipeline.end(), get->begin(), get->end());
    GLYPHA_REQUIRE(send_all(socket, store_pipeline));
    const auto put_frame = receive_response(socket);
    const auto get_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "network-value");

    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 22,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 23,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(missing_get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *missing_get));
    const auto missing_frame = receive_response(socket);
    const auto missing_response = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(missing_response.has_value());
    GLYPHA_REQUIRE(missing_response->frame.status == glyphastore::server::ResponseStatus::not_found);

    std::vector<std::byte> large_payload(1536U * 1024U, std::byte{0x5A});
    const auto large_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 13,
        .value = large_payload,
    });
    GLYPHA_REQUIRE(large_ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *large_ping));
    const auto large_response_frame = receive_response(socket);
    const auto large_response = glyphastore::server::decode_response(large_response_frame);
    GLYPHA_REQUIRE(large_response.has_value());
    GLYPHA_REQUIRE(large_response->frame.request_id == 13);
    GLYPHA_REQUIRE(large_response->frame.value.size() == large_payload.size());
    GLYPHA_REQUIRE(std::ranges::equal(large_response->frame.value, large_payload));

    static_cast<void>(::close(socket));

    const auto half_closed_socket = connect_to(server.port());
    GLYPHA_REQUIRE(half_closed_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(half_closed_socket, 0, 1));
    const auto final_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 30,
        .key = bytes("half-close-key"),
        .value = bytes("half-close-value"),
    });
    GLYPHA_REQUIRE(final_put.has_value());
    GLYPHA_REQUIRE(send_all(half_closed_socket, *final_put));
    GLYPHA_REQUIRE(::shutdown(half_closed_socket, SHUT_WR) == 0);
    const auto final_frame = receive_response(half_closed_socket);
    const auto final_response = glyphastore::server::decode_response(final_frame);
    GLYPHA_REQUIRE(final_response.has_value());
    GLYPHA_REQUIRE(final_response->frame.request_id == 30);
    GLYPHA_REQUIRE(final_response->frame.status == glyphastore::server::ResponseStatus::ok);
    static_cast<void>(::close(half_closed_socket));

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("bound Reactor redirects wrong owners without forwarding") {
    GLYPHA_REQUIRE(glyphastore::route_worker("bounded-key-0", 2) == 1);
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 2, .reuse_port = false});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto wrong_socket = connect_to(server.port());
    GLYPHA_REQUIRE(wrong_socket >= 0);
    const auto premature_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 98,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(premature_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *premature_bind));
    const auto premature_bind_frame = receive_response(wrong_socket);
    const auto premature = glyphastore::server::decode_response(premature_bind_frame);
    GLYPHA_REQUIRE(premature.has_value());
    GLYPHA_REQUIRE(premature->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(premature->frame.owner_worker == glyphastore::server::kNoWorker);

    const auto unbound_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 99,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(unbound_get.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *unbound_get));
    const auto unbound_frame = receive_response(wrong_socket);
    const auto unbound = glyphastore::server::decode_response(unbound_frame);
    GLYPHA_REQUIRE(unbound.has_value());
    GLYPHA_REQUIRE(unbound->frame.status == glyphastore::server::ResponseStatus::not_bound);
    GLYPHA_REQUIRE(unbound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(initialize_and_bind(wrong_socket, 0, 2));
    const auto repeated_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 100,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(repeated_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *repeated_bind));
    const auto repeated_bind_frame = receive_response(wrong_socket);
    const auto repeated = glyphastore::server::decode_response(repeated_bind_frame);
    GLYPHA_REQUIRE(repeated.has_value());
    GLYPHA_REQUIRE(repeated->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(repeated->frame.owner_worker == 0);

    const auto misplaced_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 101,
        .key = bytes("bounded-key-0"),
        .value = bytes("stored"),
    });
    GLYPHA_REQUIRE(misplaced_put.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *misplaced_put));
    const auto redirect_frame = receive_response(wrong_socket);
    const auto redirect = glyphastore::server::decode_response(redirect_frame);
    GLYPHA_REQUIRE(redirect.has_value());
    GLYPHA_REQUIRE(redirect->frame.status == glyphastore::server::ResponseStatus::wrong_owner);
    GLYPHA_REQUIRE(redirect->frame.owner_worker == 1);
    static_cast<void>(::close(wrong_socket));

    const auto owner_socket = connect_to(server.port());
    GLYPHA_REQUIRE(owner_socket >= 0);
    const auto owner_init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 101,
    });
    const auto owner_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 102,
        .target_worker = 1,
    });
    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 103,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(owner_init.has_value());
    GLYPHA_REQUIRE(owner_bind.has_value());
    GLYPHA_REQUIRE(missing_get.has_value());
    std::vector<std::byte> bind_pipeline;
    bind_pipeline.insert(bind_pipeline.end(), owner_init->begin(), owner_init->end());
    bind_pipeline.insert(bind_pipeline.end(), owner_bind->begin(), owner_bind->end());
    bind_pipeline.insert(bind_pipeline.end(), missing_get->begin(), missing_get->end());
    GLYPHA_REQUIRE(send_all(owner_socket, bind_pipeline));
    const auto owner_init_frame = receive_response(owner_socket);
    const auto owner_bind_frame = receive_response(owner_socket);
    const auto missing_frame = receive_response(owner_socket);
    const auto owner_initialized = glyphastore::server::decode_response(owner_init_frame);
    const auto owner_bound = glyphastore::server::decode_response(owner_bind_frame);
    const auto missing = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(owner_initialized.has_value());
    GLYPHA_REQUIRE(owner_initialized->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound.has_value());
    GLYPHA_REQUIRE(owner_bound->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(missing.has_value());
    GLYPHA_REQUIRE(missing->frame.status == glyphastore::server::ResponseStatus::not_found);
    static_cast<void>(::close(owner_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("multi-Reactor executors distribute connections and share one Store") {
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 16, .worker_count = 2, .executor_affinity = true});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.executor_count() == 2);
    GLYPHA_REQUIRE(server.start().has_value());

    for (std::uint64_t request = 0; request < 16; ++request) {
        const auto key = std::string{"reuse-key-"} + std::to_string(request);
        const auto value = std::string{"reuse-value-"} + std::to_string(request);
        const auto owner = static_cast<std::uint32_t>(glyphastore::route_worker(key, 2));
        const auto writer = connect_to(server.port());
        GLYPHA_REQUIRE(writer >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(writer, owner, 2));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = request * 2U,
            .key = bytes(key),
            .value = bytes(value),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(writer, *put));
        const auto put_frame = receive_response(writer);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(writer));

        const auto reader = connect_to(server.port());
        GLYPHA_REQUIRE(reader >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(reader, owner, 2));
        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = request * 2U + 1U,
            .key = bytes(key),
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(reader, *get));
        const auto get_frame = receive_response(reader);
        const auto get_response = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(get_response.has_value());
        GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(get_response->frame.value) == value);
        static_cast<void>(::close(reader));
    }

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    const auto adopted = server.adopted_connections_per_executor();
    GLYPHA_REQUIRE(adopted.size() == 2);
    GLYPHA_REQUIRE(adopted[0] > 0);
    GLYPHA_REQUIRE(adopted[1] > 0);
    const auto affinity = server.executor_affinity_results();
    GLYPHA_REQUIRE(affinity.size() == 2);
    GLYPHA_REQUIRE(affinity[0].mode != glyphastore::server::ExecutorAffinityMode::disabled);
    GLYPHA_REQUIRE(affinity[1].mode != glyphastore::server::ExecutorAffinityMode::disabled);
}
