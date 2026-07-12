#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/reactor.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

} // namespace

GLYPHA_TEST("TCP reactor handles partial and pipelined protocol frames") {
    auto opened = glyphastore::server::Reactor::create({.port = 0, .maximum_connections = 16});
    GLYPHA_REQUIRE(opened.has_value());
    auto& reactor = **opened;
    std::atomic<bool> failed{false};
    std::jthread reactor_thread([&](const std::stop_token stop) {
        while (!stop.stop_requested()) {
            if (!reactor.run_once(10)) {
                failed.store(true);
                return;
            }
        }
    });

    const auto socket = connect_to(reactor.port());
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

    const auto hello = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::hello,
        .request_id = 11,
    });
    const auto second_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 12,
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(hello.has_value());
    GLYPHA_REQUIRE(second_ping.has_value());
    std::vector<std::byte> pipelined;
    pipelined.insert(pipelined.end(), hello->begin(), hello->end());
    pipelined.insert(pipelined.end(), second_ping->begin(), second_ping->end());
    GLYPHA_REQUIRE(send_all(socket, pipelined));

    const auto hello_frame = receive_response(socket);
    const auto second_ping_frame = receive_response(socket);
    const auto hello_response = glyphastore::server::decode_response(hello_frame);
    const auto ping_response = glyphastore::server::decode_response(second_ping_frame);
    GLYPHA_REQUIRE(hello_response.has_value());
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(hello_response->frame.request_id == 11);
    GLYPHA_REQUIRE(text(hello_response->frame.value) == "GlyphaStore/1");
    GLYPHA_REQUIRE(ping_response->frame.request_id == 12);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "second");

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

    const auto half_closed_socket = connect_to(reactor.port());
    GLYPHA_REQUIRE(half_closed_socket >= 0);
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

    reactor_thread.request_stop();
    reactor_thread.join();
    GLYPHA_REQUIRE(!failed.load());
}

GLYPHA_TEST("TCP reactor bounds asynchronous requests per connection") {
    auto opened = glyphastore::server::Reactor::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .maximum_in_flight_per_connection = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& reactor = **opened;
    std::atomic<bool> failed{false};
    std::jthread reactor_thread([&](const std::stop_token stop) {
        while (!stop.stop_requested()) {
            if (!reactor.run_once(10)) {
                failed.store(true);
                return;
            }
        }
    });

    const auto socket = connect_to(reactor.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 100,
        .key = bytes("bounded-key"),
        .value = bytes("stored"),
    });
    const auto early_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 101,
        .key = bytes("bounded-key"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(early_get.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), put->begin(), put->end());
    pipeline.insert(pipeline.end(), early_get->begin(), early_get->end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    const auto first_frame = receive_response(socket);
    const auto second_frame = receive_response(socket);
    const auto first = glyphastore::server::decode_response(first_frame);
    const auto second = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    const auto* put_response = first->frame.request_id == 100 ? &first->frame : &second->frame;
    const auto* get_response = first->frame.request_id == 101 ? &first->frame : &second->frame;
    GLYPHA_REQUIRE(put_response->request_id == 100);
    GLYPHA_REQUIRE(put_response->status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(get_response->request_id == 101);
    GLYPHA_REQUIRE(get_response->status == glyphastore::server::ResponseStatus::overloaded);

    const auto final_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 102,
        .key = bytes("bounded-key"),
    });
    GLYPHA_REQUIRE(final_get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *final_get));
    const auto final_frame = receive_response(socket);
    const auto final = glyphastore::server::decode_response(final_frame);
    GLYPHA_REQUIRE(final.has_value());
    GLYPHA_REQUIRE(final->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(final->frame.value) == "stored");

    static_cast<void>(::close(socket));
    reactor_thread.request_stop();
    reactor_thread.join();
    GLYPHA_REQUIRE(!failed.load());
}
