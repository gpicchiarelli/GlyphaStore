#include "glyphastore/client/client.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto text(const std::span<const std::byte> value) -> std::string {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

auto receive_exact(const int descriptor, const std::span<std::byte> output) -> bool {
    std::size_t received{};
    while (received < output.size()) {
        const auto count = ::recv(descriptor, output.data() + received, output.size() - received, 0);
        if (count <= 0) {
            return false;
        }
        received += static_cast<std::size_t>(count);
    }
    return true;
}

auto load_u32(const std::span<const std::byte> input) noexcept -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[index])) << (index * 8U);
    }
    return value;
}

auto receive_request(const int descriptor) -> std::vector<std::byte> {
    std::array<std::byte, sizeof(std::uint32_t)> size{};
    if (!receive_exact(descriptor, size)) {
        return {};
    }
    const auto frame_size = static_cast<std::size_t>(load_u32(size));
    if (frame_size < glyphastore::server::kRequestHeaderBytes ||
        frame_size > glyphastore::server::kMaxFrameBytes) {
        return {};
    }
    std::vector<std::byte> frame(frame_size);
    std::ranges::copy(size, frame.begin());
    if (!receive_exact(descriptor, std::span<std::byte>{frame}.subspan(size.size()))) {
        return {};
    }
    return frame;
}

auto send_all(const int descriptor, const std::span<const std::byte> input) -> bool {
    std::size_t sent{};
    while (sent < input.size()) {
        const auto count = ::send(descriptor, input.data() + sent, input.size() - sent, 0);
        if (count <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(count);
    }
    return true;
}

class DisconnectingPipelineServer final {
  public:
    DisconnectingPipelineServer() {
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        GLYPHA_REQUIRE(listener_ >= 0);
        sockaddr_in endpoint{};
        endpoint.sin_family = AF_INET;
        endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        endpoint.sin_port = 0;
        GLYPHA_REQUIRE(::bind(listener_, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) ==
                       0);
        GLYPHA_REQUIRE(::listen(listener_, 1) == 0);
        socklen_t endpoint_size = sizeof(endpoint);
        GLYPHA_REQUIRE(::getsockname(listener_, reinterpret_cast<sockaddr*>(&endpoint), &endpoint_size) == 0);
        port_ = ntohs(endpoint.sin_port);
        thread_ = std::thread{[this] { run(); }};
    }

    ~DisconnectingPipelineServer() {
        if (thread_.joinable()) {
            thread_.join();
        }
        static_cast<void>(::close(listener_));
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return port_;
    }

  private:
    void run() noexcept {
        const auto descriptor = ::accept(listener_, nullptr, nullptr);
        if (descriptor < 0) {
            return;
        }
        const auto reply = [&](const glyphastore::server::ResponseView& response) {
            auto encoded = glyphastore::server::encode_response(response);
            return encoded && send_all(descriptor, *encoded);
        };
        auto init_frame = receive_request(descriptor);
        auto init = glyphastore::server::decode_request(init_frame);
        if (!init || !reply({.status = glyphastore::server::ResponseStatus::ok,
                             .request_id = init->frame.request_id,
                             .owner_worker = 0,
                             .worker_count = 1,
                             .routing_epoch = 7,
                             .value = bytes("GlyphaStore/2")})) {
            static_cast<void>(::close(descriptor));
            return;
        }
        auto bind_frame = receive_request(descriptor);
        auto bind = glyphastore::server::decode_request(bind_frame);
        if (!bind || !reply({.status = glyphastore::server::ResponseStatus::ok,
                             .request_id = bind->frame.request_id,
                             .owner_worker = 0,
                             .worker_count = 1,
                             .routing_epoch = 7})) {
            static_cast<void>(::close(descriptor));
            return;
        }
        for (std::size_t request = 0; request < 3; ++request) {
            if (receive_request(descriptor).empty()) {
                static_cast<void>(::close(descriptor));
                return;
            }
        }
        static_cast<void>(::close(descriptor));
    }

    int listener_{-1};
    std::uint16_t port_{};
    std::thread thread_;
};

class RunningServer final {
  public:
    explicit RunningServer(const std::size_t workers = 2) {
        auto created = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 64, .worker_count = workers});
        if (!created) {
            throw std::runtime_error{"client test server creation failed (code " +
                                     std::to_string(static_cast<int>(created.error().code)) +
                                     "): " + created.error().message};
        }
        server_ = std::move(*created);
        GLYPHA_REQUIRE(server_->start().has_value());
    }

    ~RunningServer() {
        server_->request_stop();
        static_cast<void>(server_->join());
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return server_->port();
    }

  private:
    std::unique_ptr<glyphastore::server::Server> server_;
};

auto key_for_worker(const std::size_t worker, const std::size_t worker_count) -> std::string {
    for (std::size_t candidate = 0;; ++candidate) {
        auto key = "client-worker-" + std::to_string(worker) + '-' + std::to_string(candidate);
        if (glyphastore::route_worker(glyphastore::hash_key(key), worker_count) == worker) {
            return key;
        }
    }
}

} // namespace

GLYPHA_TEST("C++ client bootstraps every worker and handles binary cache operations") {
    RunningServer server;
    auto connected = glyphastore::client::Client::connect({.port = server.port(), .maximum_frame_bytes = 64});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);
    GLYPHA_REQUIRE(client.healthy());
    GLYPHA_REQUIRE(client.worker_count() == 2);
    GLYPHA_REQUIRE(client.routing_epoch() != 0);

    const std::array<std::byte, 4> ping_payload{std::byte{0}, std::byte{1}, std::byte{0xFE}, std::byte{0xFF}};
    auto pong = client.ping(ping_payload);
    GLYPHA_REQUIRE(pong.has_value());
    GLYPHA_REQUIRE(*pong == std::vector<std::byte>(ping_payload.begin(), ping_payload.end()));

    const std::array<std::byte, 5> key{std::byte{'k'}, std::byte{0}, std::byte{'e'}, std::byte{'y'},
                                       std::byte{0xFF}};
    const std::array<std::byte, 5> value{std::byte{0}, std::byte{'v'}, std::byte{'a'}, std::byte{'l'},
                                         std::byte{0xFE}};
    const auto stored = client.put(key, value);
    GLYPHA_REQUIRE(stored.committed());
    GLYPHA_REQUIRE(!stored.error.has_value());

    auto loaded = client.get(key);
    GLYPHA_REQUIRE(loaded.has_value());
    GLYPHA_REQUIRE(*loaded == std::vector<std::byte>(value.begin(), value.end()));

    const auto erased = client.erase(key);
    GLYPHA_REQUIRE(erased.committed());
    auto missing = client.get(key);
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE(missing.error().category == "not_found");
    GLYPHA_REQUIRE(missing.error().wire_status.has_value());
    GLYPHA_REQUIRE(*missing.error().wire_status
                   == static_cast<std::uint16_t>(glyphastore::server::ResponseStatus::not_found));
    GLYPHA_REQUIRE(missing.error().retryability == "new_attempt");
    GLYPHA_REQUIRE(missing.error().operation == "get");

    const std::array<std::byte, 32> oversized_value{};
    const auto oversized = client.put(key, oversized_value);
    GLYPHA_REQUIRE(oversized.outcome == glyphastore::client::MutationOutcome::rejected);
    GLYPHA_REQUIRE(oversized.error.has_value());
    GLYPHA_REQUIRE(oversized.error->code == glyphastore::ErrorCode::record_too_large);
    GLYPHA_REQUIRE(oversized.error->category == "invalid_argument");
    GLYPHA_REQUIRE(oversized.error->retryability == "never");
    GLYPHA_REQUIRE(oversized.error->operation == "put");
    GLYPHA_REQUIRE(oversized.error->bytes_sent == 0);

    client.close();
    GLYPHA_REQUIRE(!client.healthy());
    GLYPHA_REQUIRE(!client.get("after-close").has_value());
}

GLYPHA_TEST("C++ client safely shares worker-bound connections between threads") {
    constexpr std::size_t workers = 2;
    RunningServer server{workers};
    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);

    std::array<std::string, workers> keys;
    for (std::size_t worker = 0; worker < workers; ++worker) {
        keys[worker] = key_for_worker(worker, workers);
    }
    std::atomic<bool> failed{};
    std::vector<std::thread> threads;
    for (std::size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&, worker] {
            for (std::size_t iteration = 0; iteration < 64; ++iteration) {
                const auto value = "value-" + std::to_string(worker) + '-' + std::to_string(iteration);
                if (!client.put(keys[worker], value).committed()) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
                auto loaded = client.get(keys[worker]);
                if (!loaded || text(*loaded) != value) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
}

GLYPHA_TEST("C++ client pipeline preserves order and enforces one Worker") {
    constexpr std::size_t workers = 2;
    RunningServer server{workers};
    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);

    const auto key = key_for_worker(1, workers);
    GLYPHA_REQUIRE(client.worker_for(key) == 1);
    std::vector<std::string> values;
    std::vector<glyphastore::client::PipelineRequest> requests;
    values.reserve(32);
    requests.reserve(64);
    for (std::size_t index = 0; index < 32; ++index) {
        values.push_back("pipeline-value-" + std::to_string(index));
        requests.push_back({.opcode = glyphastore::client::PipelineOpcode::put,
                            .key = bytes(key),
                            .value = bytes(values.back())});
        requests.push_back({.opcode = glyphastore::client::PipelineOpcode::get, .key = bytes(key)});
    }
    auto executed = client.execute_pipeline(requests);
    GLYPHA_REQUIRE(executed.has_value());
    GLYPHA_REQUIRE(executed->size() == requests.size());
    for (std::size_t index = 0; index < values.size(); ++index) {
        GLYPHA_REQUIRE((*executed)[index * 2U].succeeded());
        GLYPHA_REQUIRE((*executed)[index * 2U + 1U].succeeded());
        GLYPHA_REQUIRE(text((*executed)[index * 2U + 1U].value) == values[index]);
    }

    const auto other_key = key_for_worker(0, workers);
    const std::array mixed{
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key)},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(other_key)},
    };
    auto rejected = client.execute_pipeline(mixed);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_argument);

    const std::array invalid_opcode{
        glyphastore::client::PipelineRequest{.opcode = static_cast<glyphastore::client::PipelineOpcode>(255),
                                             .key = bytes(key)},
    };
    rejected = client.execute_pipeline(invalid_opcode);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_argument);
    GLYPHA_REQUIRE(client.healthy());
}

GLYPHA_TEST("C++ client batch groups Workers and restores caller order") {
    constexpr std::size_t workers = 2;
    RunningServer server{workers};
    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);

    const auto key0 = key_for_worker(0, workers);
    const auto key1 = key_for_worker(1, workers);
    GLYPHA_REQUIRE(client.worker_for(key0) == 0);
    GLYPHA_REQUIRE(client.worker_for(key1) == 1);

    const std::array requests{
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::put,
                                             .key = bytes(key1),
                                             .value = bytes("v1")},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::put,
                                             .key = bytes(key0),
                                             .value = bytes("v0")},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key1)},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key0)},
    };
    auto executed = client.execute_batch(requests);
    GLYPHA_REQUIRE(executed.has_value());
    GLYPHA_REQUIRE(executed->size() == requests.size());
    for (const auto& response : *executed) {
        GLYPHA_REQUIRE(response.succeeded());
    }
    GLYPHA_REQUIRE(text((*executed)[2].value) == "v1");
    GLYPHA_REQUIRE(text((*executed)[3].value) == "v0");
    client.close();

    auto limited = glyphastore::client::Client::connect(
        {.port = server.port(), .maximum_pipeline_requests = 1});
    GLYPHA_REQUIRE(limited.has_value());
    auto limited_client = std::move(*limited);
    const std::array oversized{
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key0)},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key0)},
    };
    auto rejected = limited_client.execute_batch(oversized);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::resource_exhausted);
}

GLYPHA_TEST("C++ client pipeline preserves indeterminate mutation outcomes after disconnect") {
    DisconnectingPipelineServer server;
    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);

    const std::array requests{
        glyphastore::client::PipelineRequest{
            .opcode = glyphastore::client::PipelineOpcode::put, .key = bytes("key"), .value = bytes("value")},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes("key")},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::erase,
                                             .key = bytes("key")},
    };
    auto executed = client.execute_pipeline(requests);
    GLYPHA_REQUIRE(executed.has_value());
    GLYPHA_REQUIRE(executed->size() == requests.size());
    GLYPHA_REQUIRE((*executed)[0].outcome == glyphastore::client::PipelineOutcome::indeterminate);
    GLYPHA_REQUIRE((*executed)[1].outcome == glyphastore::client::PipelineOutcome::failed);
    GLYPHA_REQUIRE((*executed)[2].outcome == glyphastore::client::PipelineOutcome::indeterminate);
    GLYPHA_REQUIRE((*executed)[0].error.has_value());
    GLYPHA_REQUIRE((*executed)[1].error.has_value());
    GLYPHA_REQUIRE((*executed)[2].error.has_value());
    GLYPHA_REQUIRE(client.healthy());
}

GLYPHA_TEST("C++ client rejects non-positive request timeout override") {
    RunningServer server;
    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto client = std::move(*connected);
    auto rejected = client.get("key", {.timeout = std::chrono::milliseconds{0}});
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("C++ client rejects invalid configuration before network I/O") {
    auto invalid = glyphastore::client::Client::connect({.port = 0});
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().code == glyphastore::ErrorCode::invalid_argument);

    invalid = glyphastore::client::Client::connect({.maximum_pipeline_requests = 0});
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().code == glyphastore::ErrorCode::invalid_argument);

    invalid = glyphastore::client::Client::connect({.maximum_pipeline_bytes = 1});
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().code == glyphastore::ErrorCode::invalid_argument);
}
