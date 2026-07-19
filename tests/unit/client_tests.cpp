#include "glyphastore/client/client.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/server/server.hpp"
#include "test.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

auto text(const std::span<const std::byte> value) -> std::string {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

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

    const std::array<std::byte, 32> oversized_value{};
    const auto oversized = client.put(key, oversized_value);
    GLYPHA_REQUIRE(oversized.outcome == glyphastore::client::MutationOutcome::rejected);
    GLYPHA_REQUIRE(oversized.error.has_value());
    GLYPHA_REQUIRE(oversized.error->code == glyphastore::ErrorCode::record_too_large);

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

GLYPHA_TEST("C++ client rejects invalid configuration before network I/O") {
    auto invalid = glyphastore::client::Client::connect({.port = 0});
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().code == glyphastore::ErrorCode::invalid_argument);
}
