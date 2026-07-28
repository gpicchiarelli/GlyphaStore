#include "experimental/paired_reactor.hpp"
#include "glyphastore/client/client.hpp"
#include "test.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

class RunningPairedReactor final {
  public:
    RunningPairedReactor() {
        auto created = glyphastore::experimental::PairedReactorPrototype::create();
        if (!created) {
            throw std::runtime_error{created.error().message};
        }
        reactor_ = std::move(*created);
        thread_ = std::jthread([this](const std::stop_token stop) {
            while (!stop.stop_requested()) {
                if (auto status = reactor_->run_once(10); !status) {
                    failed_.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    ~RunningPairedReactor() {
        thread_.request_stop();
        thread_.join();
        reactor_->stop_accepting();
        reactor_->close_all_connections();
    }

    [[nodiscard]] auto connect() const -> glyphastore::client::Client {
        auto connected = glyphastore::client::Client::connect(
            {.host = "127.0.0.1", .port = reactor_->port(), .request_timeout_ms = 5'000});
        if (!connected) {
            throw std::runtime_error{connected.error().message};
        }
        return std::move(*connected);
    }

    [[nodiscard]] auto reactor() noexcept -> glyphastore::experimental::PairedReactorPrototype& {
        return *reactor_;
    }

    [[nodiscard]] auto failed() const noexcept -> bool {
        return failed_.load(std::memory_order_acquire);
    }

  private:
    std::unique_ptr<glyphastore::experimental::PairedReactorPrototype> reactor_;
    std::jthread thread_;
    std::atomic_bool failed_{};
};

[[nodiscard]] auto bytes(const std::string& value) noexcept -> std::span<const std::byte> {
    return std::as_bytes(std::span{value.data(), value.size()});
}

[[nodiscard]] auto text(const std::vector<std::byte>& value) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("paired experimental Reactor serves TCP GET PUT ERASE with read-after-write") {
    RunningPairedReactor running;
    auto client = running.connect();
    GLYPHA_REQUIRE(client.worker_count() == 1);
    const auto put = client.put("reactor-key", "reactor-value");
    GLYPHA_REQUIRE(put.committed());
    const auto found = client.get("reactor-key");
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "reactor-value");
    GLYPHA_REQUIRE(client.erase("reactor-key").committed());
    GLYPHA_REQUIRE(!client.get("reactor-key"));
    GLYPHA_REQUIRE(!running.failed());
    const auto stats = running.reactor().stats();
    GLYPHA_REQUIRE(stats.gets >= 2);
    GLYPHA_REQUIRE(stats.mutations_submitted == 2);
    GLYPHA_REQUIRE(stats.mutation_completions == 2);
}

GLYPHA_TEST("paired experimental Reactor preserves ordered owner-bound TCP pipeline") {
    RunningPairedReactor running;
    auto client = running.connect();
    const std::string key{"pipeline-key"};
    const std::string first{"first"};
    const std::string second{"second"};
    const std::array requests{
        glyphastore::client::PipelineRequest{
            .opcode = glyphastore::client::PipelineOpcode::put, .key = bytes(key), .value = bytes(first)},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key)},
        glyphastore::client::PipelineRequest{
            .opcode = glyphastore::client::PipelineOpcode::put, .key = bytes(key), .value = bytes(second)},
        glyphastore::client::PipelineRequest{.opcode = glyphastore::client::PipelineOpcode::get,
                                             .key = bytes(key)},
    };
    auto responses = client.execute_pipeline(requests);
    GLYPHA_REQUIRE(responses.has_value());
    GLYPHA_REQUIRE(responses->size() == requests.size());
    for (const auto& response : *responses) {
        GLYPHA_REQUIRE(response.succeeded());
    }
    GLYPHA_REQUIRE(text((*responses)[1].value) == "first");
    GLYPHA_REQUIRE(text((*responses)[3].value) == "second");
    GLYPHA_REQUIRE(!running.failed());
}
