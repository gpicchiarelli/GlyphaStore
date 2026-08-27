#include "glyphastore/server/security_audit.hpp"
#include "test.hpp"

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

class CapturedStderr final {
  public:
    CapturedStderr() {
        old_ = std::cerr.rdbuf(capture_.rdbuf());
    }

    ~CapturedStderr() {
        std::cerr.rdbuf(old_);
    }

    [[nodiscard]] auto text() const -> std::string {
        return capture_.str();
    }

  private:
    std::stringstream capture_{};
    std::streambuf* old_{nullptr};
};

} // namespace

GLYPHA_TEST("security audit emits accept deny authz and tls json without secrets") {
    CapturedStderr capture;
    glyphastore::server::SecurityAudit audit{true, false};
    audit.auth_accept("reader.example");
    audit.auth_deny("handshake_failed", "bad.example");
    audit.authz_deny("reader.example", "put", "capability_denied");
    audit.tls_error("TLS handshake timed out");

    const auto output = capture.text();
    GLYPHA_REQUIRE(output.find("\"event\":\"auth\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"decision\":\"accept\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"decision\":\"deny\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"principal\":\"reader.example\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"authz\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"opcode\":\"put\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"reason\":\"capability_denied\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"tls\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("BEGIN CERTIFICATE") == std::string::npos);
    GLYPHA_REQUIRE(output.find("PRIVATE KEY") == std::string::npos);

    const auto stats = audit.stats();
    GLYPHA_REQUIRE(stats.auth_accepts == 1);
    GLYPHA_REQUIRE(stats.auth_denies == 1);
    GLYPHA_REQUIRE(stats.authz_denies == 1);
    GLYPHA_REQUIRE(stats.tls_errors == 1);
}

GLYPHA_TEST("security audit quiet suppresses accept events only") {
    CapturedStderr capture;
    glyphastore::server::SecurityAudit audit{true, true};
    audit.auth_accept("reader.example");
    audit.auth_deny("no_peer_cert");
    const auto output = capture.text();
    GLYPHA_REQUIRE(output.find("\"decision\":\"accept\"") == std::string::npos);
    GLYPHA_REQUIRE(output.find("\"decision\":\"deny\"") != std::string::npos);
    GLYPHA_REQUIRE(audit.stats().auth_accepts == 1);
    GLYPHA_REQUIRE(audit.stats().auth_denies == 1);
}

GLYPHA_TEST("security audit disabled emits no lines but still counts") {
    CapturedStderr capture;
    glyphastore::server::SecurityAudit audit{false, false};
    audit.auth_accept("reader.example");
    audit.authz_deny("reader.example", "get", "capability_denied");
    GLYPHA_REQUIRE(capture.text().empty());
    GLYPHA_REQUIRE(audit.stats().auth_accepts == 1);
    GLYPHA_REQUIRE(audit.stats().authz_denies == 1);
}

GLYPHA_TEST("security audit concurrent deny storm keeps counters coherent") {
    glyphastore::server::SecurityAudit audit{false, false};
    constexpr int kThreads = 8;
    constexpr int kPerThread = 128;
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int index = 0; index < kPerThread; ++index) {
                audit.authz_deny("storm.example", "put", "capability_denied");
                audit.auth_deny("handshake_failed", "storm.example");
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    const auto stats = audit.stats();
    constexpr std::uint64_t kExpected = static_cast<std::uint64_t>(kThreads) * kPerThread;
    GLYPHA_REQUIRE(stats.auth_denies == kExpected);
    GLYPHA_REQUIRE(stats.authz_denies == kExpected);
    GLYPHA_REQUIRE(stats.auth_accepts == 0);
    GLYPHA_REQUIRE(stats.tls_errors == 0);
}
