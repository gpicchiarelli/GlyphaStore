#include "glyphastore/server/security_audit.hpp"
#include "test.hpp"

#include <sstream>
#include <string>

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
