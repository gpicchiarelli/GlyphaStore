#include "glyphastore/server/daemon_log.hpp"
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

GLYPHA_TEST("daemon log format parsing rejects unknown values") {
    const auto parsed = glyphastore::server::parse_daemon_log_format("yaml");
    GLYPHA_REQUIRE(!parsed.has_value());
    GLYPHA_REQUIRE(parsed.error().message.find("human or json") != std::string::npos);
}

GLYPHA_TEST("daemon log format parsing accepts human and json") {
    const auto human = glyphastore::server::parse_daemon_log_format("human");
    GLYPHA_REQUIRE(human.has_value());
    GLYPHA_REQUIRE(*human == glyphastore::server::DaemonLogFormat::human);

    const auto json = glyphastore::server::parse_daemon_log_format("JSON");
    GLYPHA_REQUIRE(json.has_value());
    GLYPHA_REQUIRE(*json == glyphastore::server::DaemonLogFormat::json);
}

GLYPHA_TEST("daemon log json emits bounded lifecycle events") {
    CapturedStderr capture;
    const glyphastore::server::DaemonLog log{glyphastore::server::DaemonLogFormat::json, "glyphastored",
                                             false};
    log.emit_start();
    log.emit_listen("127.0.0.1", 7379, 0, 2, "volatile");
    log.emit_ready(true);
    log.emit_ready(false, glyphastore::server::ReadyLossReason::shutting_down);
    log.emit_maintenance_emergency(glyphastore::MaintenancePressureLevel::emergency);
    log.emit_maintenance_fault(glyphastore::MaintenanceState::faulted, "internal_error",
                               "maintenance scheduler failed");
    log.emit_shutdown_begin(15, false);
    log.emit_shutdown_drain_begin(30000);
    log.emit_shutdown_drain_end(false, false);
    log.emit_stopped(15);

    const auto output = capture.text();
    GLYPHA_REQUIRE(output.find("\"event\":\"start\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"listen\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"bind\":\"127.0.0.1\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"cleartext_port\":7379") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"ready\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"reason\":\"shutting_down\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"maintenance_emergency\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"maintenance_pressure\":\"emergency\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"maintenance_fault\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"shutdown_drain_begin\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"drain_ms\":30000") != std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"stopped\"") != std::string::npos);
    GLYPHA_REQUIRE(output.find("tls-cert") == std::string::npos);
}

GLYPHA_TEST("daemon log quiet suppresses start listen and stopped json events") {
    CapturedStderr capture;
    const glyphastore::server::DaemonLog log{glyphastore::server::DaemonLogFormat::json, "glyphastored",
                                             true};
    log.emit_start();
    log.emit_listen("127.0.0.1", 7379, 0, 1, "volatile");
    log.emit_ready(false, glyphastore::server::ReadyLossReason::maintenance_emergency);
    log.emit_stopped(0);

    const auto output = capture.text();
    GLYPHA_REQUIRE(output.find("\"event\":\"start\"") == std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"listen\"") == std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"stopped\"") == std::string::npos);
    GLYPHA_REQUIRE(output.find("\"event\":\"ready\"") != std::string::npos);
}

GLYPHA_TEST("daemon log human format emits no structured lines") {
    CapturedStderr capture;
    const glyphastore::server::DaemonLog log{glyphastore::server::DaemonLogFormat::human, "glyphastored",
                                             false};
    log.emit_listen("127.0.0.1", 7379, 0, 1, "volatile");
    GLYPHA_REQUIRE(capture.text().empty());
}

GLYPHA_TEST("daemon log json escapes control characters and truncates long fields") {
    CapturedStderr capture;
    const glyphastore::server::DaemonLog log{glyphastore::server::DaemonLogFormat::json, "glyphastored",
                                             false};
    std::string message(300U, 'x');
    message[10] = '\n';
    log.emit_executor_failure("io_error", message);

    const auto output = capture.text();
    GLYPHA_REQUIRE(output.find("\\n") != std::string::npos);
    GLYPHA_REQUIRE(output.find("...") != std::string::npos);
}

GLYPHA_TEST("daemon ready loss classification names are stable") {
    GLYPHA_REQUIRE(glyphastore::server::ready_loss_reason_name(
                       glyphastore::server::ReadyLossReason::maintenance_emergency) ==
                   "maintenance_emergency");
    GLYPHA_REQUIRE(glyphastore::server::ready_loss_reason_name(
                       glyphastore::server::ReadyLossReason::maintenance_fault) == "maintenance_fault");
}
