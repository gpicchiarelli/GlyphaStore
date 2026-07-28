#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string_view>

namespace glyphastore::server {

// Phase 6 security audit trail: authn/authz decisions and TLS handshake failures.
// Emits JSON-lines on stderr (no key/value payloads, no PEM bytes). Counters feed STATS.
struct SecurityAuditStats final {
    std::uint64_t auth_accepts{};
    std::uint64_t auth_denies{};
    std::uint64_t authz_denies{};
    std::uint64_t tls_errors{};
};

class SecurityAudit final {
  public:
    // emit_events: write JSON-lines (typically --log-format json or --secure-profile).
    // quiet: suppress auth_accept only; denies and TLS errors always emit when enabled.
    SecurityAudit(bool emit_events, bool quiet) noexcept;

    void auth_accept(std::string_view principal) noexcept;
    void auth_deny(std::string_view reason, std::string_view principal = {}) noexcept;
    void authz_deny(std::string_view principal, std::string_view opcode,
                    std::string_view reason) noexcept;
    void tls_error(std::string_view reason) noexcept;

    [[nodiscard]] auto stats() const noexcept -> SecurityAuditStats;
    [[nodiscard]] auto emit_events() const noexcept -> bool {
        return emit_events_;
    }

  private:
    void write_event(std::string_view event, std::string_view principal, std::string_view opcode,
                     std::string_view reason, std::string_view decision) noexcept;

    bool emit_events_{};
    bool quiet_{};
    mutable std::mutex mutex_;
    std::atomic<std::uint64_t> auth_accepts_{};
    std::atomic<std::uint64_t> auth_denies_{};
    std::atomic<std::uint64_t> authz_denies_{};
    std::atomic<std::uint64_t> tls_errors_{};
};

} // namespace glyphastore::server
