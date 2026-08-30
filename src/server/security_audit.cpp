#include "glyphastore/server/security_audit.hpp"

#include <chrono>
#include <iostream>
#include <string>

namespace glyphastore::server {
namespace {

constexpr std::size_t kMaxLoggedFieldBytes = 256U;

[[nodiscard]] auto json_escape(const std::string_view text) -> std::string {
    std::string out;
    const auto limit = text.size() > kMaxLoggedFieldBytes ? kMaxLoggedFieldBytes : text.size();
    out.reserve(limit + 16U);
    for (std::size_t index = 0; index < limit; ++index) {
        const char ch = text[index];
        switch (ch) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(ch) < 0x20U) {
                constexpr char kDigits[] = "0123456789abcdef";
                out += "\\u00";
                out += kDigits[(static_cast<unsigned char>(ch) >> 4) & 0xf];
                out += kDigits[static_cast<unsigned char>(ch) & 0xf];
            } else {
                out.push_back(ch);
            }
            break;
        }
    }
    if (text.size() > kMaxLoggedFieldBytes) {
        out += "...";
    }
    return out;
}

[[nodiscard]] auto epoch_seconds() noexcept -> std::int64_t {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

} // namespace

SecurityAudit::SecurityAudit(const bool emit_events, const bool quiet) noexcept
    : emit_events_{emit_events}, quiet_{quiet} {}

void SecurityAudit::write_event(const std::string_view event, const std::string_view principal,
                                const std::string_view opcode, const std::string_view reason,
                                const std::string_view decision) noexcept {
    if (!emit_events_) {
        return;
    }
    try {
        std::string line;
        line.reserve(256U);
        line += "{\"ts\":";
        line += std::to_string(epoch_seconds());
        line += ",\"event\":\"";
        line += event;
        line += '"';
        if (!decision.empty()) {
            line += ",\"decision\":\"";
            line += decision;
            line += '"';
        }
        if (!principal.empty()) {
            line += ",\"principal\":\"";
            line += json_escape(principal);
            line += '"';
        }
        if (!opcode.empty()) {
            line += ",\"opcode\":\"";
            line += json_escape(opcode);
            line += '"';
        }
        if (!reason.empty()) {
            line += ",\"reason\":\"";
            line += json_escape(reason);
            line += '"';
        }
        line += "}\n";
        const std::lock_guard lock{mutex_};
        std::cerr << line;
    } catch (...) {
        // Audit must never throw into the reactor accept/request path.
        return;
    }
}

void SecurityAudit::auth_accept(const std::string_view principal) noexcept {
    auth_accepts_.fetch_add(1, std::memory_order_relaxed);
    if (quiet_) {
        return;
    }
    write_event("auth", principal, {}, {}, "accept");
}

void SecurityAudit::auth_deny(const std::string_view reason, const std::string_view principal) noexcept {
    auth_denies_.fetch_add(1, std::memory_order_relaxed);
    write_event("auth", principal, {}, reason, "deny");
}

void SecurityAudit::authz_deny(const std::string_view principal, const std::string_view opcode,
                               const std::string_view reason) noexcept {
    authz_denies_.fetch_add(1, std::memory_order_relaxed);
    write_event("authz", principal, opcode, reason, "deny");
}

void SecurityAudit::tls_error(const std::string_view reason) noexcept {
    tls_errors_.fetch_add(1, std::memory_order_relaxed);
    write_event("tls", {}, {}, reason, "error");
}

auto SecurityAudit::stats() const noexcept -> SecurityAuditStats {
    return SecurityAuditStats{
        .auth_accepts = auth_accepts_.load(std::memory_order_relaxed),
        .auth_denies = auth_denies_.load(std::memory_order_relaxed),
        .authz_denies = authz_denies_.load(std::memory_order_relaxed),
        .tls_errors = tls_errors_.load(std::memory_order_relaxed),
    };
}

} // namespace glyphastore::server
