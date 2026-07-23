#include "glyphastore/server/authz.hpp"

#include <cctype>
#include <fstream>
#include <sstream>
#include <string>

namespace glyphastore::server {
namespace {

[[nodiscard]] auto trim(std::string_view text) noexcept -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

[[nodiscard]] auto split_capabilities(std::string_view text) -> Result<Capability> {
    Capability set = Capability::none;
    std::size_t begin = 0;
    while (begin <= text.size()) {
        const auto comma = text.find(',', begin);
        const auto piece = trim(
            text.substr(begin, comma == std::string_view::npos ? std::string_view::npos : comma - begin));
        if (!piece.empty()) {
            auto parsed = parse_capability_token(piece);
            if (!parsed) {
                return unexpected(parsed.error());
            }
            set = set | *parsed;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        begin = comma + 1;
    }
    if (set == Capability::none) {
        return fail(ErrorCode::invalid_argument, "authz map entry requires at least one capability");
    }
    return normalize_capabilities(set);
}

} // namespace

auto parse_capability_token(const std::string_view token) -> Result<Capability> {
    if (token == "read") {
        return Capability::read;
    }
    if (token == "write") {
        return Capability::write;
    }
    if (token == "admin") {
        return Capability::admin;
    }
    return fail(ErrorCode::invalid_argument,
                "unknown capability '" + std::string{token} + "' (expected read, write, or admin)");
}

auto capability_name(const Capability capability) noexcept -> std::string_view {
    switch (capability) {
    case Capability::read:
        return "read";
    case Capability::write:
        return "write";
    case Capability::admin:
        return "admin";
    case Capability::none:
        return "none";
    }
    return "unknown";
}

auto AuthzPolicy::parse(const std::string_view text, const std::string_view where) -> Result<AuthzPolicy> {
    AuthzPolicy policy;
    policy.enabled_ = true;
    std::size_t line_number = 0;
    std::size_t offset = 0;
    while (offset <= text.size()) {
        const auto end = text.find('\n', offset);
        auto line = text.substr(offset, end == std::string_view::npos ? std::string_view::npos : end - offset);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        ++line_number;
        offset = end == std::string_view::npos ? text.size() + 1 : end + 1;

        const auto trimmed = trim(line);
        if (trimmed.empty() || trimmed.front() == '#') {
            continue;
        }
        const auto space = trimmed.find_first_of(" \t");
        if (space == std::string_view::npos) {
            return fail(ErrorCode::invalid_argument,
                        std::string{where} + " line " + std::to_string(line_number) +
                            ": expected 'principal capability[,capability...]'");
        }
        const auto principal = trim(trimmed.substr(0, space));
        const auto caps_text = trim(trimmed.substr(space + 1));
        if (principal.empty()) {
            return fail(ErrorCode::invalid_argument,
                        std::string{where} + " line " + std::to_string(line_number) +
                            ": principal must not be empty");
        }
        auto caps = split_capabilities(caps_text);
        if (!caps) {
            return fail(ErrorCode::invalid_argument,
                        std::string{where} + " line " + std::to_string(line_number) + ": " +
                            caps.error().message);
        }
        if (!policy.principals_.emplace(std::string{principal}, *caps).second) {
            return fail(ErrorCode::invalid_argument,
                        std::string{where} + " line " + std::to_string(line_number) +
                            ": duplicate principal '" + std::string{principal} + "'");
        }
    }
    return policy;
}

auto AuthzPolicy::load_file(const std::filesystem::path& path) -> Result<AuthzPolicy> {
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec) || ec) {
        return fail(ErrorCode::invalid_argument,
                    "authz map is missing or not a regular file: " + path.string());
    }
    std::ifstream input{path};
    if (!input) {
        return fail(ErrorCode::io_error, "cannot open authz map: " + path.string());
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (!input && !input.eof()) {
        return fail(ErrorCode::io_error, "cannot read authz map: " + path.string());
    }
    return parse(buffer.str(), path.string());
}

auto AuthzPolicy::capabilities_for(const std::string_view principal) const noexcept -> Capability {
    if (!enabled_ || principal.empty()) {
        return Capability::none;
    }
    const auto found = principals_.find(std::string{principal});
    if (found == principals_.end()) {
        return Capability::none;
    }
    return found->second;
}

void AuthzPolicy::bind(std::string principal, const Capability capabilities) {
    enabled_ = true;
    principals_[std::move(principal)] = normalize_capabilities(capabilities);
}

auto required_capability(const RequestOpcode opcode) noexcept -> Capability {
    switch (opcode) {
    case RequestOpcode::init:
    case RequestOpcode::bind_worker:
    case RequestOpcode::health:
    case RequestOpcode::ready:
        return Capability::none;
    case RequestOpcode::ping:
    case RequestOpcode::get:
    case RequestOpcode::stats:
        return Capability::read;
    case RequestOpcode::put:
    case RequestOpcode::erase:
        return Capability::write;
    }
    return Capability::admin;
}

auto authorize_opcode(const AuthzPolicy& policy, const Capability granted,
                      const RequestOpcode opcode) noexcept -> bool {
    if (!policy.enabled()) {
        return true;
    }
    const auto needed = required_capability(opcode);
    if (needed == Capability::none) {
        return true;
    }
    return has_capability(granted, needed);
}

} // namespace glyphastore::server
