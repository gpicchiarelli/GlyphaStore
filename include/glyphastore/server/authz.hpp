#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/protocol.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>

namespace glyphastore::server {

enum class Capability : std::uint8_t {
    none = 0,
    read = 1 << 0,
    write = 1 << 1,
    admin = 1 << 2,
};

[[nodiscard]] constexpr auto operator|(Capability left, Capability right) noexcept -> Capability {
    return static_cast<Capability>(static_cast<std::uint8_t>(left) | static_cast<std::uint8_t>(right));
}

[[nodiscard]] constexpr auto operator&(Capability left, Capability right) noexcept -> Capability {
    return static_cast<Capability>(static_cast<std::uint8_t>(left) & static_cast<std::uint8_t>(right));
}

[[nodiscard]] constexpr auto has_capability(Capability set, Capability needle) noexcept -> bool {
    return (set & needle) == needle;
}

[[nodiscard]] constexpr auto normalize_capabilities(Capability set) noexcept -> Capability {
    if (has_capability(set, Capability::admin)) {
        set = set | Capability::write | Capability::read;
    } else if (has_capability(set, Capability::write)) {
        set = set | Capability::read;
    }
    return set;
}

[[nodiscard]] auto parse_capability_token(std::string_view token) -> Result<Capability>;
[[nodiscard]] auto capability_name(Capability capability) noexcept -> std::string_view;

class AuthzPolicy final {
  public:
    AuthzPolicy() = default;

    [[nodiscard]] static auto load_file(const std::filesystem::path& path) -> Result<AuthzPolicy>;
    [[nodiscard]] static auto parse(std::string_view text, std::string_view where = "authz map")
        -> Result<AuthzPolicy>;

    [[nodiscard]] auto enabled() const noexcept -> bool {
        return enabled_;
    }
    [[nodiscard]] auto empty() const noexcept -> bool {
        return principals_.empty();
    }
    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return principals_.size();
    }
    [[nodiscard]] auto capabilities_for(std::string_view principal) const noexcept -> Capability;

    void set_enabled(bool enabled) noexcept {
        enabled_ = enabled;
    }
    void bind(std::string principal, Capability capabilities);

  private:
    bool enabled_{};
    std::unordered_map<std::string, Capability> principals_{};
};

[[nodiscard]] auto required_capability(RequestOpcode opcode) noexcept -> Capability;
[[nodiscard]] auto authorize_opcode(const AuthzPolicy& policy, Capability granted,
                                    RequestOpcode opcode) noexcept -> bool;

} // namespace glyphastore::server
