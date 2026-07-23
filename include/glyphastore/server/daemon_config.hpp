#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/daemon_log.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/store/store.hpp"

#include <cstddef>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace glyphastore::cli {
struct OptionSpec;
}

namespace glyphastore::server {

struct DaemonOptions {
    ReactorConfig server{};
    StoreConfig store{
        .maintenance = {.mode = MaintenanceMode::background},
    };
    // Empty when no named deployment profile was selected.
    std::string deployment_profile{};
    bool show_help{};
    bool show_version{};
    bool show_dump_config{};
    bool quiet{};
    DaemonLogFormat log_format{DaemonLogFormat::human};
};

// Optional environment lookup for tests. Returning nullopt means unset.
using DaemonEnvironmentLookup =
    std::function<std::optional<std::string>(std::string_view name)>;

[[nodiscard]] auto daemon_option_specs() noexcept -> std::span<const cli::OptionSpec>;

[[nodiscard]] auto storage_mode_name(StorageMode mode) noexcept -> std::string_view;

// Stable ASCII dump of the fully resolved effective configuration (paths only for TLS files).
[[nodiscard]] auto format_daemon_config_dump(const DaemonOptions& options) -> std::string;

// Precedence: defaults < deployment profile < config file < environment < CLI.
// --config / GLYPHASTORE_CONFIG select the file; the file cannot set config=.
// --profile / GLYPHASTORE_PROFILE / profile= select dev, embedded, or production.
[[nodiscard]] auto parse_daemon_options(int argc, char* const argv[],
                                        DaemonEnvironmentLookup getenv_fn = {})
    -> Result<DaemonOptions>;

[[nodiscard]] auto load_daemon_config_file(const std::filesystem::path& path)
    -> Result<std::map<std::string, std::string, std::less<>>>;

[[nodiscard]] auto load_daemon_environment(const DaemonEnvironmentLookup& getenv_fn)
    -> Result<std::map<std::string, std::string, std::less<>>>;

[[nodiscard]] auto environment_name_for_option(std::string_view long_name) -> std::string;

} // namespace glyphastore::server
