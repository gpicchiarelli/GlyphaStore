#include "glyphastore/server/daemon_config.hpp"
#include "test.hpp"

#include <array>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {

class ConfigTemporaryDirectory final {
  public:
    ConfigTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-daemon-config-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~ConfigTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

void write_file(const std::filesystem::path& path, const std::string_view contents) {
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    GLYPHA_REQUIRE(static_cast<bool>(out));
    out << contents;
    GLYPHA_REQUIRE(static_cast<bool>(out));
}

template <std::size_t Size>
[[nodiscard]] auto parse(const std::array<const char*, Size>& input,
                         glyphastore::server::DaemonEnvironmentLookup getenv_fn = {})
    -> glyphastore::Result<glyphastore::server::DaemonOptions> {
    std::array<char*, Size> arguments{};
    for (std::size_t index = 0; index < Size; ++index) {
        arguments[index] = const_cast<char*>(input[index]);
    }
    return glyphastore::server::parse_daemon_options(static_cast<int>(arguments.size()), arguments.data(),
                                                     std::move(getenv_fn));
}

} // namespace

GLYPHA_TEST("daemon config environment names mirror long options") {
    GLYPHA_REQUIRE(glyphastore::server::environment_name_for_option("port") == "GLYPHASTORE_PORT");
    GLYPHA_REQUIRE(glyphastore::server::environment_name_for_option("data-dir") == "GLYPHASTORE_DATA_DIR");
    GLYPHA_REQUIRE(glyphastore::server::environment_name_for_option("max-store-bytes") ==
                   "GLYPHASTORE_MAX_STORE_BYTES");
}

GLYPHA_TEST("daemon config file rejects unknown keys and duplicates") {
    ConfigTemporaryDirectory temporary;
    const auto unknown = temporary.path() / "unknown.conf";
    write_file(unknown, "port = 1\nnot-a-real-key = 1\n");
    const auto unknown_result = glyphastore::server::load_daemon_config_file(unknown);
    GLYPHA_REQUIRE(!unknown_result.has_value());
    GLYPHA_REQUIRE(unknown_result.error().message.find("unknown setting") != std::string::npos);

    const auto duplicate = temporary.path() / "duplicate.conf";
    write_file(duplicate, "port = 1\nport = 2\n");
    const auto duplicate_result = glyphastore::server::load_daemon_config_file(duplicate);
    GLYPHA_REQUIRE(!duplicate_result.has_value());
    GLYPHA_REQUIRE(duplicate_result.error().message.find("duplicates") != std::string::npos);
}

GLYPHA_TEST("daemon config precedence is file then env then CLI") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "daemon.conf";
    write_file(config, "port = 1001\nworkers = 2\nquiet = true\n");

    std::unordered_map<std::string, std::string> environment{
        {"GLYPHASTORE_PORT", "2002"},
        {"GLYPHASTORE_WORKERS", "3"},
    };
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };

    const auto config_arg = config.string();
    const std::array arguments{"glyphastored", "--config", config_arg.c_str(), "--port", "3003"};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.port == 3003);
    GLYPHA_REQUIRE(parsed->server.worker_count == 3);
    GLYPHA_REQUIRE(parsed->quiet);
}

GLYPHA_TEST("daemon config CLI overrides env durable settings") {
    std::unordered_map<std::string, std::string> environment{
        {"GLYPHASTORE_STORAGE_MODE", "durable-sync"},
        {"GLYPHASTORE_DATA_DIR", "/tmp/from-env"},
    };
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };
    const std::array arguments{"glyphastored", "--storage-mode", "volatile"};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(!parsed.has_value());
    GLYPHA_REQUIRE(parsed.error().message.find("require a durable") != std::string::npos);
}

GLYPHA_TEST("daemon config env can clear a file flag") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "quiet.conf";
    write_file(config, "quiet = true\nport = 7370\n");
    std::unordered_map<std::string, std::string> environment{{"GLYPHASTORE_QUIET", "false"}};
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };
    const auto config_arg = config.string();
    const std::array arguments{"glyphastored", "--config", config_arg.c_str()};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(!parsed->quiet);
    GLYPHA_REQUIRE(parsed->server.port == 7370);
}

GLYPHA_TEST("daemon config GLYPHASTORE_CONFIG selects the file") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "from-env.conf";
    write_file(config, "port = 4242\n");
    std::unordered_map<std::string, std::string> environment{{"GLYPHASTORE_CONFIG", config.string()}};
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };
    const std::array arguments{"glyphastored"};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.port == 4242);
}
