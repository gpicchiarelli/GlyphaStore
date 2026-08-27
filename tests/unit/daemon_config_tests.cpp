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
    GLYPHA_REQUIRE(glyphastore::server::environment_name_for_option("maintenance-max-copy-bytes-per-cycle") ==
                   "GLYPHASTORE_MAINTENANCE_MAX_COPY_BYTES_PER_CYCLE");
}

GLYPHA_TEST("daemon config CLI keeps workers distinct from maximum connections") {
    const std::array arguments{
        "glyphastored", "--workers", "4", "--max-connections", "42",
    };
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.worker_count == 4);
    GLYPHA_REQUIRE(parsed->server.maximum_connections == 42);

    const std::array invalid_workers{"glyphastored", "--workers", "257"};
    const auto invalid = parse(invalid_workers);
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().message.find("--shard-pairs") != std::string::npos);
}

GLYPHA_TEST("daemon config canonicalizes shard pairs and the workers compatibility alias") {
    const std::array canonical{"glyphastored", "--shard-pairs", "6"};
    const auto parsed = parse(canonical);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.worker_count == 6);

    const std::array ambiguous{"glyphastored", "--workers", "2", "--shard-pairs", "2"};
    const auto rejected = parse(ambiguous);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().message.find("same setting") != std::string::npos);
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

GLYPHA_TEST("daemon config rejects unknown deployment profiles before listen") {
    const std::array arguments{"glyphastored", "--profile", "staging"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(!parsed.has_value());
    GLYPHA_REQUIRE(parsed.error().message.find("unknown deployment profile") != std::string::npos);

    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "profile.conf";
    write_file(config, "profile = mystery\n");
    const auto config_arg = config.string();
    const std::array from_file{"glyphastored", "--config", config_arg.c_str()};
    const auto file_result = parse(from_file);
    GLYPHA_REQUIRE(!file_result.has_value());
    GLYPHA_REQUIRE(file_result.error().message.find("unknown deployment profile") != std::string::npos);
}

GLYPHA_TEST("daemon config deployment profile precedence is profile then file then env then CLI") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "profile-precedence.conf";
    write_file(config, "profile = dev\nport = 1001\nstorage-mode = durable-sync\n");

    std::unordered_map<std::string, std::string> environment{
        {"GLYPHASTORE_PROFILE", "embedded"},
        {"GLYPHASTORE_PORT", "2002"},
        {"GLYPHASTORE_STORAGE_MODE", "durable-group"},
    };
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };

    const auto config_arg = config.string();
    const std::array arguments{
        "glyphastored", "--config", config_arg.c_str(), "--profile", "production",
        "--port",       "3003",     "--storage-mode",   "volatile",
    };
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->deployment_profile == "production");
    GLYPHA_REQUIRE(parsed->server.port == 3003);
    GLYPHA_REQUIRE(parsed->store.storage_mode == glyphastore::StorageMode::volatile_memory);
}

GLYPHA_TEST("daemon config embedded profile applies constrained durable defaults") {
    ConfigTemporaryDirectory temporary;
    const auto data_dir = temporary.path() / "store";
    const auto data_arg = data_dir.string();
    const std::array arguments{
        "glyphastored", "--profile", "embedded", "--data-dir", data_arg.c_str(),
    };
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->deployment_profile == "embedded");
    GLYPHA_REQUIRE(parsed->store.storage_mode == glyphastore::StorageMode::durable_periodic);
    GLYPHA_REQUIRE(parsed->server.worker_count == 1);
    GLYPHA_REQUIRE(parsed->store.durable_limits.max_store_bytes == 1'073'741'824);
    GLYPHA_REQUIRE(parsed->store.durable_limits.max_segment_count == 32);
    GLYPHA_REQUIRE(parsed->store.maintenance.mode == glyphastore::MaintenanceMode::background);
    GLYPHA_REQUIRE(parsed->store.maintenance.max_copy_bytes_per_sec == 67'108'864U);
}

GLYPHA_TEST("daemon config production profile paces maintenance and explicit CLI overrides it") {
    ConfigTemporaryDirectory temporary;
    const auto data_arg = (temporary.path() / "store").string();
    const std::array profile_arguments{
        "glyphastored", "--profile", "production", "--data-dir", data_arg.c_str(),
    };
    const auto profiled = parse(profile_arguments);
    GLYPHA_REQUIRE(profiled.has_value());
    GLYPHA_REQUIRE(profiled->store.maintenance.max_copy_bytes_per_sec == 134'217'728U);

    const std::array override_arguments{
        "glyphastored", "--profile",      "production",
        "--data-dir",   data_arg.c_str(), "--maintenance-max-copy-bytes-per-sec",
        "268435456",
    };
    const auto overridden = parse(override_arguments);
    GLYPHA_REQUIRE(overridden.has_value());
    GLYPHA_REQUIRE(overridden->store.maintenance.max_copy_bytes_per_sec == 268'435'456U);
}

GLYPHA_TEST("daemon config dev profile disables maintenance and keeps volatile storage") {
    const std::array arguments{"glyphastored", "--profile", "dev"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->deployment_profile == "dev");
    GLYPHA_REQUIRE(parsed->store.storage_mode == glyphastore::StorageMode::volatile_memory);
    GLYPHA_REQUIRE(parsed->store.maintenance.mode == glyphastore::MaintenanceMode::disabled);
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

GLYPHA_TEST("daemon config resolves finite normal maintenance copy budget") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "maintenance.conf";
    write_file(config, "maintenance-max-copy-bytes-per-cycle = 64MiB\n");

    std::unordered_map<std::string, std::string> environment{
        {"GLYPHASTORE_MAINTENANCE_MAX_COPY_BYTES_PER_CYCLE", "96MiB"},
    };
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };

    const auto config_arg = config.string();
    const std::array arguments{
        "glyphastored", "--config", config_arg.c_str(), "--maintenance-max-copy-bytes-per-cycle", "128MiB",
    };
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->store.maintenance.max_copy_bytes_per_cycle ==
                   glyphastore::kDefaultMaintenanceMaxCopyBytesPerCycle);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("maintenance-max-copy-bytes-per-cycle=134217728\n") != std::string::npos);

    const std::array unlimited_arguments{
        "glyphastored",
        "--maintenance-max-copy-bytes-per-cycle",
        "0",
    };
    const auto unlimited = parse(unlimited_arguments);
    GLYPHA_REQUIRE(unlimited.has_value());
    GLYPHA_REQUIRE(unlimited->store.maintenance.max_copy_bytes_per_cycle == 0);
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

GLYPHA_TEST("daemon config resolves log-format from CLI env and file") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "log-format.conf";
    write_file(config, "log-format = human\nport = 7370\n");
    std::unordered_map<std::string, std::string> environment{{"GLYPHASTORE_LOG_FORMAT", "json"}};
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };
    const auto config_arg = config.string();
    const std::array arguments{"glyphastored", "--config", config_arg.c_str(), "--log-format", "json"};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->log_format == glyphastore::server::DaemonLogFormat::json);
    GLYPHA_REQUIRE(parsed->server.port == 7370);

    const std::array invalid{"glyphastored", "--log-format", "yaml"};
    const auto rejected = parse(invalid);
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().message.find("human or json") != std::string::npos);
}

GLYPHA_TEST("daemon dump-config prints resolved effective settings") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "dump.conf";
    write_file(config, "port = 1111\nworkers = 2\nquiet = true\n");
    std::unordered_map<std::string, std::string> environment{{"GLYPHASTORE_PORT", "2222"}};
    const auto getenv_fn = [&environment](const std::string_view name) -> std::optional<std::string> {
        const auto found = environment.find(std::string{name});
        if (found == environment.end()) {
            return std::nullopt;
        }
        return found->second;
    };
    const auto config_arg = config.string();
    const std::array arguments{"glyphastored",  "--config", config_arg.c_str(),
                               "--dump-config", "--port",   "3333"};
    const auto parsed = parse(arguments, getenv_fn);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->show_dump_config);
    GLYPHA_REQUIRE(parsed->server.port == 3333);
    GLYPHA_REQUIRE(parsed->server.worker_count == 2);
    GLYPHA_REQUIRE(parsed->quiet);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.starts_with("GlyphaStore/config\n"));
    GLYPHA_REQUIRE(dump.find("profile=\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("port=3333\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("shard-pairs=2\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("quiet=true\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("log-format=human\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("bind=127.0.0.1\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("storage-mode=volatile\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("maintenance-min-eval-interval-ms=") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("max-manifest-bytes=") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("max-live-keys=") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("disk-read-queue-capacity=") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("group-min-records=") != std::string::npos);
}

GLYPHA_TEST("daemon config production profile requires data-dir for durable storage") {
    const std::array arguments{"glyphastored", "--profile", "production"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(!parsed.has_value());
    GLYPHA_REQUIRE(parsed.error().message.find("data-dir") != std::string::npos);
}

GLYPHA_TEST("daemon dump-config cannot be set from a config file") {
    ConfigTemporaryDirectory temporary;
    const auto config = temporary.path() / "bad-dump.conf";
    write_file(config, "dump-config = true\n");
    const auto result = glyphastore::server::load_daemon_config_file(config);
    GLYPHA_REQUIRE(!result.has_value());
    GLYPHA_REQUIRE(result.error().message.find("cannot set") != std::string::npos);
}

GLYPHA_TEST("daemon config maintenance rate budgets and secure-profile fail closed") {
    const std::array rate_args{"glyphastored", "--maintenance-max-copy-bytes-per-sec",
                               "1048576",      "--maintenance-max-cpu-ms-per-window",
                               "25",           "--maintenance-suspend-on-p99-latency-ms",
                               "40",           "--maintenance-suspend-on-p99-min-samples",
                               "64",           "--maintenance-max-latency-deferral-ms",
                               "5000"};
    const auto rate = parse(rate_args);
    GLYPHA_REQUIRE(rate.has_value());
    GLYPHA_REQUIRE(rate->store.maintenance.max_copy_bytes_per_sec == 1'048'576U);
    GLYPHA_REQUIRE(rate->store.maintenance.max_cpu_ms_per_window == 25U);
    GLYPHA_REQUIRE(rate->store.maintenance.suspend_on_p99_latency_ms == 40U);
    GLYPHA_REQUIRE(rate->store.maintenance.suspend_on_p99_min_samples == 64U);
    GLYPHA_REQUIRE(rate->store.maintenance.max_latency_deferral_ms == 5'000U);
    const auto dump = glyphastore::server::format_daemon_config_dump(*rate);
    GLYPHA_REQUIRE(dump.find("maintenance-max-copy-bytes-per-sec=1048576\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("maintenance-max-cpu-ms-per-window=25\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("maintenance-suspend-on-p99-latency-ms=40\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("maintenance-suspend-on-p99-min-samples=64\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("maintenance-max-latency-deferral-ms=5000\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("secure-profile=false\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("authz-enabled=false\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("max-accepts-per-sec=0\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("idle-timeout-ms=0\n") != std::string::npos);

    const std::array insecure{"glyphastored", "--secure-profile"};
    const auto missing = parse(insecure);
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().message.find("secure-profile") != std::string::npos);

    ConfigTemporaryDirectory temporary;
    const auto map_path = temporary.path() / "authz.map";
    write_file(map_path, "reader.example read\n");
    const auto map_arg = map_path.string();
    const std::array dual{"glyphastored", "--secure-profile", "--tls-cert",      "missing.crt",
                          "--tls-key",    "missing.key",      "--tls-client-ca", "missing-ca.crt",
                          "--authz-map",  map_arg.c_str(),    "--tls-port",      "7380"};
    const auto dual_result = parse(dual);
    GLYPHA_REQUIRE(!dual_result.has_value());
}

GLYPHA_TEST("daemon config Phase 5 abuse limits parse and dump") {
    const std::array arguments{"glyphastored",
                               "--max-accepts-per-sec",
                               "10",
                               "--idle-timeout-ms",
                               "1000",
                               "--request-timeout-ms",
                               "2000",
                               "--connection-max-requests-per-sec",
                               "3",
                               "--principal-max-requests-per-sec",
                               "4",
                               "--principal-max-bytes-per-sec",
                               "1MiB"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.abuse.max_accepts_per_sec == 10);
    GLYPHA_REQUIRE(parsed->server.abuse.idle_timeout_ms == 1000);
    GLYPHA_REQUIRE(parsed->server.abuse.request_timeout_ms == 2000);
    GLYPHA_REQUIRE(parsed->server.abuse.connection_max_requests_per_sec == 3);
    GLYPHA_REQUIRE(parsed->server.abuse.principal_max_requests_per_sec == 4);
    GLYPHA_REQUIRE(parsed->server.abuse.principal_max_bytes_per_sec == 1U * 1024U * 1024U);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("max-accepts-per-sec=10\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("idle-timeout-ms=1000\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("request-timeout-ms=2000\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("connection-max-requests-per-sec=3\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("principal-max-requests-per-sec=4\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("principal-max-bytes-per-sec=1048576\n") != std::string::npos);
}

GLYPHA_TEST("daemon config secure-profile refuses explicit Phase 5 zero") {
    ConfigTemporaryDirectory temporary;
    const auto map_path = temporary.path() / "authz.map";
    write_file(map_path, "reader.example read\n");
    const auto map_arg = map_path.string();
    const std::array arguments{"glyphastored", "--secure-profile", "--tls-cert",        "missing.crt",
                               "--tls-key",    "missing.key",      "--tls-client-ca",   "missing-ca.crt",
                               "--authz-map",  map_arg.c_str(),    "--idle-timeout-ms", "0"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(!parsed.has_value());
    GLYPHA_REQUIRE(parsed.error().message.find("Phase 5") != std::string::npos ||
                   parsed.error().message.find("abuse") != std::string::npos ||
                   parsed.error().message.find("secure-profile") != std::string::npos);
}

GLYPHA_TEST("daemon config authz-map enables default-deny policy") {
    ConfigTemporaryDirectory temporary;
    const auto map_path = temporary.path() / "authz.map";
    write_file(map_path, "writer.example write\n");
    const auto map_arg = map_path.string();
    const std::array arguments{"glyphastored", "--authz-map", map_arg.c_str()};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.authz.enabled());
    GLYPHA_REQUIRE(parsed->server.authz.size() == 1);
    GLYPHA_REQUIRE(parsed->authz_map_path == map_path);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("authz-enabled=true\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("authz-principals=1\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("authz-prefix-scoped=0\n") != std::string::npos);
}

GLYPHA_TEST("daemon config authz-map prefix scope is loaded and dumped") {
    ConfigTemporaryDirectory temporary;
    const auto map_path = temporary.path() / "authz.map";
    write_file(map_path, "tenant-a write prefix=a/\ntenant-b write prefix=b/\nops write\n");
    const auto map_arg = map_path.string();
    const std::array arguments{"glyphastored", "--authz-map", map_arg.c_str()};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.authz.size() == 3);
    GLYPHA_REQUIRE(parsed->server.authz.prefix_scoped_count() == 2);
    GLYPHA_REQUIRE(parsed->server.authz.key_prefix_for("tenant-a") == "a/");
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("authz-prefix-scoped=2\n") != std::string::npos);
}

GLYPHA_TEST("daemon config index-hash-seed is parsed and dumped") {
    const std::array arguments{"glyphastored", "--index-hash-seed", "42"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->index_hash_seed_explicit);
    GLYPHA_REQUIRE(parsed->index_hash_seed == 42);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("index-hash-seed=42\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("index-hash-seed-explicit=true\n") != std::string::npos);
}

GLYPHA_TEST("daemon config worker-hash-seed selects siphash24-v1") {
    const std::array arguments{"glyphastored", "--worker-hash-seed", "99"};
    const auto parsed = parse(arguments);
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->worker_hash_seed_explicit);
    GLYPHA_REQUIRE(parsed->worker_routing.algorithm == glyphastore::RoutingAlgorithm::siphash24_v1);
    GLYPHA_REQUIRE(parsed->worker_routing.seed == 99);
    GLYPHA_REQUIRE(parsed->store.worker_routing.seed_explicit);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("worker-hash-seed=99\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("worker-routing-algorithm=siphash24-v1\n") != std::string::npos);
}

GLYPHA_TEST("daemon config unix-socket and peercred flags") {
    const std::array peercred_only{"glyphastored", "--unix-peercred"};
    const auto missing_socket = parse(peercred_only);
    GLYPHA_REQUIRE(!missing_socket.has_value());
    GLYPHA_REQUIRE(missing_socket.error().message.find("--unix-socket") != std::string::npos);

    ConfigTemporaryDirectory temporary;
    const auto sock = (temporary.path() / "gs.sock").string();
    const std::array with_socket{"glyphastored", "--unix-socket", sock.c_str(), "--unix-peercred"};
    const auto parsed = parse(with_socket);
#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->server.unix_socket_path == sock);
    GLYPHA_REQUIRE(parsed->server.unix_peercred);
    GLYPHA_REQUIRE(parsed->server.unix_peercred_required);
    const auto dump = glyphastore::server::format_daemon_config_dump(*parsed);
    GLYPHA_REQUIRE(dump.find("unix-peercred=true\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("unix-peercred-required=true\n") != std::string::npos);
#else
    GLYPHA_REQUIRE(!parsed.has_value());
#endif
}

GLYPHA_TEST("daemon config secure-profile requires peercred with unix-socket") {
    ConfigTemporaryDirectory temporary;
    const auto map_path = temporary.path() / "authz.map";
    write_file(map_path, "unix:uid=1000 write\n");
    const auto map_arg = map_path.string();
    const auto sock = (temporary.path() / "gs.sock").string();
    const std::array without_peercred{"glyphastored", "--secure-profile", "--tls-cert",      "missing.crt",
                                      "--tls-key",    "missing.key",      "--tls-client-ca", "missing-ca.crt",
                                      "--authz-map",  map_arg.c_str(),    "--unix-socket",   sock.c_str()};
    const auto denied = parse(without_peercred);
    GLYPHA_REQUIRE(!denied.has_value());
    GLYPHA_REQUIRE(denied.error().message.find("--unix-peercred") != std::string::npos);
}

GLYPHA_TEST("daemon config CRL and OCSP fail-closed flags") {
    const std::array ocsp_only{"glyphastored", "--tls-ocsp-fail-closed"};
    const auto missing = parse(ocsp_only);
    GLYPHA_REQUIRE(!missing.has_value());

    ConfigTemporaryDirectory temporary;
    const auto crl = temporary.path() / "empty.crl";
    write_file(crl, "not-a-crl\n");
    const auto crl_arg = crl.string();
    const std::array crl_without_mtls{"glyphastored", "--tls-crl", crl_arg.c_str(), "--tls-cert",
                                      "missing.crt",  "--tls-key", "missing.key"};
    const auto no_ca = parse(crl_without_mtls);
    GLYPHA_REQUIRE(!no_ca.has_value());

    const std::array json_args{"glyphastored", "--log-format", "json"};
    const auto json = parse(json_args);
    GLYPHA_REQUIRE(json.has_value());
    GLYPHA_REQUIRE(json->server.security_audit_events);
    const auto dump = glyphastore::server::format_daemon_config_dump(*json);
    GLYPHA_REQUIRE(dump.find("security-audit-events=true\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("tls-crl=\n") != std::string::npos);
    GLYPHA_REQUIRE(dump.find("tls-ocsp-fail-closed=false\n") != std::string::npos);
}
