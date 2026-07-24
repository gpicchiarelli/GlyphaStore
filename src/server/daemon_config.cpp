#include "glyphastore/server/daemon_config.hpp"

#include "cli/arguments.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/server/authz.hpp"
#include "glyphastore/server/tls.hpp"

#include <array>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace glyphastore::server {
namespace {

enum OptionId : std::size_t {
    help,
    version,
    config,
    profile,
    dump_config,
    bind,
    port,
    maximum_connections,
    workers,
    handoff_capacity,
    event_batch_size,
    maximum_input_bytes,
    maximum_output_bytes,
    durable_mutation_queue_capacity,
    durable_mutation_queue_bytes,
    durable_group_mutation_concurrency,
    durable_mutation_queue_wait,
    shutdown_drain,
    reuse_port,
    no_reuse_port,
    executor_affinity,
    storage_mode,
    data_directory,
    durable_open_mode,
    maintenance_mode,
    maintenance_max_copy_bytes_per_cycle,
    maintenance_max_copy_bytes_per_sec,
    maintenance_max_cpu_ms_per_window,
    maintenance_unread_ttl_pressure_probe,
    maintenance_unread_ttl_normal_scheduling,
    sync_interval_ms,
    group_max_records,
    group_max_bytes,
    group_max_wait_ms,
    max_store_bytes,
    reserved_free_bytes,
    max_segments,
    max_hot_cache_bytes,
    max_hot_cache_value_bytes,
    disable_hot_cache,
    max_temporary_compaction_bytes,
    quiet,
    log_format,
    tls_cert,
    tls_key,
    tls_client_ca,
    tls_port,
    authz_map,
    secure_profile,
};

constexpr std::array kOptionSpecs{
    cli::OptionSpec{help, "help", 'h', cli::OptionArity::none, {}, "Show this help message and exit"},
    cli::OptionSpec{version, "version", 'V', cli::OptionArity::none, {}, "Show version information and exit"},
    cli::OptionSpec{config, "config", '\0', cli::OptionArity::required, "PATH",
                    "Load settings from PATH (defaults < profile < file < env < CLI)"},
    cli::OptionSpec{profile, "profile", '\0', cli::OptionArity::required, "NAME",
                    "Apply a deployment profile preset: dev, embedded, or production"},
    cli::OptionSpec{dump_config,
                    "dump-config",
                    '\0',
                    cli::OptionArity::none,
                    {},
                    "Print the resolved effective configuration and exit without listening"},
    cli::OptionSpec{bind, "bind", 'b', cli::OptionArity::required, "IPv4",
                    "Bind to an IPv4 address (default: 127.0.0.1; non-loopback is "
                    "cleartext with no authentication — trusted networks only)"},
    cli::OptionSpec{port, "port", 'p', cli::OptionArity::required, "PORT",
                    "Listen on PORT; 0 selects an ephemeral port (default: 7379)"},
    cli::OptionSpec{workers, "workers", 'w', cli::OptionArity::required, "COUNT",
                    "Run COUNT Store workers and reactor executors (default: 1)"},
    cli::OptionSpec{maximum_connections, "max-connections", '\0', cli::OptionArity::required, "COUNT",
                    "Limit concurrent connections (default: 4096)"},
    cli::OptionSpec{handoff_capacity, "handoff-capacity", '\0', cli::OptionArity::required, "COUNT",
                    "Bound each connection handoff queue (default: 4096)"},
    cli::OptionSpec{event_batch_size, "event-batch-size", '\0', cli::OptionArity::required, "COUNT",
                    "Process at most COUNT readiness events per wait (default: 256)"},
    cli::OptionSpec{maximum_input_bytes, "max-input-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Limit buffered input per connection (default: 4MiB)"},
    cli::OptionSpec{maximum_output_bytes, "max-output-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Limit buffered output per connection (default: 4MiB)"},
    cli::OptionSpec{durable_mutation_queue_capacity, "durable-mutation-queue-capacity", '\0',
                    cli::OptionArity::required, "COUNT",
                    "Bound admitted durable mutations per Worker (default: 256)"},
    cli::OptionSpec{durable_mutation_queue_bytes, "durable-mutation-queue-bytes", '\0',
                    cli::OptionArity::required, "BYTES",
                    "Bound owned durable mutation bytes per Worker (default: 16MiB)"},
    cli::OptionSpec{durable_group_mutation_concurrency, "durable-group-concurrency", '\0',
                    cli::OptionArity::required, "COUNT",
                    "Run up to COUNT strict-group producers per Worker (default: 4)"},
    cli::OptionSpec{durable_mutation_queue_wait, "durable-mutation-queue-wait-ms", '\0',
                    cli::OptionArity::required, "MILLISECONDS",
                    "Expire queued mutations after this wait; 0 disables (default: 1000)"},
    cli::OptionSpec{shutdown_drain, "shutdown-drain-ms", '\0', cli::OptionArity::required, "MILLISECONDS",
                    "Bound connection and durable mutation drain after stop (default: 30000; 0 "
                    "unbounded). Stop accepting, close idle connections, expire queued pre-Store "
                    "work as unavailable on timeout"},
    cli::OptionSpec{reuse_port,
                    "reuse-port",
                    '\0',
                    cli::OptionArity::none,
                    {},
                    "Enable per-executor SO_REUSEPORT listeners where supported"},
    cli::OptionSpec{
        no_reuse_port, "no-reuse-port", '\0', cli::OptionArity::none, {}, "Disable SO_REUSEPORT listeners"},
    cli::OptionSpec{executor_affinity,
                    "executor-affinity",
                    '\0',
                    cli::OptionArity::none,
                    {},
                    "Request executor CPU affinity where supported"},
    cli::OptionSpec{storage_mode, "storage-mode", '\0', cli::OptionArity::required, "MODE",
                    "Use volatile, durable-sync, durable-periodic, or durable-group storage"},
    cli::OptionSpec{data_directory, "data-dir", '\0', cli::OptionArity::required, "PATH",
                    "Store durable v1 files below PATH (required for durable storage)"},
    cli::OptionSpec{durable_open_mode, "open-mode", '\0', cli::OptionArity::required, "MODE",
                    "Use open-or-create, create-new, or open-existing (default: open-or-create)"},
    cli::OptionSpec{maintenance_mode, "maintenance-mode", '\0', cli::OptionArity::required, "MODE",
                    "Use cooperative, background (default), or disabled Store maintenance scheduling"},
    cli::OptionSpec{maintenance_max_copy_bytes_per_cycle, "maintenance-max-copy-bytes-per-cycle", '\0',
                    cli::OptionArity::required, "BYTES",
                    "Limit one normal maintenance compaction (default: 128MiB; 0 disables)"},
    cli::OptionSpec{maintenance_max_copy_bytes_per_sec, "maintenance-max-copy-bytes-per-sec", '\0',
                    cli::OptionArity::required, "BYTES",
                    "Limit normal-mode compaction copy bytes per one-second window (default: 0=unlimited; "
                    "pressure/emergency bypass)"},
    cli::OptionSpec{maintenance_max_cpu_ms_per_window, "maintenance-max-cpu-ms-per-window", '\0',
                    cli::OptionArity::required, "MILLISECONDS",
                    "Limit normal-mode compaction wall time per one-second window (default: 0=unlimited; "
                    "pressure/emergency bypass)"},
    cli::OptionSpec{maintenance_unread_ttl_pressure_probe, "maintenance-unread-ttl-pressure-probe", '\0',
                    cli::OptionArity::required, "BOOL",
                    "Probe unread expired sealed puts under pressure/emergency (default: true)"},
    cli::OptionSpec{maintenance_unread_ttl_normal_scheduling, "maintenance-unread-ttl-normal-scheduling",
                    '\0', cli::OptionArity::required, "BOOL",
                    "Include unread expired sealed puts in normal dead-byte threshold (default: false)"},
    cli::OptionSpec{sync_interval_ms, "sync-interval-ms", '\0', cli::OptionArity::required, "MILLISECONDS",
                    "Durable-periodic flush interval (default: 1000)"},
    cli::OptionSpec{group_max_records, "group-max-records", '\0', cli::OptionArity::required, "COUNT",
                    "Strict-group / periodic batch record limit (default: 32 / 4096)"},
    cli::OptionSpec{group_max_bytes, "group-max-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Strict-group / periodic batch byte limit (default: 64KiB / 4MiB)"},
    cli::OptionSpec{group_max_wait_ms, "group-max-wait-ms", '\0', cli::OptionArity::required, "MILLISECONDS",
                    "Strict-group / periodic batch close wait (default: 10 / 1000)"},
    cli::OptionSpec{max_store_bytes, "max-store-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Cap durable Store Segment bytes (default: 8GiB)"},
    cli::OptionSpec{reserved_free_bytes, "reserved-free-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Keep this much free space unused (default: 256MiB)"},
    cli::OptionSpec{max_segments, "max-segments", '\0', cli::OptionArity::required, "COUNT",
                    "Cap durable Segment count (default: 127)"},
    cli::OptionSpec{max_hot_cache_bytes, "max-hot-cache-bytes", '\0', cli::OptionArity::required, "BYTES",
                    "Cap durable hot-cache bytes across Workers (default: 256MiB; 0 disables)"},
    cli::OptionSpec{max_hot_cache_value_bytes, "max-hot-cache-value-bytes", '\0', cli::OptionArity::required,
                    "BYTES",
                    "Reject hot-cache admission for values larger than BYTES (default: 64KiB; 0 disables "
                    "admission)"},
    cli::OptionSpec{disable_hot_cache, "disable-hot-cache", '\0', cli::OptionArity::none, {},
                    "Disable durable hot-cache admission (cold pinned reads remain correct)"},
    cli::OptionSpec{max_temporary_compaction_bytes, "max-temporary-compaction-bytes", '\0',
                    cli::OptionArity::required, "BYTES",
                    "Cap temporary durable compaction peak bytes (default: 1GiB)"},
    cli::OptionSpec{
        quiet, "quiet", 'q', cli::OptionArity::none, {}, "Suppress normal startup and shutdown messages"},
    cli::OptionSpec{log_format, "log-format", '\0', cli::OptionArity::required, "FORMAT",
                    "Emit lifecycle logs as human stderr/stdout (default) or JSON-lines on stderr "
                    "(human|json)"},
    cli::OptionSpec{tls_cert, "tls-cert", '\0', cli::OptionArity::required, "PATH",
                    "PEM certificate chain for TLS 1.3 (requires --tls-key; TLS-only on --port unless "
                    "--tls-port)"},
    cli::OptionSpec{tls_key, "tls-key", '\0', cli::OptionArity::required, "PATH",
                    "PEM private key for TLS (requires --tls-cert)"},
    cli::OptionSpec{tls_client_ca, "tls-client-ca", '\0', cli::OptionArity::required, "PATH",
                    "PEM CA for client certificates (enables mTLS; requires --tls-cert/--tls-key)"},
    cli::OptionSpec{tls_port, "tls-port", '\0', cli::OptionArity::required, "PORT",
                    "Listen for TLS on PORT while --port stays cleartext (requires --tls-cert/--tls-key; "
                    "0=ephemeral; incompatible with --secure-profile)"},
    cli::OptionSpec{authz_map, "authz-map", '\0', cli::OptionArity::required, "PATH",
                    "Static principal capability map (read/write/admin; enables default-deny authz)"},
    cli::OptionSpec{secure_profile,
                    "secure-profile",
                    '\0',
                    cli::OptionArity::none,
                    {},
                    "Fail-closed secure profile: require TLS+mTLS+--authz-map; refuse --tls-port dual "
                    "cleartext"},
};

using SettingMap = std::map<std::string, std::string, std::less<>>;

[[nodiscard]] auto take_profile_name(SettingMap& settings) -> std::optional<std::string> {
    const auto found = settings.find("profile");
    if (found == settings.end()) {
        return std::nullopt;
    }
    auto name = found->second;
    settings.erase(found);
    return name;
}

[[nodiscard]] auto find_spec(const std::string_view long_name) -> const cli::OptionSpec* {
    for (const auto& spec : kOptionSpecs) {
        if (spec.long_name == long_name) {
            return &spec;
        }
    }
    return nullptr;
}

[[nodiscard]] auto find_spec(const std::size_t id) -> const cli::OptionSpec* {
    for (const auto& spec : kOptionSpecs) {
        if (spec.id == id) {
            return &spec;
        }
    }
    return nullptr;
}

[[nodiscard]] auto trim(std::string_view text) -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

[[nodiscard]] auto ascii_lower(std::string text) -> std::string {
    for (char& character : text) {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    return text;
}

[[nodiscard]] auto deployment_profile_settings(const std::string_view name) -> Result<SettingMap> {
    if (name.empty()) {
        return fail(ErrorCode::invalid_argument, "deployment profile must not be empty");
    }
    const auto lowered = ascii_lower(std::string{name});
    if (lowered == "dev") {
        return SettingMap{
            {"storage-mode", "volatile"},
            {"maintenance-mode", "disabled"},
        };
    }
    if (lowered == "embedded") {
        return SettingMap{
            {"storage-mode", "durable-periodic"},
            {"maintenance-mode", "background"},
            {"workers", "1"},
            {"max-store-bytes", "1073741824"},
            {"reserved-free-bytes", "67108864"},
            {"max-segments", "32"},
            {"max-hot-cache-bytes", "67108864"},
            {"max-temporary-compaction-bytes", "268435456"},
        };
    }
    if (lowered == "production") {
        return SettingMap{
            {"storage-mode", "durable-periodic"},
            {"maintenance-mode", "background"},
        };
    }
    return fail(ErrorCode::invalid_argument,
                "unknown deployment profile: " + std::string{name} +
                    " (expected dev, embedded, or production)");
}

[[nodiscard]] auto parse_bool_token(const std::string_view text, const std::string_view where)
    -> Result<bool> {
    const auto lowered = ascii_lower(std::string{text});
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        return false;
    }
    return fail(ErrorCode::invalid_argument,
                std::string{where} + " expects a boolean (true/false), got '" + std::string{text} + "'");
}

[[nodiscard]] auto default_getenv(const std::string_view name) -> std::optional<std::string> {
    const auto* value = std::getenv(std::string{name}.c_str());
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string{value};
}

[[nodiscard]] auto settings_from_parsed(const cli::ParsedArguments& parsed) -> Result<SettingMap> {
    SettingMap settings;
    for (const auto& option : parsed.options) {
        const auto* spec = find_spec(option.id);
        if (spec == nullptr) {
            return fail(ErrorCode::internal_error, "parsed daemon option has no matching specification");
        }
        if (option.id == help || option.id == version || option.id == config || option.id == dump_config) {
            continue;
        }
        if (spec->arity == cli::OptionArity::none) {
            settings.emplace(std::string{spec->long_name}, "true");
        } else {
            settings.emplace(std::string{spec->long_name}, std::string{option.value});
        }
    }
    if (settings.contains("reuse-port") && settings.contains("no-reuse-port")) {
        return fail(ErrorCode::invalid_argument, "--reuse-port and --no-reuse-port are mutually exclusive");
    }
    return settings;
}

void apply_layer(SettingMap& destination, const SettingMap& layer) {
    for (const auto& [key, value] : layer) {
        const auto* spec = find_spec(key);
        if (spec != nullptr && spec->arity == cli::OptionArity::none && value == "false") {
            destination.erase(key);
            continue;
        }
        if (key == "reuse-port") {
            destination.erase("no-reuse-port");
        } else if (key == "no-reuse-port") {
            destination.erase("reuse-port");
        }
        destination.insert_or_assign(key, value);
    }
}

[[nodiscard]] auto materialize_from_settings(SettingMap settings, const bool show_help,
                                             const bool show_version,
                                             std::string deployment_profile) -> Result<DaemonOptions> {
    std::vector<std::string> storage;
    storage.reserve(settings.size() * 2U + 1U);
    storage.emplace_back("glyphastored");
    for (const auto& [key, value] : settings) {
        const auto* spec = find_spec(key);
        if (spec == nullptr) {
            return fail(ErrorCode::invalid_argument, "unknown daemon setting: " + key);
        }
        if (spec->arity == cli::OptionArity::none) {
            auto enabled = parse_bool_token(value, "--" + key);
            if (!enabled) {
                return unexpected(enabled.error());
            }
            if (*enabled) {
                storage.push_back("--" + key);
            }
        } else {
            storage.push_back("--" + key);
            storage.push_back(value);
        }
    }

    std::vector<char*> argv;
    argv.reserve(storage.size());
    for (auto& entry : storage) {
        argv.push_back(entry.data());
    }
    auto parsed = cli::parse_arguments(static_cast<int>(argv.size()), argv.data(), kOptionSpecs);
    if (!parsed) {
        return unexpected(parsed.error());
    }

    DaemonOptions options;
    options.deployment_profile = std::move(deployment_profile);
    options.show_help = show_help;
    options.show_version = show_version;
    options.quiet = parsed->has(quiet);
    if (const auto format = parsed->value(log_format)) {
        auto resolved = parse_daemon_log_format(*format);
        if (!resolved) {
            return unexpected(resolved.error());
        }
        options.log_format = *resolved;
    }
    if (const auto address = parsed->value(bind)) {
        options.server.bind_address = *address;
    }

    const auto set_size_option = [&](const OptionId id, const std::string_view name,
                                     const std::size_t minimum, const std::size_t maximum,
                                     std::size_t& destination) -> Status {
        const auto text = parsed->value(id);
        if (!text) {
            return {};
        }
        auto value = cli::parse_size(*text, name, minimum, maximum);
        if (!value) {
            return unexpected(value.error());
        }
        destination = *value;
        return {};
    };
    const auto set_byte_size_option = [&](const OptionId id, const std::string_view name,
                                          const std::size_t minimum, const std::size_t maximum,
                                          std::size_t& destination) -> Status {
        const auto text = parsed->value(id);
        if (!text) {
            return {};
        }
        auto value = cli::parse_byte_size(*text, name, minimum, maximum);
        if (!value) {
            return unexpected(value.error());
        }
        destination = *value;
        return {};
    };

    std::size_t parsed_port = options.server.port;
    if (auto status =
            set_size_option(port, "--port", 0, std::numeric_limits<std::uint16_t>::max(), parsed_port);
        !status) {
        return unexpected(status.error());
    }
    options.server.port = static_cast<std::uint16_t>(parsed_port);

    constexpr auto maximum_size = std::numeric_limits<std::size_t>::max();
    if (auto status =
            set_size_option(workers, "--workers", 1, kMaximumWorkerCount, options.server.worker_count);
        !status) {
        return unexpected(status.error());
    }
    if (auto status =
            set_size_option(maximum_connections, "--max-connections", 1,
                            std::numeric_limits<std::uint32_t>::max(), options.server.maximum_connections);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_size_option(handoff_capacity, "--handoff-capacity", 1, std::size_t{1} << 30U,
                                      options.server.connection_handoff_capacity);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_size_option(event_batch_size, "--event-batch-size", 1, maximum_size,
                                      options.server.event_batch_size);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_byte_size_option(maximum_input_bytes, "--max-input-bytes", kRequestHeaderBytes,
                                           maximum_size, options.server.maximum_input_bytes);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_byte_size_option(maximum_output_bytes, "--max-output-bytes", kResponseHeaderBytes,
                                           maximum_size, options.server.maximum_output_bytes);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_size_option(durable_mutation_queue_capacity, "--durable-mutation-queue-capacity", 1,
                                      std::size_t{1} << 30U, options.server.durable_mutation_queue_capacity);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_byte_size_option(durable_mutation_queue_bytes, "--durable-mutation-queue-bytes", 1,
                                           maximum_size, options.server.durable_mutation_queue_bytes);
        !status) {
        return unexpected(status.error());
    }
    if (auto status = set_size_option(durable_group_mutation_concurrency, "--durable-group-concurrency", 1,
                                      32, options.server.durable_group_mutation_concurrency);
        !status) {
        return unexpected(status.error());
    }
    std::size_t mutation_queue_wait_ms = options.server.durable_mutation_queue_wait_ms;
    if (auto status = set_size_option(durable_mutation_queue_wait, "--durable-mutation-queue-wait-ms", 0,
                                      std::numeric_limits<std::uint32_t>::max(), mutation_queue_wait_ms);
        !status) {
        return unexpected(status.error());
    }
    options.server.durable_mutation_queue_wait_ms = static_cast<std::uint32_t>(mutation_queue_wait_ms);
    std::size_t shutdown_drain_ms = options.server.shutdown_drain_ms;
    if (auto status = set_size_option(shutdown_drain, "--shutdown-drain-ms", 0,
                                      std::numeric_limits<std::uint32_t>::max(), shutdown_drain_ms);
        !status) {
        return unexpected(status.error());
    }
    options.server.shutdown_drain_ms = static_cast<std::uint32_t>(shutdown_drain_ms);
    if (parsed->has(reuse_port) && parsed->has(no_reuse_port)) {
        return fail(ErrorCode::invalid_argument, "--reuse-port and --no-reuse-port are mutually exclusive");
    }
    if (parsed->has(reuse_port)) {
        options.server.reuse_port = true;
    }
    if (parsed->has(no_reuse_port)) {
        options.server.reuse_port = false;
    }
    options.server.executor_affinity = parsed->has(executor_affinity);

    if (const auto mode = parsed->value(storage_mode)) {
        if (*mode == "volatile") {
            options.store.storage_mode = StorageMode::volatile_memory;
        } else if (*mode == "durable-sync") {
            options.store.storage_mode = StorageMode::durable_sync;
        } else if (*mode == "durable-periodic") {
            options.store.storage_mode = StorageMode::durable_periodic;
        } else if (*mode == "durable-group") {
            options.store.storage_mode = StorageMode::durable_group;
        } else {
            return fail(ErrorCode::invalid_argument, "unknown --storage-mode: " + std::string{*mode});
        }
    }
    if (const auto path = parsed->value(data_directory)) {
        if (path->empty()) {
            return fail(ErrorCode::invalid_argument, "--data-dir must not be empty");
        }
        options.store.data_directory = std::filesystem::path{*path};
    }
    if (const auto mode = parsed->value(durable_open_mode)) {
        if (*mode == "open-or-create") {
            options.store.durable_open_mode = DurableOpenMode::open_or_create;
        } else if (*mode == "create-new") {
            options.store.durable_open_mode = DurableOpenMode::create_new;
        } else if (*mode == "open-existing") {
            options.store.durable_open_mode = DurableOpenMode::open_existing;
        } else {
            return fail(ErrorCode::invalid_argument, "unknown --open-mode: " + std::string{*mode});
        }
    }
    if (const auto mode = parsed->value(maintenance_mode)) {
        if (*mode == "cooperative") {
            options.store.maintenance.mode = MaintenanceMode::cooperative;
        } else if (*mode == "background") {
            options.store.maintenance.mode = MaintenanceMode::background;
        } else if (*mode == "disabled") {
            options.store.maintenance.mode = MaintenanceMode::disabled;
        } else {
            return fail(ErrorCode::invalid_argument, "unknown --maintenance-mode: " + std::string{*mode});
        }
    }
    std::size_t maintenance_copy_bytes =
        static_cast<std::size_t>(options.store.maintenance.max_copy_bytes_per_cycle);
    if (auto status = set_byte_size_option(maintenance_max_copy_bytes_per_cycle,
                                           "--maintenance-max-copy-bytes-per-cycle", 0, maximum_size,
                                           maintenance_copy_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.maintenance.max_copy_bytes_per_cycle = maintenance_copy_bytes;

    std::size_t maintenance_copy_bytes_per_sec =
        static_cast<std::size_t>(options.store.maintenance.max_copy_bytes_per_sec);
    if (auto status = set_byte_size_option(maintenance_max_copy_bytes_per_sec,
                                           "--maintenance-max-copy-bytes-per-sec", 0, maximum_size,
                                           maintenance_copy_bytes_per_sec);
        !status) {
        return unexpected(status.error());
    }
    options.store.maintenance.max_copy_bytes_per_sec = maintenance_copy_bytes_per_sec;

    std::size_t maintenance_cpu_ms = options.store.maintenance.max_cpu_ms_per_window;
    if (auto status = set_size_option(maintenance_max_cpu_ms_per_window,
                                      "--maintenance-max-cpu-ms-per-window", 0,
                                      std::numeric_limits<std::uint32_t>::max(), maintenance_cpu_ms);
        !status) {
        return unexpected(status.error());
    }
    options.store.maintenance.max_cpu_ms_per_window = static_cast<std::uint32_t>(maintenance_cpu_ms);

    if (parsed->has(maintenance_unread_ttl_pressure_probe)) {
        if (const auto value = parsed->value(maintenance_unread_ttl_pressure_probe)) {
            if (auto enabled = parse_bool_token(*value, "--maintenance-unread-ttl-pressure-probe"); enabled) {
                options.store.maintenance.unread_ttl_pressure_probe = *enabled;
            } else {
                return unexpected(enabled.error());
            }
        }
    }
    if (parsed->has(maintenance_unread_ttl_normal_scheduling)) {
        if (const auto value = parsed->value(maintenance_unread_ttl_normal_scheduling)) {
            if (auto enabled = parse_bool_token(*value, "--maintenance-unread-ttl-normal-scheduling");
                enabled) {
                options.store.maintenance.unread_ttl_normal_scheduling = *enabled;
            } else {
                return unexpected(enabled.error());
            }
        }
    }

    const bool has_batch_or_resource = parsed->has(sync_interval_ms) || parsed->has(group_max_records) ||
                                       parsed->has(group_max_bytes) || parsed->has(group_max_wait_ms) ||
                                       parsed->has(max_store_bytes) || parsed->has(reserved_free_bytes) ||
                                       parsed->has(max_segments) || parsed->has(max_hot_cache_bytes) ||
                                       parsed->has(max_hot_cache_value_bytes) || parsed->has(disable_hot_cache) ||
                                       parsed->has(max_temporary_compaction_bytes);
    const bool durable = options.store.storage_mode != StorageMode::volatile_memory;
    if (durable && !options.store.data_directory) {
        return fail(ErrorCode::invalid_argument, "--data-dir is required for durable storage");
    }
    if (!durable &&
        (parsed->has(data_directory) || parsed->has(durable_open_mode) || has_batch_or_resource)) {
        return fail(ErrorCode::invalid_argument,
                    "durable data-dir, open-mode, batch, and resource flags require a durable "
                    "--storage-mode");
    }

    std::size_t sync_interval = options.store.durable_periodic.sync_interval_ms;
    if (auto status = set_size_option(sync_interval_ms, "--sync-interval-ms", 1,
                                      std::numeric_limits<std::uint32_t>::max(), sync_interval);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_periodic.sync_interval_ms = static_cast<std::uint32_t>(sync_interval);

    const auto apply_group_field = [&](DurableGroupConfig& batch) -> Status {
        std::size_t max_records = batch.max_records;
        if (auto status = set_size_option(group_max_records, "--group-max-records", 1,
                                          std::numeric_limits<std::uint32_t>::max(), max_records);
            !status) {
            return status;
        }
        batch.max_records = static_cast<std::uint32_t>(max_records);
        std::size_t max_bytes = batch.max_bytes;
        if (auto status =
                set_byte_size_option(group_max_bytes, "--group-max-bytes", 1, maximum_size, max_bytes);
            !status) {
            return status;
        }
        if (max_bytes > std::numeric_limits<std::uint32_t>::max()) {
            return fail(ErrorCode::invalid_argument,
                        "--group-max-bytes exceeds the supported u32 batch limit");
        }
        batch.max_bytes = static_cast<std::uint32_t>(max_bytes);
        std::size_t max_wait = batch.max_wait_ms;
        if (auto status = set_size_option(group_max_wait_ms, "--group-max-wait-ms", 1,
                                          std::numeric_limits<std::uint32_t>::max(), max_wait);
            !status) {
            return status;
        }
        batch.max_wait_ms = static_cast<std::uint32_t>(max_wait);
        return {};
    };
    if (auto status = apply_group_field(options.store.durable_group); !status) {
        return unexpected(status.error());
    }
    if (!options.store.durable_periodic.batch.has_value()) {
        options.store.durable_periodic.batch = DurableGroupConfig{};
    }
    if (auto status = apply_group_field(*options.store.durable_periodic.batch); !status) {
        return unexpected(status.error());
    }

    std::size_t store_bytes = static_cast<std::size_t>(options.store.durable_limits.max_store_bytes);
    if (auto status =
            set_byte_size_option(max_store_bytes, "--max-store-bytes", 1, maximum_size, store_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_limits.max_store_bytes = store_bytes;
    std::size_t reserved_bytes = static_cast<std::size_t>(options.store.durable_limits.reserved_free_bytes);
    if (auto status = set_byte_size_option(reserved_free_bytes, "--reserved-free-bytes", 0, maximum_size,
                                           reserved_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_limits.reserved_free_bytes = reserved_bytes;
    if (auto status = set_size_option(max_segments, "--max-segments", 1, maximum_size,
                                      options.store.durable_limits.max_segment_count);
        !status) {
        return unexpected(status.error());
    }
    std::size_t hot_cache_bytes = static_cast<std::size_t>(options.store.durable_limits.max_hot_cache_bytes);
    if (auto status = set_byte_size_option(max_hot_cache_bytes, "--max-hot-cache-bytes", 0, maximum_size,
                                           hot_cache_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_limits.max_hot_cache_bytes = hot_cache_bytes;
    std::size_t hot_cache_value_bytes =
        static_cast<std::size_t>(options.store.durable_limits.max_hot_cache_value_bytes);
    if (auto status = set_byte_size_option(max_hot_cache_value_bytes, "--max-hot-cache-value-bytes", 0,
                                           kMaxNormalRecordSize, hot_cache_value_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_limits.max_hot_cache_value_bytes = hot_cache_value_bytes;
    if (parsed->has(disable_hot_cache)) {
        options.store.durable_limits.hot_cache_enabled = false;
    }
    std::size_t temporary_bytes =
        static_cast<std::size_t>(options.store.durable_limits.max_temporary_compaction_bytes);
    if (auto status = set_byte_size_option(max_temporary_compaction_bytes, "--max-temporary-compaction-bytes",
                                           1, maximum_size, temporary_bytes);
        !status) {
        return unexpected(status.error());
    }
    options.store.durable_limits.max_temporary_compaction_bytes = temporary_bytes;

    if (const auto path = parsed->value(tls_cert)) {
        options.server.tls.certificate_file = std::filesystem::path{*path};
    }
    if (const auto path = parsed->value(tls_key)) {
        options.server.tls.private_key_file = std::filesystem::path{*path};
    }
    if (const auto path = parsed->value(tls_client_ca)) {
        options.server.tls.client_ca_file = std::filesystem::path{*path};
    }
    if (parsed->has(tls_port)) {
        std::size_t parsed_tls_port = 0;
        if (auto status = set_size_option(tls_port, "--tls-port", 0,
                                          std::numeric_limits<std::uint16_t>::max(), parsed_tls_port);
            !status) {
            return unexpected(status.error());
        }
        options.server.tls_port = static_cast<std::uint16_t>(parsed_tls_port);
    }
    if (const auto path = parsed->value(authz_map)) {
        if (path->empty()) {
            return fail(ErrorCode::invalid_argument, "--authz-map must not be empty");
        }
        options.authz_map_path = std::filesystem::path{*path};
        auto policy = AuthzPolicy::load_file(options.authz_map_path);
        if (!policy) {
            return unexpected(policy.error());
        }
        options.server.authz = std::move(*policy);
    }
    options.secure_profile = parsed->has(secure_profile);
    if (options.secure_profile) {
        if (!options.server.tls.requested() || !options.server.tls.mtls_enabled()) {
            return fail(ErrorCode::invalid_argument,
                        "--secure-profile requires --tls-cert/--tls-key and --tls-client-ca");
        }
        if (options.authz_map_path.empty() || !options.server.authz.enabled()) {
            return fail(ErrorCode::invalid_argument, "--secure-profile requires --authz-map");
        }
        if (options.server.tls_port.has_value()) {
            return fail(ErrorCode::invalid_argument,
                        "--secure-profile refuses dual cleartext (--tls-port); use TLS-only on --port");
        }
    }
    if (auto tls = validate_tls_config(options.server.tls); !tls) {
        return unexpected(tls.error());
    }
    return options;
}

[[nodiscard]] auto normalize_setting_value(const cli::OptionSpec& spec, std::string value,
                                           const std::string_view where) -> Result<std::string> {
    if (spec.arity == cli::OptionArity::none) {
        auto enabled = parse_bool_token(value, where);
        if (!enabled) {
            return unexpected(enabled.error());
        }
        return *enabled ? std::string{"true"} : std::string{"false"};
    }
    if (value.empty()) {
        return fail(ErrorCode::invalid_argument, std::string{where} + " must not be empty");
    }
    return value;
}

[[nodiscard]] auto ingest_setting(SettingMap& settings, const std::string_view key, std::string value,
                                  const std::string_view where) -> Status {
    if (key == "help" || key == "version" || key == "config" || key == "dump-config") {
        return fail(ErrorCode::invalid_argument,
                    std::string{where} + " cannot set '" + std::string{key} + "'");
    }
    const auto* spec = find_spec(key);
    if (spec == nullptr) {
        return fail(ErrorCode::invalid_argument,
                    std::string{where} + " has unknown setting '" + std::string{key} + "'");
    }
    auto normalized = normalize_setting_value(*spec, std::move(value), where);
    if (!normalized) {
        return unexpected(normalized.error());
    }
    if (!settings.emplace(std::string{key}, std::move(*normalized)).second) {
        return fail(ErrorCode::invalid_argument,
                    std::string{where} + " duplicates setting '" + std::string{key} + "'");
    }
    return {};
}

} // namespace

auto daemon_option_specs() noexcept -> std::span<const cli::OptionSpec> {
    return kOptionSpecs;
}

auto storage_mode_name(const StorageMode mode) noexcept -> std::string_view {
    switch (mode) {
    case StorageMode::volatile_memory:
        return "volatile";
    case StorageMode::durable_sync:
        return "durable-sync";
    case StorageMode::durable_periodic:
        return "durable-periodic";
    case StorageMode::durable_group:
        return "durable-group";
    }
    return "unknown";
}

namespace {

[[nodiscard]] auto open_mode_name(const DurableOpenMode mode) noexcept -> std::string_view {
    switch (mode) {
    case DurableOpenMode::open_or_create:
        return "open-or-create";
    case DurableOpenMode::create_new:
        return "create-new";
    case DurableOpenMode::open_existing:
        return "open-existing";
    }
    return "unknown";
}

[[nodiscard]] auto maintenance_mode_name(const MaintenanceMode mode) noexcept -> std::string_view {
    switch (mode) {
    case MaintenanceMode::cooperative:
        return "cooperative";
    case MaintenanceMode::background:
        return "background";
    case MaintenanceMode::disabled:
        return "disabled";
    }
    return "unknown";
}

} // namespace

auto format_daemon_config_dump(const DaemonOptions& options) -> std::string {
    const auto& group = options.store.storage_mode == StorageMode::durable_periodic &&
                                options.store.durable_periodic.batch.has_value()
                            ? *options.store.durable_periodic.batch
                            : options.store.durable_group;
    std::string out;
    out.reserve(2048);
    out += "GlyphaStore/config\n";
    out += "profile=";
    out += options.deployment_profile;
    out += "\nbind=";
    out += options.server.bind_address;
    out += "\nport=";
    out += std::to_string(options.server.port);
    out += "\nworkers=";
    out += std::to_string(options.server.worker_count);
    out += "\nmax-connections=";
    out += std::to_string(options.server.maximum_connections);
    out += "\nhandoff-capacity=";
    out += std::to_string(options.server.connection_handoff_capacity);
    out += "\nevent-batch-size=";
    out += std::to_string(options.server.event_batch_size);
    out += "\nmax-input-bytes=";
    out += std::to_string(options.server.maximum_input_bytes);
    out += "\nmax-output-bytes=";
    out += std::to_string(options.server.maximum_output_bytes);
    out += "\ndurable-mutation-queue-capacity=";
    out += std::to_string(options.server.durable_mutation_queue_capacity);
    out += "\ndurable-mutation-queue-bytes=";
    out += std::to_string(options.server.durable_mutation_queue_bytes);
    out += "\ndurable-group-concurrency=";
    out += std::to_string(options.server.durable_group_mutation_concurrency);
    out += "\ndurable-mutation-queue-wait-ms=";
    out += std::to_string(options.server.durable_mutation_queue_wait_ms);
    out += "\nshutdown-drain-ms=";
    out += std::to_string(options.server.shutdown_drain_ms);
    out += "\nreuse-port=";
    out += options.server.reuse_port ? "true" : "false";
    out += "\nexecutor-affinity=";
    out += options.server.executor_affinity ? "true" : "false";
    out += "\nstorage-mode=";
    out += storage_mode_name(options.store.storage_mode);
    out += "\ndata-dir=";
    if (options.store.data_directory) {
        out += options.store.data_directory->string();
    }
    out += "\nopen-mode=";
    out += open_mode_name(options.store.durable_open_mode);
    out += "\nmaintenance-mode=";
    out += maintenance_mode_name(options.store.maintenance.mode);
    out += "\nmaintenance-max-copy-bytes-per-cycle=";
    out += std::to_string(options.store.maintenance.max_copy_bytes_per_cycle);
    out += "\nmaintenance-max-copy-bytes-per-sec=";
    out += std::to_string(options.store.maintenance.max_copy_bytes_per_sec);
    out += "\nmaintenance-max-cpu-ms-per-window=";
    out += std::to_string(options.store.maintenance.max_cpu_ms_per_window);
    out += "\nmaintenance-unread-ttl-pressure-probe=";
    out += options.store.maintenance.unread_ttl_pressure_probe ? "true" : "false";
    out += "\nmaintenance-unread-ttl-normal-scheduling=";
    out += options.store.maintenance.unread_ttl_normal_scheduling ? "true" : "false";
    out += "\nmaintenance-min-eval-interval-ms=";
    out += std::to_string(options.store.maintenance.min_eval_interval_ms);
    out += "\nmaintenance-max-eval-interval-ms=";
    out += std::to_string(options.store.maintenance.max_eval_interval_ms);
    out += "\nmaintenance-dead-byte-ratio-bp-normal=";
    out += std::to_string(options.store.maintenance.dead_byte_ratio_bp_normal);
    out += "\nmaintenance-segment-count-pressure-pct=";
    out += std::to_string(options.store.maintenance.segment_count_pressure_pct);
    out += "\nmaintenance-max-no-gain-attempts=";
    out += std::to_string(options.store.maintenance.max_no_gain_attempts);
    out += "\nmaintenance-free-bytes-pressure-margin=";
    out += std::to_string(options.store.maintenance.free_bytes_pressure_margin);
    out += "\ndisk-read-thread-count=";
    out += std::to_string(options.server.disk_read_thread_count);
    out += "\ndisk-read-queue-capacity=";
    out += std::to_string(options.server.disk_read_queue_capacity);
    out += "\nsync-interval-ms=";
    out += std::to_string(options.store.durable_periodic.sync_interval_ms);
    out += "\ngroup-max-records=";
    out += std::to_string(group.max_records);
    out += "\ngroup-max-bytes=";
    out += std::to_string(group.max_bytes);
    out += "\ngroup-max-wait-ms=";
    out += std::to_string(group.max_wait_ms);
    out += "\ngroup-min-records=";
    out += std::to_string(group.min_records);
    out += "\nmax-store-bytes=";
    out += std::to_string(options.store.durable_limits.max_store_bytes);
    out += "\nreserved-free-bytes=";
    out += std::to_string(options.store.durable_limits.reserved_free_bytes);
    out += "\nmax-segments=";
    out += std::to_string(options.store.durable_limits.max_segment_count);
    out += "\nmax-manifest-bytes=";
    out += std::to_string(options.store.durable_limits.max_manifest_bytes);
    out += "\nmax-open-files=";
    out += std::to_string(options.store.durable_limits.max_open_files);
    out += "\nmax-recovery-memory-bytes=";
    out += std::to_string(options.store.durable_limits.max_recovery_memory_bytes);
    out += "\nmax-live-keys=";
    out += std::to_string(options.store.durable_limits.max_live_keys);
    out += "\nmax-write-amplification=";
    out += std::to_string(options.store.durable_limits.max_write_amplification);
    out += "\nmax-hot-cache-bytes=";
    out += std::to_string(options.store.durable_limits.max_hot_cache_bytes);
    out += "\nmax-hot-cache-bytes-per-worker=";
    out += std::to_string(options.store.durable_limits.max_hot_cache_bytes_per_worker);
    out += "\nmax-hot-cache-staging-bytes-per-worker=";
    out += std::to_string(options.store.durable_limits.max_hot_cache_staging_bytes_per_worker);
    out += "\nmax-hot-cache-entries-per-worker=";
    out += std::to_string(options.store.durable_limits.max_hot_cache_entries_per_worker);
    out += "\nmax-hot-cache-value-bytes=";
    out += std::to_string(options.store.durable_limits.max_hot_cache_value_bytes);
    out += "\nhot-cache-enabled=";
    out += options.store.durable_limits.hot_cache_enabled ? "true" : "false";
    out += "\nmax-temporary-compaction-bytes=";
    out += std::to_string(options.store.durable_limits.max_temporary_compaction_bytes);
    out += "\nquiet=";
    out += options.quiet ? "true" : "false";
    out += "\nlog-format=";
    out += daemon_log_format_name(options.log_format);
    out += "\ntls-cert=";
    out += options.server.tls.certificate_file.string();
    out += "\ntls-key=";
    out += options.server.tls.private_key_file.string();
    out += "\ntls-client-ca=";
    out += options.server.tls.client_ca_file.string();
    out += "\ntls-port=";
    if (options.server.tls_port.has_value()) {
        out += std::to_string(*options.server.tls_port);
    }
    out += "\nauthz-map=";
    out += options.authz_map_path.string();
    out += "\nauthz-enabled=";
    out += options.server.authz.enabled() ? "true" : "false";
    out += "\nauthz-principals=";
    out += std::to_string(options.server.authz.size());
    out += "\nsecure-profile=";
    out += options.secure_profile ? "true" : "false";
    out += '\n';
    return out;
}

auto environment_name_for_option(const std::string_view long_name) -> std::string {
    std::string name = "GLYPHASTORE_";
    name.reserve(name.size() + long_name.size());
    for (const char character : long_name) {
        name.push_back(
            character == '-' ? '_' : static_cast<char>(std::toupper(static_cast<unsigned char>(character))));
    }
    return name;
}

auto load_daemon_config_file(const std::filesystem::path& path)
    -> Result<std::map<std::string, std::string, std::less<>>> {
    std::ifstream input{path};
    if (!input) {
        return fail(ErrorCode::io_error, "cannot open config file: " + path.string());
    }
    SettingMap settings;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        const auto trimmed = trim(line);
        if (trimmed.empty() || trimmed.front() == '#') {
            continue;
        }
        const auto separator = trimmed.find('=');
        if (separator == std::string_view::npos) {
            return fail(ErrorCode::invalid_argument, "config " + path.string() + " line " +
                                                         std::to_string(line_number) +
                                                         ": expected key = value");
        }
        const auto key = trim(trimmed.substr(0, separator));
        auto value_view = trim(trimmed.substr(separator + 1));
        std::string value{value_view};
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }
        const auto where = "config " + path.string() + " line " + std::to_string(line_number);
        if (auto status = ingest_setting(settings, key, std::move(value), where); !status) {
            return unexpected(status.error());
        }
    }
    if (settings.contains("reuse-port") && settings.at("reuse-port") == "true" &&
        settings.contains("no-reuse-port") && settings.at("no-reuse-port") == "true") {
        return fail(ErrorCode::invalid_argument,
                    "config " + path.string() + ": reuse-port and no-reuse-port are mutually exclusive");
    }
    return settings;
}

auto load_daemon_environment(const DaemonEnvironmentLookup& getenv_fn)
    -> Result<std::map<std::string, std::string, std::less<>>> {
    const DaemonEnvironmentLookup& lookup = getenv_fn ? getenv_fn : DaemonEnvironmentLookup{default_getenv};
    SettingMap settings;
    for (const auto& spec : kOptionSpecs) {
        if (spec.id == help || spec.id == version || spec.id == config || spec.id == profile ||
            spec.id == dump_config) {
            continue;
        }
        const auto env_name = environment_name_for_option(spec.long_name);
        const auto value = lookup(env_name);
        if (!value) {
            continue;
        }
        if (auto status = ingest_setting(settings, spec.long_name, *value, env_name); !status) {
            return unexpected(status.error());
        }
    }
    if (settings.contains("reuse-port") && settings.at("reuse-port") == "true" &&
        settings.contains("no-reuse-port") && settings.at("no-reuse-port") == "true") {
        return fail(ErrorCode::invalid_argument,
                    "GLYPHASTORE_REUSE_PORT and GLYPHASTORE_NO_REUSE_PORT are mutually exclusive");
    }
    return settings;
}

auto parse_daemon_options(const int argc, char* const argv[], DaemonEnvironmentLookup getenv_fn)
    -> Result<DaemonOptions> {
    auto parsed_cli = cli::parse_arguments(argc, argv, kOptionSpecs);
    if (!parsed_cli) {
        return unexpected(parsed_cli.error());
    }
    if (!parsed_cli->positionals.empty()) {
        return fail(ErrorCode::invalid_argument,
                    "unexpected positional argument: " + std::string{parsed_cli->positionals.front()});
    }

    const bool show_help = parsed_cli->has(help);
    const bool show_version = parsed_cli->has(version);
    if (show_help || show_version) {
        DaemonOptions options;
        options.show_help = show_help;
        options.show_version = show_version;
        return options;
    }

    const DaemonEnvironmentLookup& lookup = getenv_fn ? getenv_fn : DaemonEnvironmentLookup{default_getenv};
    std::optional<std::filesystem::path> config_path;
    if (const auto path = parsed_cli->value(config)) {
        if (path->empty()) {
            return fail(ErrorCode::invalid_argument, "--config must not be empty");
        }
        config_path = std::filesystem::path{*path};
    } else if (const auto from_env = lookup("GLYPHASTORE_CONFIG")) {
        if (from_env->empty()) {
            return fail(ErrorCode::invalid_argument, "GLYPHASTORE_CONFIG must not be empty");
        }
        config_path = std::filesystem::path{*from_env};
    }

    SettingMap file_settings;
    if (config_path) {
        auto loaded = load_daemon_config_file(*config_path);
        if (!loaded) {
            return unexpected(loaded.error());
        }
        file_settings = std::move(*loaded);
    }
    auto env_settings = load_daemon_environment(lookup);
    if (!env_settings) {
        return unexpected(env_settings.error());
    }
    auto cli_settings = settings_from_parsed(*parsed_cli);
    if (!cli_settings) {
        return unexpected(cli_settings.error());
    }

    std::optional<std::string> resolved_profile;
    if (auto from_file = take_profile_name(file_settings)) {
        resolved_profile = std::move(*from_file);
    }
    if (const auto from_env = lookup("GLYPHASTORE_PROFILE")) {
        if (from_env->empty()) {
            return fail(ErrorCode::invalid_argument, "GLYPHASTORE_PROFILE must not be empty");
        }
        resolved_profile = *from_env;
    }
    if (auto from_cli = take_profile_name(*cli_settings)) {
        resolved_profile = std::move(*from_cli);
    }

    SettingMap merged;
    if (resolved_profile) {
        auto profile_settings = deployment_profile_settings(*resolved_profile);
        if (!profile_settings) {
            return unexpected(profile_settings.error());
        }
        apply_layer(merged, *profile_settings);
        *resolved_profile = ascii_lower(std::move(*resolved_profile));
    }
    apply_layer(merged, file_settings);
    apply_layer(merged, *env_settings);
    apply_layer(merged, *cli_settings);

    auto options = materialize_from_settings(std::move(merged), false, false,
                                             resolved_profile.value_or(std::string{}));
    if (!options) {
        return unexpected(options.error());
    }
    options->show_dump_config = parsed_cli->has(dump_config);
    return options;
}

} // namespace glyphastore::server
