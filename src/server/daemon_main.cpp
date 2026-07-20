#include "cli/arguments.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/server/tls.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

namespace {

volatile std::sig_atomic_t g_stop_signal = 0;

extern "C" void request_stop(const int signal) {
    g_stop_signal = signal;
}

enum OptionId : std::size_t {
    help,
    version,
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
    reuse_port,
    no_reuse_port,
    executor_affinity,
    storage_mode,
    data_directory,
    durable_open_mode,
    maintenance_mode,
    quiet,
    tls_cert,
    tls_key,
    tls_client_ca,
    tls_port,
};

constexpr std::array kOptionSpecs{
    glyphastore::cli::OptionSpec{
        help, "help", 'h', glyphastore::cli::OptionArity::none, {}, "Show this help message and exit"},
    glyphastore::cli::OptionSpec{version,
                                 "version",
                                 'V',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Show version information and exit"},
    glyphastore::cli::OptionSpec{bind, "bind", 'b', glyphastore::cli::OptionArity::required, "IPv4",
                                 "Bind to an IPv4 address (default: 127.0.0.1; non-loopback is "
                                 "cleartext with no authentication — trusted networks only)"},
    glyphastore::cli::OptionSpec{port, "port", 'p', glyphastore::cli::OptionArity::required, "PORT",
                                 "Listen on PORT; 0 selects an ephemeral port (default: 7379)"},
    glyphastore::cli::OptionSpec{workers, "workers", 'w', glyphastore::cli::OptionArity::required, "COUNT",
                                 "Run COUNT Store workers and reactor executors (default: 1)"},
    glyphastore::cli::OptionSpec{maximum_connections, "max-connections", '\0',
                                 glyphastore::cli::OptionArity::required, "COUNT",
                                 "Limit concurrent connections (default: 4096)"},
    glyphastore::cli::OptionSpec{handoff_capacity, "handoff-capacity", '\0',
                                 glyphastore::cli::OptionArity::required, "COUNT",
                                 "Bound each connection handoff queue (default: 4096)"},
    glyphastore::cli::OptionSpec{event_batch_size, "event-batch-size", '\0',
                                 glyphastore::cli::OptionArity::required, "COUNT",
                                 "Process at most COUNT readiness events per wait (default: 256)"},
    glyphastore::cli::OptionSpec{maximum_input_bytes, "max-input-bytes", '\0',
                                 glyphastore::cli::OptionArity::required, "BYTES",
                                 "Limit buffered input per connection (default: 4MiB)"},
    glyphastore::cli::OptionSpec{maximum_output_bytes, "max-output-bytes", '\0',
                                 glyphastore::cli::OptionArity::required, "BYTES",
                                 "Limit buffered output per connection (default: 4MiB)"},
    glyphastore::cli::OptionSpec{durable_mutation_queue_capacity, "durable-mutation-queue-capacity", '\0',
                                 glyphastore::cli::OptionArity::required, "COUNT",
                                 "Bound admitted durable mutations per Worker (default: 256)"},
    glyphastore::cli::OptionSpec{durable_mutation_queue_bytes, "durable-mutation-queue-bytes", '\0',
                                 glyphastore::cli::OptionArity::required, "BYTES",
                                 "Bound owned durable mutation bytes per Worker (default: 16MiB)"},
    glyphastore::cli::OptionSpec{durable_group_mutation_concurrency, "durable-group-concurrency", '\0',
                                 glyphastore::cli::OptionArity::required, "COUNT",
                                 "Run up to COUNT strict-group producers per Worker (default: 4)"},
    glyphastore::cli::OptionSpec{durable_mutation_queue_wait, "durable-mutation-queue-wait-ms", '\0',
                                 glyphastore::cli::OptionArity::required, "MILLISECONDS",
                                 "Expire queued mutations after this wait; 0 disables (default: 1000)"},
    glyphastore::cli::OptionSpec{reuse_port,
                                 "reuse-port",
                                 '\0',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Enable per-executor SO_REUSEPORT listeners where supported"},
    glyphastore::cli::OptionSpec{no_reuse_port,
                                 "no-reuse-port",
                                 '\0',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Disable SO_REUSEPORT listeners"},
    glyphastore::cli::OptionSpec{executor_affinity,
                                 "executor-affinity",
                                 '\0',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Request executor CPU affinity where supported"},
    glyphastore::cli::OptionSpec{storage_mode, "storage-mode", '\0', glyphastore::cli::OptionArity::required,
                                 "MODE",
                                 "Use volatile, durable-sync, durable-periodic, or durable-group storage"},
    glyphastore::cli::OptionSpec{data_directory, "data-dir", '\0', glyphastore::cli::OptionArity::required,
                                 "PATH", "Store durable v1 files below PATH (required for durable storage)"},
    glyphastore::cli::OptionSpec{
        durable_open_mode, "open-mode", '\0', glyphastore::cli::OptionArity::required, "MODE",
        "Use open-or-create, create-new, or open-existing (default: open-or-create)"},
    glyphastore::cli::OptionSpec{
        maintenance_mode, "maintenance-mode", '\0', glyphastore::cli::OptionArity::required, "MODE",
        "Use cooperative, background (default), or disabled Store maintenance scheduling"},
    glyphastore::cli::OptionSpec{quiet,
                                 "quiet",
                                 'q',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Suppress normal startup and shutdown messages"},
    glyphastore::cli::OptionSpec{
        tls_cert, "tls-cert", '\0', glyphastore::cli::OptionArity::required, "PATH",
        "PEM certificate chain for TLS 1.3 (requires --tls-key; TLS-only on --port unless --tls-port)"},
    glyphastore::cli::OptionSpec{tls_key, "tls-key", '\0', glyphastore::cli::OptionArity::required, "PATH",
                                 "PEM private key for TLS (requires --tls-cert)"},
    glyphastore::cli::OptionSpec{
        tls_client_ca, "tls-client-ca", '\0', glyphastore::cli::OptionArity::required, "PATH",
        "PEM CA for client certificates (enables mTLS; requires --tls-cert/--tls-key)"},
    glyphastore::cli::OptionSpec{
        tls_port, "tls-port", '\0', glyphastore::cli::OptionArity::required, "PORT",
        "Listen for TLS on PORT while --port stays cleartext (requires --tls-cert/--tls-key; 0=ephemeral)"},
};

struct Options {
    glyphastore::server::ReactorConfig server;
    glyphastore::StoreConfig store{
        .maintenance = {.mode = glyphastore::MaintenanceMode::background},
    };
    bool show_help{};
    bool show_version{};
    bool quiet{};
};

[[nodiscard]] auto parse_storage_mode(const std::string_view value)
    -> std::optional<glyphastore::StorageMode> {
    if (value == "volatile") {
        return glyphastore::StorageMode::volatile_memory;
    }
    if (value == "durable-sync") {
        return glyphastore::StorageMode::durable_sync;
    }
    if (value == "durable-periodic") {
        return glyphastore::StorageMode::durable_periodic;
    }
    if (value == "durable-group") {
        return glyphastore::StorageMode::durable_group;
    }
    return std::nullopt;
}

[[nodiscard]] auto parse_durable_open_mode(const std::string_view value)
    -> std::optional<glyphastore::DurableOpenMode> {
    if (value == "open-or-create") {
        return glyphastore::DurableOpenMode::open_or_create;
    }
    if (value == "create-new") {
        return glyphastore::DurableOpenMode::create_new;
    }
    if (value == "open-existing") {
        return glyphastore::DurableOpenMode::open_existing;
    }
    return std::nullopt;
}

[[nodiscard]] auto parse_maintenance_mode(const std::string_view value)
    -> std::optional<glyphastore::MaintenanceMode> {
    if (value == "cooperative") {
        return glyphastore::MaintenanceMode::cooperative;
    }
    if (value == "background") {
        return glyphastore::MaintenanceMode::background;
    }
    if (value == "disabled") {
        return glyphastore::MaintenanceMode::disabled;
    }
    return std::nullopt;
}

[[nodiscard]] auto storage_mode_name(const glyphastore::StorageMode mode) -> std::string_view {
    switch (mode) {
    case glyphastore::StorageMode::volatile_memory:
        return "volatile";
    case glyphastore::StorageMode::durable_sync:
        return "durable-sync";
    case glyphastore::StorageMode::durable_periodic:
        return "durable-periodic";
    case glyphastore::StorageMode::durable_group:
        return "durable-group";
    }
    return "unknown";
}

[[nodiscard]] auto set_size_option(const glyphastore::cli::ParsedArguments& parsed, const OptionId id,
                                   const std::string_view name, const std::size_t minimum,
                                   const std::size_t maximum, std::size_t& destination)
    -> glyphastore::Status {
    const auto text = parsed.value(id);
    if (!text) {
        return {};
    }
    auto value = glyphastore::cli::parse_size(*text, name, minimum, maximum);
    if (!value) {
        return glyphastore::unexpected(value.error());
    }
    destination = *value;
    return {};
}

[[nodiscard]] auto set_byte_size_option(const glyphastore::cli::ParsedArguments& parsed, const OptionId id,
                                        const std::string_view name, const std::size_t minimum,
                                        const std::size_t maximum, std::size_t& destination)
    -> glyphastore::Status {
    const auto text = parsed.value(id);
    if (!text) {
        return {};
    }
    auto value = glyphastore::cli::parse_byte_size(*text, name, minimum, maximum);
    if (!value) {
        return glyphastore::unexpected(value.error());
    }
    destination = *value;
    return {};
}

[[nodiscard]] auto parse_options(const int argc, char* const argv[]) -> glyphastore::Result<Options> {
    auto parsed = glyphastore::cli::parse_arguments(argc, argv, kOptionSpecs);
    if (!parsed) {
        return glyphastore::unexpected(parsed.error());
    }
    if (!parsed->positionals.empty()) {
        return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                 "unexpected positional argument: " +
                                     std::string{parsed->positionals.front()});
    }

    Options options;
    options.show_help = parsed->has(help);
    options.show_version = parsed->has(version);
    options.quiet = parsed->has(quiet);
    if (const auto address = parsed->value(bind)) {
        options.server.bind_address = *address;
    }

    std::size_t parsed_port = options.server.port;
    if (auto status = set_size_option(*parsed, port, "--port", 0, std::numeric_limits<std::uint16_t>::max(),
                                      parsed_port);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    options.server.port = static_cast<std::uint16_t>(parsed_port);

    constexpr auto maximum_size = std::numeric_limits<std::size_t>::max();
    if (auto status = set_size_option(*parsed, workers, "--workers", 1, glyphastore::kMaximumWorkerCount,
                                      options.server.worker_count);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status =
            set_size_option(*parsed, maximum_connections, "--max-connections", 1,
                            std::numeric_limits<std::uint32_t>::max(), options.server.maximum_connections);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status = set_size_option(*parsed, handoff_capacity, "--handoff-capacity", 1,
                                      std::size_t{1} << 30U, options.server.connection_handoff_capacity);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status = set_size_option(*parsed, event_batch_size, "--event-batch-size", 1, maximum_size,
                                      options.server.event_batch_size);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status = set_byte_size_option(*parsed, maximum_input_bytes, "--max-input-bytes",
                                           glyphastore::server::kRequestHeaderBytes, maximum_size,
                                           options.server.maximum_input_bytes);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status = set_byte_size_option(*parsed, maximum_output_bytes, "--max-output-bytes",
                                           glyphastore::server::kResponseHeaderBytes, maximum_size,
                                           options.server.maximum_output_bytes);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status =
            set_size_option(*parsed, durable_mutation_queue_capacity, "--durable-mutation-queue-capacity", 1,
                            std::size_t{1} << 30U, options.server.durable_mutation_queue_capacity);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status =
            set_byte_size_option(*parsed, durable_mutation_queue_bytes, "--durable-mutation-queue-bytes", 1,
                                 maximum_size, options.server.durable_mutation_queue_bytes);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    if (auto status =
            set_size_option(*parsed, durable_group_mutation_concurrency, "--durable-group-concurrency", 1, 32,
                            options.server.durable_group_mutation_concurrency);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    std::size_t mutation_queue_wait_ms = options.server.durable_mutation_queue_wait_ms;
    if (auto status =
            set_size_option(*parsed, durable_mutation_queue_wait, "--durable-mutation-queue-wait-ms", 0,
                            std::numeric_limits<std::uint32_t>::max(), mutation_queue_wait_ms);
        !status) {
        return glyphastore::unexpected(status.error());
    }
    options.server.durable_mutation_queue_wait_ms = static_cast<std::uint32_t>(mutation_queue_wait_ms);
    if (parsed->has(reuse_port) && parsed->has(no_reuse_port)) {
        return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                 "--reuse-port and --no-reuse-port are mutually exclusive");
    }
    if (parsed->has(reuse_port)) {
        options.server.reuse_port = true;
    }
    if (parsed->has(no_reuse_port)) {
        options.server.reuse_port = false;
    }
    options.server.executor_affinity = parsed->has(executor_affinity);

    if (const auto mode = parsed->value(storage_mode)) {
        const auto selected = parse_storage_mode(*mode);
        if (!selected) {
            return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                     "unknown --storage-mode: " + std::string{*mode});
        }
        options.store.storage_mode = *selected;
    }
    if (const auto path = parsed->value(data_directory)) {
        if (path->empty()) {
            return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                     "--data-dir must not be empty");
        }
        options.store.data_directory = std::filesystem::path{*path};
    }
    if (const auto mode = parsed->value(durable_open_mode)) {
        const auto selected = parse_durable_open_mode(*mode);
        if (!selected) {
            return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                     "unknown --open-mode: " + std::string{*mode});
        }
        options.store.durable_open_mode = *selected;
    }
    if (const auto mode = parsed->value(maintenance_mode)) {
        const auto selected = parse_maintenance_mode(*mode);
        if (!selected) {
            return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                     "unknown --maintenance-mode: " + std::string{*mode});
        }
        options.store.maintenance.mode = *selected;
    }
    const bool durable = options.store.storage_mode != glyphastore::StorageMode::volatile_memory;
    if (durable && !options.store.data_directory) {
        return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                 "--data-dir is required for durable storage");
    }
    if (!durable && (parsed->has(data_directory) || parsed->has(durable_open_mode))) {
        return glyphastore::fail(glyphastore::ErrorCode::invalid_argument,
                                 "--data-dir and --open-mode require a durable --storage-mode");
    }

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
        if (auto status = set_size_option(*parsed, tls_port, "--tls-port", 0,
                                          std::numeric_limits<std::uint16_t>::max(), parsed_tls_port);
            !status) {
            return glyphastore::unexpected(status.error());
        }
        options.server.tls_port = static_cast<std::uint16_t>(parsed_tls_port);
    }
    if (auto tls = glyphastore::server::validate_tls_config(options.server.tls); !tls) {
        return glyphastore::unexpected(tls.error());
    }
    return options;
}

[[nodiscard]] auto install_signal_handler(const int signal, const char* name) -> glyphastore::Status {
    struct sigaction action{};
    action.sa_handler = request_stop;
    if (sigemptyset(&action.sa_mask) != 0 || ::sigaction(signal, &action, nullptr) != 0) {
        const auto error_number = errno;
        return glyphastore::fail(glyphastore::ErrorCode::io_error,
                                 std::string{"cannot install "} + name + " handler: " +
                                     std::error_code{error_number, std::system_category()}.message());
    }
    return {};
}

void print_help(const std::string_view program) {
    glyphastore::cli::write_help(std::cout, program, "GlyphaStore native binary key-value server.",
                                 "[OPTIONS]", kOptionSpecs);
}

} // namespace

int main(const int argc, char** argv) try {
    const auto program = glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastored");
    auto arguments = parse_options(argc, argv);
    if (!arguments) {
        std::cerr << program << ": error: " << arguments.error().message << "\nTry '" << program
                  << " --help' for more information.\n";
        return 2;
    }
    if (arguments->show_help) {
        print_help(program);
        return 0;
    }
    if (arguments->show_version) {
        std::cout << program << ' ' << GLYPHASTORE_VERSION << '\n';
        return 0;
    }

    auto server = glyphastore::server::Server::create(arguments->server, arguments->store);
    if (!server) {
        std::cerr << program << ": error: " << server.error().message << '\n';
        return 1;
    }
    if (auto installed = install_signal_handler(SIGINT, "SIGINT"); !installed) {
        std::cerr << program << ": error: " << installed.error().message << '\n';
        return 1;
    }
    if (auto installed = install_signal_handler(SIGTERM, "SIGTERM"); !installed) {
        std::cerr << program << ": error: " << installed.error().message << '\n';
        return 1;
    }
    if (auto started = (*server)->start(); !started) {
        std::cerr << program << ": error: " << started.error().message << '\n';
        return 1;
    }
    if (arguments->server.bind_address != "127.0.0.1" &&
        ((*server)->cleartext_port() != 0 || !arguments->server.tls.requested())) {
        std::cerr << program
                  << ": warning: bind address " << arguments->server.bind_address
                  << " exposes cleartext TCP with no authentication; restrict to a trusted network "
                     "or use TLS-only (--tls-cert/--tls-key without --tls-port; OpenBSD uses LibreSSL; "
                     "docs/security/roadmap.md)\n";
    }
    if (!arguments->quiet) {
        std::cout << program << ": listening address=" << arguments->server.bind_address
                  << " executors=" << (*server)->executor_count()
                  << " storage=" << storage_mode_name(arguments->store.storage_mode);
        if ((*server)->cleartext_port() != 0 && (*server)->tls_port() != 0) {
            std::cout << " cleartext_port=" << (*server)->cleartext_port()
                      << " tls_port=" << (*server)->tls_port()
                      << " transport=cleartext+tls1.3 backend=" << glyphastore::server::tls_backend_name();
            if (arguments->server.tls.mtls_enabled()) {
                std::cout << " auth=mtls";
            } else {
                std::cout << " auth=none";
            }
        } else if ((*server)->tls_port() != 0) {
            std::cout << " port=" << (*server)->tls_port()
                      << " transport=tls1.3 backend=" << glyphastore::server::tls_backend_name();
            if (arguments->server.tls.mtls_enabled()) {
                std::cout << " auth=mtls";
            } else {
                std::cout << " auth=none";
            }
        } else {
            std::cout << " port=" << (*server)->port() << " (cleartext; no authentication)";
        }
        std::cout << '\n';
    }
    while (g_stop_signal == 0 && (*server)->healthy()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    (*server)->request_stop();
    if (auto stopped = (*server)->join(); !stopped) {
        std::cerr << program << ": error: reactor failure: " << stopped.error().message << '\n';
        return 1;
    }
    if (!arguments->quiet) {
        std::cout << program << ": stopped";
        if (g_stop_signal != 0) {
            std::cout << " signal=" << g_stop_signal;
        }
        std::cout << '\n';
    }
    return 0;
} catch (const std::exception& exception) {
    const auto program = glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastored");
    std::cerr << program << ": fatal: " << exception.what() << '\n';
    return 1;
} catch (...) {
    const auto program = glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastored");
    std::cerr << program << ": fatal: unknown non-standard exception\n";
    return 1;
}
