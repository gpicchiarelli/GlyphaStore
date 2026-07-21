#include "cli/arguments.hpp"
#include "glyphastore/server/daemon_config.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/server/tls.hpp"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

namespace {

volatile std::sig_atomic_t g_stop_signal = 0;

extern "C" void request_stop(const int signal) {
    g_stop_signal = signal;
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
                                 "[OPTIONS]", glyphastore::server::daemon_option_specs());
}

} // namespace

int main(const int argc, char** argv) try {
    const auto program = glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastored");
    auto arguments = glyphastore::server::parse_daemon_options(argc, argv);
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
                  << " storage=" << glyphastore::server::storage_mode_name(arguments->store.storage_mode);
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
