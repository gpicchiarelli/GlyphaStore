#include "glyphastore/server/server.hpp"

#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <thread>

namespace {

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void request_stop(int) {
    g_stop_requested = 1;
}

struct Options {
    std::string bind_address{"127.0.0.1"};
    std::uint16_t port{7379};
    std::size_t maximum_connections{4096};
    std::size_t workers{1};
    std::size_t handoff_capacity{4096};
    bool executor_affinity{};
};

template <typename T> [[nodiscard]] auto parse_integer(const std::string_view text, const char* flag) -> T {
    std::uint64_t parsed{};
    const auto converted = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (converted.ec != std::errc{} || converted.ptr != text.data() + text.size() ||
        parsed > std::numeric_limits<T>::max()) {
        std::cerr << "invalid value for " << flag << ": " << text << '\n';
        std::exit(2);
    }
    return static_cast<T>(parsed);
}

[[nodiscard]] auto options(int argc, char** argv) -> Options {
    Options result;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        if (argument == "--bind" && index + 1 < argc) {
            result.bind_address = argv[++index];
            continue;
        }
        if (argument == "--port" && index + 1 < argc) {
            result.port = parse_integer<std::uint16_t>(argv[++index], "--port");
            continue;
        }
        if (argument == "--max-connections" && index + 1 < argc) {
            result.maximum_connections = parse_integer<std::size_t>(argv[++index], "--max-connections");
            continue;
        }
        if (argument == "--workers" && index + 1 < argc) {
            result.workers = parse_integer<std::size_t>(argv[++index], "--workers");
            continue;
        }
        if (argument == "--handoff-capacity" && index + 1 < argc) {
            result.handoff_capacity = parse_integer<std::size_t>(argv[++index], "--handoff-capacity");
            continue;
        }
        if (argument == "--executor-affinity") {
            result.executor_affinity = true;
            continue;
        }
        if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastored [--bind IPv4] [--port N] [--max-connections N]"
                         " [--workers N] [--handoff-capacity N] [--executor-affinity]\n";
            std::exit(0);
        }
        std::cerr << "unknown or incomplete argument: " << argument << '\n';
        std::exit(2);
    }
    return result;
}

} // namespace

int main(int argc, char** argv) {
    const auto arguments = options(argc, argv);
    auto server =
        glyphastore::server::Server::create({.bind_address = arguments.bind_address,
                                             .port = arguments.port,
                                             .maximum_connections = arguments.maximum_connections,
                                             .worker_count = arguments.workers,
                                             .connection_handoff_capacity = arguments.handoff_capacity,
                                             .executor_affinity = arguments.executor_affinity});
    if (!server) {
        std::cerr << "glyphastored: " << server.error().message << '\n';
        return 1;
    }

    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    if (auto started = (*server)->start(); !started) {
        std::cerr << "glyphastored: " << started.error().message << '\n';
        return 1;
    }
    std::cout << "glyphastored listening on " << arguments.bind_address << ':' << (*server)->port()
              << " with " << (*server)->executor_count() << " executors\n";
    while (g_stop_requested == 0 && (*server)->healthy()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    (*server)->request_stop();
    if (auto stopped = (*server)->join(); !stopped) {
        std::cerr << "glyphastored: reactor failure: " << stopped.error().message << '\n';
        return 1;
    }
    return 0;
}
