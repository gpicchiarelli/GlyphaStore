#include "glyphastore/server/reactor.hpp"

#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

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
    std::size_t worker_inbox_capacity{4096};
    std::size_t completion_queue_capacity{65'536};
    std::size_t maximum_in_flight{1024};
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
        if (argument == "--worker-inbox" && index + 1 < argc) {
            result.worker_inbox_capacity = parse_integer<std::size_t>(argv[++index], "--worker-inbox");
            continue;
        }
        if (argument == "--completion-queue" && index + 1 < argc) {
            result.completion_queue_capacity =
                parse_integer<std::size_t>(argv[++index], "--completion-queue");
            continue;
        }
        if (argument == "--max-in-flight" && index + 1 < argc) {
            result.maximum_in_flight = parse_integer<std::size_t>(argv[++index], "--max-in-flight");
            continue;
        }
        if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastored [--bind IPv4] [--port N] [--max-connections N]"
                         " [--workers N] [--worker-inbox N] [--completion-queue N]"
                         " [--max-in-flight N]\n";
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
    auto reactor = glyphastore::server::Reactor::create(
        {.bind_address = arguments.bind_address,
         .port = arguments.port,
         .maximum_connections = arguments.maximum_connections,
         .worker_count = arguments.workers,
         .worker_inbox_capacity = arguments.worker_inbox_capacity,
         .completion_queue_capacity = arguments.completion_queue_capacity,
         .maximum_in_flight_per_connection = arguments.maximum_in_flight});
    if (!reactor) {
        std::cerr << "glyphastored: " << reactor.error().message << '\n';
        return 1;
    }

    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    std::cout << "glyphastored listening on " << arguments.bind_address << ':' << (*reactor)->port() << '\n';
    while (g_stop_requested == 0) {
        if (auto status = (*reactor)->run_once(1000); !status) {
            std::cerr << "glyphastored: reactor failure: " << status.error().message << '\n';
            return 1;
        }
    }
    return 0;
}
