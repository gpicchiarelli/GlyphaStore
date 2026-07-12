#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "harness.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <latch>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using glyphastore::bench::Config;
using glyphastore::bench::ParallelDistribution;
using glyphastore::bench::Result;
using glyphastore::bench::RunSettings;

struct Options {
    Config config{.workers = 1, .threads = 1};
    RunSettings settings{};
    std::size_t pipeline{32};
    bool executor_affinity{};
};

struct ClientWork {
    std::vector<std::vector<std::byte>> batches;
    std::size_t response_count{};
};

struct Sample {
    std::size_t hits{};
    double seconds{};
    bool valid{};
};

[[nodiscard]] auto parse_size(const std::string_view value, const char* flag) -> std::size_t {
    std::size_t parsed{};
    const auto converted = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (converted.ec != std::errc{} || converted.ptr != value.data() + value.size()) {
        std::cerr << "invalid value for " << flag << ": " << value << '\n';
        std::exit(2);
    }
    return parsed;
}

[[nodiscard]] auto options(const int argc, char** argv) -> Options {
    Options result;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        const auto next_size = [&](const char* flag) {
            if (index + 1 >= argc) {
                std::cerr << "missing value for " << flag << '\n';
                std::exit(2);
            }
            return parse_size(argv[++index], flag);
        };
        if (argument == "--ops") {
            result.config.operations = next_size("--ops");
        } else if (argument == "--key-size") {
            result.config.key_size = next_size("--key-size");
        } else if (argument == "--value-size") {
            result.config.value_size = next_size("--value-size");
        } else if (argument == "--workers") {
            result.config.workers = next_size("--workers");
        } else if (argument == "--clients") {
            result.config.threads = next_size("--clients");
        } else if (argument == "--pipeline") {
            result.pipeline = next_size("--pipeline");
        } else if (argument == "--warmup") {
            result.settings.warmup_iterations = next_size("--warmup");
        } else if (argument == "--repeats") {
            result.settings.measured_iterations = next_size("--repeats");
        } else if (argument == "--executor-affinity") {
            result.executor_affinity = true;
        } else if (argument == "--routing" && index + 1 < argc) {
            const std::string_view routing{argv[++index]};
            if (routing == "uniform") {
                result.config.distribution = ParallelDistribution::uniform;
            } else if (routing == "affine") {
                result.config.distribution = ParallelDistribution::worker_affine;
            } else {
                std::cerr << "unknown routing: " << routing << '\n';
                std::exit(2);
            }
        } else if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastore_server_benchmarks [--ops N] [--key-size N]"
                         " [--value-size N] [--workers N] [--clients N] [--pipeline N]"
                         " [--routing uniform|affine] [--executor-affinity]"
                         " [--warmup N] [--repeats N]\n";
            std::exit(0);
        } else {
            std::cerr << "unknown or incomplete argument: " << argument << '\n';
            std::exit(2);
        }
    }
    return result;
}

[[nodiscard]] auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto connect_to(const std::uint16_t port) -> int {
    const auto descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
    if (descriptor < 0) {
        return -1;
    }
    const int enabled = 1;
    static_cast<void>(::setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)));
    timeval timeout{.tv_sec = 10, .tv_usec = 0};
    static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)));
#if defined(__APPLE__) || defined(__FreeBSD__)
    static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)));
#endif
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    static_cast<void>(::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr));
    if (::connect(descriptor, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        static_cast<void>(::close(descriptor));
        return -1;
    }
    return descriptor;
}

[[nodiscard]] auto send_all(const int descriptor, const std::span<const std::byte> data) -> bool {
    std::size_t sent{};
    while (sent < data.size()) {
#if defined(__linux__)
        const auto written = ::send(descriptor, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
#else
        const auto written = ::send(descriptor, data.data() + sent, data.size() - sent, 0);
#endif
        if (written > 0) {
            sent += static_cast<std::size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] auto receive_exact(const int descriptor, const std::span<std::byte> output) -> bool {
    std::size_t received{};
    while (received < output.size()) {
        const auto count = ::recv(descriptor, output.data() + received, output.size() - received, 0);
        if (count > 0) {
            received += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] auto load_u32(const std::span<const std::byte> input) noexcept -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t byte = 0; byte < sizeof(value); ++byte) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[byte])) << (byte * 8U);
    }
    return value;
}

[[nodiscard]] auto receive_response(const int descriptor, std::vector<std::byte>& frame)
    -> glyphastore::Result<glyphastore::server::DecodedFrame<glyphastore::server::ResponseView>> {
    frame.resize(glyphastore::server::kResponseHeaderBytes);
    if (!receive_exact(descriptor, frame)) {
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "benchmark response header failed");
    }
    const auto size = static_cast<std::size_t>(load_u32(frame));
    if (size < frame.size() || size > glyphastore::server::kMaxFrameBytes) {
        return glyphastore::fail(glyphastore::ErrorCode::invalid_record,
                                 "benchmark response size is invalid");
    }
    const auto header_size = frame.size();
    frame.resize(size);
    if (!receive_exact(descriptor, std::span<std::byte>{frame}.subspan(header_size))) {
        return glyphastore::fail(glyphastore::ErrorCode::io_error, "benchmark response body failed");
    }
    return glyphastore::server::decode_response(frame);
}

[[nodiscard]] auto make_material(const Config& config) -> glyphastore::bench::KeyMaterial {
    glyphastore::bench::KeyMaterial material;
    material.keys.reserve(config.operations);
    material.values.reserve(config.operations);
    std::size_t candidate{};
    for (std::size_t operation = 0; operation < config.operations; ++operation) {
        std::string key;
        const auto client = operation % config.threads;
        do {
            key = glyphastore::bench::make_key(candidate++, config.key_size);
        } while (config.distribution == ParallelDistribution::worker_affine &&
                 glyphastore::route_worker(key, config.workers) != client % config.workers);
        material.keys.push_back(std::move(key));
        material.values.push_back(glyphastore::bench::make_value(operation, config.value_size));
    }
    return material;
}

[[nodiscard]] auto prepare_work(const Config& config, const std::size_t pipeline,
                                const glyphastore::bench::KeyMaterial& material) -> std::vector<ClientWork> {
    std::vector<ClientWork> work(config.threads);
    for (std::size_t client = 0; client < config.threads; ++client) {
        std::vector<std::byte> batch;
        std::size_t keys_in_batch{};
        for (std::size_t operation = client; operation < config.operations; operation += config.threads) {
            const auto put = glyphastore::server::encode_request({
                .opcode = glyphastore::server::RequestOpcode::put,
                .request_id = operation * 2U,
                .key = bytes(material.keys[operation]),
                .value = material.values[operation],
            });
            const auto get = glyphastore::server::encode_request({
                .opcode = glyphastore::server::RequestOpcode::get,
                .request_id = operation * 2U + 1U,
                .key = bytes(material.keys[operation]),
            });
            if (!put || !get) {
                return {};
            }
            batch.insert(batch.end(), put->begin(), put->end());
            batch.insert(batch.end(), get->begin(), get->end());
            ++keys_in_batch;
            work[client].response_count += 2U;
            if (keys_in_batch == pipeline) {
                work[client].batches.push_back(std::move(batch));
                batch.clear();
                keys_in_batch = 0;
            }
        }
        if (!batch.empty()) {
            work[client].batches.push_back(std::move(batch));
        }
    }
    return work;
}

[[nodiscard]] auto run_client(const int descriptor, const ClientWork& work,
                              const glyphastore::bench::KeyMaterial& material, const std::size_t pipeline)
    -> std::size_t {
    std::vector<std::byte> response;
    std::size_t hits{};
    std::size_t responses_remaining = work.response_count;
    for (const auto& batch : work.batches) {
        if (!send_all(descriptor, batch)) {
            return 0;
        }
        const auto batch_responses = std::min(pipeline * 2U, responses_remaining);
        for (std::size_t index = 0; index < batch_responses; ++index) {
            auto decoded = receive_response(descriptor, response);
            if (!decoded || !decoded->complete ||
                decoded->frame.status != glyphastore::server::ResponseStatus::ok) {
                return 0;
            }
            const auto request_id = decoded->frame.request_id;
            const auto operation = request_id / 2U;
            if (operation >= material.values.size()) {
                return 0;
            }
            if ((request_id & 1U) != 0U &&
                !std::ranges::equal(decoded->frame.value, material.values[operation])) {
                return 0;
            }
            ++hits;
        }
        responses_remaining -= batch_responses;
    }
    return hits;
}

[[nodiscard]] auto run_sample(const Options& options, const glyphastore::bench::KeyMaterial& material,
                              const std::vector<ClientWork>& work) -> Sample {
    auto server = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = std::max(std::size_t{16}, options.config.threads * 2U),
        .worker_count = options.config.workers,
        .reuse_port = options.config.distribution != ParallelDistribution::worker_affine,
        .executor_affinity = options.executor_affinity,
    });
    if (!server || !(*server)->start()) {
        return {};
    }
    std::vector<int> descriptors;
    descriptors.reserve(options.config.threads);
    for (std::size_t client = 0; client < options.config.threads; ++client) {
        const auto descriptor = connect_to((*server)->port());
        if (descriptor < 0) {
            for (const auto connected : descriptors) {
                static_cast<void>(::close(connected));
            }
            (*server)->request_stop();
            static_cast<void>((*server)->join());
            return {};
        }
        descriptors.push_back(descriptor);
    }

    std::latch ready{static_cast<std::ptrdiff_t>(options.config.threads)};
    std::latch start{1};
    std::vector<std::size_t> client_hits(options.config.threads);
    std::vector<std::thread> clients;
    clients.reserve(options.config.threads);
    for (std::size_t client = 0; client < options.config.threads; ++client) {
        clients.emplace_back([&, client] {
            ready.count_down();
            start.wait();
            client_hits[client] = run_client(descriptors[client], work[client], material, options.pipeline);
        });
    }
    ready.wait();
    const auto started = std::chrono::steady_clock::now();
    start.count_down();
    for (auto& client : clients) {
        client.join();
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::size_t hits{};
    for (const auto client_result : client_hits) {
        hits += client_result;
    }
    for (const auto descriptor : descriptors) {
        static_cast<void>(::close(descriptor));
    }
    (*server)->request_stop();
    const auto stopped = (*server)->join();
    const auto expected = options.config.operations * 2U;
    return {.hits = hits, .seconds = elapsed, .valid = stopped.has_value() && hits == expected};
}

[[nodiscard]] auto run_benchmark(const Options& options) -> Result {
    const auto material = make_material(options.config);
    const auto work = prepare_work(options.config, options.pipeline, material);
    if (work.size() != options.config.threads) {
        return {};
    }
    for (std::size_t iteration = 0; iteration < options.settings.warmup_iterations; ++iteration) {
        if (!run_sample(options, material, work).valid) {
            return {};
        }
    }
    std::vector<double> seconds;
    seconds.reserve(options.settings.measured_iterations);
    std::size_t hits{};
    for (std::size_t iteration = 0; iteration < options.settings.measured_iterations; ++iteration) {
        const auto sample = run_sample(options, material, work);
        if (!sample.valid) {
            return {};
        }
        hits = sample.hits;
        seconds.push_back(sample.seconds);
    }
    return glyphastore::bench::finalize_result("server_tcp_read_after_write", options.config,
                                               options.settings, options.config.operations * 2U, hits,
                                               std::move(seconds));
}

} // namespace

int main(int argc, char** argv) {
    const auto parsed = options(argc, argv);
    if (!glyphastore::bench::validate_run_settings(parsed.settings, parsed.config) || parsed.pipeline == 0) {
        return 2;
    }
    std::cout << "# glyphastore TCP server benchmark\n";
    glyphastore::bench::print_metadata(std::cout, parsed.settings);
    std::cout << "# pipeline=" << parsed.pipeline << '\n';
    std::cout << "# executor_affinity_requested=" << (parsed.executor_affinity ? 1 : 0) << '\n';
#if defined(__APPLE__)
    std::cout << "# executor_affinity_semantics=mach-advisory\n";
#elif defined(__linux__)
    std::cout << "# executor_affinity_semantics=cpu-pinned\n";
#else
    std::cout << "# executor_affinity_semantics=unavailable\n";
#endif
    const auto result = run_benchmark(parsed);
    if (result.samples != parsed.settings.measured_iterations) {
        std::cerr << "benchmark error: TCP sample validation failed\n";
        return 1;
    }
    glyphastore::bench::print_result(std::cout, result);
    return 0;
}
