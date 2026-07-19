#include "glyphastore/client/client.hpp"
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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <iterator>
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
    Config config{.workers = 1, .threads = 1, .distribution = ParallelDistribution::owner_bound};
    RunSettings settings{};
    std::size_t pipeline{32};
    bool executor_affinity{};
    bool latency{};
    bool client_api{};
};

struct ClientWork {
    std::vector<std::vector<std::byte>> batches;
    std::size_t response_count{};
};

struct Sample {
    std::size_t hits{};
    double seconds{};
    glyphastore::bench::ResourceSample resources{};
    std::vector<double> latency_ns;
    bool valid{};
};

struct ClientResult {
    std::size_t hits{};
    std::size_t ingress_bytes{};
    std::size_t egress_bytes{};
    std::vector<double> latency_ns;
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
        } else if (argument == "--latency") {
            result.latency = true;
        } else if (argument == "--client-api") {
            result.client_api = true;
        } else if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastore_server_benchmarks [--ops N] [--key-size N]"
                         " [--value-size N] [--workers N] [--clients N] [--pipeline N]"
                         " [--executor-affinity] [--latency] [--client-api]"
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
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
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

[[nodiscard]] auto load_u32(const std::span<const std::byte> input) noexcept -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t byte = 0; byte < sizeof(value); ++byte) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[byte])) << (byte * 8U);
    }
    return value;
}

class BufferedResponseReader final {
  public:
    explicit BufferedResponseReader(const std::size_t reserve_bytes) {
        buffer_.reserve(reserve_bytes);
    }

    [[nodiscard]] auto receive(const int descriptor)
        -> glyphastore::Result<glyphastore::server::DecodedFrame<glyphastore::server::ResponseView>> {
        while (true) {
            const auto available = buffer_.size() - offset_;
            if (available >= sizeof(std::uint32_t)) {
                const std::span<const std::byte> pending{buffer_.data() + offset_, available};
                const auto size = static_cast<std::size_t>(load_u32(pending));
                if (size < glyphastore::server::kResponseHeaderBytes ||
                    size > glyphastore::server::kMaxFrameBytes) {
                    return glyphastore::fail(glyphastore::ErrorCode::invalid_record,
                                             "benchmark response size is invalid");
                }
                if (available >= size) {
                    auto decoded = glyphastore::server::decode_response(pending.first(size));
                    if (decoded) {
                        offset_ += size;
                    }
                    return decoded;
                }
            }

            if (offset_ > 0) {
                buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset_));
                offset_ = 0;
            }
            std::array<std::byte, 64U * 1024U> chunk;
            const auto count = ::recv(descriptor, chunk.data(), chunk.size(), 0);
            if (count > 0) {
                buffer_.insert(buffer_.end(), chunk.begin(),
                               chunk.begin() + static_cast<std::ptrdiff_t>(count));
                continue;
            }
            if (count < 0 && errno == EINTR) {
                continue;
            }
            return glyphastore::fail(glyphastore::ErrorCode::io_error, "benchmark response receive failed");
        }
    }

  private:
    std::vector<std::byte> buffer_;
    std::size_t offset_{};
};

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
        } while (glyphastore::route_worker(key, config.workers) != client % config.workers);
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
                              const glyphastore::bench::KeyMaterial& material, const std::size_t pipeline,
                              const bool measure_latency) -> ClientResult {
    const auto bytes_per_pair =
        2U * (glyphastore::server::kResponseHeaderBytes + material.values.front().size());
    const auto response_capacity = pipeline > glyphastore::server::kMaxFrameBytes / bytes_per_pair
                                       ? glyphastore::server::kMaxFrameBytes
                                       : pipeline * bytes_per_pair;
    BufferedResponseReader responses{response_capacity};
    ClientResult result;
    if (measure_latency) {
        result.latency_ns.reserve(work.response_count);
    }
    std::size_t responses_remaining = work.response_count;
    for (const auto& batch : work.batches) {
        const auto batch_started =
            measure_latency ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        if (!send_all(descriptor, batch)) {
            return {};
        }
        result.ingress_bytes += batch.size();
        const auto batch_responses = std::min(pipeline * 2U, responses_remaining);
        for (std::size_t index = 0; index < batch_responses; ++index) {
            auto decoded = responses.receive(descriptor);
            if (!decoded || !decoded->complete ||
                decoded->frame.status != glyphastore::server::ResponseStatus::ok) {
                return {};
            }
            result.egress_bytes += decoded->consumed;
            if (measure_latency) {
                const auto elapsed = std::chrono::steady_clock::now() - batch_started;
                result.latency_ns.push_back(std::chrono::duration<double, std::nano>(elapsed).count());
            }
            const auto request_id = decoded->frame.request_id;
            const auto operation = request_id / 2U;
            if (operation >= material.values.size()) {
                return {};
            }
            if ((request_id & 1U) != 0U &&
                !std::ranges::equal(decoded->frame.value, material.values[operation])) {
                return {};
            }
            ++result.hits;
        }
        responses_remaining -= batch_responses;
    }
    return result;
}

[[nodiscard]] auto run_sample(const Options& options, const glyphastore::bench::KeyMaterial& material,
                              const std::vector<ClientWork>& work) -> Sample {
    auto server = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = std::max(std::size_t{16}, options.config.threads * 2U),
        .worker_count = options.config.workers,
        .executor_affinity = options.executor_affinity,
    });
    if (!server || !(*server)->start()) {
        return {};
    }
    std::vector<int> descriptors;
    descriptors.reserve(options.config.threads);
    const auto cleanup = [&] {
        for (const auto descriptor : descriptors) {
            static_cast<void>(::close(descriptor));
        }
        (*server)->request_stop();
        static_cast<void>((*server)->join());
    };
    for (std::size_t client = 0; client < options.config.threads; ++client) {
        const auto descriptor = connect_to((*server)->port());
        if (descriptor < 0) {
            cleanup();
            return {};
        }
        descriptors.push_back(descriptor);
    }
    for (std::size_t client = 0; client < descriptors.size(); ++client) {
        BufferedResponseReader responses{glyphastore::server::kResponseHeaderBytes + 64U};
        const auto init = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::init,
            .request_id = 0xFFFF'FFFF'0000'0000ULL + client * 2U,
        });
        const auto bind = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::bind_worker,
            .request_id = 0xFFFF'FFFF'0000'0001ULL + client * 2U,
            .target_worker = static_cast<std::uint32_t>(client % options.config.workers),
        });
        if (!init || !bind || !send_all(descriptors[client], *init)) {
            cleanup();
            return {};
        }
        auto initialized = responses.receive(descriptors[client]);
        if (!initialized || initialized->frame.status != glyphastore::server::ResponseStatus::ok ||
            initialized->frame.worker_count != options.config.workers ||
            !send_all(descriptors[client], *bind)) {
            cleanup();
            return {};
        }
        auto bound = responses.receive(descriptors[client]);
        if (!bound || bound->frame.status != glyphastore::server::ResponseStatus::ok ||
            bound->frame.owner_worker != client % options.config.workers) {
            cleanup();
            return {};
        }
    }

    auto resources = glyphastore::bench::process_memory_snapshot();
    std::latch ready{static_cast<std::ptrdiff_t>(options.config.threads)};
    std::latch start{1};
    std::vector<ClientResult> client_results(options.config.threads);
    std::vector<std::thread> clients;
    clients.reserve(options.config.threads);
    for (std::size_t client = 0; client < options.config.threads; ++client) {
        clients.emplace_back([&, client] {
            ready.count_down();
            start.wait();
            client_results[client] =
                run_client(descriptors[client], work[client], material, options.pipeline, options.latency);
        });
    }
    ready.wait();
    const auto started = std::chrono::steady_clock::now();
    start.count_down();
    for (auto& client : clients) {
        client.join();
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    const auto after = glyphastore::bench::process_memory_snapshot();
    resources.rss_after_bytes = after.rss_after_bytes;
    resources.peak_rss_bytes = after.peak_rss_bytes;
    std::size_t hits{};
    for (const auto& client_result : client_results) {
        hits += client_result.hits;
        resources.ingress_bytes += client_result.ingress_bytes;
        resources.egress_bytes += client_result.egress_bytes;
    }
    const auto expected = options.config.operations * 2U;
    std::vector<double> latency_ns;
    if (options.latency) {
        latency_ns.reserve(expected);
        for (auto& client_result : client_results) {
            latency_ns.insert(latency_ns.end(), std::make_move_iterator(client_result.latency_ns.begin()),
                              std::make_move_iterator(client_result.latency_ns.end()));
        }
    }
    for (const auto descriptor : descriptors) {
        static_cast<void>(::close(descriptor));
    }
    (*server)->request_stop();
    const auto stopped = (*server)->join();
    return {.hits = hits,
            .seconds = elapsed,
            .resources = resources,
            .latency_ns = std::move(latency_ns),
            .valid = stopped.has_value() && hits == expected};
}

[[nodiscard]] auto run_client_api_sample(const Options& options,
                                         const glyphastore::bench::KeyMaterial& material) -> Sample {
    auto server = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = std::max(std::size_t{16}, options.config.workers * 2U),
        .worker_count = options.config.workers,
        .executor_affinity = options.executor_affinity,
    });
    if (!server || !(*server)->start()) {
        return {};
    }
    auto connected = glyphastore::client::Client::connect({.port = (*server)->port()});
    if (!connected) {
        (*server)->request_stop();
        static_cast<void>((*server)->join());
        return {};
    }
    auto client = std::move(*connected);
    auto resources = glyphastore::bench::process_memory_snapshot();
    std::latch ready{static_cast<std::ptrdiff_t>(options.config.threads)};
    std::latch start{1};
    std::vector<ClientResult> client_results(options.config.threads);
    std::vector<std::thread> clients;
    clients.reserve(options.config.threads);
    for (std::size_t client_index = 0; client_index < options.config.threads; ++client_index) {
        clients.emplace_back([&, client_index] {
            auto& result = client_results[client_index];
            if (options.latency) {
                result.latency_ns.reserve((options.config.operations / options.config.threads + 1U) * 2U);
            }
            ready.count_down();
            start.wait();
            for (std::size_t operation = client_index; operation < options.config.operations;
                 operation += options.config.threads) {
                const auto put_started = std::chrono::steady_clock::now();
                if (!client
                         .put(bytes(material.keys[operation]),
                              {material.values[operation].data(), material.values[operation].size()})
                         .committed()) {
                    return;
                }
                if (options.latency) {
                    result.latency_ns.push_back(std::chrono::duration<double, std::nano>(
                                                    std::chrono::steady_clock::now() - put_started)
                                                    .count());
                }
                ++result.hits;
                const auto get_started = std::chrono::steady_clock::now();
                auto loaded = client.get(material.keys[operation]);
                if (!loaded || !std::ranges::equal(*loaded, material.values[operation])) {
                    return;
                }
                if (options.latency) {
                    result.latency_ns.push_back(std::chrono::duration<double, std::nano>(
                                                    std::chrono::steady_clock::now() - get_started)
                                                    .count());
                }
                ++result.hits;
                result.ingress_bytes += 2U * glyphastore::server::kRequestHeaderBytes +
                                        material.keys[operation].size() * 2U +
                                        material.values[operation].size();
                result.egress_bytes +=
                    2U * glyphastore::server::kResponseHeaderBytes + material.values[operation].size();
            }
        });
    }
    ready.wait();
    const auto started = std::chrono::steady_clock::now();
    start.count_down();
    for (auto& thread : clients) {
        thread.join();
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    const auto after = glyphastore::bench::process_memory_snapshot();
    resources.rss_after_bytes = after.rss_after_bytes;
    resources.peak_rss_bytes = after.peak_rss_bytes;
    std::size_t hits{};
    std::vector<double> latency_ns;
    for (auto& result : client_results) {
        hits += result.hits;
        resources.ingress_bytes += result.ingress_bytes;
        resources.egress_bytes += result.egress_bytes;
        latency_ns.insert(latency_ns.end(), std::make_move_iterator(result.latency_ns.begin()),
                          std::make_move_iterator(result.latency_ns.end()));
    }
    client.close();
    (*server)->request_stop();
    const auto stopped = (*server)->join();
    const auto expected = options.config.operations * 2U;
    return {.hits = hits,
            .seconds = elapsed,
            .resources = resources,
            .latency_ns = std::move(latency_ns),
            .valid = stopped.has_value() && hits == expected};
}

[[nodiscard]] auto percentile(const std::vector<double>& sorted, const double quantile) -> double {
    if (sorted.empty()) {
        return 0.0;
    }
    const auto rank = static_cast<std::size_t>(std::ceil(quantile * static_cast<double>(sorted.size())));
    return sorted[std::min(std::max(std::size_t{1}, rank), sorted.size()) - 1U];
}

[[nodiscard]] auto run_benchmark(const Options& options) -> Result {
    const auto material = make_material(options.config);
    const auto work = options.client_api ? std::vector<ClientWork>{}
                                         : prepare_work(options.config, options.pipeline, material);
    if (!options.client_api && work.size() != options.config.threads) {
        return {};
    }
    const auto sample = [&] {
        return options.client_api ? run_client_api_sample(options, material)
                                  : run_sample(options, material, work);
    };
    for (std::size_t iteration = 0; iteration < options.settings.warmup_iterations; ++iteration) {
        if (!sample().valid) {
            return {};
        }
    }
    std::vector<double> seconds;
    std::vector<glyphastore::bench::ResourceSample> resources;
    std::vector<double> latency_ns;
    seconds.reserve(options.settings.measured_iterations);
    resources.reserve(options.settings.measured_iterations);
    std::size_t hits{};
    for (std::size_t iteration = 0; iteration < options.settings.measured_iterations; ++iteration) {
        auto measured = sample();
        if (!measured.valid) {
            return {};
        }
        hits = measured.hits;
        seconds.push_back(measured.seconds);
        resources.push_back(measured.resources);
        latency_ns.insert(latency_ns.end(), std::make_move_iterator(measured.latency_ns.begin()),
                          std::make_move_iterator(measured.latency_ns.end()));
    }
    const auto benchmark_name =
        options.client_api ? "cpp_client_read_after_write" : "server_tcp_read_after_write";
    auto result = glyphastore::bench::finalize_result(benchmark_name, options.config, options.settings,
                                                      options.config.operations * 2U, hits,
                                                      std::move(seconds), std::move(resources));
    if (!latency_ns.empty()) {
        std::ranges::sort(latency_ns);
        result.latency_samples = latency_ns.size();
        result.p50_latency_ns = percentile(latency_ns, 0.50);
        result.p95_latency_ns = percentile(latency_ns, 0.95);
        result.p99_latency_ns = percentile(latency_ns, 0.99);
        result.p999_latency_ns = percentile(latency_ns, 0.999);
    }
    return result;
}

} // namespace

int main(int argc, char** argv) {
    const auto parsed = options(argc, argv);
    if (!glyphastore::bench::validate_run_settings(parsed.settings, parsed.config) || parsed.pipeline == 0) {
        return 2;
    }
    std::cout << "# glyphastore TCP server benchmark\n";
    glyphastore::bench::print_metadata(std::cout, parsed.settings);
    std::cout << "# client_mode=" << (parsed.client_api ? "public-cpp-api" : "raw-wire") << '\n';
    std::cout << "# pipeline=" << (parsed.client_api ? 1 : parsed.pipeline) << '\n';
    std::cout << "# routing=owner-bound-connections\n";
    std::cout << "# traffic_scope=timed-protocol-frames-excluding-init-bind\n";
    std::cout << "# memory_scope=whole-benchmark-process-rss\n";
    std::cout << "# executor_affinity_requested=" << (parsed.executor_affinity ? 1 : 0) << '\n';
    std::cout << "# latency_measurement="
              << (parsed.latency ? (parsed.client_api ? "synchronous-api-call" : "pipelined-response")
                                 : "disabled")
              << '\n';
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
