#include "experimental/paired_reactor.hpp"
#include "glyphastore/client/client.hpp"
#include "glyphastore/server/server.hpp"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options final {
    std::size_t operations{200'000};
    std::size_t keys{4'096};
    std::size_t value_bytes{64};
    std::size_t pipeline{32};
    std::size_t clients{4};
    std::size_t repeats{5};
    std::size_t warmup{1};
    std::size_t put_percent{5};
    std::size_t batch_wait_us{2};
};

struct Material final {
    std::vector<std::string> keys;
    std::vector<std::byte> value;
    std::vector<std::byte> update;
    std::vector<std::size_t> order;
};

struct Measurement final {
    std::string implementation;
    std::size_t repeat{};
    double operations_per_second{};
    double p50_batch_us{};
    double p99_batch_us{};
    double p999_batch_us{};
    std::uint64_t checksum{};
};

[[nodiscard]] auto parse_positive(const char* value, const std::string_view flag) -> std::size_t {
    if (value == nullptr) {
        throw std::invalid_argument{"missing " + std::string{flag}};
    }
    const auto parsed = std::stoull(value);
    if (parsed == 0 || parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument{"invalid " + std::string{flag}};
    }
    return static_cast<std::size_t>(parsed);
}

[[nodiscard]] auto parse_options(const int argc, char** argv) -> Options {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        if (argument == "--help") {
            std::cout << "usage: glyphastore_paired_reactor_benchmark [--ops N] [--keys N] "
                         "[--value-bytes N] [--pipeline N] [--clients N] [--put-percent N] [--repeats N] "
                         "[--warmup N] [--batch-wait-us N]\n";
            std::exit(0);
        }
        if (index + 1 >= argc) {
            throw std::invalid_argument{"missing option value"};
        }
        if (argument == "--ops") {
            options.operations = parse_positive(argv[++index], argument);
        } else if (argument == "--keys") {
            options.keys = parse_positive(argv[++index], argument);
        } else if (argument == "--value-bytes") {
            options.value_bytes = parse_positive(argv[++index], argument);
        } else if (argument == "--pipeline") {
            options.pipeline = parse_positive(argv[++index], argument);
        } else if (argument == "--clients") {
            options.clients = parse_positive(argv[++index], argument);
        } else if (argument == "--put-percent") {
            options.put_percent = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--repeats") {
            options.repeats = parse_positive(argv[++index], argument);
        } else if (argument == "--warmup") {
            options.warmup = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--batch-wait-us") {
            options.batch_wait_us = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else {
            throw std::invalid_argument{"unknown option: " + std::string{argument}};
        }
    }
    if (options.keys > options.operations || options.value_bytes > 256U * 1024U || options.pipeline > 128 ||
        options.clients > 16 || options.put_percent > 100 || options.batch_wait_us > 1'000) {
        throw std::invalid_argument{"paired Reactor benchmark limits are invalid"};
    }
    return options;
}

[[nodiscard]] auto make_material(const Options& options) -> Material {
    Material material;
    material.keys.reserve(options.keys);
    for (std::size_t index = 0; index < options.keys; ++index) {
        material.keys.push_back("paired-reactor-key-" + std::to_string(index));
    }
    material.value.resize(options.value_bytes);
    material.update.resize(options.value_bytes);
    for (std::size_t index = 0; index < options.value_bytes; ++index) {
        material.value[index] = static_cast<std::byte>((index * 131U + 17U) & 0xFFU);
        material.update[index] = static_cast<std::byte>((index * 197U + 91U) & 0xFFU);
    }
    material.order.reserve(options.operations);
    std::uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (std::size_t index = 0; index < options.operations; ++index) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        material.order.push_back(static_cast<std::size_t>(state % options.keys));
    }
    return material;
}

[[nodiscard]] auto key_bytes(const std::string& value) noexcept -> std::span<const std::byte> {
    return std::as_bytes(std::span{value.data(), value.size()});
}

[[nodiscard]] auto percentile(std::vector<double> values, const double quantile) -> double {
    std::sort(values.begin(), values.end());
    const auto rank = static_cast<std::size_t>(std::ceil(quantile * static_cast<double>(values.size())));
    return values[std::min(values.size() - 1U, rank == 0 ? 0U : rank - 1U)];
}

class CurrentServer final {
  public:
    explicit CurrentServer(const std::size_t clients) {
        auto created = glyphastore::server::Server::create({.port = 0,
                                                            .maximum_connections = clients + 2U,
                                                            .worker_count = 1,
                                                            .maximum_input_bytes = 4U * 1024U * 1024U,
                                                            .maximum_output_bytes = 4U * 1024U * 1024U},
                                                           {.worker_config = {.explicit_count = 1}});
        if (!created || !(*created)->start()) {
            throw std::runtime_error{"cannot start current TCP baseline"};
        }
        server_ = std::move(*created);
    }

    ~CurrentServer() {
        server_->request_stop();
        static_cast<void>(server_->join());
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return server_->port();
    }

  private:
    std::unique_ptr<glyphastore::server::Server> server_;
};

class PairedServer final {
  public:
    PairedServer(const Options& options) {
        auto created = glyphastore::experimental::PairedReactorPrototype::create(
            {.maximum_connections = options.clients + 2U,
             .maximum_value_bytes = options.value_bytes,
             .merge_delta_entries = options.keys,
             .writer_batch = {.max_records = 32,
                              .max_wait = std::chrono::microseconds{options.batch_wait_us}}});
        if (!created) {
            throw std::runtime_error{created.error().message};
        }
        reactor_ = std::move(*created);
        thread_ = std::jthread([this](const std::stop_token stop) {
            while (!stop.stop_requested()) {
                if (auto status = reactor_->run_once(10); !status) {
                    failed_.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    ~PairedServer() {
        thread_.request_stop();
        thread_.join();
        reactor_->stop_accepting();
        reactor_->close_all_connections();
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return reactor_->port();
    }

    [[nodiscard]] auto reactor() noexcept -> glyphastore::experimental::PairedReactorPrototype& {
        return *reactor_;
    }

    [[nodiscard]] auto failed() const noexcept -> bool {
        return failed_.load(std::memory_order_acquire);
    }

  private:
    std::unique_ptr<glyphastore::experimental::PairedReactorPrototype> reactor_;
    std::jthread thread_;
    std::atomic_bool failed_{};
};

[[nodiscard]] auto connect(const std::uint16_t port) -> glyphastore::client::Client {
    auto client = glyphastore::client::Client::connect({.host = "127.0.0.1",
                                                        .port = port,
                                                        .request_timeout_ms = 30'000,
                                                        .maximum_pipeline_requests = 256,
                                                        .maximum_pipeline_bytes = 4U * 1024U * 1024U});
    if (!client) {
        throw std::runtime_error{client.error().message};
    }
    return std::move(*client);
}

void seed(glyphastore::client::Client& client, const Material& material) {
    for (const auto& key : material.keys) {
        const auto stored = client.put(key_bytes(key), material.value);
        if (!stored.committed()) {
            throw std::runtime_error{"TCP seed failed"};
        }
    }
}

struct ThreadMeasurement final {
    std::vector<double> latencies;
    std::uint64_t checksum{};
    std::exception_ptr failure;
};

[[nodiscard]] auto run(std::vector<glyphastore::client::Client>& clients, const Material& material,
                       const Options& options,
                       std::string implementation, const std::size_t repeat) -> Measurement {
    std::vector<ThreadMeasurement> results(clients.size());
    std::barrier start_gate{static_cast<std::ptrdiff_t>(clients.size() + 1U)};
    std::vector<std::jthread> threads;
    threads.reserve(clients.size());
    for (std::size_t client_index = 0; client_index < clients.size(); ++client_index) {
        threads.emplace_back([&, client_index] {
            auto& result = results[client_index];
            const auto begin = options.operations * client_index / clients.size();
            const auto end = options.operations * (client_index + 1U) / clients.size();
            result.latencies.reserve((end - begin + options.pipeline - 1U) / options.pipeline);
            std::vector<glyphastore::client::PipelineRequest> requests;
            requests.reserve(options.pipeline);
            start_gate.arrive_and_wait();
            try {
                for (std::size_t first = begin; first < end; first += options.pipeline) {
                    requests.clear();
                    const auto count = std::min(options.pipeline, end - first);
                    for (std::size_t offset = 0; offset < count; ++offset) {
                        const auto operation = first + offset;
                        const auto& key = material.keys[material.order[operation]];
                        const auto put = options.put_percent != 0 &&
                                         (operation * 37U) % 100U < options.put_percent;
                        requests.push_back(
                            {.opcode = put ? glyphastore::client::PipelineOpcode::put
                                           : glyphastore::client::PipelineOpcode::get,
                             .key = key_bytes(key),
                             .value = put ? std::span<const std::byte>{material.update}
                                          : std::span<const std::byte>{}});
                    }
                    const auto batch_started = Clock::now();
                    auto responses = clients[client_index].execute_pipeline(requests);
                    const auto batch_elapsed = Clock::now() - batch_started;
                    if (!responses || responses->size() != requests.size()) {
                        throw std::runtime_error{"TCP pipeline failed"};
                    }
                    for (const auto& response : *responses) {
                        if (!response.succeeded()) {
                            throw std::runtime_error{"TCP pipeline response failed"};
                        }
                        result.checksum += response.value.size();
                    }
                    result.latencies.push_back(
                        std::chrono::duration<double, std::micro>(batch_elapsed).count());
                }
            } catch (...) {
                result.failure = std::current_exception();
            }
        });
    }
    const auto started = Clock::now();
    start_gate.arrive_and_wait();
    threads.clear();
    const auto elapsed = Clock::now() - started;
    std::vector<double> latencies;
    latencies.reserve((options.operations + options.pipeline - 1U) / options.pipeline + clients.size());
    std::uint64_t checksum{};
    for (auto& result : results) {
        if (result.failure) {
            std::rethrow_exception(result.failure);
        }
        checksum += result.checksum;
        latencies.insert(latencies.end(), result.latencies.begin(), result.latencies.end());
    }
    return {.implementation = std::move(implementation),
            .repeat = repeat,
            .operations_per_second =
                static_cast<double>(options.operations) / std::chrono::duration<double>(elapsed).count(),
            .p50_batch_us = percentile(latencies, 0.50),
            .p99_batch_us = percentile(latencies, 0.99),
            .p999_batch_us = percentile(std::move(latencies), 0.999),
            .checksum = checksum};
}

void print(const Measurement& measurement) {
    std::cout << "sample," << measurement.implementation << ',' << measurement.repeat << ','
              << measurement.operations_per_second << ',' << measurement.p50_batch_us << ','
              << measurement.p99_batch_us << ',' << measurement.p999_batch_us << ',' << measurement.checksum
              << '\n';
}

void summary(const std::vector<Measurement>& measurements, const std::string_view implementation) {
    std::vector<double> throughput;
    std::vector<double> p99;
    std::vector<double> p999;
    for (const auto& measurement : measurements) {
        if (measurement.implementation == implementation) {
            throughput.push_back(measurement.operations_per_second);
            p99.push_back(measurement.p99_batch_us);
            p999.push_back(measurement.p999_batch_us);
        }
    }
    std::cout << "summary," << implementation << ',' << percentile(std::move(throughput), 0.50) << ','
              << percentile(std::move(p99), 0.50) << ',' << percentile(std::move(p999), 0.50) << '\n';
}

} // namespace

int main(int argc, char** argv) {
    try {
        const auto options = parse_options(argc, argv);
        const auto material = make_material(options);
        CurrentServer current_server{options.clients};
        PairedServer paired_server{options};
        std::vector<glyphastore::client::Client> current;
        std::vector<glyphastore::client::Client> paired;
        current.reserve(options.clients);
        paired.reserve(options.clients);
        for (std::size_t index = 0; index < options.clients; ++index) {
            current.push_back(connect(current_server.port()));
            paired.push_back(connect(paired_server.port()));
        }
        seed(current.front(), material);
        seed(paired.front(), material);
        std::vector<Measurement> measurements;
        std::cout << "# paired Reactor TCP A/B git=" << GLYPHASTORE_GIT_SHA << " ops=" << options.operations
                  << " keys=" << options.keys << " value_bytes=" << options.value_bytes
                  << " pipeline=" << options.pipeline << " clients=" << options.clients
                  << " put_percent=" << options.put_percent
                  << " batch_wait_us=" << options.batch_wait_us << '\n';
        std::cout << "kind,implementation,repeat,ops_per_second,p50_batch_us,p99_batch_us,"
                     "p999_batch_us,checksum\n";
        for (std::size_t iteration = 0; iteration < options.warmup + options.repeats; ++iteration) {
            const auto measured = iteration >= options.warmup;
            const auto repeat = measured ? iteration - options.warmup : 0;
            auto first = iteration % 2U == 0 ? run(current, material, options, "current", repeat)
                                             : run(paired, material, options, "paired", repeat);
            auto second = iteration % 2U == 0 ? run(paired, material, options, "paired", repeat)
                                              : run(current, material, options, "current", repeat);
            if (measured) {
                print(first);
                print(second);
                measurements.push_back(std::move(first));
                measurements.push_back(std::move(second));
            }
        }
        std::cout << "kind,implementation,median_ops_per_second,median_p99_batch_us,"
                     "median_p999_batch_us\n";
        summary(measurements, "current");
        summary(measurements, "paired");
        const auto reactor = paired_server.reactor().stats();
        const auto pair = paired_server.reactor().pair_stats();
        std::cout << "telemetry,writev_calls," << reactor.writev_calls << ",partial_writes,"
                  << reactor.partial_writes << ",slow_output_pins," << reactor.slow_output_pins
                  << ",mutation_backpressure," << reactor.mutation_backpressure << ",publications,"
                  << pair.publications << ",generation_output_pin_high_watermark,"
                  << pair.generation_output_pin_high_watermark << ",publication_backpressure,"
                  << pair.publication_backpressure << '\n';
        if (paired_server.failed()) {
            throw std::runtime_error{"paired Reactor thread failed"};
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "paired Reactor benchmark error: " << error.what() << '\n';
        return 2;
    }
}
