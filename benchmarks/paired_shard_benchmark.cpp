#include "experimental/paired_shard.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/store.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using glyphastore::experimental::PrototypeSubmitStatus;
using glyphastore::experimental::PrototypeWriterBatchConfig;
using glyphastore::experimental::VolatileShardPairPrototype;

struct Options final {
    std::size_t operations{500'000};
    std::size_t keys{4'096};
    std::size_t value_bytes{64};
    std::size_t repeats{7};
    std::size_t warmup{1};
    std::size_t batch_records{32};
    std::size_t batch_wait_us{2};
};

struct Material final {
    std::vector<std::string> keys;
    std::vector<std::byte> value;
    std::vector<std::byte> update_value;
    std::vector<std::size_t> order;
};

struct Measurement final {
    std::string implementation;
    std::string workload;
    std::size_t repeat{};
    double seconds{};
    double operations_per_second{};
    double p50_ns{};
    double p95_ns{};
    double p99_ns{};
    double p999_ns{};
    std::size_t latency_samples{};
    std::uint64_t checksum{};
};

[[nodiscard]] auto parse_size(const char* text, const std::string_view flag) -> std::size_t {
    if (text == nullptr) {
        throw std::invalid_argument{"missing value for " + std::string{flag}};
    }
    const auto value = std::stoull(text);
    if (value == 0 || value > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument{"invalid value for " + std::string{flag}};
    }
    return static_cast<std::size_t>(value);
}

[[nodiscard]] auto parse_options(const int argc, char** argv) -> Options {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastore_paired_benchmark [--ops N] [--keys N] "
                         "[--value-bytes N] [--repeats N] [--warmup N] "
                         "[--batch-records N] [--batch-wait-us N]\n";
            std::exit(0);
        }
        if (index + 1 >= argc) {
            throw std::invalid_argument{"missing argument value"};
        }
        if (argument == "--ops") {
            options.operations = parse_size(argv[++index], argument);
        } else if (argument == "--keys") {
            options.keys = parse_size(argv[++index], argument);
        } else if (argument == "--value-bytes") {
            options.value_bytes = parse_size(argv[++index], argument);
        } else if (argument == "--repeats") {
            options.repeats = parse_size(argv[++index], argument);
        } else if (argument == "--warmup") {
            options.warmup = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--batch-records") {
            options.batch_records = parse_size(argv[++index], argument);
        } else if (argument == "--batch-wait-us") {
            options.batch_wait_us = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else {
            throw std::invalid_argument{"unknown argument: " + std::string{argument}};
        }
    }
    if (options.keys > options.operations || options.value_bytes > 256U * 1024U ||
        options.batch_records > 32 || options.batch_wait_us > 1'000) {
        throw std::invalid_argument{
            "keys must not exceed ops; value-bytes maximum is 256 KiB; invalid batch limits"};
    }
    return options;
}

[[nodiscard]] auto make_material(const Options& options) -> Material {
    Material material;
    material.keys.reserve(options.keys);
    for (std::size_t index = 0; index < options.keys; ++index) {
        auto key = std::string{"paired-key-"} + std::to_string(index);
        material.keys.push_back(std::move(key));
    }
    material.value.resize(options.value_bytes);
    material.update_value.resize(options.value_bytes);
    for (std::size_t index = 0; index < material.value.size(); ++index) {
        material.value[index] = static_cast<std::byte>((index * 131U + 17U) & 0xFFU);
        material.update_value[index] = static_cast<std::byte>((index * 197U + 91U) & 0xFFU);
    }
    material.order.reserve(options.operations);
    std::uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (std::size_t operation = 0; operation < options.operations; ++operation) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        material.order.push_back(static_cast<std::size_t>(state % options.keys));
    }
    return material;
}

[[nodiscard]] auto percentile(std::vector<double> values, const double quantile) -> double {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto rank = static_cast<std::size_t>(std::ceil(quantile * static_cast<double>(values.size())));
    return values[std::min(values.size() - 1U, rank == 0 ? 0U : rank - 1U)];
}

[[nodiscard]] auto finish_measurement(std::string implementation, std::string workload,
                                      const std::size_t repeat, const std::size_t operations,
                                      const Clock::duration elapsed, std::vector<double> latencies,
                                      const std::uint64_t checksum) -> Measurement {
    const auto seconds = std::chrono::duration<double>(elapsed).count();
    Measurement result{.implementation = std::move(implementation),
                       .workload = std::move(workload),
                       .repeat = repeat,
                       .seconds = seconds,
                       .operations_per_second = static_cast<double>(operations) / seconds,
                       .latency_samples = latencies.size(),
                       .checksum = checksum};
    result.p50_ns = percentile(latencies, 0.50);
    result.p95_ns = percentile(latencies, 0.95);
    result.p99_ns = percentile(latencies, 0.99);
    result.p999_ns = percentile(std::move(latencies), 0.999);
    return result;
}

[[nodiscard]] auto open_current_store(const Material& material) -> std::unique_ptr<glyphastore::Store> {
    glyphastore::StoreConfig config{.worker_config = {.explicit_count = 1}};
    auto opened = glyphastore::Store::open(config);
    if (!opened) {
        throw std::runtime_error{"cannot open current Store baseline"};
    }
    for (const auto& key : material.keys) {
        if (auto status = (*opened)->put(key, material.value); !status) {
            throw std::runtime_error{"cannot seed current Store baseline"};
        }
    }
    return std::move(*opened);
}

[[nodiscard]] auto wait_completion(VolatileShardPairPrototype& pair)
    -> glyphastore::experimental::PrototypeCompletion {
    for (;;) {
        if (auto completion = pair.try_pop_completion()) {
            return *completion;
        }
        pair.adopt_publication();
        std::this_thread::yield();
    }
}

[[nodiscard]] auto open_pair(const Material& material, const Options& options)
    -> std::unique_ptr<VolatileShardPairPrototype> {
    auto created = VolatileShardPairPrototype::create(
        material.value.size(), material.keys.size(),
        PrototypeWriterBatchConfig{.max_records = options.batch_records,
                                   .max_wait = std::chrono::microseconds{options.batch_wait_us}});
    if (!created) {
        throw std::runtime_error{"cannot open paired prototype"};
    }
    auto pair = std::move(*created);
    constexpr std::size_t kSeedWindow = 128;
    std::uint64_t request = 1;
    for (std::size_t first = 0; first < material.keys.size(); first += kSeedWindow) {
        const auto count = std::min(kSeedWindow, material.keys.size() - first);
        for (std::size_t offset = 0; offset < count; ++offset) {
            const auto status =
                pair->try_submit_put(request++, material.keys[first + offset], material.value);
            if (status != PrototypeSubmitStatus::submitted) {
                throw std::runtime_error{"cannot seed paired prototype"};
            }
        }
        for (std::size_t offset = 0; offset < count; ++offset) {
            if (auto completion = wait_completion(*pair); completion.error) {
                throw std::runtime_error{"paired seed publication failed"};
            }
        }
        pair->adopt_publication();
    }
    pair->adopt_publication();
    return pair;
}

[[nodiscard]] auto run_current_get(glyphastore::Store& store, const Material& material,
                                   const std::size_t repeat) -> Measurement {
    std::vector<double> latencies;
    latencies.reserve(material.order.size() / 64U + 1U);
    std::uint64_t checksum = 0;
    const auto started = Clock::now();
    for (std::size_t operation = 0; operation < material.order.size(); ++operation) {
        const auto sampled = (operation & 63U) == 0;
        const auto sample_start = sampled ? Clock::now() : started;
        auto found = store.get_copy(material.keys[material.order[operation]]);
        if (!found) {
            throw std::runtime_error{"current Store GET failed"};
        }
        checksum += found->bytes.size() + found->sequence;
        if (sampled) {
            latencies.push_back(static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sample_start).count()));
        }
    }
    return finish_measurement("current-store-copy", "get100", repeat, material.order.size(),
                              Clock::now() - started, std::move(latencies), checksum);
}

[[nodiscard]] auto run_pair_get(VolatileShardPairPrototype& pair, const Material& material,
                                const std::size_t repeat) -> Measurement {
    pair.adopt_publication();
    std::vector<double> latencies;
    latencies.reserve(material.order.size() / 64U + 1U);
    std::uint64_t checksum = 0;
    const auto started = Clock::now();
    for (std::size_t operation = 0; operation < material.order.size(); ++operation) {
        const auto sampled = (operation & 63U) == 0;
        const auto sample_start = sampled ? Clock::now() : started;
        auto found = pair.get(material.keys[material.order[operation]]);
        if (!found) {
            throw std::runtime_error{"paired GET failed"};
        }
        checksum += found->value.size() + found->sequence;
        if (sampled) {
            latencies.push_back(static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sample_start).count()));
        }
    }
    return finish_measurement("paired-span", "get100", repeat, material.order.size(), Clock::now() - started,
                              std::move(latencies), checksum);
}

[[nodiscard]] auto run_current_mixed(glyphastore::Store& store, const Material& material,
                                     const std::size_t repeat) -> Measurement {
    std::vector<double> get_latencies;
    get_latencies.reserve(material.order.size() / 64U + 1U);
    std::uint64_t checksum = 0;
    const auto started = Clock::now();
    for (std::size_t operation = 0; operation < material.order.size(); ++operation) {
        const auto& key = material.keys[material.order[operation]];
        if (operation % 20U == 19U) {
            if (auto status = store.put(key, material.update_value); !status) {
                throw std::runtime_error{"current Store mixed PUT failed"};
            }
            continue;
        }
        const auto sampled = (operation & 63U) == 0;
        const auto sample_start = sampled ? Clock::now() : started;
        auto found = store.get_copy(key);
        if (!found) {
            throw std::runtime_error{"current Store mixed GET failed"};
        }
        checksum += found->bytes.size() + found->sequence;
        if (sampled) {
            get_latencies.push_back(static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sample_start).count()));
        }
    }
    return finish_measurement("current-store-copy", "get95-put5", repeat, material.order.size(),
                              Clock::now() - started, std::move(get_latencies), checksum);
}

[[nodiscard]] auto run_pair_mixed(VolatileShardPairPrototype& pair, const Material& material,
                                  const std::size_t repeat) -> Measurement {
    std::vector<double> get_latencies;
    get_latencies.reserve(material.order.size() / 64U + 1U);
    std::size_t pending = 0;
    std::uint64_t request = (repeat + 1U) * (material.order.size() + 1U);
    std::uint64_t checksum = 0;

    const auto drain = [&] {
        while (auto completion = pair.try_pop_completion()) {
            if (completion->error) {
                throw std::runtime_error{"paired mixed publication failed"};
            }
            --pending;
            checksum += completion->visible_through;
        }
    };

    const auto started = Clock::now();
    for (std::size_t operation = 0; operation < material.order.size(); ++operation) {
        if ((operation & 31U) == 0) {
            pair.adopt_publication();
            drain();
        }
        const auto& key = material.keys[material.order[operation]];
        if (operation % 20U == 19U) {
            for (;;) {
                const auto status = pair.try_submit_put(request++, key, material.update_value);
                if (status == PrototypeSubmitStatus::submitted) {
                    ++pending;
                    break;
                }
                if (status != PrototypeSubmitStatus::queue_full) {
                    throw std::runtime_error{"paired mixed PUT admission failed"};
                }
                pair.adopt_publication();
                drain();
                std::this_thread::yield();
            }
            continue;
        }
        const auto sampled = (operation & 63U) == 0;
        const auto sample_start = sampled ? Clock::now() : started;
        auto found = pair.get(key);
        if (!found) {
            throw std::runtime_error{"paired mixed GET failed"};
        }
        checksum += found->value.size() + found->sequence;
        if (sampled) {
            get_latencies.push_back(static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sample_start).count()));
        }
    }
    while (pending != 0) {
        pair.adopt_publication();
        drain();
        std::this_thread::yield();
    }
    pair.adopt_publication();
    return finish_measurement("paired-span-async", "get95-put5", repeat, material.order.size(),
                              Clock::now() - started, std::move(get_latencies), checksum);
}

void print_measurement(const Measurement& result) {
    std::cout << "sample," << result.implementation << ',' << result.workload << ',' << result.repeat << ','
              << std::fixed << std::setprecision(3) << result.operations_per_second << ',' << result.p50_ns
              << ',' << result.p95_ns << ',' << result.p99_ns << ',' << result.p999_ns << ','
              << result.latency_samples << ',' << result.checksum << '\n';
}

void print_summaries(const std::vector<Measurement>& measurements) {
    std::vector<std::pair<std::string, std::string>> groups;
    for (const auto& sample : measurements) {
        const auto group = std::pair{sample.implementation, sample.workload};
        if (std::find(groups.begin(), groups.end(), group) == groups.end()) {
            groups.push_back(group);
        }
    }
    for (const auto& [implementation, workload] : groups) {
        std::vector<double> throughput;
        std::vector<double> p99;
        std::vector<double> p999;
        for (const auto& sample : measurements) {
            if (sample.implementation == implementation && sample.workload == workload) {
                throughput.push_back(sample.operations_per_second);
                p99.push_back(sample.p99_ns);
                p999.push_back(sample.p999_ns);
            }
        }
        std::cout << "summary," << implementation << ',' << workload << ',' << throughput.size() << ','
                  << std::fixed << std::setprecision(3) << percentile(std::move(throughput), 0.50) << ','
                  << percentile(std::move(p99), 0.50) << ',' << percentile(std::move(p999), 0.50) << '\n';
    }
}

void verify_final_state(glyphastore::Store& current, VolatileShardPairPrototype& pair,
                        const Material& material) {
    pair.adopt_publication();
    for (const auto& key : material.keys) {
        const auto current_value = current.get_copy(key);
        const auto paired_value = pair.get(key);
        if (!current_value || !paired_value || current_value->view().size() != paired_value->value.size() ||
            !std::equal(current_value->view().begin(), current_value->view().end(),
                        paired_value->value.begin())) {
            throw std::runtime_error{"A/B final-state verification failed"};
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        const auto options = parse_options(argc, argv);
        const auto material = make_material(options);
        auto current = open_current_store(material);
        auto pair = open_pair(material, options);
        std::vector<Measurement> measurements;
        measurements.reserve((options.warmup + options.repeats) * 4U);

        std::cout << "# glyphastore paired-shard A/B benchmark\n"
                  << "# git=" << GLYPHASTORE_GIT_SHA << "\n"
                  << "# operations=" << options.operations << " keys=" << options.keys
                  << " value_bytes=" << options.value_bytes << " repeats=" << options.repeats
                  << " warmup=" << options.warmup << " batch_records=" << options.batch_records
                  << " batch_wait_us=" << options.batch_wait_us << "\n"
                  << "# latency sampling=1/64 GET operations; current returns an owning copy; paired returns "
                     "a span\n"
                  << "kind,implementation,workload,repeat,ops_per_second,p50_get_ns,p95_get_ns,p99_get_ns,"
                     "p999_get_ns,latency_samples,checksum\n";

        const auto total_repeats = options.warmup + options.repeats;
        for (std::size_t iteration = 0; iteration < total_repeats; ++iteration) {
            const auto measured = iteration >= options.warmup;
            const auto repeat = measured ? iteration - options.warmup : 0;
            // Alternate order to reduce systematic thermal/frequency bias.
            if ((iteration & 1U) == 0) {
                const auto current_get = run_current_get(*current, material, repeat);
                const auto pair_get = run_pair_get(*pair, material, repeat);
                const auto current_mixed = run_current_mixed(*current, material, repeat);
                const auto pair_mixed = run_pair_mixed(*pair, material, repeat);
                if (measured) {
                    measurements.insert(measurements.end(),
                                        {current_get, pair_get, current_mixed, pair_mixed});
                }
            } else {
                const auto pair_get = run_pair_get(*pair, material, repeat);
                const auto current_get = run_current_get(*current, material, repeat);
                const auto pair_mixed = run_pair_mixed(*pair, material, repeat);
                const auto current_mixed = run_current_mixed(*current, material, repeat);
                if (measured) {
                    measurements.insert(measurements.end(),
                                        {pair_get, current_get, pair_mixed, current_mixed});
                }
            }
        }
        for (const auto& measurement : measurements) {
            print_measurement(measurement);
        }
        verify_final_state(*current, *pair, material);
        std::cout << "# final_state_verification=pass\n";
        std::cout << "kind,implementation,workload,samples,median_ops_per_second,median_p99_get_ns,"
                     "median_p999_get_ns\n";
        print_summaries(measurements);
        const auto stats = pair->stats();
        std::cout << "telemetry,publications," << stats.publications << ",publication_records,"
                  << stats.publication_records << ",publication_latency_ns," << stats.publication_latency_ns
                  << ",writer_batch_wait_ns," << stats.writer_batch_wait_ns
                  << ",writer_batch_deadline_closes," << stats.writer_batch_deadline_closes
                  << ",publication_storage_bytes," << stats.publication_bytes
                  << ",ingress_value_bytes_copied," << stats.ingress_value_bytes_copied
                  << ",payload_allocations," << stats.payload_allocations << ",payload_bytes_allocated,"
                  << stats.payload_bytes_allocated << ",delta_directory_entries_copied,"
                  << stats.delta_directory_entries_copied << ",delta_page_view_entries_copied,"
                  << stats.delta_page_view_entries_copied << ",delta_pages_copied,"
                  << stats.delta_pages_copied << ",delta_pages_allocated," << stats.delta_pages_allocated
                  << ",delta_merges," << stats.delta_merges << ",generation_high_watermark,"
                  << stats.generation_high_watermark << ",generation_retired,"
                  << stats.generation_retire_count << ",generation_retire_delay_ns,"
                  << stats.generation_retire_delay_ns << ",publication_backpressure,"
                  << stats.publication_backpressure << ",generation_slot_reuses,"
                  << stats.generation_slot_reuses << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "paired benchmark error: " << error.what() << '\n';
        return 2;
    }
}
