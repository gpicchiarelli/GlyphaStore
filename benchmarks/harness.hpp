#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore::bench {

inline constexpr std::size_t kDefaultOperations = 200'000;
inline constexpr std::size_t kDefaultWarmupIterations = 1;
inline constexpr std::size_t kDefaultMeasuredIterations = 7;

enum class BenchmarkKind {
    index_insert,
    index_replace,
    index_find_hit,
    index_find_miss,
    index_erase,
    index_insert_find,
    index_all,
    store_put,
    store_get,
    store_put_get,
    store_read_after_write,
    all
};

struct Config {
    std::size_t operations{kDefaultOperations};
    std::size_t key_size{16};
    std::size_t value_size{64};
    std::size_t workers{1};
    bool random_access{false};
};

struct RunSettings {
    std::size_t warmup_iterations{kDefaultWarmupIterations};
    std::size_t measured_iterations{kDefaultMeasuredIterations};
    bool pin_cpu{false};
};

struct Result {
    std::string name;
    Config config;
    RunSettings settings{};
    std::size_t operations{};
    std::size_t hits{};
    std::size_t samples{};
    double median_seconds{};
    double min_seconds{};
    double max_seconds{};
    double median_ns_per_op{};
    double min_ns_per_op{};
    double max_ns_per_op{};
    double median_ops_per_second{};
    double min_ops_per_second{};
    double max_ops_per_second{};
};

struct KeyMaterial {
    std::vector<std::string> keys;
    std::vector<std::vector<std::byte>> values;
    std::vector<std::size_t> order;
};

[[nodiscard]] auto cpu_pin_applied() noexcept -> bool;
[[nodiscard]] auto try_cpu_pin(bool requested) -> bool;

[[nodiscard]] inline auto validate_run_settings(const RunSettings& settings, const Config& config) -> bool {
    if (config.operations == 0) {
        std::cerr << "benchmark error: --ops must be greater than zero\n";
        return false;
    }
    if (settings.measured_iterations == 0) {
        std::cerr << "benchmark error: --repeats must be greater than zero\n";
        return false;
    }
    return true;
}

[[nodiscard]] inline auto make_key(const std::size_t index, const std::size_t key_size) -> std::string {
    const auto decimal = std::to_string(index);
    if (decimal.size() >= key_size) {
        return decimal.substr(0, key_size);
    }
    std::string key = decimal;
    key.resize(key_size, 'k');
    return key;
}

[[nodiscard]] inline auto make_value(const std::size_t index, const std::size_t value_size)
    -> std::vector<std::byte> {
    std::vector<std::byte> value(value_size);
    for (std::size_t offset = 0; offset < value_size; ++offset) {
        value[offset] = static_cast<std::byte>((index + offset) & 0xFFU);
    }
    return value;
}

[[nodiscard]] inline auto make_key_material(const Config& config) -> KeyMaterial {
    KeyMaterial material;
    material.keys.reserve(config.operations);
    material.values.reserve(config.operations);
    material.order.reserve(config.operations);
    for (std::size_t index = 0; index < config.operations; ++index) {
        material.keys.push_back(make_key(index, config.key_size));
        material.values.push_back(make_value(index, config.value_size));
        material.order.push_back(index);
    }
    if (config.random_access) {
        std::mt19937_64 random{0xC0FFEEULL};
        std::ranges::shuffle(material.order, random);
    }
    return material;
}

[[nodiscard]] inline auto make_miss_key_material(const Config& config) -> KeyMaterial {
    KeyMaterial material;
    material.keys.reserve(config.operations);
    material.order.reserve(config.operations);
    for (std::size_t index = 0; index < config.operations; ++index) {
        material.keys.push_back(make_key(config.operations + index, config.key_size));
        material.order.push_back(index);
    }
    if (config.random_access) {
        std::mt19937_64 random{0xDEADBEEFULL};
        std::ranges::shuffle(material.order, random);
    }
    return material;
}

[[nodiscard]] inline auto median(std::vector<double> values) -> double {
    if (values.empty()) {
        return 0.0;
    }
    std::ranges::sort(values);
    const auto middle = values.size() / 2U;
    if (values.size() % 2U == 1U) {
        return values[middle];
    }
    return (values[middle - 1U] + values[middle]) / 2.0;
}

[[nodiscard]] inline auto finalize_result(std::string name, const Config& config, const RunSettings& settings,
                                          const std::size_t operations, const std::size_t hits,
                                          std::vector<double> seconds) -> Result {
    if (seconds.empty()) {
        return Result{.name = std::move(name),
                      .config = config,
                      .settings = settings,
                      .operations = operations,
                      .hits = hits};
    }

    std::vector<double> ns_per_op;
    std::vector<double> ops_per_second;
    ns_per_op.reserve(seconds.size());
    ops_per_second.reserve(seconds.size());
    for (const auto elapsed : seconds) {
        ns_per_op.push_back(operations > 0 ? elapsed * 1.0e9 / static_cast<double>(operations) : 0.0);
        ops_per_second.push_back(elapsed > 0.0 ? static_cast<double>(operations) / elapsed : 0.0);
    }

    return Result{
        .name = std::move(name),
        .config = config,
        .settings = settings,
        .operations = operations,
        .hits = hits,
        .samples = seconds.size(),
        .median_seconds = median(seconds),
        .min_seconds = *std::ranges::min_element(seconds),
        .max_seconds = *std::ranges::max_element(seconds),
        .median_ns_per_op = median(ns_per_op),
        .min_ns_per_op = *std::ranges::min_element(ns_per_op),
        .max_ns_per_op = *std::ranges::max_element(ns_per_op),
        .median_ops_per_second = median(ops_per_second),
        .min_ops_per_second = *std::ranges::min_element(ops_per_second),
        .max_ops_per_second = *std::ranges::max_element(ops_per_second),
    };
}

[[nodiscard]] inline auto validate_sample_hits(const std::string_view name, const std::size_t expected_hits,
                                               const std::size_t actual_hits, const std::size_t sample_index)
    -> bool {
    if (actual_hits == expected_hits) {
        return true;
    }
    std::cerr << "benchmark error: " << name << " sample " << sample_index
              << " expected hits=" << expected_hits << " got hits=" << actual_hits << '\n';
    return false;
}

template <typename SetupFn, typename BodyFn>
[[nodiscard]] inline auto benchmark_collect_timed(const RunSettings& settings, const std::size_t operations,
                                                  const Config& config, std::string name,
                                                  const std::size_t expected_hits, SetupFn&& setup,
                                                  BodyFn&& body) -> Result {
    const auto run_iteration =
        [&](const bool timed,
            const std::size_t sample_index) -> std::optional<std::pair<std::size_t, double>> {
        auto context = setup();
        if (!timed) {
            (void)body(context);
            return std::nullopt;
        }
        const auto started = std::chrono::steady_clock::now();
        const auto hits = body(context);
        const auto elapsed =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        if (!validate_sample_hits(name, expected_hits, hits, sample_index)) {
            return std::nullopt;
        }
        return std::pair{hits, elapsed};
    };

    for (std::size_t iteration = 0; iteration < settings.warmup_iterations; ++iteration) {
        (void)run_iteration(false, iteration);
    }

    std::vector<double> samples;
    samples.reserve(settings.measured_iterations);
    std::size_t hits = 0;
    for (std::size_t iteration = 0; iteration < settings.measured_iterations; ++iteration) {
        const auto measured = run_iteration(true, iteration + 1U);
        if (!measured) {
            return Result{
                .name = std::move(name), .config = config, .settings = settings, .operations = operations};
        }
        hits = measured->first;
        samples.push_back(measured->second);
    }

    return finalize_result(std::move(name), config, settings, operations, hits, std::move(samples));
}

inline void print_metadata(std::ostream& out, const RunSettings& settings) {
#ifdef GLYPHASTORE_GIT_SHA
    out << "# git_sha=" << GLYPHASTORE_GIT_SHA << '\n';
#else
    out << "# git_sha=unknown\n";
#endif
#if defined(__APPLE__) && defined(__aarch64__)
    out << "# arch=arm64\n";
#elif defined(__x86_64__)
    out << "# arch=x86_64\n";
#else
    out << "# arch=unknown\n";
#endif
#if defined(__APPLE__)
    out << "# platform=macos\n";
#elif defined(__linux__)
    out << "# platform=linux\n";
#else
    out << "# platform=unknown\n";
#endif
    out << "# compiler=" << __VERSION__ << '\n';
    out << "# benchmark_warmup=" << settings.warmup_iterations << '\n';
    out << "# benchmark_repeats=" << settings.measured_iterations << '\n';
    out << "# cpu_pin_requested=" << (settings.pin_cpu ? 1 : 0) << '\n';
    out << "# cpu_pin_applied=" << (cpu_pin_applied() ? 1 : 0) << '\n';
    out << "# note=use plugged-in power; thermal throttling affects spread\n";
}

inline void print_result(std::ostream& out, const Result& result) {
    out << "name=" << result.name << ' ' << "key_size=" << result.config.key_size << ' '
        << "value_size=" << result.config.value_size << ' ' << "workers=" << result.config.workers << ' '
        << "random=" << (result.config.random_access ? 1 : 0) << ' ' << "operations=" << result.operations
        << ' ' << "hits=" << result.hits << ' ' << "samples=" << result.samples << ' '
        << "warmup=" << result.settings.warmup_iterations << ' ' << "median_seconds=" << result.median_seconds
        << ' ' << "min_seconds=" << result.min_seconds << ' ' << "max_seconds=" << result.max_seconds << ' '
        << "median_ops_per_second=" << result.median_ops_per_second << ' '
        << "min_ops_per_second=" << result.min_ops_per_second << ' '
        << "max_ops_per_second=" << result.max_ops_per_second << ' '
        << "median_ns_per_op=" << result.median_ns_per_op << ' ' << "min_ns_per_op=" << result.min_ns_per_op
        << ' ' << "max_ns_per_op=" << result.max_ns_per_op << '\n';
}

[[nodiscard]] inline auto parse_kind(std::string_view value) -> BenchmarkKind {
    if (value == "index-insert") {
        return BenchmarkKind::index_insert;
    }
    if (value == "index-replace") {
        return BenchmarkKind::index_replace;
    }
    if (value == "index-find-hit") {
        return BenchmarkKind::index_find_hit;
    }
    if (value == "index-find-miss") {
        return BenchmarkKind::index_find_miss;
    }
    if (value == "index-erase") {
        return BenchmarkKind::index_erase;
    }
    if (value == "index-all") {
        return BenchmarkKind::index_all;
    }
    if (value == "index") {
        return BenchmarkKind::index_insert_find;
    }
    if (value == "store-put") {
        return BenchmarkKind::store_put;
    }
    if (value == "store-get") {
        return BenchmarkKind::store_get;
    }
    if (value == "store-put-get") {
        return BenchmarkKind::store_put_get;
    }
    if (value == "store-read-after-write") {
        return BenchmarkKind::store_read_after_write;
    }
    return BenchmarkKind::all;
}

} // namespace glyphastore::bench
