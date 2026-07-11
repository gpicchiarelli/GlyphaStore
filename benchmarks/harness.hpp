#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore::bench {

inline constexpr std::size_t kDefaultOperations = 200'000;

enum class BenchmarkKind { index_insert_find, store_put, store_get, store_put_get, all };

struct Config {
    std::size_t operations{kDefaultOperations};
    std::size_t key_size{16};
    std::size_t value_size{64};
    std::size_t workers{1};
    bool random_access{false};
};

struct Result {
    std::string name;
    Config config;
    std::size_t operations{};
    std::size_t hits{};
    double seconds{};
};

struct KeyMaterial {
    std::vector<std::string> keys;
    std::vector<std::vector<std::byte>> values;
    std::vector<std::size_t> order;
};

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

template <typename Fn> [[nodiscard]] inline auto measure(const std::size_t operations, Fn&& fn) -> Result {
    const auto started = std::chrono::steady_clock::now();
    const std::size_t hits = fn();
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    return Result{.operations = operations, .hits = hits, .seconds = elapsed};
}

[[nodiscard]] inline auto ops_per_second(const Result& result) -> double {
    return result.seconds > 0.0 ? static_cast<double>(result.operations) / result.seconds : 0.0;
}

[[nodiscard]] inline auto ns_per_operation(const Result& result) -> double {
    return result.operations > 0 ? result.seconds * 1.0e9 / static_cast<double>(result.operations) : 0.0;
}

inline void print_metadata(std::ostream& out) {
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
}

inline void print_result(std::ostream& out, const Result& result) {
    out << "name=" << result.name << ' ' << "key_size=" << result.config.key_size << ' '
        << "value_size=" << result.config.value_size << ' ' << "workers=" << result.config.workers << ' '
        << "random=" << (result.config.random_access ? 1 : 0) << ' ' << "operations=" << result.operations
        << ' ' << "hits=" << result.hits << ' ' << "seconds=" << result.seconds << ' '
        << "ops_per_second=" << ops_per_second(result) << ' ' << "ns_per_op=" << ns_per_operation(result)
        << '\n';
}

[[nodiscard]] inline auto parse_kind(std::string_view value) -> BenchmarkKind {
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
    return BenchmarkKind::all;
}

} // namespace glyphastore::bench
