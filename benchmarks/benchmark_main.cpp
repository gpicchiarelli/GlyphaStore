#include "harness.hpp"
#include "suite.hpp"

#include <cstdlib>
#include <iostream>
#include <optional>
#include <string_view>
#include <vector>

namespace {

struct Options {
    glyphastore::bench::BenchmarkKind kind{glyphastore::bench::BenchmarkKind::all};
    bool full_suite{};
    std::optional<std::size_t> operations;
    std::optional<std::size_t> key_size;
    std::optional<std::size_t> value_size;
    std::optional<std::size_t> workers;
    std::optional<bool> random_access;
};

[[nodiscard]] auto parse_size(const char* value, const char* flag) -> std::size_t {
    if (value == nullptr) {
        std::cerr << "missing value for " << flag << '\n';
        std::exit(2);
    }
    return static_cast<std::size_t>(std::stoull(value));
}

[[nodiscard]] auto parse_options(int argc, char** argv) -> Options {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view arg{argv[index]};
        if (arg == "--suite") {
            options.full_suite = true;
            continue;
        }
        if (arg == "--random") {
            options.random_access = true;
            continue;
        }
        if (arg == "--filter" && index + 1 < argc) {
            options.kind = glyphastore::bench::parse_kind(argv[++index]);
            continue;
        }
        if (arg == "--ops" && index + 1 < argc) {
            options.operations = parse_size(argv[++index], "--ops");
            continue;
        }
        if (arg == "--key-size" && index + 1 < argc) {
            options.key_size = parse_size(argv[++index], "--key-size");
            continue;
        }
        if (arg == "--value-size" && index + 1 < argc) {
            options.value_size = parse_size(argv[++index], "--value-size");
            continue;
        }
        if (arg == "--workers" && index + 1 < argc) {
            options.workers = parse_size(argv[++index], "--workers");
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout << "usage: glyphastore_benchmarks [--suite] [--random]\n"
                      << "       [--filter all|index|store-put|store-get|store-put-get]\n"
                      << "       [--ops N] [--key-size N] [--value-size N] [--workers N]\n";
            std::exit(0);
        }
        std::cerr << "unknown argument: " << arg << '\n';
        std::exit(2);
    }
    return options;
}

void apply_overrides(glyphastore::bench::Config& config, const Options& options) {
    if (options.operations) {
        config.operations = *options.operations;
    }
    if (options.key_size) {
        config.key_size = *options.key_size;
    }
    if (options.value_size) {
        config.value_size = *options.value_size;
    }
    if (options.workers) {
        config.workers = *options.workers;
    }
    if (options.random_access) {
        config.random_access = *options.random_access;
    }
}

[[nodiscard]] auto has_custom_config(const Options& options) -> bool {
    return options.operations || options.key_size || options.value_size || options.workers ||
           options.random_access;
}

} // namespace

int main(int argc, char** argv) {
    const auto options = parse_options(argc, argv);
    std::vector<glyphastore::bench::Config> configs;
    if (has_custom_config(options)) {
        glyphastore::bench::Config config;
        apply_overrides(config, options);
        configs.push_back(config);
    } else if (options.full_suite) {
        configs = glyphastore::bench::suite_configs();
    } else {
        configs = glyphastore::bench::quick_configs();
    }

    std::cout << "# glyphastore benchmark\n";
    glyphastore::bench::print_metadata(std::cout);

    for (auto config : configs) {
        apply_overrides(config, options);
        for (const auto& result : glyphastore::bench::run_benchmark(options.kind, config)) {
            glyphastore::bench::print_result(std::cout, result);
        }
    }
    return 0;
}
