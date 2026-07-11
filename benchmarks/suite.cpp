#include "suite.hpp"

#include "glyphastore/index/index.hpp"
#include "glyphastore/store/store.hpp"
#include "harness.hpp"

#include <memory>
#include <span>

namespace glyphastore::bench {
namespace {

[[nodiscard]] auto bytes(std::span<const std::byte> value) -> std::span<const std::byte> {
    return value;
}

[[nodiscard]] auto run_index_insert_find(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect(
        settings, config.operations * 2U, config, "index_insert_find", [&]() -> std::size_t {
            Index index;
            if (!index.reserve(config.operations)) {
                return 0;
            }
            for (const auto index_in_order : material.order) {
                const auto& key = material.keys[index_in_order];
                if (!index.insert_or_assign(key,
                                            RecordRef{SegmentId{1}, RecordOffset{0}, RecordSize{64},
                                                      SequenceNumber{index_in_order + 1}, GenerationId{1}})) {
                    return 0;
                }
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                hits += index.find(material.keys[index_in_order]).has_value() ? 1U : 0U;
            }
            return hits;
        });
}

[[nodiscard]] auto run_store_put(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect(settings, config.operations, config, "store_put", [&]() -> std::size_t {
        auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
        if (!opened) {
            return 0;
        }
        auto& store = **opened;
        std::size_t writes = 0;
        for (const auto index_in_order : material.order) {
            if (store.put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                    .has_value()) {
                ++writes;
            }
        }
        return writes;
    });
}

[[nodiscard]] auto run_store_get(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "store_get",
        [&]() -> std::unique_ptr<Store> {
            auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
            if (!opened) {
                return nullptr;
            }
            auto store = std::move(*opened);
            for (const auto index_in_order : material.order) {
                if (!store->put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                         .has_value()) {
                    return nullptr;
                }
            }
            return store;
        },
        [&](std::unique_ptr<Store>& store) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                if (store->get(material.keys[index_in_order]).has_value()) {
                    ++hits;
                }
            }
            return hits;
        });
}

[[nodiscard]] auto run_store_put_get(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect(settings, config.operations * 2U, config, "store_put_get", [&]() -> std::size_t {
        auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
        if (!opened) {
            return 0;
        }
        auto& store = **opened;
        for (const auto index_in_order : material.order) {
            if (!store.put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                     .has_value()) {
                return 0;
            }
        }
        std::size_t hits = 0;
        for (const auto index_in_order : material.order) {
            if (store.get(material.keys[index_in_order]).has_value()) {
                ++hits;
            }
        }
        return hits;
    });
}

[[nodiscard]] auto run_store_read_after_write(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect(
        settings, config.operations * 2U, config, "store_read_after_write", [&]() -> std::size_t {
            auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
            if (!opened) {
                return 0;
            }
            auto& store = **opened;
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                if (store.put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                        .has_value()) {
                    ++hits;
                }
                if (store.get(material.keys[index_in_order]).has_value()) {
                    ++hits;
                }
            }
            return hits;
        });
}

[[nodiscard]] auto run_single(const BenchmarkKind kind, const Config& config, const RunSettings& settings)
    -> Result {
    switch (kind) {
    case BenchmarkKind::index_insert_find:
        return run_index_insert_find(config, settings);
    case BenchmarkKind::store_put:
        return run_store_put(config, settings);
    case BenchmarkKind::store_get:
        return run_store_get(config, settings);
    case BenchmarkKind::store_put_get:
        return run_store_put_get(config, settings);
    case BenchmarkKind::store_read_after_write:
        return run_store_read_after_write(config, settings);
    case BenchmarkKind::all:
        break;
    }
    return {};
}

} // namespace

auto run_benchmark(const BenchmarkKind kind, const Config& config, const RunSettings& settings)
    -> std::vector<Result> {
    std::vector<Result> results;
    if (kind == BenchmarkKind::all) {
        for (const auto isolated :
             {BenchmarkKind::index_insert_find, BenchmarkKind::store_put, BenchmarkKind::store_get,
              BenchmarkKind::store_put_get, BenchmarkKind::store_read_after_write}) {
            results.push_back(run_single(isolated, config, settings));
        }
        return results;
    }
    results.push_back(run_single(kind, config, settings));
    return results;
}

auto quick_configs() -> std::vector<Config> {
    return {Config{.operations = kDefaultOperations,
                   .key_size = 16,
                   .value_size = 64,
                   .workers = 1,
                   .random_access = false},
            Config{.operations = kDefaultOperations,
                   .key_size = 16,
                   .value_size = 64,
                   .workers = 1,
                   .random_access = true}};
}

auto suite_configs() -> std::vector<Config> {
    std::vector<Config> configs;
    for (const std::size_t key_size : {8U, 16U, 32U, 64U, 256U}) {
        for (const std::size_t value_size : {0U, 64U, 256U}) {
            for (const bool random_access : {false, true}) {
                configs.push_back(Config{.operations = kDefaultOperations,
                                         .key_size = key_size,
                                         .value_size = value_size,
                                         .workers = 1,
                                         .random_access = random_access});
            }
        }
    }
    return configs;
}

} // namespace glyphastore::bench
