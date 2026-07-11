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

[[nodiscard]] auto run_index_insert_find(const Config& config) -> Result {
    const auto material = make_key_material(config);
    Index index;
    if (auto reserved = index.reserve(config.operations); !reserved) {
        return Result{.name = "index_insert_find", .config = config};
    }

    const auto insert_phase = measure(config.operations, [&]() -> std::size_t {
        std::size_t inserted = 0;
        for (const auto index_in_order : material.order) {
            const auto& key = material.keys[index_in_order];
            auto mutation =
                index.insert_or_assign(key, RecordRef{SegmentId{1}, RecordOffset{0}, RecordSize{64},
                                                      SequenceNumber{index_in_order + 1}, GenerationId{1}});
            if (mutation) {
                ++inserted;
            }
        }
        return inserted;
    });

    const auto find_phase = measure(config.operations, [&]() -> std::size_t {
        std::size_t hits = 0;
        for (const auto index_in_order : material.order) {
            hits += index.find(material.keys[index_in_order]).has_value() ? 1U : 0U;
        }
        return hits;
    });

    return Result{.name = "index_insert_find",
                  .config = config,
                  .operations = insert_phase.operations + find_phase.operations,
                  .hits = find_phase.hits,
                  .seconds = insert_phase.seconds + find_phase.seconds};
}

[[nodiscard]] auto run_store_put(const Config& config) -> Result {
    const auto material = make_key_material(config);
    auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
    if (!opened) {
        return Result{.name = "store_put", .config = config};
    }
    auto& store = **opened;

    auto measured = measure(config.operations, [&]() -> std::size_t {
        std::size_t writes = 0;
        for (const auto index_in_order : material.order) {
            if (store.put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                    .has_value()) {
                ++writes;
            }
        }
        return writes;
    });
    measured.name = "store_put";
    measured.config = config;
    return measured;
}

[[nodiscard]] auto run_store_get(const Config& config) -> Result {
    const auto material = make_key_material(config);
    auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
    if (!opened) {
        return Result{.name = "store_get", .config = config};
    }
    auto& store = **opened;
    for (const auto index_in_order : material.order) {
        if (!store.put(material.keys[index_in_order], bytes(material.values[index_in_order])).has_value()) {
            return Result{.name = "store_get", .config = config};
        }
    }

    auto measured = measure(config.operations, [&]() -> std::size_t {
        std::size_t hits = 0;
        for (const auto index_in_order : material.order) {
            if (store.get(material.keys[index_in_order]).has_value()) {
                ++hits;
            }
        }
        return hits;
    });
    measured.name = "store_get";
    measured.config = config;
    return measured;
}

[[nodiscard]] auto run_store_put_get(const Config& config) -> Result {
    const auto material = make_key_material(config);
    auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
    if (!opened) {
        return Result{.name = "store_put_get", .config = config};
    }
    auto& store = **opened;

    const auto put_phase = measure(config.operations, [&]() -> std::size_t {
        std::size_t writes = 0;
        for (const auto index_in_order : material.order) {
            if (store.put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                    .has_value()) {
                ++writes;
            }
        }
        return writes;
    });
    const auto get_phase = measure(config.operations, [&]() -> std::size_t {
        std::size_t hits = 0;
        for (const auto index_in_order : material.order) {
            if (store.get(material.keys[index_in_order]).has_value()) {
                ++hits;
            }
        }
        return hits;
    });

    return Result{.name = "store_put_get",
                  .config = config,
                  .operations = put_phase.operations + get_phase.operations,
                  .hits = get_phase.hits,
                  .seconds = put_phase.seconds + get_phase.seconds};
}

[[nodiscard]] auto run_store_read_after_write(const Config& config) -> Result {
    const auto material = make_key_material(config);
    auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
    if (!opened) {
        return Result{.name = "store_read_after_write", .config = config};
    }
    auto& store = **opened;

    auto measured = measure(config.operations * 2U, [&]() -> std::size_t {
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
    measured.name = "store_read_after_write";
    measured.config = config;
    return measured;
}

} // namespace

auto run_benchmark(const BenchmarkKind kind, const Config& config) -> std::vector<Result> {
    std::vector<Result> results;
    switch (kind) {
    case BenchmarkKind::index_insert_find:
        results.push_back(run_index_insert_find(config));
        break;
    case BenchmarkKind::store_put:
        results.push_back(run_store_put(config));
        break;
    case BenchmarkKind::store_get:
        results.push_back(run_store_get(config));
        break;
    case BenchmarkKind::store_put_get:
        results.push_back(run_store_put_get(config));
        break;
    case BenchmarkKind::store_read_after_write:
        results.push_back(run_store_read_after_write(config));
        break;
    case BenchmarkKind::all:
        results.push_back(run_index_insert_find(config));
        results.push_back(run_store_put(config));
        results.push_back(run_store_get(config));
        results.push_back(run_store_put_get(config));
        results.push_back(run_store_read_after_write(config));
        break;
    }
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
