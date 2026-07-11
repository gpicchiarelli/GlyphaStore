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

[[nodiscard]] auto make_index_ref(const std::size_t index_in_order) -> RecordRef {
    return RecordRef{
        .segment_id = SegmentId{1},
        .offset = RecordOffset{static_cast<std::uint32_t>((index_in_order * 64U) % 1'000'000U)},
        .size = RecordSize{64},
        .sequence = SequenceNumber{index_in_order + 1},
        .generation = GenerationId{1},
    };
}

[[nodiscard]] auto make_reserved_index(const std::size_t operations) -> std::unique_ptr<Index> {
    auto index = std::make_unique<Index>();
    if (!index->reserve(operations)) {
        return nullptr;
    }
    return index;
}

[[nodiscard]] auto populate_index(Index& index, const KeyMaterial& material) -> bool {
    for (const auto index_in_order : material.order) {
        if (!index.insert_or_assign(material.keys[index_in_order], make_index_ref(index_in_order))) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] auto run_index_insert(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "index_insert", config.operations,
        [&]() -> std::unique_ptr<Index> { return make_reserved_index(config.operations); },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            std::size_t inserted = 0;
            for (const auto index_in_order : material.order) {
                if (index->insert_or_assign(material.keys[index_in_order], make_index_ref(index_in_order))) {
                    ++inserted;
                }
            }
            return inserted;
        });
}

[[nodiscard]] auto run_index_replace(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "index_replace", config.operations,
        [&]() -> std::unique_ptr<Index> {
            auto index = make_reserved_index(config.operations);
            if (index == nullptr || !populate_index(*index, material)) {
                return nullptr;
            }
            return index;
        },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            std::size_t replaced = 0;
            for (const auto index_in_order : material.order) {
                const auto replacement = make_index_ref(index_in_order + config.operations);
                if (index->insert_or_assign(material.keys[index_in_order], replacement)) {
                    ++replaced;
                }
            }
            return replaced;
        });
}

[[nodiscard]] auto run_index_find_hit(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "index_find_hit", config.operations,
        [&]() -> std::unique_ptr<Index> {
            auto index = make_reserved_index(config.operations);
            if (index == nullptr || !populate_index(*index, material)) {
                return nullptr;
            }
            return index;
        },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                hits += index->find(material.keys[index_in_order]).has_value() ? 1U : 0U;
            }
            return hits;
        });
}

[[nodiscard]] auto run_index_find_miss(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    const auto miss_material = make_miss_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "index_find_miss", config.operations,
        [&]() -> std::unique_ptr<Index> {
            auto index = make_reserved_index(config.operations);
            if (index == nullptr || !populate_index(*index, material)) {
                return nullptr;
            }
            return index;
        },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            std::size_t misses = 0;
            for (const auto index_in_order : miss_material.order) {
                misses += index->find(miss_material.keys[index_in_order]).has_value() ? 0U : 1U;
            }
            return misses;
        });
}

[[nodiscard]] auto run_index_erase(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "index_erase", config.operations,
        [&]() -> std::unique_ptr<Index> {
            auto index = make_reserved_index(config.operations);
            if (index == nullptr || !populate_index(*index, material)) {
                return nullptr;
            }
            return index;
        },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            std::size_t erased = 0;
            for (const auto index_in_order : material.order) {
                if (index->erase(material.keys[index_in_order]).previous.has_value()) {
                    ++erased;
                }
            }
            return erased;
        });
}

[[nodiscard]] auto run_index_insert_find(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations * 2U, config, "index_insert_find", config.operations,
        [&]() -> std::unique_ptr<Index> { return make_reserved_index(config.operations); },
        [&](std::unique_ptr<Index>& index) -> std::size_t {
            if (index == nullptr) {
                return 0;
            }
            for (const auto index_in_order : material.order) {
                const auto& key = material.keys[index_in_order];
                if (!index->insert_or_assign(key, make_index_ref(index_in_order))) {
                    return 0;
                }
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                hits += index->find(material.keys[index_in_order]).has_value() ? 1U : 0U;
            }
            return hits;
        });
}

[[nodiscard]] auto run_store_put(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    return benchmark_collect_timed(
        settings, config.operations, config, "store_put", config.operations,
        [&]() -> std::unique_ptr<Store> {
            auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
            if (!opened) {
                return nullptr;
            }
            return std::move(*opened);
        },
        [&](std::unique_ptr<Store>& store) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t writes = 0;
            for (const auto index_in_order : material.order) {
                if (store->put(material.keys[index_in_order], bytes(material.values[index_in_order]))
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
        settings, config.operations, config, "store_get", config.operations,
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
    return benchmark_collect_timed(
        settings, config.operations * 2U, config, "store_put_get", config.operations,
        [&]() -> std::unique_ptr<Store> {
            auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
            if (!opened) {
                return nullptr;
            }
            return std::move(*opened);
        },
        [&](std::unique_ptr<Store>& store) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            for (const auto index_in_order : material.order) {
                if (!store->put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                         .has_value()) {
                    return 0;
                }
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

[[nodiscard]] auto run_store_read_after_write(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_key_material(config);
    const auto expected_hits = config.operations * 2U;
    return benchmark_collect_timed(
        settings, expected_hits, config, "store_read_after_write", expected_hits,
        [&]() -> std::unique_ptr<Store> {
            auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
            if (!opened) {
                return nullptr;
            }
            return std::move(*opened);
        },
        [&](std::unique_ptr<Store>& store) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.order) {
                if (store->put(material.keys[index_in_order], bytes(material.values[index_in_order]))
                        .has_value()) {
                    ++hits;
                }
                if (store->get(material.keys[index_in_order]).has_value()) {
                    ++hits;
                }
            }
            return hits;
        });
}

[[nodiscard]] auto run_single(const BenchmarkKind kind, const Config& config, const RunSettings& settings)
    -> Result {
    switch (kind) {
    case BenchmarkKind::index_insert:
        return run_index_insert(config, settings);
    case BenchmarkKind::index_replace:
        return run_index_replace(config, settings);
    case BenchmarkKind::index_find_hit:
        return run_index_find_hit(config, settings);
    case BenchmarkKind::index_find_miss:
        return run_index_find_miss(config, settings);
    case BenchmarkKind::index_erase:
        return run_index_erase(config, settings);
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
    case BenchmarkKind::index_all:
        break;
    }
    return {};
}

[[nodiscard]] auto index_benchmark_kinds() -> std::vector<BenchmarkKind> {
    return {BenchmarkKind::index_insert, BenchmarkKind::index_replace, BenchmarkKind::index_find_hit,
            BenchmarkKind::index_find_miss, BenchmarkKind::index_erase};
}

[[nodiscard]] auto all_benchmark_kinds() -> std::vector<BenchmarkKind> {
    auto kinds = index_benchmark_kinds();
    kinds.push_back(BenchmarkKind::store_put);
    kinds.push_back(BenchmarkKind::store_get);
    kinds.push_back(BenchmarkKind::store_put_get);
    kinds.push_back(BenchmarkKind::store_read_after_write);
    return kinds;
}

} // namespace

auto run_benchmark(const BenchmarkKind kind, const Config& config, const RunSettings& settings)
    -> std::vector<Result> {
    std::vector<Result> results;
    if (kind == BenchmarkKind::index_all) {
        for (const auto isolated : index_benchmark_kinds()) {
            results.push_back(run_single(isolated, config, settings));
        }
        return results;
    }
    if (kind == BenchmarkKind::all) {
        for (const auto isolated : all_benchmark_kinds()) {
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
