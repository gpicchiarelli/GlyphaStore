#include "suite.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/store/store.hpp"
#include "harness.hpp"

#include <cmath>
#include <latch>
#include <memory>
#include <span>
#include <thread>

namespace glyphastore::bench {
namespace {

[[nodiscard]] auto bytes(std::span<const std::byte> value) -> std::span<const std::byte> {
    return value;
}

struct ParallelMaterial {
    KeyMaterial material;
    std::vector<std::vector<std::size_t>> thread_order;
};

struct ParallelSample {
    std::size_t hits{};
    double seconds{};
};

[[nodiscard]] auto make_parallel_material(const Config& config) -> ParallelMaterial {
    ParallelMaterial result;
    result.material.keys.reserve(config.operations);
    result.material.values.reserve(config.operations);
    result.material.order.reserve(config.operations);
    result.thread_order.resize(config.threads);

    std::vector<double> zipf_weights(config.workers);
    for (std::size_t worker = 0; worker < config.workers; ++worker) {
        zipf_weights[worker] = 1.0 / std::pow(static_cast<double>(worker + 1U), 1.2);
    }
    std::mt19937_64 random{0x51A7D15AULL};
    std::discrete_distribution<std::size_t> zipf_owner(zipf_weights.begin(), zipf_weights.end());

    std::size_t candidate = 0;
    for (std::size_t operation = 0; operation < config.operations; ++operation) {
        std::size_t desired_owner = 0;
        switch (config.distribution) {
        case ParallelDistribution::uniform:
        case ParallelDistribution::worker_affine:
            desired_owner = operation % config.workers;
            break;
        case ParallelDistribution::single_worker:
            desired_owner = 0;
            break;
        case ParallelDistribution::zipf:
            desired_owner = zipf_owner(random);
            break;
        }

        std::string key;
        do {
            key = make_key(candidate++, config.key_size);
        } while (route_worker(key, config.workers) != desired_owner);

        result.material.keys.push_back(std::move(key));
        result.material.values.push_back(make_value(operation, config.value_size));
        result.material.order.push_back(operation);
        const auto client_thread = config.distribution == ParallelDistribution::worker_affine
                                       ? desired_owner % config.threads
                                       : (operation / config.workers) % config.threads;
        result.thread_order[client_thread].push_back(operation);
    }

    if (config.random_access) {
        for (std::size_t thread = 0; thread < result.thread_order.size(); ++thread) {
            std::mt19937_64 thread_random{0xC0FFEEULL + thread};
            std::ranges::shuffle(result.thread_order[thread], thread_random);
        }
    }
    return result;
}

template <typename Fn>
[[nodiscard]] auto run_parallel_threads(const std::size_t thread_count, Fn&& fn) -> ParallelSample {
    std::latch ready{static_cast<std::ptrdiff_t>(thread_count)};
    std::latch start{1};
    std::latch done{static_cast<std::ptrdiff_t>(thread_count)};
    std::vector<std::size_t> thread_hits(thread_count);
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (std::size_t thread = 0; thread < thread_count; ++thread) {
        threads.emplace_back([&, thread]() {
            ready.count_down();
            start.wait();
            thread_hits[thread] = fn(thread);
            done.count_down();
        });
    }
    ready.wait();
    const auto started = std::chrono::steady_clock::now();
    start.count_down();
    done.wait();
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    for (auto& thread : threads) {
        thread.join();
    }
    std::size_t hits = 0;
    for (const auto thread_result : thread_hits) {
        hits += thread_result;
    }
    return {.hits = hits, .seconds = elapsed};
}

template <typename SetupFn, typename BodyFn>
[[nodiscard]] auto benchmark_collect_parallel(const RunSettings& settings, const std::size_t operations,
                                              const Config& config, std::string name,
                                              const std::size_t expected_hits, SetupFn&& setup, BodyFn&& body)
    -> Result {
    for (std::size_t iteration = 0; iteration < settings.warmup_iterations; ++iteration) {
        auto context = setup();
        (void)run_parallel_threads(config.threads,
                                   [&](const std::size_t thread) { return body(context, thread); });
    }

    std::vector<double> samples;
    samples.reserve(settings.measured_iterations);
    std::size_t hits = 0;
    for (std::size_t iteration = 0; iteration < settings.measured_iterations; ++iteration) {
        auto context = setup();
        const auto sample = run_parallel_threads(
            config.threads, [&](const std::size_t thread) { return body(context, thread); });
        if (!validate_sample_hits(name, expected_hits, sample.hits, iteration + 1U)) {
            return Result{
                .name = std::move(name), .config = config, .settings = settings, .operations = operations};
        }
        hits = sample.hits;
        samples.push_back(sample.seconds);
    }
    return finalize_result(std::move(name), config, settings, operations, hits, std::move(samples));
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

[[nodiscard]] auto open_parallel_store(const Config& config) -> std::unique_ptr<Store> {
    auto opened = Store::open({.worker_config = {.explicit_count = config.workers}});
    return opened ? std::move(*opened) : nullptr;
}

[[nodiscard]] auto run_store_parallel_put(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_parallel_material(config);
    return benchmark_collect_parallel(
        settings, config.operations, config, "store_parallel_put", config.operations,
        [&]() { return open_parallel_store(config); },
        [&](std::unique_ptr<Store>& store, const std::size_t thread) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t writes = 0;
            for (const auto index_in_order : material.thread_order[thread]) {
                if (store->put(material.material.keys[index_in_order],
                               bytes(material.material.values[index_in_order]))) {
                    ++writes;
                }
            }
            return writes;
        });
}

[[nodiscard]] auto run_store_parallel_get(const Config& config, const RunSettings& settings) -> Result {
    const auto material = make_parallel_material(config);
    return benchmark_collect_parallel(
        settings, config.operations, config, "store_parallel_get", config.operations,
        [&]() -> std::unique_ptr<Store> {
            auto store = open_parallel_store(config);
            if (store == nullptr) {
                return nullptr;
            }
            for (const auto index_in_order : material.material.order) {
                if (!store->put(material.material.keys[index_in_order],
                                bytes(material.material.values[index_in_order]))) {
                    return nullptr;
                }
            }
            return store;
        },
        [&](std::unique_ptr<Store>& store, const std::size_t thread) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.thread_order[thread]) {
                hits += store->get(material.material.keys[index_in_order]).has_value() ? 1U : 0U;
            }
            return hits;
        });
}

[[nodiscard]] auto run_store_parallel_read_after_write(const Config& config, const RunSettings& settings)
    -> Result {
    const auto material = make_parallel_material(config);
    const auto expected_hits = config.operations * 2U;
    return benchmark_collect_parallel(
        settings, expected_hits, config, "store_parallel_read_after_write", expected_hits,
        [&]() { return open_parallel_store(config); },
        [&](std::unique_ptr<Store>& store, const std::size_t thread) -> std::size_t {
            if (store == nullptr) {
                return 0;
            }
            std::size_t hits = 0;
            for (const auto index_in_order : material.thread_order[thread]) {
                const auto& key = material.material.keys[index_in_order];
                if (store->put(key, bytes(material.material.values[index_in_order]))) {
                    ++hits;
                }
                if (store->get(key)) {
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
    case BenchmarkKind::store_parallel_put:
        return run_store_parallel_put(config, settings);
    case BenchmarkKind::store_parallel_get:
        return run_store_parallel_get(config, settings);
    case BenchmarkKind::store_parallel_read_after_write:
        return run_store_parallel_read_after_write(config, settings);
    case BenchmarkKind::all:
    case BenchmarkKind::index_all:
    case BenchmarkKind::store_parallel_all:
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

[[nodiscard]] auto parallel_benchmark_kinds() -> std::vector<BenchmarkKind> {
    return {BenchmarkKind::store_parallel_put, BenchmarkKind::store_parallel_get,
            BenchmarkKind::store_parallel_read_after_write};
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
    if (kind == BenchmarkKind::store_parallel_all) {
        for (const auto isolated : parallel_benchmark_kinds()) {
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
