#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/store.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <latch>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

enum class Mode : std::uint8_t {
    disabled,
    cooperative,
    background,
};

struct Options {
    std::size_t warmups{1};
    std::size_t repeats{3};
    std::size_t operations{15'360};
    std::size_t threads{4};
    std::size_t keys{128};
    std::size_t value_bytes{64U * 1024U};
    std::size_t reclaim_value_bytes{256U * 1024U};
    std::size_t put_percent{5};
    std::size_t maintenance_interval_ms{10};
    std::size_t cooldown_ms{250};
    std::optional<Mode> mode;
};

struct LatencySummary {
    std::size_t samples{};
    std::uint64_t p50_ns{};
    std::uint64_t p95_ns{};
    std::uint64_t p99_ns{};
    std::uint64_t maximum_ns{};
};

struct CooperativeStats {
    std::uint64_t attempts{};
    std::uint64_t useful{};
    std::uint64_t conflicts{};
    std::uint64_t bytes_copied{};
    std::uint64_t unexpected_errors{};
};

struct CompactionStartGate {
    std::latch* start{};
    std::filesystem::path store;
    std::thread::id opener_thread;
    std::atomic_bool claimed{false};

    static auto available_space_bytes(void* context) -> glyphastore::Result<std::uint64_t> {
        auto& gate = *static_cast<CompactionStartGate*>(context);
        if (std::this_thread::get_id() != gate.opener_thread &&
            !gate.claimed.exchange(true, std::memory_order_acq_rel)) {
            gate.start->wait();
        }
        std::error_code error;
        const auto space = std::filesystem::space(gate.store, error);
        if (error) {
            return glyphastore::fail(glyphastore::ErrorCode::io_error,
                                     "maintenance benchmark space probe failed");
        }
        return static_cast<std::uint64_t>(space.available);
    }
};

struct ThreadStats {
    std::uint64_t gets{};
    std::uint64_t get_hits{};
    std::uint64_t puts{};
    std::uint64_t put_successes{};
    std::uint64_t failures{};
    std::vector<std::uint64_t> all_latency_ns;
    std::vector<std::uint64_t> get_latency_ns;
    std::vector<std::uint64_t> put_latency_ns;
};

struct Sample {
    Mode mode{};
    std::size_t repeat{};
    double foreground_seconds{};
    std::uint64_t operations{};
    std::uint64_t gets{};
    std::uint64_t puts{};
    LatencySummary all_latency;
    LatencySummary get_latency;
    LatencySummary put_latency;
    std::uint64_t maintenance_attempts{};
    std::uint64_t maintenance_completed{};
    std::uint64_t maintenance_useful{};
    std::uint64_t maintenance_conflicts{};
    std::uint64_t maintenance_bytes_copied{};
    std::uint64_t maintenance_skips{};
    std::size_t segments_after{};
};

class TemporaryDirectory final {
  public:
    TemporaryDirectory() {
        auto pattern =
            (std::filesystem::temp_directory_path() / "glyphastore-maintenance-bench-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        if (created == nullptr) {
            throw std::runtime_error("failed to create maintenance benchmark directory");
        }
        root_ = created;
        store_ = root_ / "store";
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    auto operator=(const TemporaryDirectory&) -> TemporaryDirectory& = delete;

    [[nodiscard]] auto store() const -> const std::filesystem::path& {
        return store_;
    }

  private:
    std::filesystem::path root_;
    std::filesystem::path store_;
};

[[nodiscard]] auto mode_name(const Mode mode) noexcept -> std::string_view {
    switch (mode) {
    case Mode::disabled:
        return "disabled";
    case Mode::cooperative:
        return "cooperative";
    case Mode::background:
        return "background";
    }
    return "unknown";
}

[[nodiscard]] auto parse_mode(const std::string_view value) -> Mode {
    if (value == "disabled") {
        return Mode::disabled;
    }
    if (value == "cooperative") {
        return Mode::cooperative;
    }
    if (value == "background") {
        return Mode::background;
    }
    throw std::runtime_error("unknown maintenance mode: " + std::string{value});
}

[[nodiscard]] auto parse_size(const char* value, const std::string_view flag) -> std::size_t {
    if (value == nullptr) {
        throw std::runtime_error("missing value for " + std::string{flag});
    }
    return static_cast<std::size_t>(std::stoull(value));
}

[[nodiscard]] auto parse_options(int argc, char** argv) -> Options {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        if (argument == "--warmup" && index + 1 < argc) {
            options.warmups = parse_size(argv[++index], argument);
        } else if (argument == "--repeats" && index + 1 < argc) {
            options.repeats = parse_size(argv[++index], argument);
        } else if (argument == "--operations" && index + 1 < argc) {
            options.operations = parse_size(argv[++index], argument);
        } else if (argument == "--threads" && index + 1 < argc) {
            options.threads = parse_size(argv[++index], argument);
        } else if (argument == "--keys" && index + 1 < argc) {
            options.keys = parse_size(argv[++index], argument);
        } else if (argument == "--value-bytes" && index + 1 < argc) {
            options.value_bytes = parse_size(argv[++index], argument);
        } else if (argument == "--reclaim-value-bytes" && index + 1 < argc) {
            options.reclaim_value_bytes = parse_size(argv[++index], argument);
        } else if (argument == "--put-percent" && index + 1 < argc) {
            options.put_percent = parse_size(argv[++index], argument);
        } else if (argument == "--maintenance-interval-ms" && index + 1 < argc) {
            options.maintenance_interval_ms = parse_size(argv[++index], argument);
        } else if (argument == "--cooldown-ms" && index + 1 < argc) {
            options.cooldown_ms = parse_size(argv[++index], argument);
        } else if (argument == "--mode" && index + 1 < argc) {
            options.mode = parse_mode(argv[++index]);
        } else if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastore_maintenance_benchmark [--warmup N] [--repeats N]"
                         " [--operations N] [--threads N] [--keys N] [--value-bytes N]"
                         " [--reclaim-value-bytes N]"
                         " [--put-percent N] [--maintenance-interval-ms N]"
                         " [--cooldown-ms N]"
                         " [--mode disabled|cooperative|background]\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown or incomplete argument: " + std::string{argument});
        }
    }
    if (options.repeats == 0 || options.operations == 0 || options.threads == 0 || options.keys == 0 ||
        options.value_bytes < sizeof(std::uint64_t) || options.reclaim_value_bytes < sizeof(std::uint64_t) ||
        options.put_percent == 0 || options.put_percent >= 100 || options.maintenance_interval_ms == 0 ||
        options.maintenance_interval_ms > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error(
            "repeats/operations/threads/keys/interval must be nonzero; value must hold a marker; "
            "put-percent must be in 1..99");
    }
    return options;
}

[[nodiscard]] auto maintenance_mode(const Mode mode) noexcept -> glyphastore::MaintenanceMode {
    switch (mode) {
    case Mode::disabled:
        return glyphastore::MaintenanceMode::disabled;
    case Mode::cooperative:
        return glyphastore::MaintenanceMode::cooperative;
    case Mode::background:
        return glyphastore::MaintenanceMode::background;
    }
    return glyphastore::MaintenanceMode::disabled;
}

[[nodiscard]] auto store_config(const Options& options, const Mode mode,
                                const std::filesystem::path& directory,
                                const glyphastore::DurableOpenMode open_mode, CompactionStartGate* start_gate)
    -> glyphastore::StoreConfig {
    const auto interval = static_cast<std::uint32_t>(options.maintenance_interval_ms);
    glyphastore::StoreConfig config{
        .worker_config = {.explicit_count = 2},
        .storage_mode = glyphastore::StorageMode::durable_periodic,
        .data_directory = directory,
        .durable_open_mode = open_mode,
        .durable_periodic =
            {
                .sync_interval_ms = 60'000,
                .batch =
                    glyphastore::DurableGroupConfig{
                        .max_records = 4'096,
                        .max_bytes = 32U * 1024U * 1024U,
                        .max_wait_ms = 60'000,
                    },
            },
        .maintenance =
            {
                .mode = maintenance_mode(mode),
                .min_eval_interval_ms = interval,
                .max_eval_interval_ms = interval,
                .max_no_gain_attempts = 1'000,
                .dead_byte_ratio_bp_normal = 5'000,
            },
    };
    if (start_gate != nullptr) {
        config.filesystem_hooks = {
            .context = start_gate,
            .available_space_bytes = &CompactionStartGate::available_space_bytes,
        };
    }
    return config;
}

[[nodiscard]] auto open_store(const Options& options, const Mode mode, const std::filesystem::path& directory,
                              const glyphastore::DurableOpenMode open_mode,
                              CompactionStartGate* start_gate = nullptr)
    -> std::unique_ptr<glyphastore::Store> {
    auto opened = glyphastore::Store::open(store_config(options, mode, directory, open_mode, start_gate));
    if (!opened) {
        throw std::runtime_error("failed to open maintenance benchmark Store: " + opened.error().message);
    }
    return std::move(*opened);
}

void require_status(const glyphastore::Status& status, const std::string_view operation) {
    if (!status) {
        throw std::runtime_error(std::string{operation} + " failed: " + status.error().message);
    }
}

[[nodiscard]] auto key_for_worker(const std::size_t worker, const std::size_t ordinal,
                                  const std::string_view prefix) -> std::string {
    std::size_t matched{};
    for (std::size_t suffix = 0; suffix < 1'000'000; ++suffix) {
        auto candidate = std::string{prefix} + std::to_string(suffix);
        if (glyphastore::route_worker(candidate, 2) != worker) {
            continue;
        }
        if (matched++ == ordinal) {
            return candidate;
        }
    }
    throw std::runtime_error("failed to construct a routed maintenance benchmark key");
}

[[nodiscard]] auto mix(std::uint64_t value) noexcept -> std::uint64_t {
    value += 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31U);
}

[[nodiscard]] auto elapsed_ns(const Clock::time_point started) -> std::uint64_t {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
}

[[nodiscard]] auto summarize(std::vector<std::uint64_t> values) -> LatencySummary {
    if (values.empty()) {
        return {};
    }
    std::ranges::sort(values);
    const auto percentile = [&values](const double quantile) {
        const auto rank = static_cast<std::size_t>(std::ceil(quantile * static_cast<double>(values.size())));
        return values[std::min(std::max(std::size_t{1}, rank), values.size()) - 1U];
    };
    return {
        .samples = values.size(),
        .p50_ns = percentile(0.50),
        .p95_ns = percentile(0.95),
        .p99_ns = percentile(0.99),
        .maximum_ns = values.back(),
    };
}

[[nodiscard]] auto merge_latency(const std::vector<ThreadStats>& threads,
                                 const std::vector<std::uint64_t> ThreadStats::* member)
    -> std::vector<std::uint64_t> {
    std::size_t count{};
    for (const auto& thread : threads) {
        count += (thread.*member).size();
    }
    std::vector<std::uint64_t> merged;
    merged.reserve(count);
    for (const auto& thread : threads) {
        const auto& values = thread.*member;
        merged.insert(merged.end(), values.begin(), values.end());
    }
    return merged;
}

[[nodiscard]] auto segment_count(const std::filesystem::path& directory) -> std::size_t {
    std::size_t count{};
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const auto name = entry.path().filename().string();
        if (name.starts_with("segment-") && name.ends_with(".glypha")) {
            ++count;
        }
    }
    return count;
}

void verify_reopened(glyphastore::Store& store, const std::vector<std::string>& keys,
                     const std::size_t value_bytes) {
    require_status(store.verify_index(), "verify_index");
    for (const auto& key : keys) {
        const auto found = store.get(key);
        if (!found || found->bytes.size() != value_bytes) {
            throw std::runtime_error("reopened maintenance benchmark key is missing or truncated");
        }
    }
}

[[nodiscard]] auto run_sample(const Options& options, const Mode mode, const std::size_t repeat) -> Sample {
    TemporaryDirectory directory;
    std::vector<std::string> reclaim_keys;
    std::vector<std::string> foreground_keys;
    reclaim_keys.reserve(options.keys);
    foreground_keys.reserve(options.keys);
    for (std::size_t index = 0; index < options.keys; ++index) {
        reclaim_keys.push_back(key_for_worker(0, index, "maintenance-reclaim-"));
        foreground_keys.push_back(key_for_worker(1, index, "maintenance-foreground-"));
    }

    auto seed_store =
        open_store(options, Mode::disabled, directory.store(), glyphastore::DurableOpenMode::create_new);
    std::vector<std::byte> seed_value(options.reclaim_value_bytes, std::byte{0x5A});
    constexpr std::size_t seed_operations{1'024};
    for (std::size_t operation = 0; operation < seed_operations; ++operation) {
        const auto key_index = operation % reclaim_keys.size();
        const auto marker = static_cast<std::uint64_t>(operation);
        std::memcpy(seed_value.data(), &marker, sizeof(marker));
        require_status(seed_store->put(reclaim_keys[key_index], seed_value), "reclaim seed put");
    }
    std::vector<std::byte> foreground_value(options.value_bytes, std::byte{0x3C});
    for (std::size_t index = 0; index < foreground_keys.size(); ++index) {
        const auto marker = static_cast<std::uint64_t>(seed_operations + index);
        std::memcpy(foreground_value.data(), &marker, sizeof(marker));
        require_status(seed_store->put(foreground_keys[index], foreground_value), "foreground preload put");
    }
    require_status(seed_store->flush(), "seed flush");
    require_status(seed_store->close(), "seed Store close");
    seed_store.reset();

    std::latch start{1};
    CompactionStartGate start_gate{
        .start = &start,
        .store = directory.store(),
        .opener_thread = std::this_thread::get_id(),
    };
    auto store = open_store(options, mode, directory.store(), glyphastore::DurableOpenMode::open_existing,
                            &start_gate);

    std::vector<ThreadStats> thread_stats(options.threads);
    for (auto& thread : thread_stats) {
        const auto capacity =
            options.operations / options.threads + (options.operations % options.threads == 0 ? 0U : 1U);
        thread.all_latency_ns.reserve(capacity);
        thread.get_latency_ns.reserve(capacity);
        thread.put_latency_ns.reserve(capacity * options.put_percent / 100U + 1U);
    }

    const bool cooperative = mode == Mode::cooperative;
    std::latch ready{static_cast<std::ptrdiff_t>(options.threads + (cooperative ? 1U : 0U))};
    CooperativeStats cooperative_stats;
    std::vector<std::thread> workers;
    workers.reserve(options.threads);

    for (std::size_t thread_index = 0; thread_index < options.threads; ++thread_index) {
        workers.emplace_back([&, thread_index] {
            auto& stats = thread_stats[thread_index];
            std::vector<std::byte> value(options.value_bytes, std::byte{0xA5});
            ready.count_down();
            start.wait();
            for (std::size_t operation = thread_index; operation < options.operations;
                 operation += options.threads) {
                const auto selected = mix(operation);
                const auto key_index = static_cast<std::size_t>((selected >> 8U) % options.keys);
                const bool put = selected % 100U < options.put_percent;
                const auto started = Clock::now();
                bool succeeded{};
                if (put) {
                    const auto marker = static_cast<std::uint64_t>(operation + options.keys);
                    std::memcpy(value.data(), &marker, sizeof(marker));
                    succeeded = store->put(foreground_keys[key_index], value).has_value();
                    ++stats.puts;
                    stats.put_successes += succeeded ? 1U : 0U;
                } else {
                    const auto found = store->get(foreground_keys[key_index]);
                    succeeded = found.has_value() && found->bytes.size() == options.value_bytes;
                    ++stats.gets;
                    stats.get_hits += succeeded ? 1U : 0U;
                }
                const auto latency = elapsed_ns(started);
                stats.all_latency_ns.push_back(latency);
                (put ? stats.put_latency_ns : stats.get_latency_ns).push_back(latency);
                stats.failures += succeeded ? 0U : 1U;
            }
        });
    }

    std::thread cooperative_worker;
    if (cooperative) {
        cooperative_worker = std::thread([&] {
            ready.count_down();
            start.wait();
            ++cooperative_stats.attempts;
            const auto compacted = store->compact();
            if (compacted) {
                if (compacted->compacted) {
                    ++cooperative_stats.useful;
                    cooperative_stats.bytes_copied += compacted->bytes_copied;
                }
            } else if (compacted.error().code == glyphastore::ErrorCode::sequence_conflict) {
                ++cooperative_stats.conflicts;
            } else {
                ++cooperative_stats.unexpected_errors;
            }
        });
    }

    ready.wait();
    const auto foreground_started = Clock::now();
    start.count_down();
    for (auto& worker : workers) {
        worker.join();
    }
    const auto foreground_seconds = std::chrono::duration<double>(Clock::now() - foreground_started).count();
    if (cooperative_worker.joinable()) {
        cooperative_worker.join();
    }

    std::uint64_t operations{};
    std::uint64_t gets{};
    std::uint64_t get_hits{};
    std::uint64_t puts{};
    std::uint64_t put_successes{};
    std::uint64_t failures{};
    for (const auto& stats : thread_stats) {
        operations += stats.all_latency_ns.size();
        gets += stats.gets;
        get_hits += stats.get_hits;
        puts += stats.puts;
        put_successes += stats.put_successes;
        failures += stats.failures;
    }
    if (operations != options.operations || get_hits != gets || put_successes != puts || failures != 0 ||
        cooperative_stats.unexpected_errors != 0) {
        throw std::runtime_error(
            "foreground or cooperative maintenance validation failed: operations=" +
            std::to_string(operations) + "/" + std::to_string(options.operations) +
            " get_hits=" + std::to_string(get_hits) + "/" + std::to_string(gets) +
            " put_successes=" + std::to_string(put_successes) + "/" + std::to_string(puts) +
            " failures=" + std::to_string(failures) +
            " maintenance_errors=" + std::to_string(cooperative_stats.unexpected_errors));
    }

    auto maintenance = store->maintenance_snapshot();
    if (mode == Mode::background) {
        const auto deadline = Clock::now() + std::chrono::seconds{5};
        while (maintenance.useful_compactions == 0 && Clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            maintenance = store->maintenance_snapshot();
        }
        if (maintenance.useful_compactions == 0) {
            throw std::runtime_error("background maintenance did not complete the seeded compaction");
        }
    }
    require_status(store->close(), "Store close");
    store.reset();
    const auto segments_after = segment_count(directory.store());

    auto reopened =
        open_store(options, Mode::disabled, directory.store(), glyphastore::DurableOpenMode::open_existing);
    verify_reopened(*reopened, reclaim_keys, options.reclaim_value_bytes);
    verify_reopened(*reopened, foreground_keys, options.value_bytes);
    require_status(reopened->close(), "reopened Store close");

    const auto all_latency = summarize(merge_latency(thread_stats, &ThreadStats::all_latency_ns));
    const auto get_latency = summarize(merge_latency(thread_stats, &ThreadStats::get_latency_ns));
    const auto put_latency = summarize(merge_latency(thread_stats, &ThreadStats::put_latency_ns));
    return {
        .mode = mode,
        .repeat = repeat,
        .foreground_seconds = foreground_seconds,
        .operations = operations,
        .gets = gets,
        .puts = puts,
        .all_latency = all_latency,
        .get_latency = get_latency,
        .put_latency = put_latency,
        .maintenance_attempts = cooperative ? cooperative_stats.attempts : maintenance.compact_attempts,
        .maintenance_completed = cooperative ? cooperative_stats.useful : maintenance.compact_completed,
        .maintenance_useful = cooperative ? cooperative_stats.useful : maintenance.useful_compactions,
        .maintenance_conflicts = cooperative ? cooperative_stats.conflicts : 0,
        .maintenance_bytes_copied =
            cooperative ? cooperative_stats.bytes_copied : maintenance.total_bytes_copied,
        .maintenance_skips = cooperative ? 0 : maintenance.skips,
        .segments_after = segments_after,
    };
}

[[nodiscard]] auto microseconds(const std::uint64_t nanoseconds) -> double {
    return static_cast<double>(nanoseconds) / 1'000.0;
}

void print_sample(const Sample& sample) {
    const auto operations_per_second =
        sample.foreground_seconds > 0.0 ? static_cast<double>(sample.operations) / sample.foreground_seconds
                                        : 0.0;
    std::cout << mode_name(sample.mode) << ',' << sample.repeat << ',' << sample.operations << ','
              << sample.gets << ',' << sample.puts << ',' << sample.foreground_seconds << ','
              << operations_per_second << ',' << microseconds(sample.all_latency.p50_ns) << ','
              << microseconds(sample.all_latency.p95_ns) << ',' << microseconds(sample.all_latency.p99_ns)
              << ',' << microseconds(sample.all_latency.maximum_ns) << ','
              << microseconds(sample.get_latency.p50_ns) << ',' << microseconds(sample.get_latency.p95_ns)
              << ',' << microseconds(sample.get_latency.p99_ns) << ','
              << microseconds(sample.get_latency.maximum_ns) << ',' << microseconds(sample.put_latency.p50_ns)
              << ',' << microseconds(sample.put_latency.p95_ns) << ','
              << microseconds(sample.put_latency.p99_ns) << ',' << microseconds(sample.put_latency.maximum_ns)
              << ',' << sample.maintenance_attempts << ',' << sample.maintenance_completed << ','
              << sample.maintenance_useful << ',' << sample.maintenance_conflicts << ','
              << sample.maintenance_bytes_copied << ',' << sample.maintenance_skips << ','
              << sample.segments_after << '\n';
}

} // namespace

int main(int argc, char** argv) {
    try {
        const auto options = parse_options(argc, argv);
        constexpr std::array modes{Mode::disabled, Mode::cooperative, Mode::background};
        std::vector<Mode> selected;
        for (const auto mode : modes) {
            if (!options.mode || *options.mode == mode) {
                selected.push_back(mode);
            }
        }

        std::cout << "# benchmark=glyphastore_concurrent_maintenance\n";
        std::cout << "# git_sha=" << GLYPHASTORE_GIT_SHA << '\n';
        std::cout << "# storage_mode=durable-periodic;workers=2;reclaim_worker=0;"
                     "foreground_worker=1;seed_operations=1024;seed_flush=true\n";
        std::cout << "# cooperative_policy=one public Store::compact at foreground start\n";
        std::cout << "# background_policy=min_eval_interval_ms=max_eval_interval_ms="
                  << options.maintenance_interval_ms
                  << ";dead_byte_ratio_bp_normal=5000;"
                     "max_copy_bytes_per_cycle=default\n";
        std::cout << "# latency_measurement=per-operation steady_clock;instrumented throughput\n";
        std::cout << "# warmups=" << options.warmups << ";measured_repeats=" << options.repeats
                  << ";threads=" << options.threads << ";operations=" << options.operations
                  << ";keys=" << options.keys << ";value_bytes=" << options.value_bytes
                  << ";reclaim_value_bytes=" << options.reclaim_value_bytes
                  << ";put_percent=" << options.put_percent << ";cooldown_ms=" << options.cooldown_ms << '\n';
        std::cout << std::fixed << std::setprecision(6);
        std::cout << "mode,repeat,operations,get_operations,put_operations,foreground_s,"
                     "foreground_ops_s,p50_us,p95_us,p99_us,max_us,get_p50_us,get_p95_us,"
                     "get_p99_us,get_max_us,put_p50_us,put_p95_us,put_p99_us,put_max_us,"
                     "maintenance_attempts,maintenance_completed,maintenance_useful,"
                     "maintenance_conflicts,maintenance_bytes_copied,maintenance_skips,"
                     "segments_after\n";

        for (std::size_t warmup = 0; warmup < options.warmups; ++warmup) {
            const auto offset = warmup % selected.size();
            for (std::size_t index = 0; index < selected.size(); ++index) {
                static_cast<void>(run_sample(options, selected[(index + offset) % selected.size()], 0));
                std::this_thread::sleep_for(std::chrono::milliseconds{options.cooldown_ms});
            }
        }
        for (std::size_t repeat = 1; repeat <= options.repeats; ++repeat) {
            const auto offset = (repeat - 1U) % selected.size();
            for (std::size_t index = 0; index < selected.size(); ++index) {
                print_sample(run_sample(options, selected[(index + offset) % selected.size()], repeat));
                std::this_thread::sleep_for(std::chrono::milliseconds{options.cooldown_ms});
            }
        }
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "maintenance benchmark error: " << exception.what() << '\n';
        return 1;
    }
}
