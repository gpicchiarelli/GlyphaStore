#include "crash_checkpoint.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/bootstrap.hpp"
#include "glyphastore/persistence/recovery.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/store.hpp"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr std::string_view kSeedKey{"seed"};
constexpr std::string_view kSeedValue{"seed-value"};
constexpr std::string_view kCrashKey{"crash-key"};
constexpr std::string_view kCrashValue{"crash-value"};
constexpr std::string_view kRotateKey{"rotate-key"};

[[nodiscard]] auto crash_run_suffix() -> const std::string& {
    static const auto suffix = std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
                               std::to_string(static_cast<unsigned long long>(
                                   std::chrono::steady_clock::now().time_since_epoch().count()));
    return suffix;
}

struct Options {
    std::string mode{"matrix"};
    std::string scenario{};
    std::string boundary{};
    std::string storage{"sync"};
    std::filesystem::path data_dir{};
    std::filesystem::path checkpoint_dir{};
};

[[nodiscard]] auto parse_options(int argc, char** argv) -> std::optional<Options> {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view arg{argv[index]};
        const auto require_value = [&](const char* flag) -> std::optional<std::string_view> {
            if (arg != flag || index + 1 >= argc) {
                return std::nullopt;
            }
            return std::string_view{argv[++index]};
        };
        if (arg == "--mode") {
            const auto value = require_value("--mode");
            if (!value) {
                return std::nullopt;
            }
            options.mode = std::string{*value};
        } else if (arg == "--scenario") {
            const auto value = require_value("--scenario");
            if (!value) {
                return std::nullopt;
            }
            options.scenario = std::string{*value};
        } else if (arg == "--boundary") {
            const auto value = require_value("--boundary");
            if (!value) {
                return std::nullopt;
            }
            options.boundary = std::string{*value};
        } else if (arg == "--storage") {
            const auto value = require_value("--storage");
            if (!value) {
                return std::nullopt;
            }
            options.storage = std::string{*value};
        } else if (arg == "--data-dir") {
            const auto value = require_value("--data-dir");
            if (!value) {
                return std::nullopt;
            }
            options.data_dir = *value;
        } else if (arg == "--checkpoint-dir") {
            const auto value = require_value("--checkpoint-dir");
            if (!value) {
                return std::nullopt;
            }
            options.checkpoint_dir = *value;
        } else if (arg == "--help" || arg == "-h") {
            return std::nullopt;
        } else {
            return std::nullopt;
        }
    }
    return options;
}

void print_usage(const char* program) {
    std::cerr
        << "usage: " << program
        << " --mode {worker|verify|matrix|periodic-matrix|group-matrix} [--scenario bootstrap|put|rotate] "
           "[--boundary OP]\n"
        << "       [--storage {sync|periodic|group}] [--data-dir PATH] [--checkpoint-dir PATH]\n";
}

[[nodiscard]] auto periodic_storage(const Options& options) -> bool {
    return options.storage == "periodic";
}

[[nodiscard]] auto group_storage(const Options& options) -> bool {
    return options.storage == "group";
}

[[nodiscard]] auto durable_runtime_options(const Options& options) -> glyphastore::DurableRuntimeOptions {
    if (group_storage(options)) {
        return {.commit_sync = glyphastore::SegmentCommitSync::immediate,
                .sync_interval_ms = 50,
                .batch =
                    glyphastore::DurableGroupConfig{.max_records = 32, .max_bytes = 65536, .max_wait_ms = 50},
                .strict_ack = true};
    }
    if (periodic_storage(options)) {
        return {.commit_sync = glyphastore::SegmentCommitSync::deferred,
                .sync_interval_ms = 50,
                .batch = glyphastore::DurableGroupConfig{
                    .max_records = 32, .max_bytes = 65536, .max_wait_ms = 50}};
    }
    return {};
}

[[nodiscard]] auto recovery_storage_mode(const Options& options) -> glyphastore::StorageMode {
    if (group_storage(options)) {
        return glyphastore::StorageMode::durable_group;
    }
    return periodic_storage(options) ? glyphastore::StorageMode::durable_periodic
                                     : glyphastore::StorageMode::durable_sync;
}

[[nodiscard]] auto open_runtime_for_worker(const Options& options, glyphastore::FilesystemHooks hooks)
    -> std::unique_ptr<glyphastore::DurableRuntimeCatalog> {
    auto directory = glyphastore::DataDirectory::open_and_lock(
        options.data_dir, glyphastore::DataDirectoryOpenMode::existing, hooks);
    if (!directory) {
        return nullptr;
    }
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0,
                                                                   durable_runtime_options(options));
    if (!runtime) {
        return nullptr;
    }
    return std::move(*runtime);
}

[[nodiscard]] auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto owned_text(const glyphastore::OwnedValue& value) -> std::string {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

[[nodiscard]] auto recovery_store_id() -> glyphastore::StoreId {
    return {std::byte{0x20}, std::byte{0x21}, std::byte{0x22}, std::byte{0x23},
            std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27},
            std::byte{0x28}, std::byte{0x29}, std::byte{0x2A}, std::byte{0x2B},
            std::byte{0x2C}, std::byte{0x2D}, std::byte{0x2E}, std::byte{0x2F}};
}

void append_record(glyphastore::DurableSegmentFile& file, const std::uint64_t sequence,
                   const std::string_view key, const std::string_view value) {
    const auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{sequence},
        .opcode = glyphastore::Opcode::put,
        .type = glyphastore::ValueType::bytes,
        .flags = 0,
        .key_hash = glyphastore::hash_key(key),
        .expire_at_ns = 0,
        .key = bytes(key),
        .value = bytes(value),
    });
    if (!encoded || !file.append(*encoded).committed()) {
        throw std::runtime_error("failed to append durable test record");
    }
}

void seed_put_store(const std::filesystem::path& data_dir) {
    std::filesystem::create_directories(data_dir.parent_path());
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = data_dir,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    if (!opened) {
        throw std::runtime_error("failed to seed durable store");
    }
    if (!(*opened)->put(kSeedKey, bytes(kSeedValue)).has_value()) {
        throw std::runtime_error("failed to seed durable key");
    }
}

void seed_rotate_store(const std::filesystem::path& data_dir) {
    std::filesystem::create_directories(data_dir.parent_path());
    const auto store_id = recovery_store_id();
    const glyphastore::ManifestSegmentEntry active{
        .segment_id = glyphastore::SegmentId{1},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const std::string fill_key{"fill"};
    const std::string maximum_value(
        glyphastore::kMaxNormalRecordSize - glyphastore::kEncodedRecordHeaderSize - fill_key.size(), 'x');
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            data_dir, glyphastore::DataDirectoryOpenMode::open_or_create);
        if (!directory) {
            throw std::runtime_error("failed to open data directory for rotate seed");
        }
        auto created =
            glyphastore::DurableSegmentFile::create(*directory, {.store_id = store_id,
                                                                 .segment_id = active.segment_id,
                                                                 .generation = active.generation,
                                                                 .owner_worker = active.owner_worker});
        if (!created.durable() || !created.file) {
            throw std::runtime_error("failed to create rotate seed segment");
        }
        for (std::uint64_t sequence = 1; sequence <= 63; ++sequence) {
            append_record(*created.file, sequence, fill_key, maximum_value);
        }
        const glyphastore::Manifest manifest{
            .store_id = store_id,
            .manifest_generation = 1,
            .routing_algorithm = glyphastore::RoutingAlgorithm::fnv1a64_v1,
            .worker_count = 1,
            .routing_epoch = 1,
            .next_segment_id = glyphastore::SegmentId{2},
            .next_segment_generation = glyphastore::GenerationId{1},
            .segments = {active},
        };
        if (!directory->publish_manifest(manifest).durable()) {
            throw std::runtime_error("failed to publish rotate seed manifest");
        }
    }
    auto runtime = glyphastore::DurableRuntimeCatalog::open_existing(data_dir);
    if (!runtime) {
        throw std::runtime_error("failed to open rotate seed runtime");
    }
    if (!(*runtime)->put(bytes(kSeedKey), bytes(kSeedValue)).committed()) {
        throw std::runtime_error("failed to seed rotate baseline key");
    }
}

void run_worker(const Options& options) {
    glyphastore::crash::remove_checkpoint_markers(options.checkpoint_dir);
    std::filesystem::create_directories(options.checkpoint_dir);
    glyphastore::crash::CheckpointState checkpoint{
        .checkpoint_dir = options.checkpoint_dir,
        .kill_at = options.boundary,
    };
    const auto hooks = checkpoint.hooks();

    if (options.scenario == "bootstrap") {
        auto directory = glyphastore::DataDirectory::open_and_lock(
            options.data_dir, glyphastore::DataDirectoryOpenMode::create_new, hooks);
        if (!directory) {
            throw std::runtime_error("bootstrap worker failed to open data directory");
        }
        if (auto prepared = glyphastore::prepare_durable_store(
                *directory, glyphastore::DurableOpenMode::create_new, 1, std::optional<std::size_t>{1});
            !prepared) {
            throw std::runtime_error("bootstrap worker failed to prepare store");
        }
        auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0,
                                                                       durable_runtime_options(options));
        if (!runtime) {
            throw std::runtime_error("bootstrap worker failed to open runtime");
        }
        if (!(*runtime)->put(bytes(kSeedKey), bytes(kSeedValue)).committed()) {
            throw std::runtime_error("bootstrap worker failed to commit seed put");
        }
        return;
    }

    if (options.scenario == "put") {
        auto runtime = open_runtime_for_worker(options, hooks);
        if (!runtime) {
            throw std::runtime_error("put worker failed to open runtime");
        }
        if (!runtime->put(bytes(kCrashKey), bytes(kCrashValue)).committed()) {
            throw std::runtime_error("put worker failed to commit crash key");
        }
        return;
    }

    if (options.scenario == "rotate") {
        auto runtime = open_runtime_for_worker(options, hooks);
        if (!runtime) {
            throw std::runtime_error("rotate worker failed to open runtime");
        }
        const std::string rotate_value(glyphastore::kMaxNormalRecordSize -
                                           glyphastore::kEncodedRecordHeaderSize - kRotateKey.size(),
                                       'r');
        if (!runtime->put(bytes(kRotateKey), bytes(rotate_value)).committed()) {
            throw std::runtime_error("rotate worker failed to commit rotation put");
        }
        return;
    }

    throw std::runtime_error("unknown crash scenario");
}

enum class RecoveryExpectation { absent, optional, present };

[[nodiscard]] auto mutation_expectation(const Options& options) -> RecoveryExpectation {
    if (options.boundary == "sync_commit_slot") {
        return RecoveryExpectation::present;
    }
    if (options.boundary == "write_commit_slot") {
        return RecoveryExpectation::optional;
    }
    return RecoveryExpectation::absent;
}

[[nodiscard]] auto verify_expected_value(const glyphastore::Result<glyphastore::OwnedValue>& value,
                                         const std::string_view expected,
                                         const RecoveryExpectation expectation, const std::string_view label)
    -> bool {
    if (expectation == RecoveryExpectation::absent) {
        if (value) {
            std::cerr << "verify: " << label << " should be absent before the commit point\n";
            return false;
        }
        if (value.error().code != glyphastore::ErrorCode::not_found) {
            std::cerr << "verify: " << label << " failed with an unexpected recovery error\n";
            return false;
        }
        return true;
    }
    if (!value) {
        if (expectation == RecoveryExpectation::optional &&
            value.error().code == glyphastore::ErrorCode::not_found) {
            return true;
        }
        std::cerr << "verify: " << label << " should be durable after the commit point\n";
        return false;
    }
    if (owned_text(*value) != expected) {
        std::cerr << "verify: " << label << " value mismatch\n";
        return false;
    }
    return true;
}

[[nodiscard]] auto verify_recovery(const Options& options) -> bool {
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .storage_mode = recovery_storage_mode(options),
         .data_directory = options.data_dir,
         .durable_open_mode = glyphastore::DurableOpenMode::open_or_create,
         .durable_periodic = {.sync_interval_ms = 60'000},
         .durable_group = {.max_records = 32, .max_bytes = 65536, .max_wait_ms = 60'000}});
    if (!opened) {
        std::cerr << "verify: failed to reopen durable store\n";
        return false;
    }
    if (!(*opened)->verify_index().has_value()) {
        std::cerr << "verify: index verification failed\n";
        return false;
    }

    if (options.scenario == "bootstrap") {
        const auto seed = (*opened)->get(kSeedKey);
        return verify_expected_value(seed, kSeedValue, mutation_expectation(options), "bootstrap seed key");
    }

    if (options.scenario == "put") {
        const auto seed = (*opened)->get(kSeedKey);
        if (!seed || owned_text(*seed) != kSeedValue) {
            std::cerr << "verify: seed key not preserved after crash\n";
            return false;
        }
        const auto crash = (*opened)->get(kCrashKey);
        return verify_expected_value(crash, kCrashValue, mutation_expectation(options), "crash key");
    }

    if (options.scenario == "rotate") {
        const auto seed = (*opened)->get(kSeedKey);
        if (!seed || owned_text(*seed) != kSeedValue) {
            std::cerr << "verify: rotate seed key not preserved\n";
            return false;
        }
        return true;
    }

    std::cerr << "verify: unknown scenario\n";
    return false;
}

[[nodiscard]] auto scenario_boundaries(const std::string_view scenario) -> std::vector<std::string_view> {
    if (scenario == "bootstrap") {
        return {"create_data_directory", "sync_parent_directory", "write_bootstrap",      "sync_bootstrap",
                "rename_bootstrap",      "sync_directory",        "write_manifest",       "sync_manifest",
                "rename_manifest",       "preallocate_segment",   "write_segment_header", "sync_segment_file",
                "rename_segment",        "remove_bootstrap",      "write_record",         "sync_record",
                "write_commit_slot",     "sync_commit_slot"};
    }
    if (scenario == "put") {
        return {"write_record", "sync_record", "write_commit_slot", "sync_commit_slot"};
    }
    if (scenario == "rotate") {
        return {"write_commit_slot", "sync_commit_slot", "preallocate_segment", "write_segment_header",
                "sync_segment_file", "rename_segment",   "sync_directory",      "write_manifest",
                "sync_manifest",     "rename_manifest",  "write_record",        "sync_record",
                "write_commit_slot", "sync_commit_slot"};
    }
    return {};
}

[[nodiscard]] auto run_single_case(const Options& base_options) -> bool {
    Options options = base_options;
    std::error_code ignored;
    std::filesystem::remove_all(options.data_dir, ignored);
    if (options.scenario == "bootstrap") {
        std::filesystem::create_directories(options.data_dir.parent_path());
    } else if (options.scenario == "put") {
        seed_put_store(options.data_dir);
    } else if (options.scenario == "rotate") {
        seed_rotate_store(options.data_dir);
    }

    std::filesystem::create_directories(options.checkpoint_dir);
    glyphastore::crash::remove_checkpoint_markers(options.checkpoint_dir);

    const pid_t child = ::fork();
    if (child < 0) {
        std::cerr << "fork failed: " << std::strerror(errno) << '\n';
        return false;
    }
    if (child == 0) {
        try {
            run_worker(options);
            std::exit(0);
        } catch (const std::exception& exception) {
            std::cerr << "worker failed: " << exception.what() << '\n';
            std::exit(1);
        }
    }

    if (!glyphastore::crash::wait_for_checkpoint(options.checkpoint_dir, options.boundary)) {
        std::cerr << "timed out waiting for checkpoint " << options.boundary << " in scenario "
                  << options.scenario << '\n';
        ::kill(child, SIGKILL);
        int status = 0;
        ::waitpid(child, &status, 0);
        return false;
    }
    ::kill(child, SIGKILL);
    int status = 0;
    ::waitpid(child, &status, 0);

    if (!verify_recovery(options)) {
        std::cerr << "recovery verification failed for scenario " << options.scenario << " at boundary "
                  << options.boundary << '\n';
        return false;
    }
    return true;
}

void cleanup_matrix_case(const Options& options) {
    std::error_code ignored;
    std::filesystem::remove_all(options.data_dir.parent_path(), ignored);
    std::filesystem::remove_all(options.checkpoint_dir, ignored);
}

[[nodiscard]] auto run_matrix() -> bool {
    const std::vector<std::string_view> scenarios{"bootstrap", "put", "rotate"};
    bool success = true;
    for (const auto scenario : scenarios) {
        for (const auto boundary : scenario_boundaries(scenario)) {
            Options options{
                .mode = "matrix",
                .scenario = std::string{scenario},
                .boundary = std::string{boundary},
                .data_dir = std::filesystem::temp_directory_path() /
                            ("glyphastore-crash-" + crash_run_suffix() + "-" + std::string{scenario} + "-" +
                             std::string{boundary}) /
                            "store",
                .checkpoint_dir = std::filesystem::temp_directory_path() /
                                  ("glyphastore-crash-checkpoints-" + crash_run_suffix() + "-" +
                                   std::string{scenario} + "-" + std::string{boundary}),
            };
            std::cout << "# crash scenario=" << scenario << " boundary=" << boundary << '\n';
            if (!run_single_case(options)) {
                success = false;
            }
            cleanup_matrix_case(options);
        }
    }
    return success;
}

[[nodiscard]] auto run_periodic_matrix() -> bool {
    const std::vector<std::string_view> boundaries{"write_record", "sync_record", "write_commit_slot",
                                                   "sync_commit_slot"};
    bool success = true;
    for (const auto boundary : boundaries) {
        Options options{
            .mode = "periodic-matrix",
            .scenario = "put",
            .boundary = std::string{boundary},
            .storage = "periodic",
            .data_dir =
                std::filesystem::temp_directory_path() /
                ("glyphastore-crash-periodic-" + crash_run_suffix() + "-put-" + std::string{boundary}) /
                "store",
            .checkpoint_dir = std::filesystem::temp_directory_path() /
                              ("glyphastore-crash-periodic-checkpoints-" + crash_run_suffix() + "-put-" +
                               std::string{boundary}),
        };
        std::cout << "# crash storage=periodic scenario=put boundary=" << boundary << '\n';
        if (!run_single_case(options)) {
            success = false;
        }
        cleanup_matrix_case(options);
    }
    return success;
}

[[nodiscard]] auto run_group_matrix() -> bool {
    const std::vector<std::string_view> boundaries{"write_record", "sync_record", "write_commit_slot",
                                                   "sync_commit_slot"};
    bool success = true;
    for (const auto boundary : boundaries) {
        Options options{
            .mode = "group-matrix",
            .scenario = "put",
            .boundary = std::string{boundary},
            .storage = "group",
            .data_dir = std::filesystem::temp_directory_path() /
                        ("glyphastore-crash-group-" + crash_run_suffix() + "-put-" + std::string{boundary}) /
                        "store",
            .checkpoint_dir = std::filesystem::temp_directory_path() /
                              ("glyphastore-crash-group-checkpoints-" + crash_run_suffix() + "-put-" +
                               std::string{boundary}),
        };
        std::cout << "# crash storage=group scenario=put boundary=" << boundary << '\n';
        if (!run_single_case(options)) {
            success = false;
        }
        cleanup_matrix_case(options);
    }
    return success;
}

} // namespace

int main(int argc, char** argv) {
    const auto options = parse_options(argc, argv);
    if (!options) {
        print_usage(argv[0]);
        return 2;
    }

    try {
        if (options->mode == "worker") {
            run_worker(*options);
            return 0;
        }
        if (options->mode == "verify") {
            return verify_recovery(*options) ? 0 : 1;
        }
        if (options->mode == "matrix") {
            return run_matrix() ? 0 : 1;
        }
        if (options->mode == "periodic-matrix") {
            return run_periodic_matrix() ? 0 : 1;
        }
        if (options->mode == "group-matrix") {
            return run_group_matrix() ? 0 : 1;
        }
        print_usage(argv[0]);
        return 2;
    } catch (const std::exception& exception) {
        std::cerr << "glyphastore_crash_persistence: fatal: " << exception.what() << '\n';
        return 1;
    }
}
