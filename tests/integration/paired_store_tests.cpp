#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/paired/publication_coordinator.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("paired Store read-after-write and close drain") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("alpha", bytes("one")).has_value());
    const auto first = store.get("alpha");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(first->bytes.data()), first->bytes.size()) == "one");
    GLYPHA_REQUIRE(store.put("alpha", bytes("two")).has_value());
    const auto second = store.get("alpha");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(second->bytes.data()), second->bytes.size()) == "two");
    GLYPHA_REQUIRE(store.erase("alpha").has_value());
    GLYPHA_REQUIRE(!store.get("alpha").has_value());
    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(!store.put("alpha", bytes("late")).has_value());
}

GLYPHA_TEST("paired Store concurrent GET and PUT on one key stay linearized") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("shared", bytes("0")).has_value());

    std::atomic_bool failed{false};
    std::atomic_uint64_t writes{0};
    std::thread writer([&] {
        for (std::uint64_t value = 1; value <= 200; ++value) {
            if (!store.put("shared", bytes(std::to_string(value))).has_value()) {
                failed.store(true);
                return;
            }
            writes.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::thread reader([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (std::chrono::steady_clock::now() < deadline) {
            auto value = store.get("shared");
            if (!value.has_value()) {
                failed.store(true);
                return;
            }
            std::this_thread::yield();
        }
    });
    writer.join();
    reader.join();
    GLYPHA_REQUIRE(!failed.load());
    GLYPHA_REQUIRE(writes.load() == 200);
    const auto final_value = store.get("shared");
    GLYPHA_REQUIRE(final_value.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(final_value->bytes.data()),
                                    final_value->bytes.size()) == "200");
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store concurrent read-after-write keeps adopted generations alive") {
    constexpr std::size_t kThreadCount = 4;
    constexpr std::size_t kWritesPerThread = 1'024;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 4}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::atomic_bool failed{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (std::size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&, thread] {
            for (std::size_t write = 0; write < kWritesPerThread; ++write) {
                const auto key = "lease-" + std::to_string(thread) + '-' + std::to_string(write);
                const auto value = "value-" + std::to_string(write);
                if (!store.put(key, bytes(value)).has_value() || !store.get(key).has_value()) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired durable group threshold requires final commit-slot synchronization") {
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-group-sync-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};

    struct FailFirstCommitSlotSync final {
        bool fired{};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& self = *static_cast<FailFirstCommitSlotSync*>(context);
            if (operation == glyphastore::FilesystemOperation::sync_commit_slot && !self.fired) {
                self.fired = true;
                return glyphastore::fail(glyphastore::ErrorCode::io_error,
                                         "injected strict group commit-slot sync failure");
            }
            return {};
        }
    } failure;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = root / "store",
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 60'000, .min_records = 2},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &failure, .before = &FailFirstCommitSlotSync::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    const std::array items{
        glyphastore::Store::PutItem{.key = "strict-a", .value = bytes("alpha")},
        glyphastore::Store::PutItem{.key = "strict-b", .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(failure.fired);
    GLYPHA_REQUIRE(statuses.size() == items.size());
    GLYPHA_REQUIRE(!statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(statuses[0].error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(statuses[1].error().code == glyphastore::ErrorCode::unavailable);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());
    static_cast<void>(store.close());

    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("ADR 0036 V5 production shutdown finalization rejects a live Reader lease") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->finalize_reader_shutdown().has_value());

    {
        glyphastore::store::paired::ShardPairRuntime::ReadLease lease{*runtime, 0};
        GLYPHA_REQUIRE(static_cast<bool>(lease));
        GLYPHA_REQUIRE(store.put("shutdown-final-generation", bytes("value")).has_value());
        GLYPHA_REQUIRE(runtime->stats()[0].retired_generation_count >= 1);
        GLYPHA_REQUIRE(runtime->stop_and_drain().has_value());

        const auto blocked = runtime->finalize_reader_shutdown();
        GLYPHA_REQUIRE(!blocked.has_value());
        GLYPHA_REQUIRE(blocked.error().code == glyphastore::ErrorCode::unavailable);
        GLYPHA_REQUIRE(runtime->adopt_read_generation(0) != nullptr);
        GLYPHA_REQUIRE(!runtime->stats()[0].reader_shutdown_finalized);
    }

    std::atomic_bool first_finalized{};
    std::atomic_bool second_finalized{};
    std::thread first_finalizer{[&] {
        first_finalized.store(runtime->finalize_reader_shutdown().has_value(), std::memory_order_release);
    }};
    std::thread second_finalizer{[&] {
        second_finalized.store(runtime->finalize_reader_shutdown().has_value(), std::memory_order_release);
    }};
    first_finalizer.join();
    second_finalizer.join();
    GLYPHA_REQUIRE(first_finalized.load(std::memory_order_acquire));
    GLYPHA_REQUIRE(second_finalized.load(std::memory_order_acquire));
    GLYPHA_REQUIRE(runtime->finalize_reader_shutdown().has_value());
    GLYPHA_REQUIRE(runtime->adopt_read_generation(0) == nullptr);
    const auto finalized = runtime->stats()[0];
    GLYPHA_REQUIRE(finalized.reader_shutdown_finalized);
    GLYPHA_REQUIRE(finalized.retired_generation_count == 0);
    GLYPHA_REQUIRE(finalized.shutdown_generations_reclaimed >= 2);
    GLYPHA_REQUIRE(finalized.reader_safe_epoch > finalized.writer_epoch);
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("ADR 0036 V5 Store close owns terminal Reader generation finalization") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(store.put("close-finalization", bytes("value")).has_value());
    GLYPHA_REQUIRE(runtime->adopt_read_generation(0) != nullptr);

    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(runtime->adopt_read_generation(0) == nullptr);
    const auto stats = runtime->stats()[0];
    GLYPHA_REQUIRE(stats.reader_shutdown_finalized);
    GLYPHA_REQUIRE(stats.retired_generation_count == 0);
    GLYPHA_REQUIRE(stats.shutdown_generations_reclaimed >= 1);
    GLYPHA_REQUIRE(stats.reader_safe_epoch > stats.writer_epoch);
}

GLYPHA_TEST("paired generation admission rejects embedded sync before Store at retire bound") {
    using glyphastore::store::paired::decide_generation_admission;
    using glyphastore::store::paired::GenerationAdmissionDecision;
    using glyphastore::store::paired::ShardPairRuntime;

    GLYPHA_REQUIRE(decide_generation_admission(ShardPairRuntime::kMaximumRetiredReadGenerations - 1U,
                                               ShardPairRuntime::kMaximumRetiredReadGenerations,
                                               true) == GenerationAdmissionDecision::admitted);
    GLYPHA_REQUIRE(decide_generation_admission(ShardPairRuntime::kMaximumRetiredReadGenerations,
                                               ShardPairRuntime::kMaximumRetiredReadGenerations, true) ==
                   GenerationAdmissionDecision::reader_quiescence_required);
    GLYPHA_REQUIRE(decide_generation_admission(0U, ShardPairRuntime::kMaximumRetiredReadGenerations, false) ==
                   GenerationAdmissionDecision::incremental_merge_required);

    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    {
        ShardPairRuntime::ReadLease pinned_reader{*runtime, 0};
        GLYPHA_REQUIRE(static_cast<bool>(pinned_reader));
        for (std::size_t publication = 0; publication < ShardPairRuntime::kMaximumRetiredReadGenerations;
             ++publication) {
            const auto value = "generation-" + std::to_string(publication);
            GLYPHA_REQUIRE(store.put("retire-pressure", bytes(value)).has_value());
        }
        GLYPHA_REQUIRE(runtime->stats()[0].retired_generation_count ==
                       ShardPairRuntime::kMaximumRetiredReadGenerations);

        const auto blocked = store.put("must-not-enter-store", bytes("blocked"));
        GLYPHA_REQUIRE(!blocked.has_value());
        GLYPHA_REQUIRE(blocked.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(blocked.error().message == "mutation rejected until paired Reader reaches quiescence");
        GLYPHA_REQUIRE(runtime->stats()[0].retired_generation_count ==
                       ShardPairRuntime::kMaximumRetiredReadGenerations);
        GLYPHA_REQUIRE(runtime->stats()[0].generation_admission_backpressure_total == 1U);
        const auto absent = store.get("must-not-enter-store");
        GLYPHA_REQUIRE(!absent.has_value());
        GLYPHA_REQUIRE(absent.error().code == glyphastore::ErrorCode::not_found);
    }

    GLYPHA_REQUIRE(store.put("must-not-enter-store", bytes("after-quiescence")).has_value());
    GLYPHA_REQUIRE(store.get("must-not-enter-store").has_value());
    GLYPHA_REQUIRE(runtime->stats()[0].retired_generation_count <
                   ShardPairRuntime::kMaximumRetiredReadGenerations);
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired generation admission rejects dedicated Writer sync before Store at retire bound") {
    using glyphastore::store::paired::ShardPairRuntime;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .paired = {.async_lane_capacity = 8, .async_lane_payload_bytes = 64U * 1024U}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    {
        ShardPairRuntime::ReadLease pinned_reader{*runtime, 0};
        GLYPHA_REQUIRE(static_cast<bool>(pinned_reader));
        for (std::size_t publication = 0; publication < ShardPairRuntime::kMaximumRetiredReadGenerations;
             ++publication) {
            const auto value = "dedicated-generation-" + std::to_string(publication);
            GLYPHA_REQUIRE(store.put("dedicated-retire-pressure", bytes(value)).has_value());
        }
        const auto blocked = store.put("dedicated-must-not-enter", bytes("blocked"));
        GLYPHA_REQUIRE(!blocked.has_value());
        GLYPHA_REQUIRE(blocked.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(runtime->stats()[0].retired_generation_count ==
                       ShardPairRuntime::kMaximumRetiredReadGenerations);
        GLYPHA_REQUIRE(runtime->stats()[0].generation_admission_backpressure_total == 1U);
        GLYPHA_REQUIRE(!store.get("dedicated-must-not-enter").has_value());
    }

    GLYPHA_REQUIRE(store.put("dedicated-must-not-enter", bytes("after-quiescence")).has_value());
    GLYPHA_REQUIRE(store.get("dedicated-must-not-enter").has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired generation admission performs no durable write at retire bound") {
    using glyphastore::store::paired::ShardPairRuntime;

    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-generation-admission-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};

    struct WriteCounter final {
        std::atomic_uint64_t records{};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& self = *static_cast<WriteCounter*>(context);
            if (operation == glyphastore::FilesystemOperation::write_record) {
                self.records.fetch_add(1U, std::memory_order_relaxed);
            }
            return {};
        }
    } writes;

    auto opened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = root / "store",
                                  .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                  .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
                                  .filesystem_hooks = {.context = &writes, .before = &WriteCounter::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    {
        ShardPairRuntime::ReadLease pinned_reader{*runtime, 0};
        GLYPHA_REQUIRE(static_cast<bool>(pinned_reader));
        for (std::size_t publication = 0; publication < ShardPairRuntime::kMaximumRetiredReadGenerations;
             ++publication) {
            const auto value = "durable-generation-" + std::to_string(publication);
            GLYPHA_REQUIRE(store.put("durable-retire-pressure", bytes(value)).has_value());
        }
        const auto writes_before_rejection = writes.records.load(std::memory_order_relaxed);
        const auto blocked = store.put("durable-must-not-enter", bytes("blocked"));
        GLYPHA_REQUIRE(!blocked.has_value());
        GLYPHA_REQUIRE(blocked.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(writes.records.load(std::memory_order_relaxed) == writes_before_rejection);
        GLYPHA_REQUIRE(!store.get("durable-must-not-enter").has_value());
        GLYPHA_REQUIRE(runtime->stats()[0].generation_admission_backpressure_total == 1U);
    }

    const auto writes_before_resume = writes.records.load(std::memory_order_relaxed);
    GLYPHA_REQUIRE(store.put("durable-must-not-enter", bytes("after-quiescence")).has_value());
    GLYPHA_REQUIRE(writes.records.load(std::memory_order_relaxed) > writes_before_resume);
    GLYPHA_REQUIRE(store.get("durable-must-not-enter").has_value());
    GLYPHA_REQUIRE(store.close().has_value());

    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired generation admission rejects every durable group batch item before Store") {
    using glyphastore::store::paired::ShardPairRuntime;

    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-generation-group-admission-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};

    struct WriteCounter final {
        std::atomic_uint64_t records{};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& self = *static_cast<WriteCounter*>(context);
            if (operation == glyphastore::FilesystemOperation::write_record) {
                self.records.fetch_add(1U, std::memory_order_relaxed);
            }
            return {};
        }
    } writes;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = root / "store",
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 64U * 1024U, .max_wait_ms = 1, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &writes, .before = &WriteCounter::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    const std::string first_key{"group-must-not-enter-a"};
    const std::string second_key{"group-must-not-enter-b"};
    const std::array<glyphastore::Store::PutItem, 2> rejected_items{
        glyphastore::Store::PutItem{.key = first_key, .value = bytes("first")},
        glyphastore::Store::PutItem{.key = second_key, .value = bytes("second")},
    };
    {
        ShardPairRuntime::ReadLease pinned_reader{*runtime, 0};
        GLYPHA_REQUIRE(static_cast<bool>(pinned_reader));
        for (std::size_t publication = 0; publication < ShardPairRuntime::kMaximumRetiredReadGenerations;
             ++publication) {
            const auto value = "group-generation-" + std::to_string(publication);
            GLYPHA_REQUIRE(store.put("group-retire-pressure", bytes(value)).has_value());
        }

        const auto writes_before_rejection = writes.records.load(std::memory_order_relaxed);
        const auto statuses = store.put_batch(rejected_items);
        GLYPHA_REQUIRE(statuses.size() == rejected_items.size());
        for (const auto& status : statuses) {
            GLYPHA_REQUIRE(!status.has_value());
            GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::resource_exhausted);
        }
        GLYPHA_REQUIRE(writes.records.load(std::memory_order_relaxed) == writes_before_rejection);
        GLYPHA_REQUIRE(runtime->stats()[0].generation_admission_backpressure_total == rejected_items.size());
        GLYPHA_REQUIRE(!store.get(first_key).has_value());
        GLYPHA_REQUIRE(!store.get(second_key).has_value());
    }

    const auto writes_before_resume = writes.records.load(std::memory_order_relaxed);
    const auto resumed = store.put_batch(rejected_items);
    GLYPHA_REQUIRE(resumed.size() == rejected_items.size());
    GLYPHA_REQUIRE(resumed[0].has_value());
    GLYPHA_REQUIRE(resumed[1].has_value());
    GLYPHA_REQUIRE(writes.records.load(std::memory_order_relaxed) > writes_before_resume);
    GLYPHA_REQUIRE(store.get(first_key).has_value());
    GLYPHA_REQUIRE(store.get(second_key).has_value());
    GLYPHA_REQUIRE(store.close().has_value());

    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired Store concurrent GET observes live generation under overwrite storm") {
    // ADR 0036 V2/V3 baseline under production shared_ptr + ReadLease (not slot-pool).
    // Slot-pool landing must keep this class of race green (see ADR 0036 verification matrix).
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key = "overwrite-storm";
    GLYPHA_REQUIRE(store.put(key, bytes("seed")).has_value());

    std::atomic_bool stop{false};
    std::atomic_bool failed{false};
    std::thread reader{[&] {
        while (!stop.load(std::memory_order_acquire)) {
            const auto got = store.get(key);
            if (!got.has_value() || got->bytes.empty()) {
                failed.store(true, std::memory_order_relaxed);
                return;
            }
        }
    }};
    for (std::size_t write = 0; write < 8'192; ++write) {
        const auto value = "v-" + std::to_string(write);
        if (!store.put(key, bytes(value)).has_value()) {
            failed.store(true, std::memory_order_relaxed);
            break;
        }
    }
    stop.store(true, std::memory_order_release);
    reader.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store put_batch publishes once per shard and keeps RAW") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(64);
    values.reserve(64);
    std::vector<glyphastore::Store::PutItem> items;
    items.reserve(64);
    for (int index = 0; index < 64; ++index) {
        keys.push_back("batch-key-" + std::to_string(index));
        values.push_back("batch-value-" + std::to_string(index));
        items.push_back(glyphastore::Store::PutItem{
            .key = keys.back(),
            .value = bytes(values.back()),
        });
    }

    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == items.size());
    for (const auto& status : statuses) {
        GLYPHA_REQUIRE(status.has_value());
    }
    for (std::size_t index = 0; index < items.size(); ++index) {
        const auto got = store.get(keys[index]);
        GLYPHA_REQUIRE(got.has_value());
        GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()),
                                        got->bytes.size()) == values[index]);
    }
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Store put_batch preserves same-key FIFO within one batch") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key = "same-key";
    const std::string first = "first";
    const std::string second = "second";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes(first)},
        {.key = key, .value = bytes(second)},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 2);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(statuses[1].has_value());
    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   second);
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired embedded merge pays bounded debt before exhausting a tiny post delta") {
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .paired = {.merge_delta_entries = 2, .merge_maximum_post_entries = 2, .merge_quantum_slots = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("merge-cut-a", bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put("merge-cut-b", bytes("b")).has_value());

    const std::array<glyphastore::Store::PutItem, 2> post_items{
        glyphastore::Store::PutItem{.key = "merge-post-a", .value = bytes("c")},
        glyphastore::Store::PutItem{.key = "merge-post-b", .value = bytes("d")},
    };
    const auto statuses = store.put_batch(post_items);
    GLYPHA_REQUIRE(statuses.size() == post_items.size());
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(statuses[1].has_value());

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    const auto completed_stats = runtime->stats()[0];
    GLYPHA_REQUIRE(completed_stats.read_merge_starts >= 1U);
    GLYPHA_REQUIRE(completed_stats.read_merge_completions >= 1U);
    GLYPHA_REQUIRE(completed_stats.read_merge_remaining_slots == 0U);
    GLYPHA_REQUIRE(completed_stats.read_merge_post_capacity_remaining == 0U);
    GLYPHA_REQUIRE(completed_stats.maximum_read_merge_quantum_slots > 1U);

    GLYPHA_REQUIRE(store.put("merge-after-post", bytes("e")).has_value());
    const auto final_stats = runtime->stats()[0];
    GLYPHA_REQUIRE(final_stats.read_merge_backpressure == 0U);
    GLYPHA_REQUIRE(final_stats.generation_admission_backpressure_total == 0U);
    GLYPHA_REQUIRE(store.get("merge-cut-a").has_value());
    GLYPHA_REQUIRE(store.get("merge-post-b").has_value());
    GLYPHA_REQUIRE(store.get("merge-after-post").has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired dedicated Writer merge pays bounded debt before exhausting a tiny post delta") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 64U * 1024U,
                                                       .merge_delta_entries = 2,
                                                       .merge_maximum_post_entries = 2,
                                                       .merge_quantum_slots = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("writer-merge-cut-a", bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put("writer-merge-cut-b", bytes("b")).has_value());

    const std::array<glyphastore::Store::PutItem, 2> post_items{
        glyphastore::Store::PutItem{.key = "writer-merge-post-a", .value = bytes("c")},
        glyphastore::Store::PutItem{.key = "writer-merge-post-b", .value = bytes("d")},
    };
    const auto statuses = store.put_batch(post_items);
    GLYPHA_REQUIRE(statuses.size() == post_items.size());
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(statuses[1].has_value());
    GLYPHA_REQUIRE(store.put("writer-merge-after-post", bytes("e")).has_value());

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    const auto stats = runtime->stats()[0];
    GLYPHA_REQUIRE(stats.read_merge_starts >= 1U);
    GLYPHA_REQUIRE(stats.read_merge_completions >= 1U);
    GLYPHA_REQUIRE(stats.read_merge_backpressure == 0U);
    GLYPHA_REQUIRE(!stats.read_merge_active || stats.read_merge_remaining_slots > 0U);
    GLYPHA_REQUIRE(stats.generation_admission_backpressure_total == 0U);
    GLYPHA_REQUIRE(store.get("writer-merge-cut-a").has_value());
    GLYPHA_REQUIRE(store.get("writer-merge-post-b").has_value());
    GLYPHA_REQUIRE(store.get("writer-merge-after-post").has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("paired durable Writer fail-closes when mutate throws after durable I/O begins") {
    // ADR 0036 V6 durable_sync seam: Site::publish throws after commit + read-generation
    // publish. Client keeps success ACK (RAW); pair sticky-fails. before(write_record)
    // throws stay known-not-committed — see sibling "before-hook throw" litmus.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-fc-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = store_path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const std::string key_a = "fc-a";
    const std::string key_b = "fc-b";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes("alpha")},
        {.key = key_b, .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(statuses.size() == items.size());
    // First item: committed+published before throw → success ACK (no inverted RAW).
    GLYPHA_REQUIRE(statuses[0].has_value());
    // Second: never Store-entered after sticky → known not committed.
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto late = store.put("fc-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(late.error().message.find("fail-closed") != std::string::npos);
    const auto seed_after = store.get("seed");
    GLYPHA_REQUIRE(seed_after.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(seed_after->bytes.data()),
                                    seed_after->bytes.size()) == "ok");
    GLYPHA_REQUIRE(store.get(key_a).has_value());
    GLYPHA_REQUIRE(!store.get(key_b).has_value());

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}
#endif

GLYPHA_TEST("paired durable sync Writer does not success-ACK abandoned unflushed batch siblings") {
    // Distinct keys in one mutate_durable_batch: A returns committed before flush/index,
    // B fails and clears pending_group. A must not keep a clean success ACK (RAW lie).
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-unflush-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnSecondArmedWrite final {
        std::atomic_bool armed{false};
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowOnSecondArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                const auto count = self->writes.fetch_add(1, std::memory_order_relaxed);
                if (count >= 1U) {
                    throw std::bad_alloc{};
                }
            }
            return {};
        }
    } thrower;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnSecondArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    const std::string key_a = "unflush-a";
    const std::string key_b = "unflush-b";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes("alpha")},
        {.key = key_b, .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 2);
    GLYPHA_REQUIRE(!statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(statuses[0].error().code == glyphastore::ErrorCode::unavailable ||
                   statuses[0].error().code == glyphastore::ErrorCode::resource_exhausted ||
                   statuses[0].error().code == glyphastore::ErrorCode::internal_error);
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 2U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto* generation = runtime->adopt_read_generation(0);
    GLYPHA_REQUIRE(generation != nullptr);
    const auto view_a =
        generation->prepare_durable({.key = key_a, .hash = glyphastore::hash_key_routing(key_a)});
    GLYPHA_REQUIRE(!view_a.has_value());

    const auto late = store.put("unflush-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable sync Writer keeps flushed siblings through orphan commit fail") {
    // Same Writer batch: A alone hits max_bytes → flush+index (durable_through), B appends
    // unflushed, C throws on write_record. commit_writer_batch fails (orphaned pending).
    // A must keep success ACK and appear in the published generation; B/C must not.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-orphan-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnThirdArmedWrite final {
        std::atomic_bool armed{false};
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowOnThirdArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                const auto count = self->writes.fetch_add(1, std::memory_order_relaxed);
                if (count >= 2U) {
                    throw std::bad_alloc{};
                }
            }
            return {};
        }
    } thrower;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 256, .max_wait_ms = 60'000, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnThirdArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    const std::string key_a = "orphan-a";
    const std::string key_b = "orphan-b";
    const std::string key_c = "orphan-c";
    const std::string large_a(300, 'A');
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes(large_a)},
        {.key = key_b, .value = bytes("beta")},
        {.key = key_c, .value = bytes("gamma")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 3);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(!statuses[2].has_value());
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 3U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto got_a = store.get(key_a);
    GLYPHA_REQUIRE(got_a.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(got_a->bytes.data()), got_a->bytes.size()) == large_a);
    GLYPHA_REQUIRE(!store.get(key_b).has_value());

    const auto late = store.put("orphan-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST(
    "paired durable sync Writer snapshot-publishes committed sibling after later known-not-committed") {
    // Sync durable_group same-key split: first sub-batch commits+indexes A; second hits
    // before(write_record) throw (known not committed). A keeps success ACK and stays
    // GET-visible. Pair stays healthy — sticky is for post-commit / Store-entered throws.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-sib-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnSecondArmedWrite final {
        std::atomic_bool armed{false};
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowOnSecondArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                const auto count = self->writes.fetch_add(1, std::memory_order_relaxed);
                if (count >= 1U) {
                    throw std::bad_alloc{};
                }
            }
            return {};
        }
    } thrower;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnSecondArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    // Same key forces duplicate-key sub-batch split so the first mutate_durable_batch
    // commit_writer_batch indexes A before the second sub-batch fails.
    const std::string key = "sib-key";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes("alpha")},
        {.key = key, .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 2);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 2U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(runtime->healthy());

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "alpha");

    thrower.armed.store(false, std::memory_order_release);
    const auto late = store.put("sib-late", bytes("yes"));
    GLYPHA_REQUIRE(late.has_value());

    static_cast<void>(store.close());
    std::error_code ignored2;
    std::filesystem::remove_all(root, ignored2);
}

GLYPHA_TEST("paired durable sync Writer does not success-ACK unprocessed batch items after drain") {
    // Same-key split: first sub-batch commits; second hits a pre-write before-hook throw;
    // third never starts. Pre-write failures are known not committed (resource_exhausted),
    // not unavailable / false indeterminate.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-unproc-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnSecondArmedWrite final {
        std::atomic_bool armed{false};
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowOnSecondArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                const auto count = self->writes.fetch_add(1, std::memory_order_relaxed);
                if (count >= 1U) {
                    throw std::bad_alloc{};
                }
            }
            return {};
        }
    } thrower;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnSecondArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    const std::string key = "unproc-key";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes("alpha")},
        {.key = key, .value = bytes("beta")},
        {.key = key, .value = bytes("gamma")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 3);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    // before(write_record) throw never crossed the durable write boundary.
    GLYPHA_REQUIRE(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(!statuses[2].has_value());
    GLYPHA_REQUIRE(statuses[2].error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 2U);

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "alpha");

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("paired durable sync single-op keeps known-not-committed after record write poison") {
    // append_record: pwrite failure poisons the data directory and returns
    // SegmentCommitOutcome::not_committed. Catalog goes fail-closed via
    // !directory_.healthy(); Writer rewrites to resource_exhausted. When the
    // fail-closed epilogue drain also fails (Site::drain_snapshot), the old
    // epilogue overwrote that resolved error to unavailable (wire INTERNAL_ERROR).
    // Keep known-not-committed polarity — matching catch / durable_group sync.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-paired-write-poison-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct FailArmedRecordWrite final {
        std::atomic_bool armed{false};

        static auto write_some_at(void* context, const int descriptor, const std::span<const std::byte> bytes,
                                  const std::uint64_t offset) -> std::ptrdiff_t {
            auto* self = static_cast<FailArmedRecordWrite*>(context);
            if (self->armed.load(std::memory_order_acquire)) {
                errno = EIO;
                return -1;
            }
            return static_cast<std::ptrdiff_t>(
                ::pwrite(descriptor, bytes.data(), bytes.size(), static_cast<off_t>(offset)));
        }
    } io;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {
             .file_io = {.context = &io, .write_some_at = &FailArmedRecordWrite::write_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    io.armed.store(true, std::memory_order_release);
    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::drain_snapshot);

    const auto put = store.put("poisoned", bytes("no"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(!store.get("poisoned").has_value());

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto late = store.put("late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}
#endif

GLYPHA_TEST("durable before-hook throw before write_record is known not committed") {
    // A throwing filesystem before(write_record) must not become indeterminate /
    // INTERNAL_ERROR — the hook runs ahead of write_all_at.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-paired-before-throw-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnWriteRecord final {
        static auto before(void* /*context*/, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            if (operation == glyphastore::FilesystemOperation::write_record) {
                throw std::runtime_error{"injected before-hook failure"};
            }
            return {};
        }
    };

    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = store_path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
                                            .filesystem_hooks = {.before = &ThrowOnWriteRecord::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    const auto put = store.put("before-throw", bytes("no"));
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(!store.get("before-throw").has_value());

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("paired durable_group pre-mutate batch alloc stays known not committed") {
    // Writer sets durable_mutate_entered before mutate_durable_batch. A throw from
    // that call before any durable mutate must not escape as Writer catch sticky /
    // unavailable (wire INTERNAL_ERROR). StoreAccess converts it to not_committed
    // resource_exhausted and keeps the pair healthy.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-batch-pre-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::durable_batch_pre);
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = "pre-a", .value = bytes("alpha")},
        {.key = "pre-b", .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(statuses.size() == 2);
    for (const auto& status : statuses) {
        GLYPHA_REQUIRE(!status.has_value());
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(status.error().code != glyphastore::ErrorCode::unavailable);
    }
    GLYPHA_REQUIRE(!store.get("pre-a").has_value());
    GLYPHA_REQUIRE(!store.get("pre-b").has_value());
    GLYPHA_REQUIRE(runtime->healthy());

    const auto retry = store.put("pre-a", bytes("ok"));
    GLYPHA_REQUIRE(retry.has_value());
    const auto got = store.get("pre-a");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "ok");

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable_group post-mutate catch keeps Store-entered siblings unavailable") {
    // mutate_durable_batch commits the sub-batch then Site::post_mutate throws before
    // classification finishes. Those Store-entered items must not become
    // resource_exhausted (wire OVERLOADED) while drain still makes them GET-visible.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-paired-post-mutate-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::post_mutate);
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = "pm-a", .value = bytes("alpha")},
        {.key = "pm-b", .value = bytes("beta")},
        {.key = "pm-c", .value = bytes("gamma")},
    };
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(statuses.size() == 3);
    for (const auto& status : statuses) {
        GLYPHA_REQUIRE(!status.has_value());
        // Store-entered (mutate returned) → unavailable, never resource_exhausted/OVERLOADED.
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::unavailable);
        GLYPHA_REQUIRE(status.error().code != glyphastore::ErrorCode::resource_exhausted);
    }

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    // Drain publishes Index authority; siblings must be GET-visible with non-OVERLOADED polarity.
    for (const auto* key : {"pm-a", "pm-b", "pm-c"}) {
        GLYPHA_REQUIRE(store.get(key).has_value());
    }

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}
#endif

GLYPHA_TEST("paired durable sync catch drain does not success-ACK put-hit on pre-existing key") {
    // Attempted put that fails before write must not success-ACK solely because the key
    // was already Index-visible from a prior put (false ACK with unchanged value).
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-puthit-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowOnFirstArmedWrite final {
        std::atomic_bool armed{false};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowOnFirstArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                throw std::bad_alloc{};
            }
            return {};
        }
    } thrower;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnFirstArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    const std::string key = "put-hit-key";
    GLYPHA_REQUIRE(store.put(key, bytes("old")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    // put_batch takes the sync durable batch Writer path (ack_attempted lived there).
    const std::vector<glyphastore::Store::PutItem> items{{.key = key, .value = bytes("new")}};
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 1);
    GLYPHA_REQUIRE(!statuses[0].has_value());
    // before(write_record) throw is known not committed — not sticky unavailable.
    GLYPHA_REQUIRE(statuses[0].error().code == glyphastore::ErrorCode::resource_exhausted);

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "old");

    static_cast<void>(store.close());
    std::error_code ignored2;
    std::filesystem::remove_all(root, ignored2);
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("paired volatile pre-append rotation failure is known not committed") {
    // Pre-append invalid_reference (rotation/catalog) must rewrite to
    // resource_exhausted → wire OVERLOADED — not INTERNAL_ERROR / reconcile.
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::rotate);
    const auto put = store.put("pre-append", bytes("no"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::invalid_reference);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(!store.get("pre-append").has_value());
    GLYPHA_REQUIRE(runtime->healthy());

    const auto retry = store.put("pre-append", bytes("yes"));
    GLYPHA_REQUIRE(retry.has_value());
    const auto got = store.get("pre-append");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "yes");
    static_cast<void>(store.close());
}

GLYPHA_TEST("paired durable pre-write rotation failure is known not committed") {
    // rotate_active runs only after this PUT's append returned segment_full
    // (not_committed). A throw before seal/create/publish must stay not_committed →
    // resource_exhausted — not indeterminate sticky INTERNAL_ERROR.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-rot-pre-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct SegmentFullOnce final {
        std::size_t write_records{};
        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<SegmentFullOnce*>(opaque);
            if (operation != glyphastore::FilesystemOperation::write_record) {
                return {};
            }
            ++state.write_records;
            if (state.write_records == 1) {
                return glyphastore::fail(glyphastore::ErrorCode::segment_full, "injected segment full");
            }
            return {};
        }
    } failure;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &failure, .before = &SegmentFullOnce::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::rotate);
    const auto put = store.put("rot-pre", bytes("no"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(failure.write_records >= 1);
    GLYPHA_REQUIRE(!store.get("rot-pre").has_value());
    GLYPHA_REQUIRE(runtime->healthy());

    const auto retry = store.put("rot-pre", bytes("yes"));
    GLYPHA_REQUIRE(retry.has_value());
    const auto got = store.get("rot-pre");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "yes");

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable post-seal rotation reader-open is sticky indeterminate") {
    // After rotate_active commits a seal, sealed-reader open failure must stay
    // indeterminate / unavailable (fail-closed) — not known-not-committed OVERLOADED.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-rot-seal-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct SegmentFullOnSecond final {
        std::size_t write_records{};
        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<SegmentFullOnSecond*>(opaque);
            if (operation != glyphastore::FilesystemOperation::write_record) {
                return {};
            }
            ++state.write_records;
            if (state.write_records == 2) {
                return glyphastore::fail(glyphastore::ErrorCode::segment_full, "injected segment full");
            }
            return {};
        }
    } failure;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &failure, .before = &SegmentFullOnSecond::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    const auto warm = store.put("rot-seal-warm", bytes("warm"));
    GLYPHA_REQUIRE(warm.has_value());

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::segment_open);
    const auto put = store.put("rot-seal", bytes("no"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(failure.write_records >= 2);
    GLYPHA_REQUIRE(!store.get("rot-seal").has_value());
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto late = store.put("rot-seal-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable post-seal rotation create reject is sticky indeterminate") {
    // After rotate_active commits a seal, a pre-rename create reject (not_published)
    // must stay indeterminate / unavailable — not known-not-committed OVERLOADED.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-rot-create-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct SegmentFullThenCreateReject final {
        std::size_t write_records{};
        std::size_t preallocates{};
        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<SegmentFullThenCreateReject*>(opaque);
            if (operation == glyphastore::FilesystemOperation::write_record) {
                ++state.write_records;
                if (state.write_records == 2) {
                    return glyphastore::fail(glyphastore::ErrorCode::segment_full, "injected segment full");
                }
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::preallocate_segment) {
                ++state.preallocates;
                // Only reject replacement create during rotation (after segment_full),
                // not bootstrap Segment creation.
                if (state.write_records >= 2) {
                    return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected create reject");
                }
                return {};
            }
            return {};
        }
    } failure;

    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .concurrency = glyphastore::StoreConcurrencyMode::paired,
         .paired = {.async_lane_capacity = 8,
                    .async_lane_payload_bytes = 1U * 1024U * 1024U,
                    .reader_epoch_lease = true},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &failure, .before = &SegmentFullThenCreateReject::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    const auto warm = store.put("rot-create-warm", bytes("warm"));
    GLYPHA_REQUIRE(warm.has_value());

    const auto put = store.put("rot-create", bytes("no"));
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(failure.write_records >= 2);
    GLYPHA_REQUIRE(failure.preallocates >= 1);
    GLYPHA_REQUIRE(!store.get("rot-create").has_value());
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto late = store.put("rot-create-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable pre-append segment open failure is known not committed") {
    // DurableSegmentFile::open before any Record write must stay not_committed →
    // resource_exhausted / wire OVERLOADED — not indeterminate sticky INTERNAL_ERROR.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-seg-open-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .storage_mode = glyphastore::StorageMode::durable_sync,
                                            .data_directory = store_path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::segment_open);
    const auto put = store.put("seg-open", bytes("no"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::descriptor_exhausted ||
                   put.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(put.error().code != glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(!store.get("seg-open").has_value());
    GLYPHA_REQUIRE(runtime->healthy());

    const auto retry = store.put("seg-open", bytes("yes"));
    GLYPHA_REQUIRE(retry.has_value());
    const auto got = store.get("seg-open");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "yes");

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired volatile post-append index failure stays indeterminate") {
    // After append, Index publication failure must stay unavailable (sticky) —
    // rewrite_known_not_committed must not demote it to OVERLOADED.
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    const auto put = store.put("post-append", bytes("orphan"));
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(!store.get("post-append").has_value());
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto late = store.put("late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);
    static_cast<void>(store.close());
}

GLYPHA_TEST("paired volatile exclusive compact gates Index publish") {
    // Exclusive Writer put/erase_locked_published elides mutex_; compact must arm
    // Index quiesce + drain hot_path_depth before Index touch. Sibling put under
    // the gate sees sequence_conflict (rewritten to resource_exhausted on the
    // paired wire) — never a torn Index vs unlocked publish.
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .concurrency = glyphastore::StoreConcurrencyMode::paired,
                                            .paired = {.async_lane_capacity = 8,
                                                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                                                       .reader_epoch_lease = true},
                                            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());

    glyphastore::fault::reset();
    glyphastore::fault::arm_block(glyphastore::fault::Site::compact);
    glyphastore::Result<glyphastore::CompactionResult> compacted{
        glyphastore::fail(glyphastore::ErrorCode::internal_error, "unset")};
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(glyphastore::fault::wait_until_blocked(glyphastore::fault::Site::compact));

    bool saw_gate{};
    const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (std::uint32_t attempt = 0; std::chrono::steady_clock::now() < gate_deadline; ++attempt) {
        const auto key = std::string{"gated-"} + std::to_string(attempt);
        const auto put = store.put(key, bytes("x"));
        if (!put.has_value() && (put.error().code == glyphastore::ErrorCode::sequence_conflict ||
                                 put.error().code == glyphastore::ErrorCode::resource_exhausted)) {
            saw_gate = true;
            break;
        }
        GLYPHA_REQUIRE(put.has_value());
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(saw_gate);

    glyphastore::fault::release_block(glyphastore::fault::Site::compact);
    compactor.join();
    glyphastore::fault::reset();

    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(store.put("after", bytes("y")).has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    static_cast<void>(store.close());
}
#endif
