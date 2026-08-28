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
