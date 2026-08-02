#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <span>
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
        GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                       values[index]);
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

GLYPHA_TEST("paired durable Writer fail-closes when mutate throws after durable I/O begins") {
    // ADR 0036 V6 production seam: exception after entering durable mutate must sticky-fail
    // the pair (no healthy runtime with unpublished committed bytes; no inverted RAW).
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-paired-fc-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    GLYPHA_REQUIRE(::mkdtemp(writable.data()) != nullptr);
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct ThrowAfterArmedWrite final {
        std::atomic_bool armed{false};
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<ThrowAfterArmedWrite*>(context);
            if (!self->armed.load(std::memory_order_acquire)) {
                return {};
            }
            if (operation == glyphastore::FilesystemOperation::write_record) {
                self->writes.fetch_add(1, std::memory_order_relaxed);
                // Throw on the first armed durable record write: mutate has begun, so the
                // Writer must sticky-fail-closed even if commit outcome is indeterminate.
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
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = store_path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &thrower, .before = &ThrowAfterArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    const std::string key_a = "fc-a";
    const std::string key_b = "fc-b";
    const std::string value_a = "alpha";
    const std::string value_b = "beta";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes(value_a)},
        {.key = key_b, .value = bytes(value_b)},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == items.size());
    bool saw_failure = false;
    for (const auto& status : statuses) {
        GLYPHA_REQUIRE(!status.has_value());
        saw_failure = true;
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::unavailable ||
                       status.error().code == glyphastore::ErrorCode::resource_exhausted ||
                       status.error().code == glyphastore::ErrorCode::internal_error);
    }
    GLYPHA_REQUIRE(saw_failure);
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 1U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    // Sticky fail-closed: sync mutate must refuse before enqueue (healthy_), not only
    // after the Writer hits an already-unhealthy durable catalog. Prior published
    // seed remains RAW via immutable generation GET.
    const auto late = store.put("fc-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(late.error().message.find("fail-closed") != std::string::npos);
    const auto seed_after = store.get("seed");
    GLYPHA_REQUIRE(seed_after.has_value());
    GLYPHA_REQUIRE(
        std::string_view(reinterpret_cast<const char*>(seed_after->bytes.data()), seed_after->bytes.size()) ==
        "ok");

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable sync Writer does not success-ACK abandoned unflushed batch siblings") {
    // Distinct keys in one mutate_durable_batch: A returns committed before flush/index,
    // B fails and clears pending_group. A must not keep a clean success ACK (RAW lie).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-paired-unflush-XXXXXX").string();
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
         .durable_group = {.max_records = 32,
                           .max_bytes = 65'536,
                           .max_wait_ms = 10,
                           .min_records = 1},
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
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-paired-orphan-XXXXXX").string();
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
         .durable_group = {.max_records = 32,
                           .max_bytes = 256,
                           .max_wait_ms = 60'000,
                           .min_records = 1},
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

GLYPHA_TEST("paired durable sync Writer snapshot-publishes siblings before sticky fail-closed") {
    // Sync durable_group same-key split: first sub-batch commits+indexes A, second
    // hits write-boundary failure. A must keep success ACK and appear in the published
    // generation via drain snapshot (even when durable already self-closed).
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
         .durable_group = {.max_records = 32,
                           .max_bytes = 65'536,
                           .max_wait_ms = 10,
                           .min_records = 1},
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
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 2U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "alpha");

    const auto late = store.put("sib-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(late.error().message.find("fail-closed") != std::string::npos);

    static_cast<void>(store.close());
    std::error_code ignored2;
    std::filesystem::remove_all(root, ignored2);
}

GLYPHA_TEST("paired durable sync Writer does not success-ACK unprocessed batch items after drain") {
    // Same-key split commits+indexes the first sub-batch; the second sub-batch throws
    // on write before a trailing distinct key is mutated. That trailing key must not
    // success-ACK (default Status{} used to be preserved after drain).
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
         .durable_group = {.max_records = 32,
                           .max_bytes = 65'536,
                           .max_wait_ms = 10,
                           .min_records = 1},
         .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
         .filesystem_hooks = {.context = &thrower, .before = &ThrowOnSecondArmedWrite::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("seed", bytes("ok")).has_value());
    thrower.armed.store(true, std::memory_order_release);

    const std::string key = "unproc-key";
    const std::string key_c = "unproc-c";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes("alpha")},
        {.key = key, .value = bytes("beta")},
        {.key = key_c, .value = bytes("gamma")},
    };
    const auto statuses = store.put_batch(items);
    GLYPHA_REQUIRE(statuses.size() == 3);
    GLYPHA_REQUIRE(statuses[0].has_value());
    GLYPHA_REQUIRE(!statuses[1].has_value());
    GLYPHA_REQUIRE(!statuses[2].has_value());
    GLYPHA_REQUIRE(thrower.writes.load(std::memory_order_relaxed) >= 2U);

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "alpha");
    GLYPHA_REQUIRE(!store.get(key_c).has_value());

    const auto late = store.put("unproc-late", bytes("no"));
    GLYPHA_REQUIRE(!late.has_value());
    GLYPHA_REQUIRE(late.error().code == glyphastore::ErrorCode::unavailable);

    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

GLYPHA_TEST("paired durable sync catch drain does not success-ACK put-hit on pre-existing key") {
    // Catch-path used to upgrade attempted puts solely because the key was already
    // Index-visible from a prior successful put — false success ACK with unchanged value.
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
         .durable_group = {.max_records = 32,
                           .max_bytes = 65'536,
                           .max_wait_ms = 10,
                           .min_records = 1},
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
    GLYPHA_REQUIRE(statuses[0].error().code == glyphastore::ErrorCode::unavailable ||
                   statuses[0].error().code == glyphastore::ErrorCode::resource_exhausted);

    const auto got = store.get(key);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                   "old");

    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    GLYPHA_REQUIRE(runtime != nullptr);
    GLYPHA_REQUIRE(!runtime->healthy());

    static_cast<void>(store.close());
    std::error_code ignored2;
    std::filesystem::remove_all(root, ignored2);
}
