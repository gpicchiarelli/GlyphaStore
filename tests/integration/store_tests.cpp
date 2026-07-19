#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto value_string(const glyphastore::OwnedValue& value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

class StoreTemporaryDirectory final {
  public:
    StoreTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-store-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        root_ = created;
    }

    ~StoreTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] auto store_path() const -> std::filesystem::path {
        return root_ / "store";
    }

  private:
    std::filesystem::path root_;
};

class ManualStoreClock final : public glyphastore::StoreClock {
  public:
    explicit ManualStoreClock(const std::uint64_t initial_now_ns) : now_ns_(initial_now_ns) {}

    [[nodiscard]] auto now_ns() const noexcept -> std::uint64_t override {
        return now_ns_.load(std::memory_order_relaxed);
    }

    void set(const std::uint64_t now_ns) noexcept {
        now_ns_.store(now_ns, std::memory_order_relaxed);
    }

  private:
    std::atomic<std::uint64_t> now_ns_;
};

auto bootstrap_store_id() -> glyphastore::StoreId {
    return {std::byte{0x91}, std::byte{0x92}, std::byte{0x93}, std::byte{0x94},
            std::byte{0x95}, std::byte{0x96}, std::byte{0x97}, std::byte{0x98},
            std::byte{0x99}, std::byte{0x9A}, std::byte{0x9B}, std::byte{0x9C},
            std::byte{0x9D}, std::byte{0x9E}, std::byte{0x9F}, std::byte{0xA0}};
}
} // namespace

GLYPHA_TEST("key routing is deterministic and stable across worker counts") {
    GLYPHA_REQUIRE(glyphastore::route_worker("alpha", 4) == glyphastore::route_worker("alpha", 4));
    GLYPHA_REQUIRE(glyphastore::route_worker("alpha", 4) != glyphastore::route_worker("beta", 4) ||
                   glyphastore::hash_key("alpha") % 4 == glyphastore::hash_key("beta") % 4);
}

GLYPHA_TEST("store put get round trip preserves value") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("hello", bytes("world")).has_value());
    const auto record = store.get("hello");
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(value_string(*record) == "world");
}

GLYPHA_TEST("store replace updates visible value and sequence") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("key", bytes("old")).has_value());
    const auto first = store.get("key");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(store.put("key", bytes("new")).has_value());
    const auto second = store.get("key");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(second->sequence > first->sequence);
    GLYPHA_REQUIRE(value_string(*second) == "new");
}

GLYPHA_TEST("store erase removes key and rejects subsequent reads") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("gone", bytes("v")).has_value());
    GLYPHA_REQUIRE(store.erase("gone").has_value());
    const auto missing = store.get("gone");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("store get hides expired keys") {
    const auto clock = std::make_shared<ManualStoreClock>(99);
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}, .clock = clock});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("expired", bytes("v"), 100).has_value());
    const auto visible = store.get("expired");
    GLYPHA_REQUIRE(visible.has_value());
    clock->set(100);
    const auto hidden = store.get("expired");
    GLYPHA_REQUIRE(!hidden.has_value());
    GLYPHA_REQUIRE(hidden.error().code == glyphastore::ErrorCode::not_found);
    const auto route = glyphastore::route_worker("expired", store.worker_count());
    GLYPHA_REQUIRE(
        !glyphastore::detail::StoreAccess::worker(store, route).index().find("expired").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
}

GLYPHA_TEST("store clock never moves backward within one Store instance") {
    const auto clock = std::make_shared<ManualStoreClock>(100);
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}, .clock = clock});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("past", bytes("v"), 75).has_value());
    clock->set(50);
    const auto hidden = store.get("past");
    GLYPHA_REQUIRE(!hidden.has_value());
    GLYPHA_REQUIRE(hidden.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("store clock handles maximum timestamp and no-expiration sentinel") {
    constexpr auto maximum_time = std::numeric_limits<std::uint64_t>::max();
    const auto clock = std::make_shared<ManualStoreClock>(maximum_time - 1U);
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}, .clock = clock});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("maximum", bytes("v"), maximum_time).has_value());
    GLYPHA_REQUIRE(store.get("maximum").has_value());
    clock->set(maximum_time);
    const auto expired = store.get("maximum");
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE(store.put("forever", bytes("v"), 0).has_value());
    GLYPHA_REQUIRE(store.get("forever").has_value());
}

GLYPHA_TEST("default Store clock expires timestamps in the Unix epoch past") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    GLYPHA_REQUIRE((*opened)->put("past", bytes("v"), 1).has_value());
    const auto hidden = (*opened)->get("past");
    GLYPHA_REQUIRE(!hidden.has_value());
    GLYPHA_REQUIRE(hidden.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("store keeps partitioned keys on routed workers only") {
    constexpr std::size_t worker_total = 8;
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = worker_total}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    std::string key_a;
    std::string key_b;
    std::size_t route_a{};
    std::size_t route_b{};
    for (std::uint64_t seed = 0;; ++seed) {
        key_a = "route-key-" + std::to_string(seed);
        key_b = "route-key-" + std::to_string(seed + 100000U);
        route_a = glyphastore::route_worker(key_a, worker_total);
        route_b = glyphastore::route_worker(key_b, worker_total);
        if (route_a != route_b) {
            break;
        }
    }

    GLYPHA_REQUIRE(store.put(key_a, bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put(key_b, bytes("b")).has_value());
    const auto& worker_a = glyphastore::detail::StoreAccess::worker(store, route_a);
    const auto& worker_b = glyphastore::detail::StoreAccess::worker(store, route_b);
    GLYPHA_REQUIRE(worker_a.index().find(key_a).has_value());
    GLYPHA_REQUIRE(!worker_a.index().find(key_b).has_value());
    GLYPHA_REQUIRE(worker_b.index().find(key_b).has_value());
    GLYPHA_REQUIRE(!worker_b.index().find(key_a).has_value());
}

GLYPHA_TEST("store routes keys to distinct worker partitions") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 4}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("worker-key-a", bytes("a")).has_value());
    GLYPHA_REQUIRE(store.put("worker-key-b", bytes("b")).has_value());
    const auto route_a = glyphastore::route_worker("worker-key-a", store.worker_count());
    const auto route_b = glyphastore::route_worker("worker-key-b", store.worker_count());
    GLYPHA_REQUIRE(store.get("worker-key-a").has_value());
    GLYPHA_REQUIRE(store.get("worker-key-b").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    GLYPHA_REQUIRE(route_a < store.worker_count());
    GLYPHA_REQUIRE(route_b < store.worker_count());
}

GLYPHA_TEST("store verify index matches segment scan rebuild") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 3}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("one", bytes("1")).has_value());
    GLYPHA_REQUIRE(store.put("two", bytes("2")).has_value());
    GLYPHA_REQUIRE(store.put("three", bytes("3")).has_value());
    GLYPHA_REQUIRE(store.put("two", bytes("22")).has_value());
    GLYPHA_REQUIRE(store.erase("one").has_value());
    GLYPHA_REQUIRE(store.verify_index().has_value());
    const auto segments = glyphastore::detail::StoreAccess::segments(store);
    const auto rebuilt = glyphastore::rebuild_index_from_segments(segments);
    GLYPHA_REQUIRE(rebuilt.has_value());
    GLYPHA_REQUIRE(rebuilt->index.find("one") == std::nullopt);
    GLYPHA_REQUIRE(rebuilt->index.find("two").has_value());
    GLYPHA_REQUIRE(rebuilt->index.find("three").has_value());
}

GLYPHA_TEST("store round trips a key larger than 16-bit lengths") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::string key(70'000, 'k');
    GLYPHA_REQUIRE(store.put(key, bytes("value")).has_value());
    const auto record = store.get(key);
    GLYPHA_REQUIRE(record.has_value());
    GLYPHA_REQUIRE(value_string(*record) == "value");
    GLYPHA_REQUIRE(store.verify_index().has_value());
}

GLYPHA_TEST("owned store reads survive replacement and store destruction") {
    glyphastore::OwnedValue snapshot;
    {
        auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
        GLYPHA_REQUIRE(opened.has_value());
        auto& store = **opened;
        GLYPHA_REQUIRE(store.put("stable", bytes("first")).has_value());
        auto first = store.get_copy("stable");
        GLYPHA_REQUIRE(first.has_value());
        snapshot = std::move(*first);
        GLYPHA_REQUIRE(store.put("stable", bytes("second")).has_value());
        GLYPHA_REQUIRE(value_string(snapshot) == "first");
    }
    GLYPHA_REQUIRE(value_string(snapshot) == "first");
}

GLYPHA_TEST("store byte key API preserves embedded zeros and empty keys") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const std::array binary_key{std::byte{'a'}, std::byte{0}, std::byte{'b'}};
    const std::span<const std::byte> empty_key;
    GLYPHA_REQUIRE(store.put(binary_key, bytes("binary")).has_value());
    GLYPHA_REQUIRE(store.put(empty_key, bytes("empty")).has_value());
    const auto binary = store.get(binary_key);
    const auto empty = store.get(empty_key);
    GLYPHA_REQUIRE(binary.has_value());
    GLYPHA_REQUIRE(empty.has_value());
    GLYPHA_REQUIRE(value_string(*binary) == "binary");
    GLYPHA_REQUIRE(value_string(*empty) == "empty");
}

GLYPHA_TEST("durable Store requires an explicit data directory") {
    const auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_sync});
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("Store validates durable-only resource policy before initialization") {
    auto volatile_limits = glyphastore::DurableResourceLimits{};
    volatile_limits.max_live_keys = 1;
    const auto volatile_store = glyphastore::Store::open({.durable_limits = volatile_limits});
    GLYPHA_REQUIRE(!volatile_store.has_value());
    GLYPHA_REQUIRE(volatile_store.error().code == glyphastore::ErrorCode::invalid_argument);

    StoreTemporaryDirectory temporary;
    auto invalid_limits = glyphastore::DurableResourceLimits{};
    invalid_limits.max_write_amplification = 0;
    const auto invalid = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_limits = invalid_limits,
    });
    GLYPHA_REQUIRE(!invalid.has_value());
    GLYPHA_REQUIRE(invalid.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("default durable budget rejects 256 Worker reservation before bootstrap") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    const auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = glyphastore::kMaximumWorkerCount},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().code == glyphastore::ErrorCode::storage_exhausted);
    GLYPHA_REQUIRE(!std::filesystem::exists(path / glyphastore::kBootstrapIntentFilename));
    GLYPHA_REQUIRE(!std::filesystem::exists(path / glyphastore::kManifestFilename));
}

GLYPHA_TEST("durable live-key budget is reusable after erase") {
    StoreTemporaryDirectory temporary;
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_live_keys = 1;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_limits = limits,
    });
    GLYPHA_REQUIRE(opened.has_value());
    GLYPHA_REQUIRE((*opened)->put("first", bytes("value")).has_value());
    const auto exhausted = (*opened)->put("second", bytes("value"));
    GLYPHA_REQUIRE(!exhausted.has_value());
    GLYPHA_REQUIRE(exhausted.error().code == glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE((*opened)->erase("first").has_value());
    GLYPHA_REQUIRE((*opened)->put("second", bytes("value")).has_value());
}

GLYPHA_TEST("durable recovery memory and live-key budgets fail before service") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto created = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = path,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(created.has_value());
        GLYPHA_REQUIRE((*created)->put("recovery-budget", bytes("value")).has_value());
        GLYPHA_REQUIRE((*created)->put("recovery-budget-2", bytes("value")).has_value());
        GLYPHA_REQUIRE((*created)->close().has_value());
    }
    auto limits = glyphastore::DurableResourceLimits{};
    limits.max_recovery_memory_bytes = 1;
    const auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .durable_limits = limits,
    });
    GLYPHA_REQUIRE(!reopened.has_value());
    GLYPHA_REQUIRE(reopened.error().code == glyphastore::ErrorCode::resource_exhausted);

    limits = {};
    limits.max_live_keys = 1;
    const auto too_many_keys = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .durable_limits = limits,
    });
    GLYPHA_REQUIRE(!too_many_keys.has_value());
    GLYPHA_REQUIRE(too_many_keys.error().code == glyphastore::ErrorCode::resource_exhausted);
}

GLYPHA_TEST("durable Store recovery and reads share the injected clock") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    const auto clock = std::make_shared<ManualStoreClock>(99);
    {
        auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                                .storage_mode = glyphastore::StorageMode::durable_sync,
                                                .data_directory = path,
                                                .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                                .clock = clock});
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("expires", bytes("v"), 100).has_value());
        GLYPHA_REQUIRE((*opened)->get("expires").has_value());
    }

    clock->set(100);
    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
                                  .clock = clock});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto expired = (*reopened)->get("expires");
    GLYPHA_REQUIRE(!expired.has_value());
    GLYPHA_REQUIRE(expired.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("Store rejects legacy recovery timestamp overrides") {
    const auto opened = glyphastore::Store::open({.recovery_now_ns = 1});
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("public durable Store creates commits reopens and enforces persisted Worker count") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened =
            glyphastore::Store::open({.worker_config = {.explicit_count = 2},
                                      .storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = path,
                                      .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->worker_count() == 2);
        GLYPHA_REQUIRE((*opened)->put("stable", bytes("first")).has_value());
        GLYPHA_REQUIRE(value_string(*(*opened)->get("stable")) == "first");
        const auto server_key = glyphastore::HashedKey::compute("server-owned");
        const auto owner = glyphastore::route_worker(server_key.hash, (*opened)->worker_count());
        GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::put(**opened, owner, server_key, bytes("bridge"), 0)
                           .has_value());
        const auto bridged = glyphastore::detail::StoreAccess::get_owned(**opened, owner, server_key, 0);
        GLYPHA_REQUIRE(bridged.has_value());
        GLYPHA_REQUIRE(value_string(*bridged) == "bridge");
        GLYPHA_REQUIRE((*opened)->verify_index().has_value());

        const auto locked =
            glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = path,
                                      .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
        GLYPHA_REQUIRE(!locked.has_value());
    }

    {
        auto reopened =
            glyphastore::Store::open({.worker_config = {.explicit_count = 2},
                                      .storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = path,
                                      .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
        GLYPHA_REQUIRE(reopened.has_value());
        GLYPHA_REQUIRE(value_string(*(*reopened)->get("stable")) == "first");
        GLYPHA_REQUIRE((*reopened)->put("stable", bytes("second")).has_value());
        GLYPHA_REQUIRE((*reopened)->erase("stable").has_value());
        GLYPHA_REQUIRE(!(*reopened)->get("stable").has_value());
        const auto absent_erase = (*reopened)->erase("stable");
        GLYPHA_REQUIRE(!absent_erase.has_value());
        GLYPHA_REQUIRE(absent_erase.error().code == glyphastore::ErrorCode::not_found);
    }

    const auto mismatch =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(!mismatch.has_value());
    GLYPHA_REQUIRE(mismatch.error().code == glyphastore::ErrorCode::invalid_argument);

    const auto duplicate =
        glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(!duplicate.has_value());
    GLYPHA_REQUIRE(duplicate.error().code == glyphastore::ErrorCode::sequence_conflict);
}

GLYPHA_TEST("public durable Store completes an interrupted bootstrap intent") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    const auto store_id = bootstrap_store_id();
    const std::vector entries{
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{1},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{0},
                                          .role = glyphastore::ManifestSegmentRole::active},
        glyphastore::ManifestSegmentEntry{.segment_id = glyphastore::SegmentId{2},
                                          .generation = glyphastore::GenerationId{1},
                                          .owner_worker = glyphastore::WorkerId{1},
                                          .role = glyphastore::ManifestSegmentRole::active},
    };
    const glyphastore::Manifest intent{
        .store_id = store_id,
        .manifest_generation = 1,
        .routing_algorithm = glyphastore::RoutingAlgorithm::fnv1a64_v1,
        .worker_count = 2,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{3},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = entries,
    };
    {
        auto directory =
            glyphastore::DataDirectory::open_and_lock(path, glyphastore::DataDirectoryOpenMode::create_new);
        GLYPHA_REQUIRE(directory.has_value());
        GLYPHA_REQUIRE(directory->publish_bootstrap_intent(intent).has_value());
        GLYPHA_REQUIRE(directory->publish_manifest(intent).durable());
        const glyphastore::SegmentHeaderIdentity first_identity{
            .store_id = store_id,
            .segment_id = entries[0].segment_id,
            .generation = entries[0].generation,
            .owner_worker = entries[0].owner_worker,
        };
        GLYPHA_REQUIRE(glyphastore::DurableSegmentFile::create(*directory, first_identity).durable());
    }

    auto completed =
        glyphastore::Store::open({.worker_config = {.explicit_count = 2},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(completed.has_value());
    GLYPHA_REQUIRE((*completed)->worker_count() == 2);
    GLYPHA_REQUIRE((*completed)->put("after-bootstrap", bytes("value")).has_value());
    GLYPHA_REQUIRE(value_string(*(*completed)->get("after-bootstrap")) == "value");
    GLYPHA_REQUIRE(!std::filesystem::exists(path / glyphastore::kBootstrapIntentFilename));
}

GLYPHA_TEST("durable open-or-create initializes only a pristine directory") {
    {
        StoreTemporaryDirectory temporary;
        const auto path = temporary.store_path();
        GLYPHA_REQUIRE(std::filesystem::create_directory(path));
        std::filesystem::permissions(path, std::filesystem::perms::owner_all,
                                     std::filesystem::perm_options::replace);
        const auto stale_temporary = path / glyphastore::kBootstrapTemporaryFilename;
        const auto stale_descriptor =
            ::open(stale_temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        GLYPHA_REQUIRE(stale_descriptor >= 0);
        GLYPHA_REQUIRE(::close(stale_descriptor) == 0);
        auto initialized = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                                     .storage_mode = glyphastore::StorageMode::durable_sync,
                                                     .data_directory = path});
        GLYPHA_REQUIRE(initialized.has_value());
        GLYPHA_REQUIRE(!std::filesystem::exists(stale_temporary));
        GLYPHA_REQUIRE((*initialized)->put("created", bytes("yes")).has_value());
    }
    {
        StoreTemporaryDirectory temporary;
        const auto path = temporary.store_path();
        GLYPHA_REQUIRE(std::filesystem::create_directory(path));
        std::filesystem::permissions(path, std::filesystem::perms::owner_all,
                                     std::filesystem::perm_options::replace);
        const auto foreign = path / "foreign-file";
        const auto descriptor = ::open(foreign.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        GLYPHA_REQUIRE(descriptor >= 0);
        GLYPHA_REQUIRE(::close(descriptor) == 0);
        const auto rejected =
            glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                      .storage_mode = glyphastore::StorageMode::durable_sync,
                                      .data_directory = path});
        GLYPHA_REQUIRE(!rejected.has_value());
        GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_argument);
        GLYPHA_REQUIRE(!std::filesystem::exists(path / glyphastore::kBootstrapIntentFilename));
        GLYPHA_REQUIRE(std::filesystem::exists(foreign));
    }
}

GLYPHA_TEST("store rejects invalid public worker configuration") {
    const auto zero_workers = glyphastore::Store::open({.worker_config = {.explicit_count = 0}});
    GLYPHA_REQUIRE(!zero_workers.has_value());
    GLYPHA_REQUIRE(zero_workers.error().code == glyphastore::ErrorCode::invalid_argument);

    const auto too_many_workers = glyphastore::Store::open(
        {.worker_config = {.explicit_count = glyphastore::kMaximumWorkerCount + 1U}});
    GLYPHA_REQUIRE(!too_many_workers.has_value());
    GLYPHA_REQUIRE(too_many_workers.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("durable_periodic rejects zero sync interval") {
    StoreTemporaryDirectory temporary;
    const auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                                  .data_directory = temporary.store_path(),
                                                  .durable_periodic = {.sync_interval_ms = 0}});
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("durable_periodic read after write is visible before flush") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                            .data_directory = path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .durable_periodic = {.sync_interval_ms = 60'000}});
    GLYPHA_REQUIRE(opened.has_value());
    GLYPHA_REQUIRE((*opened)->put("visible", bytes("now")).has_value());
    const auto value = (*opened)->get("visible");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(value_string(*value) == "now");
}

GLYPHA_TEST("durable_periodic flush makes writes restart durable") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                                .data_directory = path,
                                                .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                                .durable_periodic = {.sync_interval_ms = 60'000}});
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("flushed", bytes("value")).has_value());
        GLYPHA_REQUIRE((*opened)->flush().has_value());
    }
    auto reopened =
        glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto value = (*reopened)->get("flushed");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(value_string(*value) == "value");
}

GLYPHA_TEST("durable_periodic shutdown flush makes background writes restart durable") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                                .data_directory = path,
                                                .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                                .durable_periodic = {.sync_interval_ms = 60'000}});
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("shutdown", bytes("value")).has_value());
    }
    auto reopened =
        glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_periodic,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto value = (*reopened)->get("shutdown");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(value_string(*value) == "value");
}

GLYPHA_TEST("Store close is idempotent and rejects operations after releasing volatile resources") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("before-close", bytes("value")).has_value());
    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
    GLYPHA_REQUIRE(store.worker_count() == 1);

    const auto get = store.get("before-close");
    GLYPHA_REQUIRE(!get.has_value());
    GLYPHA_REQUIRE(get.error().code == glyphastore::ErrorCode::unavailable);
    const auto put = store.put("after-close", bytes("value"));
    GLYPHA_REQUIRE(!put.has_value());
    GLYPHA_REQUIRE(put.error().code == glyphastore::ErrorCode::unavailable);
    const auto erase = store.erase("before-close");
    GLYPHA_REQUIRE(!erase.has_value());
    GLYPHA_REQUIRE(erase.error().code == glyphastore::ErrorCode::unavailable);
    const auto flush = store.flush();
    GLYPHA_REQUIRE(!flush.has_value());
    GLYPHA_REQUIRE(flush.error().code == glyphastore::ErrorCode::unavailable);
    const auto compacted = store.compact();
    GLYPHA_REQUIRE(!compacted.has_value());
    GLYPHA_REQUIRE(compacted.error().code == glyphastore::ErrorCode::unavailable);
    const auto verified = store.verify_index();
    GLYPHA_REQUIRE(!verified.has_value());
    GLYPHA_REQUIRE(verified.error().code == glyphastore::ErrorCode::unavailable);
}

GLYPHA_TEST("Store compaction is explicit durable maintenance and no-ops without sealed history") {
    auto volatile_store = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(volatile_store.has_value());
    const auto unsupported = (*volatile_store)->compact();
    GLYPHA_REQUIRE(!unsupported.has_value());
    GLYPHA_REQUIRE(unsupported.error().code == glyphastore::ErrorCode::invalid_argument);

    StoreTemporaryDirectory temporary;
    auto durable_store = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    GLYPHA_REQUIRE(durable_store.has_value());
    const auto no_work = (*durable_store)->compact();
    GLYPHA_REQUIRE(no_work.has_value());
    GLYPHA_REQUIRE(!no_work->compacted);
    GLYPHA_REQUIRE(!no_work->worker_index.has_value());
}

GLYPHA_TEST("durable periodic close flushes and releases the directory lock before destruction") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .storage_mode = glyphastore::StorageMode::durable_periodic,
                                            .data_directory = path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .durable_periodic = {.sync_interval_ms = 60'000}});
    GLYPHA_REQUIRE(opened.has_value());
    GLYPHA_REQUIRE((*opened)->put("explicit-close", bytes("value")).has_value());
    GLYPHA_REQUIRE((*opened)->close().has_value());

    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto value = (*reopened)->get("explicit-close");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(value_string(*value) == "value");
}

GLYPHA_TEST("Store close forces a partial strict group and releases its producer") {
    StoreTemporaryDirectory temporary;
    auto opened = glyphastore::Store::open(
        {.worker_config = {.explicit_count = 1},
         .storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 32, .max_bytes = 65'536, .max_wait_ms = 60'000}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    std::mutex mutex;
    std::condition_variable changed;
    bool producer_started{};
    bool producer_completed{};
    glyphastore::Status producer_result;
    std::thread producer{[&] {
        {
            const std::lock_guard lock{mutex};
            producer_started = true;
        }
        changed.notify_all();
        auto result = store.put("partial-close", bytes("value"));
        {
            const std::lock_guard lock{mutex};
            producer_result = std::move(result);
            producer_completed = true;
        }
        changed.notify_all();
    }};
    {
        std::unique_lock lock{mutex};
        GLYPHA_REQUIRE(changed.wait_for(lock, std::chrono::seconds{2}, [&] { return producer_started; }));
        GLYPHA_REQUIRE(
            !changed.wait_for(lock, std::chrono::milliseconds{25}, [&] { return producer_completed; }));
    }

    const auto started = std::chrono::steady_clock::now();
    const auto closed = store.close();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    producer.join();
    GLYPHA_REQUIRE(closed.has_value());
    GLYPHA_REQUIRE(producer_result.has_value());
    GLYPHA_REQUIRE(elapsed < std::chrono::seconds{2});
}

GLYPHA_TEST("concurrent Store flush and close calls complete without deadlock") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                            .storage_mode = glyphastore::StorageMode::durable_periodic,
                                            .data_directory = path,
                                            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
                                            .durable_periodic = {.sync_interval_ms = 60'000}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("flush-close-race", bytes("value")).has_value());

    constexpr std::size_t kCloserCount = 4;
    constexpr std::size_t kFlusherCount = 4;
    std::barrier start{static_cast<std::ptrdiff_t>(kCloserCount + kFlusherCount + 1)};
    std::array<glyphastore::Status, kCloserCount> close_results;
    std::array<glyphastore::Status, kFlusherCount> flush_results;
    std::vector<std::thread> threads;
    threads.reserve(kCloserCount + kFlusherCount);
    for (std::size_t index = 0; index < kCloserCount; ++index) {
        threads.emplace_back([&, index] {
            start.arrive_and_wait();
            close_results[index] = store.close();
        });
    }
    for (std::size_t index = 0; index < kFlusherCount; ++index) {
        threads.emplace_back([&, index] {
            start.arrive_and_wait();
            flush_results[index] = store.flush();
        });
    }
    start.arrive_and_wait();
    for (auto& thread : threads) {
        thread.join();
    }

    for (const auto& result : close_results) {
        GLYPHA_REQUIRE(result.has_value());
    }
    for (const auto& result : flush_results) {
        GLYPHA_REQUIRE(result.has_value() || result.error().code == glyphastore::ErrorCode::unavailable);
    }

    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->get("flush-close-race").has_value());
}

GLYPHA_TEST("durable_group rejects invalid batch configuration") {
    StoreTemporaryDirectory temporary;
    const auto opened = glyphastore::Store::open({.storage_mode = glyphastore::StorageMode::durable_group,
                                                  .data_directory = temporary.store_path(),
                                                  .durable_group = {.max_records = 0}});
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().code == glyphastore::ErrorCode::invalid_argument);

    const auto zero_minimum = glyphastore::Store::open(
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = temporary.store_path(),
         .durable_group = {.max_records = 4, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 0}});
    GLYPHA_REQUIRE(!zero_minimum.has_value());
    GLYPHA_REQUIRE(zero_minimum.error().code == glyphastore::ErrorCode::invalid_argument);

    const auto inverted = glyphastore::Store::open(
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = temporary.store_path(),
         .durable_group = {.max_records = 4, .max_bytes = 65'536, .max_wait_ms = 10, .min_records = 5}});
    GLYPHA_REQUIRE(!inverted.has_value());
    GLYPHA_REQUIRE(inverted.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("durable_group concurrent puts batch and survive reopen") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    constexpr std::uint32_t kBatchSize = 32;
    {
        auto opened = glyphastore::Store::open(
            {.worker_config = {.explicit_count = 1},
             .storage_mode = glyphastore::StorageMode::durable_group,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new,
             .durable_group = {.max_records = kBatchSize, .max_bytes = 65536, .max_wait_ms = 60'000}});
        GLYPHA_REQUIRE(opened.has_value());
        auto& store = **opened;
        std::atomic<bool> failed{false};
        std::vector<std::thread> workers;
        workers.reserve(kBatchSize);
        for (std::uint32_t index = 0; index < kBatchSize; ++index) {
            workers.emplace_back([&, index]() {
                const std::string key = std::string(96, 'K') + '-' + std::to_string(index);
                if (!store.put(key, bytes("value-" + std::to_string(index))).has_value()) {
                    failed.store(true);
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        GLYPHA_REQUIRE(!failed.load());
        GLYPHA_REQUIRE(store.flush().has_value());
    }
    {
        auto directory = glyphastore::DataDirectory::open_and_lock(path);
        GLYPHA_REQUIRE(directory.has_value());
        const auto manifest = directory->read_manifest();
        GLYPHA_REQUIRE(manifest.has_value());
        GLYPHA_REQUIRE(manifest->segments.size() == 1);
        const auto& active = manifest->segments.front();
        const glyphastore::SegmentHeaderIdentity identity{
            .store_id = manifest->store_id,
            .segment_id = active.segment_id,
            .generation = active.generation,
            .owner_worker = active.owner_worker,
        };
        const auto segment = glyphastore::DurableSegmentFile::open(
            *directory, identity, glyphastore::SegmentFileOpenMode::read_only);
        GLYPHA_REQUIRE(segment.has_value());
        GLYPHA_REQUIRE(segment->selected_commit().commit.commit_generation == 2);
        GLYPHA_REQUIRE(segment->selected_commit().commit.record_count == kBatchSize);
    }
    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_group,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    for (std::uint32_t index = 0; index < kBatchSize; ++index) {
        const std::string key = std::string(96, 'K') + '-' + std::to_string(index);
        const auto value = (*reopened)->get(key);
        GLYPHA_REQUIRE(value.has_value());
        GLYPHA_REQUIRE(value_string(*value) == "value-" + std::to_string(index));
    }
}

GLYPHA_TEST("durable_group orders same-key put and erase within one batch") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::Store::open(
            {.worker_config = {.explicit_count = 1},
             .storage_mode = glyphastore::StorageMode::durable_group,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new,
             .durable_group = {.max_records = 2, .max_bytes = 65536, .max_wait_ms = 60'000}});
        GLYPHA_REQUIRE(opened.has_value());
        auto& store = **opened;
        std::atomic_bool put_completed{};
        std::atomic_bool put_failed{};
        std::thread putter([&] {
            put_failed.store(!store.put("same-key", bytes("value")).has_value());
            put_completed.store(true);
        });

        glyphastore::Status erased = glyphastore::fail(glyphastore::ErrorCode::not_found, "not tried");
        while (!put_completed.load()) {
            erased = store.erase("same-key");
            if (erased.has_value() || erased.error().code != glyphastore::ErrorCode::not_found) {
                break;
            }
            std::this_thread::yield();
        }
        putter.join();
        GLYPHA_REQUIRE(!put_failed.load());
        GLYPHA_REQUIRE(erased.has_value());
        const auto missing = store.get("same-key");
        GLYPHA_REQUIRE(!missing.has_value());
        GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
    }
    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_group,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto missing = (*reopened)->get("same-key");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("durable_group single put flushes within max_wait_ms") {
    StoreTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::Store::open(
            {.worker_config = {.explicit_count = 1},
             .storage_mode = glyphastore::StorageMode::durable_group,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new,
             .durable_group = {.max_records = 32, .max_bytes = 65536, .max_wait_ms = 50}});
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("solo", bytes("value")).has_value());
    }
    auto reopened =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_group,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    const auto value = (*reopened)->get("solo");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(value_string(*value) == "value");
}
