#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/worker_routing.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

[[nodiscard]] auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return std::as_bytes(std::span{value.data(), value.size()});
}

[[nodiscard]] auto value_string(const glyphastore::OwnedValue& value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

struct RoutingGuard final {
    explicit RoutingGuard(const glyphastore::WorkerRoutingState state)
        : previous_(glyphastore::get_worker_routing()) {
        glyphastore::set_worker_routing(state);
    }
    ~RoutingGuard() {
        glyphastore::set_worker_routing(previous_);
    }
    RoutingGuard(const RoutingGuard&) = delete;
    auto operator=(const RoutingGuard&) -> RoutingGuard& = delete;

    glyphastore::WorkerRoutingState previous_;
};

[[nodiscard]] auto temp_dir(const std::string_view name) -> std::filesystem::path {
    const auto parent = std::filesystem::temp_directory_path() / "glyphastore-worker-routing";
    std::filesystem::create_directories(parent);
    const auto path = parent / name;
    std::filesystem::remove_all(path);
    return path;
}

} // namespace

GLYPHA_TEST("worker routing defaults to fnv1a64-v1 with zero seed") {
    RoutingGuard guard{{}};
    GLYPHA_REQUIRE(glyphastore::get_worker_routing().algorithm == glyphastore::RoutingAlgorithm::fnv1a64_v1);
    GLYPHA_REQUIRE(glyphastore::get_worker_routing().seed == 0);
    GLYPHA_REQUIRE(glyphastore::hash_key_routing("alpha") == glyphastore::hash_key("alpha"));
}

GLYPHA_TEST("worker routing seed is stable within a process") {
    RoutingGuard guard{{glyphastore::RoutingAlgorithm::siphash24_v1, 0x1111222233334444ULL}};
    const auto first = glyphastore::hash_key_routing("tenant-a/orders/1");
    const auto second = glyphastore::hash_key_routing("tenant-a/orders/1");
    GLYPHA_REQUIRE(first == second);
    GLYPHA_REQUIRE(first != glyphastore::hash_key("tenant-a/orders/1"));
}

GLYPHA_TEST("worker routing publishes algorithm and seed as one coherent revision") {
    constexpr glyphastore::WorkerRoutingState kFNV{};
    constexpr glyphastore::WorkerRoutingState kSip{glyphastore::RoutingAlgorithm::siphash24_v1,
                                                   0xA55A'1234'9876'FEDCULL};
    RoutingGuard guard{kFNV};
    std::atomic_bool start{};
    std::atomic_bool done{};
    std::atomic_bool torn{};

    std::thread writer{[&] {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (std::size_t iteration = 0; iteration < 100'000U; ++iteration) {
            glyphastore::set_worker_routing((iteration & 1U) == 0U ? kSip : kFNV);
        }
        done.store(true, std::memory_order_release);
    }};
    std::array<std::thread, 4> readers;
    for (auto& reader : readers) {
        reader = std::thread{[&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            while (!done.load(std::memory_order_acquire)) {
                const auto observed = glyphastore::get_worker_routing();
                if (observed != kFNV && observed != kSip) {
                    torn.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        }};
    }
    start.store(true, std::memory_order_release);
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    GLYPHA_REQUIRE(!torn.load(std::memory_order_relaxed));
}

GLYPHA_TEST("different worker routing seeds select different owners") {
    constexpr std::string_view kKey = "flood-candidate-key";
    constexpr std::size_t kWorkers = 8;
    const auto fnv_owner = glyphastore::route_worker(glyphastore::hash_key(kKey), kWorkers);

    std::size_t matches = 0;
    for (std::uint64_t seed = 1; seed <= 64; ++seed) {
        const glyphastore::WorkerRoutingState state{glyphastore::RoutingAlgorithm::siphash24_v1, seed};
        const auto owner = glyphastore::route_worker(glyphastore::hash_key_routing(kKey, state), kWorkers);
        if (owner == fnv_owner) {
            ++matches;
        }
    }
    GLYPHA_REQUIRE(matches < 64);
}

GLYPHA_TEST("INIT identity stays plain for FNV and extends for SipHash") {
    const auto plain = glyphastore::encode_init_identity_value({});
    GLYPHA_REQUIRE(plain.size() == glyphastore::kWireProtocolIdentity.size());
    auto decoded_plain = glyphastore::decode_init_identity_value(plain);
    GLYPHA_REQUIRE(decoded_plain.has_value());
    GLYPHA_REQUIRE(!decoded_plain->keyed());

    const glyphastore::WorkerRoutingState keyed{glyphastore::RoutingAlgorithm::siphash24_v1,
                                                0xABCDEF0123456789ULL};
    const auto extended = glyphastore::encode_init_identity_value(keyed);
    GLYPHA_REQUIRE(extended.size() == glyphastore::kWireInitIdentityExtendedBytes);
    auto decoded = glyphastore::decode_init_identity_value(extended);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(*decoded == keyed);
}

GLYPHA_TEST("durable Store persists worker hash seed and refuses mismatch") {
    RoutingGuard guard{{}};
    const auto dir = temp_dir("persist-seed");
    {
        auto created = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .worker_routing = {.algorithm = glyphastore::RoutingAlgorithm::siphash24_v1,
                               .seed = 42,
                               .seed_explicit = true},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = dir,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
        });
        GLYPHA_REQUIRE(created.has_value());
        GLYPHA_REQUIRE((*created)->put("k", bytes("v")).has_value());
        GLYPHA_REQUIRE((*created)->close().has_value());
    }

    {
        auto reopened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = dir,
            .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
        });
        GLYPHA_REQUIRE(reopened.has_value());
        // Opening a Store must not mutate process-global defaults used by
        // unrelated standalone Index/test contexts.
        GLYPHA_REQUIRE(glyphastore::get_worker_routing() == glyphastore::WorkerRoutingState{});
        auto value = (*reopened)->get("k");
        GLYPHA_REQUIRE(value.has_value());
        GLYPHA_REQUIRE(value_string(*value) == "v");
        GLYPHA_REQUIRE((*reopened)->close().has_value());
    }

    {
        auto mismatched = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .worker_routing = {.algorithm = glyphastore::RoutingAlgorithm::siphash24_v1,
                               .seed = 99,
                               .seed_explicit = true},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = dir,
            .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
            .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
        });
        GLYPHA_REQUIRE(!mismatched.has_value());
        GLYPHA_REQUIRE(mismatched.error().code == glyphastore::ErrorCode::invalid_argument);
    }
}

GLYPHA_TEST("manifest round-trip preserves siphash worker hash seed") {
    glyphastore::StoreId store_id{};
    store_id[0] = std::byte{1};
    glyphastore::Manifest manifest{
        .store_id = store_id,
        .manifest_generation = 1,
        .routing_algorithm = glyphastore::RoutingAlgorithm::siphash24_v1,
        .worker_count = 1,
        .routing_epoch = 1,
        .worker_hash_seed = 0xDEADBEEFCAFEBABEULL,
        .next_segment_id = glyphastore::SegmentId{2},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = {{.segment_id = glyphastore::SegmentId{1},
                      .generation = glyphastore::GenerationId{1},
                      .owner_worker = glyphastore::WorkerId{0},
                      .role = glyphastore::ManifestSegmentRole::active}},
    };
    auto encoded = glyphastore::encode_manifest(manifest);
    GLYPHA_REQUIRE(encoded.has_value());
    auto decoded = glyphastore::decode_manifest(*encoded);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->worker_hash_seed == 0xDEADBEEFCAFEBABEULL);
    GLYPHA_REQUIRE(decoded->routing_algorithm == glyphastore::RoutingAlgorithm::siphash24_v1);
}
