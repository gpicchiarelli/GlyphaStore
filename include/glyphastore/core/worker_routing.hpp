#pragma once

#include "glyphastore/core/error.hpp"

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace glyphastore {

enum class RoutingAlgorithm : std::uint32_t {
    fnv1a64_v1 = 1,
    siphash24_v1 = 2,
};

inline constexpr std::uint64_t kDefaultWorkerHashSeed = 0;
inline constexpr std::uint64_t kWorkerRoutingSipKey1Xor = 0x6a09e667f3bcc909ULL;
inline constexpr std::string_view kWireProtocolIdentity = "GlyphaStore/2";
inline constexpr std::size_t kWireInitIdentityExtendedBytes =
    kWireProtocolIdentity.size() + 1U + sizeof(std::uint32_t) + sizeof(std::uint64_t);

struct WorkerRoutingState {
    RoutingAlgorithm algorithm{RoutingAlgorithm::fnv1a64_v1};
    std::uint64_t seed{kDefaultWorkerHashSeed};

    [[nodiscard]] auto keyed() const noexcept -> bool {
        return algorithm == RoutingAlgorithm::siphash24_v1;
    }

    auto operator==(const WorkerRoutingState&) const -> bool = default;
};

void set_worker_routing(RoutingAlgorithm algorithm, std::uint64_t seed) noexcept;
void set_worker_routing(WorkerRoutingState state) noexcept;
[[nodiscard]] auto get_worker_routing() noexcept -> WorkerRoutingState;
[[nodiscard]] auto generate_worker_hash_seed() noexcept -> std::uint64_t;

[[nodiscard]] auto hash_key_routing(std::span<const std::byte> key) noexcept -> std::uint64_t;
[[nodiscard]] auto hash_key_routing(std::string_view key) noexcept -> std::uint64_t;
[[nodiscard]] auto hash_key_routing(std::span<const std::byte> key, WorkerRoutingState state) noexcept
    -> std::uint64_t;
[[nodiscard]] auto hash_key_routing(std::string_view key, WorkerRoutingState state) noexcept
    -> std::uint64_t;

[[nodiscard]] auto encode_init_identity_value(WorkerRoutingState state) -> std::vector<std::byte>;
[[nodiscard]] auto decode_init_identity_value(std::span<const std::byte> value)
    -> Result<WorkerRoutingState>;
[[nodiscard]] auto validate_worker_routing_state(WorkerRoutingState state) -> Status;

} // namespace glyphastore
