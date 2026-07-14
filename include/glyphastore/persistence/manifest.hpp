#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore {

inline constexpr std::uint32_t kManifestMagic = 0x4D594C47U;
inline constexpr std::uint16_t kManifestFormatVersion = 1;
inline constexpr std::size_t kManifestHeaderBytes = 128;
inline constexpr std::size_t kManifestSegmentEntryBytes = 32;
inline constexpr std::size_t kMaximumManifestSegmentCount = 1'000'000;
inline constexpr std::size_t kMaximumManifestBytes =
    kManifestHeaderBytes + kMaximumManifestSegmentCount * kManifestSegmentEntryBytes;

enum class RoutingAlgorithm : std::uint32_t { fnv1a64_v1 = 1 };
enum class ManifestSegmentRole : std::uint16_t { active = 1, sealed = 2 };

struct ManifestSegmentEntry {
    SegmentId segment_id{};
    GenerationId generation{};
    WorkerId owner_worker{};
    ManifestSegmentRole role{ManifestSegmentRole::sealed};

    auto operator==(const ManifestSegmentEntry&) const -> bool = default;
};

struct Manifest {
    StoreId store_id{};
    std::uint64_t manifest_generation{};
    RoutingAlgorithm routing_algorithm{RoutingAlgorithm::fnv1a64_v1};
    std::uint32_t worker_count{};
    std::uint64_t routing_epoch{};
    SegmentId next_segment_id{};
    GenerationId next_segment_generation{GenerationId{1}};
    std::vector<ManifestSegmentEntry> segments;

    auto operator==(const Manifest&) const -> bool = default;
};

struct SelectedManifest {
    std::size_t candidate_index{};
    std::uint64_t manifest_generation{};
};

[[nodiscard]] auto encoded_manifest_size(const Manifest& manifest) -> Result<std::size_t>;
[[nodiscard]] auto encode_manifest(std::span<std::byte> out, const Manifest& manifest) -> Status;
[[nodiscard]] auto encode_manifest(const Manifest& manifest) -> Result<std::vector<std::byte>>;
[[nodiscard]] auto decode_manifest(std::span<const std::byte> bytes) -> Result<Manifest>;
[[nodiscard]] auto select_newest_manifest(std::span<const Manifest> candidates) -> Result<SelectedManifest>;

} // namespace glyphastore
