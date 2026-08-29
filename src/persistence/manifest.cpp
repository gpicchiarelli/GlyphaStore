#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/core/little_endian.hpp"

#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/config.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>

namespace glyphastore {
namespace {

inline constexpr std::size_t kManifestChecksumOffset = 80;
inline constexpr std::size_t kChecksumBytes = 4;

using le::put_u16;
using le::put_u32;
using le::put_u64;
using le::get_u16;
using le::get_u32;
using le::get_u64;

auto all_zero(std::span<const std::byte> bytes) -> bool {
    return std::ranges::all_of(bytes, [](std::byte value) { return value == std::byte{0}; });
}

auto checksum_with_zeroed_field(std::span<const std::byte> bytes) -> std::uint32_t {
    std::vector<std::byte> copy(bytes.begin(), bytes.end());
    std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kManifestChecksumOffset), kChecksumBytes,
                std::byte{0});
    return crc32c(copy);
}

auto manifest_size_for_count(std::size_t segment_count) -> Result<std::size_t> {
    if (segment_count > kMaximumManifestSegmentCount) {
        return fail(ErrorCode::invalid_argument, "manifest Segment count exceeds the supported limit");
    }
    if (segment_count >
        (std::numeric_limits<std::size_t>::max() - kManifestHeaderBytes) / kManifestSegmentEntryBytes) {
        return fail(ErrorCode::arithmetic_overflow, "manifest encoded size overflows size_t");
    }
    const auto size = kManifestHeaderBytes + segment_count * kManifestSegmentEntryBytes;
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "manifest encoded size exceeds its format field");
    }
    return size;
}

auto manifest_failure(ErrorCode code, std::string message) -> Status {
    return fail(code, std::move(message));
}

auto validate_manifest(const Manifest& manifest, ErrorCode error_code) -> Status {
    if (all_zero(manifest.store_id)) {
        return manifest_failure(error_code, "manifest Store ID cannot be all zero");
    }
    if (manifest.manifest_generation == 0) {
        return manifest_failure(error_code, "manifest generation must be non-zero");
    }
    if (auto routing = validate_worker_routing_state(manifest.worker_routing()); !routing) {
        return manifest_failure(error_code, routing.error().message);
    }
    if (manifest.worker_count == 0 || manifest.worker_count > kMaximumWorkerCount) {
        return manifest_failure(error_code, "manifest Worker count is outside supported bounds");
    }
    if (manifest.routing_epoch == 0) {
        return manifest_failure(error_code, "manifest routing epoch must be non-zero");
    }
    if (manifest.next_segment_id.value == 0 || manifest.next_segment_generation.value == 0) {
        return manifest_failure(error_code, "manifest next Segment identity must be non-zero");
    }
    if (manifest.segments.size() > kMaximumManifestSegmentCount) {
        return manifest_failure(error_code, "manifest Segment count exceeds the supported limit");
    }

    std::vector<bool> active_workers(manifest.worker_count, false);
    SegmentId previous_id{};
    for (const auto& segment : manifest.segments) {
        if (segment.segment_id.value == 0 || segment.generation.value == 0) {
            return manifest_failure(error_code, "manifest Segment identity must be non-zero");
        }
        if (segment.segment_id.value <= previous_id.value) {
            return manifest_failure(error_code, "manifest Segment entries are not strictly ordered");
        }
        if (segment.owner_worker.value >= manifest.worker_count) {
            return manifest_failure(error_code, "manifest Segment owner is outside the Worker set");
        }
        if (segment.role != ManifestSegmentRole::active && segment.role != ManifestSegmentRole::sealed) {
            return manifest_failure(error_code, "manifest Segment role is invalid");
        }
        if (segment.role == ManifestSegmentRole::active) {
            if (active_workers[segment.owner_worker.value]) {
                return manifest_failure(error_code,
                                        "manifest contains multiple active Segments for a Worker");
            }
            active_workers[segment.owner_worker.value] = true;
        }
        previous_id = segment.segment_id;
    }
    if (!std::ranges::all_of(active_workers, [](bool active) { return active; })) {
        return manifest_failure(error_code, "manifest must contain one active Segment per Worker");
    }
    if (!manifest.segments.empty() &&
        manifest.next_segment_id.value <= manifest.segments.back().segment_id.value) {
        return manifest_failure(error_code, "manifest next Segment ID does not advance beyond the catalog");
    }
    return {};
}

} // namespace

auto encoded_manifest_size(const Manifest& manifest) -> Result<std::size_t> {
    if (auto valid = validate_manifest(manifest, ErrorCode::invalid_argument); !valid) {
        return unexpected(valid.error());
    }
    return manifest_size_for_count(manifest.segments.size());
}

auto encode_manifest(const std::span<std::byte> out, const Manifest& manifest) -> Status {
    const auto size = encoded_manifest_size(manifest);
    if (!size) {
        return unexpected(size.error());
    }
    if (out.size() < *size) {
        return fail(ErrorCode::invalid_argument, "manifest encode buffer is too small");
    }

    std::vector<std::byte> encoded(*size, std::byte{0});
    put_u32(encoded, 0, kManifestMagic);
    put_u16(encoded, 4, kManifestFormatVersion);
    put_u16(encoded, 6, static_cast<std::uint16_t>(kManifestHeaderBytes));
    put_u32(encoded, 8, static_cast<std::uint32_t>(*size));
    put_u16(encoded, 12, static_cast<std::uint16_t>(kManifestSegmentEntryBytes));
    put_u16(encoded, 14, 0);
    std::memcpy(encoded.data() + 16, manifest.store_id.data(), manifest.store_id.size());
    put_u64(encoded, 32, manifest.manifest_generation);
    put_u32(encoded, 40, static_cast<std::uint32_t>(manifest.routing_algorithm));
    put_u32(encoded, 44, manifest.worker_count);
    put_u64(encoded, 48, manifest.routing_epoch);
    put_u32(encoded, 56, static_cast<std::uint32_t>(manifest.segments.size()));
    put_u16(encoded, 60, kSegmentFormatVersion);
    put_u16(encoded, 62, kRecordFormatVersion);
    put_u16(encoded, 64, kSegmentHeaderFormatVersion);
    put_u16(encoded, 66, kSegmentCommitFormatVersion);
    put_u64(encoded, 68, manifest.next_segment_id.value);
    put_u32(encoded, 76, manifest.next_segment_generation.value);
    put_u64(encoded, kManifestWorkerHashSeedOffset, manifest.worker_hash_seed);
    put_u32(encoded, kManifestChecksumOffset, 0);

    for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
        const auto offset = kManifestHeaderBytes + index * kManifestSegmentEntryBytes;
        const auto& segment = manifest.segments[index];
        put_u64(encoded, offset, segment.segment_id.value);
        put_u32(encoded, offset + 8, segment.generation.value);
        put_u32(encoded, offset + 12, segment.owner_worker.value);
        put_u16(encoded, offset + 16, static_cast<std::uint16_t>(segment.role));
        put_u16(encoded, offset + 18, 0);
    }
    put_u32(encoded, kManifestChecksumOffset, checksum_with_zeroed_field(encoded));
    std::memcpy(out.data(), encoded.data(), encoded.size());
    return {};
}

auto encode_manifest(const Manifest& manifest) -> Result<std::vector<std::byte>> {
    const auto size = encoded_manifest_size(manifest);
    if (!size) {
        return unexpected(size.error());
    }
    std::vector<std::byte> encoded(*size, std::byte{0});
    if (auto status = encode_manifest(encoded, manifest); !status) {
        return unexpected(status.error());
    }
    return encoded;
}

auto decode_manifest(const std::span<const std::byte> bytes) -> Result<Manifest> {
    if (bytes.size() < kManifestHeaderBytes) {
        return fail(ErrorCode::invalid_record, "manifest is shorter than its v1 header");
    }
    if (get_u32(bytes, 0) != kManifestMagic || get_u16(bytes, 4) != kManifestFormatVersion ||
        get_u16(bytes, 6) != kManifestHeaderBytes || get_u16(bytes, 12) != kManifestSegmentEntryBytes ||
        get_u16(bytes, 14) != 0 || get_u16(bytes, 60) != kSegmentFormatVersion ||
        get_u16(bytes, 62) != kRecordFormatVersion || get_u16(bytes, 64) != kSegmentHeaderFormatVersion ||
        get_u16(bytes, 66) != kSegmentCommitFormatVersion ||
        !all_zero(bytes.subspan(kManifestWorkerHashSeedOffset + sizeof(std::uint64_t),
                                44U - sizeof(std::uint64_t)))) {
        return fail(ErrorCode::invalid_record,
                    "manifest magic, versions, sizes, or reserved fields are invalid");
    }

    const auto segment_count = get_u32(bytes, 56);
    const auto expected_size = manifest_size_for_count(segment_count);
    if (!expected_size) {
        return fail(ErrorCode::invalid_record, "manifest Segment count is outside format limits");
    }
    if (get_u32(bytes, 8) != *expected_size || bytes.size() != *expected_size) {
        return fail(ErrorCode::invalid_record, "manifest encoded extent is inconsistent");
    }
    if (get_u32(bytes, kManifestChecksumOffset) != checksum_with_zeroed_field(bytes)) {
        return fail(ErrorCode::checksum_mismatch, "manifest checksum mismatch");
    }

    Manifest manifest{
        .manifest_generation = get_u64(bytes, 32),
        .routing_algorithm = static_cast<RoutingAlgorithm>(get_u32(bytes, 40)),
        .worker_count = get_u32(bytes, 44),
        .routing_epoch = get_u64(bytes, 48),
        .worker_hash_seed = get_u64(bytes, kManifestWorkerHashSeedOffset),
        .next_segment_id = SegmentId{get_u64(bytes, 68)},
        .next_segment_generation = GenerationId{get_u32(bytes, 76)},
    };
    std::memcpy(manifest.store_id.data(), bytes.data() + 16, manifest.store_id.size());
    manifest.segments.reserve(segment_count);
    for (std::size_t index = 0; index < segment_count; ++index) {
        const auto offset = kManifestHeaderBytes + index * kManifestSegmentEntryBytes;
        if (get_u16(bytes, offset + 18) != 0 ||
            !all_zero(bytes.subspan(offset + 20, kManifestSegmentEntryBytes - 20))) {
            return fail(ErrorCode::invalid_record, "manifest Segment entry reserved fields are invalid");
        }
        manifest.segments.push_back({
            .segment_id = SegmentId{get_u64(bytes, offset)},
            .generation = GenerationId{get_u32(bytes, offset + 8)},
            .owner_worker = WorkerId{get_u32(bytes, offset + 12)},
            .role = static_cast<ManifestSegmentRole>(get_u16(bytes, offset + 16)),
        });
    }
    if (auto valid = validate_manifest(manifest, ErrorCode::corrupted_data); !valid) {
        return unexpected(valid.error());
    }
    return manifest;
}

auto select_newest_manifest(const std::span<const Manifest> candidates) -> Result<SelectedManifest> {
    if (candidates.empty()) {
        return fail(ErrorCode::invalid_argument, "manifest selection requires at least one candidate");
    }

    std::optional<SelectedManifest> selected;
    for (std::size_t index = 0; index < candidates.size(); ++index) {
        const auto& candidate = candidates[index];
        if (auto valid = validate_manifest(candidate, ErrorCode::corrupted_data); !valid) {
            return unexpected(valid.error());
        }
        if (!selected || candidate.manifest_generation > selected->manifest_generation) {
            selected = SelectedManifest{.candidate_index = index,
                                        .manifest_generation = candidate.manifest_generation};
            continue;
        }
        if (candidate.manifest_generation == selected->manifest_generation &&
            candidate != candidates[selected->candidate_index]) {
            return fail(ErrorCode::corrupted_data,
                        "equal manifest generations contain conflicting catalog metadata");
        }
    }
    return *selected;
}

} // namespace glyphastore
