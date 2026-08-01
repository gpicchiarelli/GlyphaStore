#pragma once

#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace glyphastore {

struct DurableStoreVerifiedSegment {
    ManifestSegmentEntry entry{};
    SelectedSegmentCommit selected{};
    std::uint64_t scanned_records{};
    bool active_requires_rotation{};
};

struct DurableStoreVerifyReport {
    std::filesystem::path path{};
    Manifest manifest{};
    NamespaceAuditReport namespace_audit{};
    std::vector<DurableStoreVerifiedSegment> segments{};
    std::uint64_t scanned_records{};
    std::size_t active_requires_rotation_count{};
};

// Read-only structural validation of a stopped durable Store: Manifest decode,
// namespace audit (recovery-safe), and per-catalog Segment open + optional
// committed CRC scan. Requires an already-locked DataDirectory. Does not rebuild
// Indexes, check routing, or repair files.
[[nodiscard]] auto verify_durable_store(DataDirectory& directory, bool scan_records = true,
                                        const DurableResourceLimits& limits = {})
    -> Result<DurableStoreVerifyReport>;

// Exclusive-locks the data directory, verifies, then releases the lock on return.
[[nodiscard]] auto verify_durable_store_path(const std::filesystem::path& path, bool scan_records = true,
                                             const DurableResourceLimits& limits = {})
    -> Result<DurableStoreVerifyReport>;

} // namespace glyphastore
