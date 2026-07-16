#pragma once

#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace glyphastore {

enum class DurableCompactionBuildOutcome { prepared, not_started, recovery_required };

struct DurableCompactionCopyStats {
    std::uint64_t source_index_records_verified{};
    std::uint64_t source_bytes_verified{};
    std::uint64_t records_copied{};
    std::uint64_t bytes_copied{};
    std::uint64_t expired_records_dropped{};
};

// A successful build leaves the old manifest authoritative, a durable
// compaction intent installed, and every replacement sealed and revalidated.
// The caller can publish next_manifest and atomically move index into the
// frozen Worker without further Index allocation.
struct PreparedDurableCompaction {
    DurableCompactionPlan plan;
    Index index;
    std::vector<SelectedSegmentCommit> replacement_commits;
    DurableCompactionCopyStats stats;
};

struct DurableCompactionBuildResult {
    DurableCompactionBuildOutcome outcome{DurableCompactionBuildOutcome::not_started};
    std::optional<PreparedDurableCompaction> prepared;
    std::optional<Error> error;

    [[nodiscard]] auto succeeded() const noexcept -> bool {
        return outcome == DurableCompactionBuildOutcome::prepared && prepared.has_value();
    }
};

// The caller must freeze the target Worker for the complete call. Before the
// durable intent, failures are not_started and publish no replacement name.
// Once intent publication may have occurred, failures are recovery_required;
// the runtime must fail closed and let restart resolve the old authority.
[[nodiscard]] auto build_durable_worker_compaction(DataDirectory& directory, const Manifest& current,
                                                   WorkerId worker_id, const Index& current_index,
                                                   std::uint64_t now_ns,
                                                   const DurableResourceLimits& limits = {})
    -> DurableCompactionBuildResult;

} // namespace glyphastore
