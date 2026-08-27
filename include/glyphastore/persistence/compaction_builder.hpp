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

enum class DurableCompactionBuildOutcome { prepared, not_started, not_beneficial, recovery_required };

struct DurableCompactionCopyStats {
    std::uint64_t source_index_records_verified{};
    std::uint64_t source_bytes_verified{};
    std::uint64_t records_copied{};
    std::uint64_t bytes_copied{};
    std::uint64_t expired_records_dropped{};
    // Wall time spent before requesting the global Manifest publication
    // lease. This includes the exact scan, output copy, seal and verification.
    std::uint64_t pre_intent_duration_ns{};
    // Wall time for the recovery-sensitive intent -> promotion -> Manifest ->
    // retirement -> intent removal window. Filled by the runtime owner.
    std::uint64_t publication_lease_duration_ns{};
    // Delay deliberately injected before private staged Record writes by the
    // normal-mode byte-rate limiter. Pacing never runs after intent authority.
    std::uint64_t pacing_delay_ns{};
    std::uint64_t pacing_sleep_count{};
    std::uint64_t pacing_burst_bytes{};
    // Conservative lower bound for peak transient compaction metadata. It
    // covers fixed vector storage, output Index tables/arena and Record scratch;
    // allocator metadata and heap storage owned by copied std::strings are not
    // included and must be measured separately by an allocation census.
    std::uint64_t transient_metadata_lower_bound_bytes{};
};

// A successful build leaves the old manifest authoritative, a durable
// compaction intent installed, and every replacement sealed and revalidated.
// The caller can publish next_manifest and atomically move index into the
// frozen Worker without further Index allocation.
struct PreparedDurableCompaction {
    DurableCompactionPlan plan;
    Index index;
    std::uint64_t active_live_record_bytes{};
    std::vector<SelectedSegmentCommit> replacement_commits;
    DurableCompactionCopyStats stats;
};

struct DurableCompactionBuildResult {
    DurableCompactionBuildOutcome outcome{DurableCompactionBuildOutcome::not_started};
    std::optional<PreparedDurableCompaction> prepared;
    DurableCompactionCopyStats stats{};
    std::optional<Error> error;

    [[nodiscard]] auto succeeded() const noexcept -> bool {
        return outcome == DurableCompactionBuildOutcome::prepared && prepared.has_value();
    }
};

// Optional runtime gate acquired after the complete staged output copy, seal
// and verification, immediately before the durable intent. The gate must remain
// owned until the caller has either rolled the prepared transaction back or
// completed the v1 transaction. A rejected gate performs no authoritative
// persistent namespace mutation; private temporary files are discarded.
struct DurableCompactionIntentGate {
    void* context{};
    auto (*acquire)(void* context) -> Status{};
};

struct DurableCompactionPacing final {
    // Zero disables pacing. A nonzero value applies only to replacement
    // Record writes under private staged names, before intent publication.
    std::uint64_t bytes_per_second{};
};

// `current_index` must remain immutable for the complete call. Runtime callers
// use the owning-entry overload so Worker locks can be released first. Before
// the durable intent, failures are not_started and publish no replacement name.
// not_beneficial is a successful policy decision made before the intent when
// the exact output layout would reclaim no physical Segment.
// Once intent publication may have occurred, failures are recovery_required;
// the runtime must fail closed and let restart resolve the old authority.
[[nodiscard]] auto build_durable_worker_compaction(DataDirectory& directory, const Manifest& current,
                                                   WorkerId worker_id, const Index& current_index,
                                                   std::uint64_t now_ns,
                                                   const DurableResourceLimits& limits = {})
    -> DurableCompactionBuildResult;

// Runtime snapshot overload. The entries are owning and may be consumed after
// every runtime lock has been released.
[[nodiscard]] auto
build_durable_worker_compaction(DataDirectory& directory, const Manifest& current, WorkerId worker_id,
                                std::vector<IndexEntry> current_entries, std::uint64_t now_ns,
                                const DurableResourceLimits& limits = {},
                                DurableCompactionIntentGate intent_gate = {},
                                DurableCompactionPacing pacing = {}) -> DurableCompactionBuildResult;

} // namespace glyphastore
