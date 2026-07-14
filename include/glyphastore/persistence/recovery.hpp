#pragma once

#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace glyphastore {

struct RecoveredSegmentState {
    SelectedSegmentCommit selected;
};

struct RecoveredWorkerState {
    WorkerId worker_id;
    Index index;
    SequenceNumber next_sequence;
    SegmentId active_segment;
    bool active_requires_rotation{};
};

struct DurableRecoveryStats {
    RebuildStats rebuild;
    std::size_t segments_scanned{};
    std::size_t workers_requiring_rotation{};
};

struct DurableRecoveryState {
    Manifest manifest;
    NamespaceAuditReport namespace_audit;
    // Index-for-index aligned with manifest.segments; catalog identity is not duplicated.
    std::vector<RecoveredSegmentState> segments;
    std::vector<RecoveredWorkerState> workers;
    DurableRecoveryStats stats;
};

// DataDirectory must remain alive while any recovered RecordRef is used to
// reopen its backing Segment. Recovery itself keeps at most one Segment file
// descriptor and one Worker's temporary latest-key map live at a time.
[[nodiscard]] auto recover_durable_state(DataDirectory& directory, std::uint64_t now_ns = 0)
    -> Result<DurableRecoveryState>;

} // namespace glyphastore
