#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore {

inline constexpr std::size_t kNamespaceAnomalyBudget = 4096;
inline constexpr std::size_t kMaximumNamespaceIssueCount =
    kMaximumManifestSegmentCount + kNamespaceAnomalyBudget;

enum class NamespaceIssueKind {
    stale_manifest_temporary,
    stale_segment_temporary,
    unlisted_segment,
    malformed_engine_name,
    unknown_entry,
    unsafe_entry,
    missing_catalog_segment,
    missing_required_entry,
};

struct ParsedSegmentFilename {
    SegmentId segment_id;
    GenerationId generation;
    bool temporary{};
};

struct NamespaceIssue {
    NamespaceIssueKind kind;
    std::string name;
    std::optional<SegmentId> segment_id;
    std::optional<GenerationId> generation;
};

struct NamespaceAuditReport {
    std::size_t entries_scanned{};
    std::size_t catalog_segments_seen{};
    std::vector<NamespaceIssue> issues;

    [[nodiscard]] auto clean() const noexcept -> bool {
        return issues.empty();
    }
    [[nodiscard]] auto recovery_safe() const noexcept -> bool;
};

[[nodiscard]] auto parse_segment_filename(std::string_view name) -> Result<ParsedSegmentFilename>;
[[nodiscard]] auto namespace_issue_name(NamespaceIssueKind kind) noexcept -> std::string_view;
[[nodiscard]] auto audit_data_directory(DataDirectory& directory, const Manifest& manifest)
    -> Result<NamespaceAuditReport>;
[[nodiscard]] auto validate_namespace_for_recovery(const NamespaceAuditReport& report) -> Status;

} // namespace glyphastore
