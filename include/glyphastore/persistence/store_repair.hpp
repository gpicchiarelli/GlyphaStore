#pragma once

#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/persistence/store_verify.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace glyphastore {

struct DurableStoreQuarantinedFile {
    std::string source_name;
    NamespaceIssueKind kind{};
    std::filesystem::path quarantine_path;
    std::uint64_t bytes_copied{};
};

struct DurableStoreRepairReport {
    std::filesystem::path source;
    std::filesystem::path workspace;
    std::filesystem::path repaired_store;
    std::filesystem::path quarantine_directory;
    std::size_t catalog_files_copied{};
    std::uint64_t catalog_bytes_copied{};
    std::vector<DurableStoreQuarantinedFile> quarantined;
    NamespaceAuditReport source_namespace_audit{};
    DurableStoreVerifyReport repaired_verification{};
};

// Offline fail-closed repair into an explicit empty workspace directory.
// Never mutates the source. Creates:
//   <workspace>/store       — clean Manifest + catalog Segments
//   <workspace>/quarantine  — non-catalog namespace anomalies + audit.txt
// Blocking catalog faults (missing Segment/required entry) and unsafe entries
// (symlinks/hard-links/non-regular) fail closed without writing a usable store.
[[nodiscard]] auto repair_durable_store(const std::filesystem::path& source,
                                        const std::filesystem::path& workspace, bool scan_records = true,
                                        const DurableResourceLimits& limits = {})
    -> Result<DurableStoreRepairReport>;

} // namespace glyphastore
