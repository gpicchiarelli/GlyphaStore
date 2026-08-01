#pragma once

#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/store_verify.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace glyphastore {

struct DurableStoreBackupReport {
    std::filesystem::path source;
    std::filesystem::path destination;
    std::size_t files_copied{};
    std::uint64_t bytes_copied{};
    DurableStoreVerifyReport source_verification{};
    DurableStoreVerifyReport destination_verification{};
};

// Offline fail-closed backup: exclusive-lock source, verify, create an empty destination,
// copy only Manifest + catalog Segment files (Segments first, Manifest last), sync, then
// verify the destination. Does not copy locks, intents, or crash temporaries.
// Concurrent external use against a live data directory still fails on the Store lock.
[[nodiscard]] auto backup_durable_store(const std::filesystem::path& source,
                                        const std::filesystem::path& destination, bool scan_records = true,
                                        const DurableResourceLimits& limits = {})
    -> Result<DurableStoreBackupReport>;

// Online catalog copy from an already-locked DataDirectory (open Store path).
// Caller must fence mutations for the copy window (Store::backup_to does this).
// Still copies only Manifest + catalog Segments; destination must be empty.
[[nodiscard]] auto backup_durable_store_from_open_directory(
    DataDirectory& source, const Manifest& catalog_manifest, const std::filesystem::path& destination,
    bool scan_records = true, const DurableResourceLimits& limits = {}) -> Result<DurableStoreBackupReport>;

// Restore is the same offline verified copy into an empty destination directory.
[[nodiscard]] inline auto
restore_durable_store(const std::filesystem::path& backup, const std::filesystem::path& destination,
                      bool scan_records = true, const DurableResourceLimits& limits = {})
    -> Result<DurableStoreBackupReport> {
    return backup_durable_store(backup, destination, scan_records, limits);
}

} // namespace glyphastore
