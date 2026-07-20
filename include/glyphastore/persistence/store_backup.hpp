#pragma once

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
// verify the destination. Does not copy locks, intents, or crash temporaries. Live/hot
// backup under writers is intentionally unsupported.
[[nodiscard]] auto backup_durable_store(const std::filesystem::path& source,
                                        const std::filesystem::path& destination,
                                        bool scan_records = true,
                                        const DurableResourceLimits& limits = {})
    -> Result<DurableStoreBackupReport>;

// Restore is the same offline verified copy into an empty destination directory.
[[nodiscard]] inline auto restore_durable_store(const std::filesystem::path& backup,
                                                const std::filesystem::path& destination,
                                                bool scan_records = true,
                                                const DurableResourceLimits& limits = {})
    -> Result<DurableStoreBackupReport> {
    return backup_durable_store(backup, destination, scan_records, limits);
}

} // namespace glyphastore
