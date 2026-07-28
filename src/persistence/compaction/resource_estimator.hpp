#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/store/config.hpp"

#include <cstddef>
#include <cstdint>

namespace glyphastore {

struct CompactionResourceEstimate final {
    std::uint64_t temporary_bytes{};
    std::uint64_t reclaimed_bytes{};
    std::uint64_t peak_store_bytes{};
    std::uint64_t intent_bytes{};
};

// Pure resource arithmetic for durable compaction. Planning stays free of I/O.
[[nodiscard]] auto estimate_compaction_resources(const Manifest& current, const Manifest& next,
                                                 std::size_t source_count, std::size_t output_count)
    -> Result<CompactionResourceEstimate>;

[[nodiscard]] auto validate_compaction_resources(const CompactionResourceEstimate& estimate,
                                                 const DurableResourceLimits& limits) -> Status;

[[nodiscard]] auto validate_compaction_write_amplification(std::size_t source_count,
                                                           std::size_t output_count,
                                                           const DurableResourceLimits& limits)
    -> Status;

} // namespace glyphastore
