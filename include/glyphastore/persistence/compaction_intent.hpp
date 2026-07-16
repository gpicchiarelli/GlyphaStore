#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/persistence/manifest.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace glyphastore {

inline constexpr std::uint32_t kCompactionIntentMagic = 0x43594C47U;
inline constexpr std::uint16_t kCompactionIntentFormatVersion = 1;
inline constexpr std::size_t kCompactionIntentHeaderBytes = 128;
inline constexpr std::size_t kMaximumCompactionIntentBytes =
    kCompactionIntentHeaderBytes + 2U * kMaximumManifestBytes;

struct DurableCompactionIntent {
    WorkerId worker_id{};
    Manifest old_manifest;
    Manifest next_manifest;

    auto operator==(const DurableCompactionIntent&) const -> bool = default;
};

[[nodiscard]] auto encoded_compaction_intent_size(const DurableCompactionIntent& intent)
    -> Result<std::size_t>;
[[nodiscard]] auto encode_compaction_intent(std::span<std::byte> out, const DurableCompactionIntent& intent)
    -> Status;
[[nodiscard]] auto encode_compaction_intent(const DurableCompactionIntent& intent)
    -> Result<std::vector<std::byte>>;
[[nodiscard]] auto decode_compaction_intent(std::span<const std::byte> bytes)
    -> Result<DurableCompactionIntent>;

} // namespace glyphastore
