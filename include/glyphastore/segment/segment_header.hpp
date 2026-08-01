#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace glyphastore {

inline constexpr std::uint32_t kSegmentHeaderMagic = 0x48594C47U;
inline constexpr std::uint32_t kSegmentCommitMagic = 0x43594C47U;
inline constexpr std::uint16_t kSegmentHeaderFormatVersion = 1;
inline constexpr std::uint16_t kSegmentFormatVersion = 1;
inline constexpr std::uint16_t kSegmentCommitFormatVersion = 1;
inline constexpr std::size_t kSegmentImmutableHeaderBytes = 128;
inline constexpr std::size_t kSegmentCommitSlotBytes = 128;
inline constexpr std::size_t kSegmentCommitSlotCount = 2;
inline constexpr std::size_t kSegmentCommitSlotsOffset = kSegmentImmutableHeaderBytes;

using StoreId = std::array<std::byte, 16>;

enum class PersistedSegmentState : std::uint16_t { active = 1, sealed = 2 };

struct SegmentHeaderIdentity {
    StoreId store_id{};
    SegmentId segment_id{};
    GenerationId generation{};
    WorkerId owner_worker{};

    auto operator<=>(const SegmentHeaderIdentity&) const = default;
};

struct SegmentCommit {
    std::uint64_t commit_generation{};
    std::uint32_t committed_end{};
    PersistedSegmentState state{PersistedSegmentState::active};
    std::uint64_t record_count{};
    SequenceNumber first_sequence{};
    SequenceNumber last_sequence{};

    auto operator<=>(const SegmentCommit&) const = default;
};

enum class CommitSlotValidity { empty, valid, invalid };

struct DecodedCommitSlot {
    CommitSlotValidity validity{CommitSlotValidity::empty};
    std::optional<SegmentCommit> commit{};
};

struct SegmentHeader {
    SegmentHeaderIdentity identity;
    std::array<std::optional<SegmentCommit>, kSegmentCommitSlotCount> commits;
};

struct DecodedSegmentHeader {
    SegmentHeaderIdentity identity{};
    std::array<DecodedCommitSlot, kSegmentCommitSlotCount> slots{};
};

struct SelectedSegmentCommit {
    std::size_t slot_index{};
    SegmentCommit commit;

    auto operator==(const SelectedSegmentCommit&) const -> bool = default;
};

[[nodiscard]] auto encode_segment_commit_slot(std::span<std::byte> out, const SegmentCommit& commit)
    -> Status;
[[nodiscard]] auto decode_segment_commit_slot(std::span<const std::byte> bytes) -> Result<DecodedCommitSlot>;
[[nodiscard]] auto encode_segment_header(std::span<std::byte> out, const SegmentHeader& header) -> Status;
[[nodiscard]] auto decode_segment_header(std::span<const std::byte> bytes) -> Result<DecodedSegmentHeader>;
[[nodiscard]] auto select_newest_segment_commit(const DecodedSegmentHeader& header)
    -> Result<SelectedSegmentCommit>;

} // namespace glyphastore
