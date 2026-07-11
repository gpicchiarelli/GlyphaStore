#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace glyphastore {

inline constexpr std::size_t kSegmentSizeBytes = 64ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kSegmentHeaderReservedBytes = 4096;
inline constexpr std::size_t kRecordAlignment = 8;
inline constexpr std::size_t kMaxNormalRecordSize = 1ULL * 1024ULL * 1024ULL;

template <typename Tag, typename T> struct StrongId {
    T value{};
    auto operator<=>(const StrongId&) const = default;
};

struct SegmentIdTag;
struct WorkerIdTag;
struct SequenceNumberTag;
struct GenerationIdTag;
struct RecordOffsetTag;
struct RecordSizeTag;

using SegmentId = StrongId<SegmentIdTag, std::uint64_t>;
using WorkerId = StrongId<WorkerIdTag, std::uint32_t>;
using SequenceNumber = StrongId<SequenceNumberTag, std::uint64_t>;
using GenerationId = StrongId<GenerationIdTag, std::uint32_t>;
using RecordOffset = StrongId<RecordOffsetTag, std::uint32_t>;
using RecordSize = StrongId<RecordSizeTag, std::uint32_t>;

struct RecordRef {
    SegmentId segment_id;
    RecordOffset offset;
    RecordSize size;
    SequenceNumber sequence;
    GenerationId generation;

    auto operator<=>(const RecordRef&) const = default;
};

} // namespace glyphastore

namespace std {

template <typename Tag, typename T> struct hash<glyphastore::StrongId<Tag, T>> {
    auto operator()(const glyphastore::StrongId<Tag, T>& id) const noexcept -> std::size_t {
        return hash<T>{}(id.value);
    }
};

} // namespace std
