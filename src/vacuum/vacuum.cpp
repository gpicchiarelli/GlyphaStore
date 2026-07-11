#include "glyphastore/vacuum/vacuum.hpp"

#include <algorithm>
#include <unordered_map>

namespace glyphastore {
namespace {

auto make_catalog(std::span<const SegmentPtr> segments) -> std::unordered_map<SegmentId, Segment*> {
    std::unordered_map<SegmentId, Segment*> result;
    for (const auto& segment : segments) {
        result.emplace(segment->id(), segment.get());
    }
    return result;
}

} // namespace

auto VacuumPlanner::candidates(std::span<const SegmentPtr> segments) const -> std::vector<SegmentId> {
    struct Candidate {
        SegmentId id;
        double ratio;
    };
    std::vector<Candidate> ranked;
    for (const auto& segment : segments) {
        const auto stats = segment->stats();
        if (segment->state() != SegmentState::sealed || stats.used_bytes == 0) {
            continue;
        }
        const auto ratio = static_cast<double>(stats.live_bytes) / static_cast<double>(stats.used_bytes);
        if (ratio <= policy_.maximum_live_ratio) {
            ranked.push_back({segment->id(), ratio});
        }
    }
    std::ranges::sort(ranked, {}, &Candidate::ratio);
    if (ranked.size() > policy_.maximum_segments_per_run) {
        ranked.resize(policy_.maximum_segments_per_run);
    }
    std::vector<SegmentId> result;
    result.reserve(ranked.size());
    for (const auto& candidate : ranked) {
        result.push_back(candidate.id);
    }
    return result;
}

auto VacuumBuilder::rebuild(const Index& current_index, std::span<const SegmentPtr> current_segments,
                            SegmentId first_new_segment, WorkerId owner) const -> Result<VacuumResult> {
    const auto catalog = make_catalog(current_segments);
    auto new_index = current_index.make_empty();
    new_index.reserve(current_index.stats().size);
    std::vector<SegmentPtr> new_segments;
    new_segments.push_back(std::make_shared<Segment>(first_new_segment, owner));
    VacuumStats stats{.old_segments = current_segments.size()};

    for (const auto& entry : current_index.entries()) {
        const auto found = catalog.find(entry.record.segment_id);
        if (found == catalog.end()) {
            return fail(ErrorCode::invalid_reference, "index points to a segment absent from the catalog");
        }
        auto record = found->second->read(entry.record);
        if (!record) {
            return unexpected(record.error());
        }
        const RecordInput input{
            .sequence = record->sequence,
            .opcode = record->opcode,
            .type = record->type,
            .flags = record->flags,
            .key_hash = record->key_hash,
            .expire_at_ns = record->expire_at_ns,
            .key = record->key,
            .value = record->value,
        };
        auto ref = new_segments.back()->append(input);
        if (!ref && ref.error().code == ErrorCode::segment_full) {
            if (auto sealed = new_segments.back()->seal(); !sealed) {
                return unexpected(sealed.error());
            }
            new_segments.push_back(
                std::make_shared<Segment>(SegmentId{first_new_segment.value + new_segments.size()}, owner));
            ref = new_segments.back()->append(input);
        }
        if (!ref) {
            return unexpected(ref.error());
        }
        if (auto live = new_segments.back()->mark_live(*ref); !live) {
            return unexpected(live.error());
        }
        new_index.insert_or_assign(entry.key, *ref);
        ++stats.records_copied;
        stats.bytes_copied += ref->size.value;
    }
    for (const auto& segment : new_segments) {
        if (segment->state() == SegmentState::active) {
            if (auto sealed = segment->seal(); !sealed) {
                return unexpected(sealed.error());
            }
        }
    }
    stats.new_segments = new_segments.size();
    return VacuumResult{.index = std::move(new_index), .segments = std::move(new_segments), .stats = stats};
}

} // namespace glyphastore
