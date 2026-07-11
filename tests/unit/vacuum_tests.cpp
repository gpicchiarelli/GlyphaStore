#include "glyphastore/vacuum/vacuum.hpp"
#include "test.hpp"

#include <memory>
#include <string_view>
#include <vector>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto append(glyphastore::Segment& segment, std::uint64_t seq, std::string_view key,
            std::string_view value = {}) -> glyphastore::RecordRef {
    auto ref = segment.append({
        .sequence = glyphastore::SequenceNumber{seq},
        .opcode = glyphastore::Opcode::put,
        .key = bytes(key),
        .value = bytes(value),
    });
    GLYPHA_REQUIRE(ref.has_value());
    return *ref;
}

auto sealed_segment_with_ratio(glyphastore::SegmentId id, std::size_t live_records, std::size_t total_records,
                               std::string_view value = "value") -> glyphastore::SegmentPtr {
    auto segment = std::make_shared<glyphastore::Segment>(id);
    std::vector<glyphastore::RecordRef> refs;
    refs.reserve(total_records);
    for (std::size_t index = 0; index < total_records; ++index) {
        refs.push_back(append(*segment, index + 1, "key", value));
    }
    for (std::size_t index = 0; index < live_records; ++index) {
        GLYPHA_REQUIRE(segment->mark_live(refs[index]).has_value());
    }
    GLYPHA_REQUIRE(segment->seal().has_value());
    return segment;
}
} // namespace

GLYPHA_TEST("vacuum planner excludes active and high live ratio segments") {
    const auto active = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{1});
    const auto active_ref = append(*active, 1, "active", "value");
    GLYPHA_REQUIRE(active->mark_live(active_ref).has_value());

    const auto dense = sealed_segment_with_ratio(glyphastore::SegmentId{2}, 20, 20, std::string(512, 'x'));
    const auto sparse = sealed_segment_with_ratio(glyphastore::SegmentId{3}, 1, 20);

    const std::vector<glyphastore::SegmentPtr> segments{active, dense, sparse};
    const glyphastore::VacuumPlanner planner{{.maximum_live_ratio = 0.25, .maximum_segments_per_run = 4}};
    const auto candidates = planner.candidates(segments);

    GLYPHA_REQUIRE(candidates.size() == 1);
    GLYPHA_REQUIRE(candidates.front() == glyphastore::SegmentId{3});
}

GLYPHA_TEST("vacuum planner ranks lowest live ratio first and respects per run limit") {
    const auto first = sealed_segment_with_ratio(glyphastore::SegmentId{10}, 3, 8);
    const auto second = sealed_segment_with_ratio(glyphastore::SegmentId{11}, 1, 8);
    const auto third = sealed_segment_with_ratio(glyphastore::SegmentId{12}, 2, 8);
    const std::vector<glyphastore::SegmentPtr> segments{first, second, third};

    const glyphastore::VacuumPlanner planner{{.maximum_live_ratio = 0.50, .maximum_segments_per_run = 2}};
    const auto candidates = planner.candidates(segments);

    GLYPHA_REQUIRE(candidates.size() == 2);
    GLYPHA_REQUIRE(candidates[0] == glyphastore::SegmentId{11});
    GLYPHA_REQUIRE(candidates[1] == glyphastore::SegmentId{12});
}

GLYPHA_TEST("vacuum planner ignores empty sealed segments") {
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{20});
    GLYPHA_REQUIRE(segment->seal().has_value());
    const std::vector<glyphastore::SegmentPtr> segments{segment};

    const glyphastore::VacuumPlanner planner;
    GLYPHA_REQUIRE(planner.candidates(segments).empty());
}
