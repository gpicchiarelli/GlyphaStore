#include "glyphastore/index/index.hpp"
#include "glyphastore/vacuum/vacuum.hpp"
#include "test.hpp"

#include <string_view>

namespace {
auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}
auto append(glyphastore::Segment& segment, std::uint64_t seq, glyphastore::Opcode op, std::string_view key,
            std::string_view value = {}, std::uint64_t hash = 7, std::uint64_t expiry = 0)
    -> glyphastore::RecordRef {
    auto ref = segment.append({
        .sequence = glyphastore::SequenceNumber{seq},
        .opcode = op,
        .key_hash = hash,
        .expire_at_ns = expiry,
        .key = bytes(key),
        .value = bytes(value),
    });
    GLYPHA_REQUIRE(ref.has_value());
    return *ref;
}
} // namespace

GLYPHA_TEST("rebuild chooses highest sequence independent of segment order and hash collision") {
    auto first = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{1});
    auto second = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{2});
    const auto old = append(*first, 10, glyphastore::Opcode::put, "alpha", "old", 99);
    const auto other = append(*first, 11, glyphastore::Opcode::put, "beta", "other", 99);
    const auto newest = append(*second, 30, glyphastore::Opcode::put, "alpha", "new", 99);
    static_cast<void>(old);
    static_cast<void>(other);
    const std::vector<glyphastore::SegmentPtr> segments{second, first};
    auto rebuilt = glyphastore::rebuild_index_from_segments(segments);
    GLYPHA_REQUIRE(rebuilt.has_value());
    GLYPHA_REQUIRE(rebuilt->index.find("alpha") == newest);
    GLYPHA_REQUIRE(rebuilt->index.find("beta").has_value());
}

GLYPHA_TEST("rebuild applies tombstone and expiration semantics") {
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{3});
    static_cast<void>(append(*segment, 1, glyphastore::Opcode::put, "gone", "v"));
    static_cast<void>(append(*segment, 2, glyphastore::Opcode::erase, "gone"));
    static_cast<void>(append(*segment, 3, glyphastore::Opcode::put, "expired", "v", 8, 100));
    const std::vector<glyphastore::SegmentPtr> segments{segment};
    auto rebuilt = glyphastore::rebuild_index_from_segments(segments, 101);
    GLYPHA_REQUIRE(rebuilt.has_value());
    GLYPHA_REQUIRE(!rebuilt->index.find("gone").has_value());
    GLYPHA_REQUIRE(!rebuilt->index.find("expired").has_value());
    GLYPHA_REQUIRE(rebuilt->stats.tombstones == 1);
    GLYPHA_REQUIRE(rebuilt->stats.expired == 1);
}

GLYPHA_TEST("vacuum copies exactly visible records and leaves source immutable") {
    auto source = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{10});
    auto first = append(*source, 1, glyphastore::Opcode::put, "key", "old");
    auto current = append(*source, 2, glyphastore::Opcode::put, "key", "new");
    GLYPHA_REQUIRE(source->mark_live(current).has_value());
    GLYPHA_REQUIRE(source->seal().has_value());
    glyphastore::Index index;
    GLYPHA_REQUIRE(index.insert_or_assign("key", current).has_value());
    const auto before = source->stats();
    const std::vector<glyphastore::SegmentPtr> segments{source};

    glyphastore::VacuumBuilder builder;
    auto vacuumed = builder.rebuild(index, segments, glyphastore::SegmentId{100});
    GLYPHA_REQUIRE(vacuumed.has_value());
    GLYPHA_REQUIRE(vacuumed->index.find("key").has_value());
    GLYPHA_REQUIRE(vacuumed->segments.size() == 1);
    GLYPHA_REQUIRE(vacuumed->stats.records_copied == 1);
    GLYPHA_REQUIRE(source->stats().record_count == before.record_count);
    GLYPHA_REQUIRE(source->read(first).has_value());
}

GLYPHA_TEST("selective vacuum preserves references outside its candidate set") {
    auto candidate = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{20});
    const auto moved = append(*candidate, 10, glyphastore::Opcode::put, "moved", "value");
    GLYPHA_REQUIRE(candidate->mark_live(moved).has_value());
    GLYPHA_REQUIRE(candidate->seal().has_value());

    auto retained = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{21});
    const auto stable = append(*retained, 11, glyphastore::Opcode::put, "stable", "value");
    GLYPHA_REQUIRE(retained->mark_live(stable).has_value());

    glyphastore::Index index;
    GLYPHA_REQUIRE(index.insert_or_assign("moved", moved).has_value());
    GLYPHA_REQUIRE(index.insert_or_assign("stable", stable).has_value());
    const std::vector<glyphastore::SegmentPtr> segments{candidate, retained};
    const std::vector<glyphastore::SegmentId> candidates{candidate->id()};

    glyphastore::VacuumBuilder builder;
    auto vacuumed =
        builder.rebuild(index, segments, candidates, []() -> glyphastore::Result<glyphastore::SegmentPtr> {
            return std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{100});
        });
    GLYPHA_REQUIRE(vacuumed.has_value());
    GLYPHA_REQUIRE(vacuumed->index.find("stable") == stable);
    GLYPHA_REQUIRE(vacuumed->index.find("moved").has_value());
    GLYPHA_REQUIRE(vacuumed->index.find("moved") != moved);
    GLYPHA_REQUIRE(vacuumed->stats.source_records_verified == 1);
    GLYPHA_REQUIRE(vacuumed->stats.records_copied == 1);
    GLYPHA_REQUIRE(vacuumed->segments.size() == 1);
}

GLYPHA_TEST("selective vacuum drops expired candidates without allocating output") {
    auto candidate = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{30});
    const auto expired = append(*candidate, 20, glyphastore::Opcode::put, "expired", "value", 7, 100);
    GLYPHA_REQUIRE(candidate->mark_live(expired).has_value());
    GLYPHA_REQUIRE(candidate->seal().has_value());
    glyphastore::Index index;
    GLYPHA_REQUIRE(index.insert_or_assign("expired", expired).has_value());
    const std::vector<glyphastore::SegmentPtr> segments{candidate};
    const std::vector<glyphastore::SegmentId> candidates{candidate->id()};
    std::size_t allocations{};

    glyphastore::VacuumBuilder builder;
    auto vacuumed = builder.rebuild(
        index, segments, candidates,
        [&allocations]() -> glyphastore::Result<glyphastore::SegmentPtr> {
            ++allocations;
            return std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{101});
        },
        101);
    GLYPHA_REQUIRE(vacuumed.has_value());
    GLYPHA_REQUIRE(!vacuumed->index.find("expired").has_value());
    GLYPHA_REQUIRE(vacuumed->segments.empty());
    GLYPHA_REQUIRE(vacuumed->stats.expired_records_dropped == 1);
    GLYPHA_REQUIRE(allocations == 0);
}
