#include "glyphastore/server/pair_read_generation.hpp"
#include "test.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

[[nodiscard]] auto bytes(const std::string_view text) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

[[nodiscard]] auto text(const glyphastore::OwnedValue& value) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

[[nodiscard]] auto append(glyphastore::Segment& segment, const glyphastore::WorkerRoutingState routing,
                          const std::string_view key, const std::string_view value,
                          const std::uint64_t sequence, const glyphastore::Opcode opcode,
                          const std::uint64_t expire_at_ns = 0) -> glyphastore::RecordRef {
    auto record = segment.append({.sequence = glyphastore::SequenceNumber{sequence},
                                  .opcode = opcode,
                                  .key_hash = glyphastore::hash_key_routing(key, routing),
                                  .expire_at_ns = expire_at_ns,
                                  .key = bytes(key),
                                  .value = bytes(value)});
    GLYPHA_REQUIRE(record.has_value());
    return *record;
}

} // namespace

GLYPHA_TEST("paired read generation overlays delta on base with tombstones and TTL") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{7});
    auto generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(generation.has_value());

    const std::string key{"generation-key"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    const auto first = append(*segment, routing, key, "base-value", 1, glyphastore::Opcode::put);
    const glyphastore::server::ReadMutation first_mutation{
        .key = hashed, .record = first, .segment = segment, .opcode = glyphastore::Opcode::put};
    generation = glyphastore::server::PairReadGeneration::publish(std::move(*generation),
                                                                  std::span{&first_mutation, 1}, 1);
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE((*generation)->base_entries() == 1);
    GLYPHA_REQUIRE((*generation)->delta_entries() == 0);
    auto found = (*generation)->get(hashed, 1);
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "base-value");

    const auto erased = append(*segment, routing, key, {}, 2, glyphastore::Opcode::erase);
    const glyphastore::server::ReadMutation erase_mutation{
        .key = hashed, .record = erased, .segment = segment, .opcode = glyphastore::Opcode::erase};
    generation = glyphastore::server::PairReadGeneration::publish(std::move(*generation),
                                                                  std::span{&erase_mutation, 1}, 8);
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE(!(*generation)->get(hashed, 1).has_value());

    const auto expiring = append(*segment, routing, key, "short", 3, glyphastore::Opcode::put, 10);
    const glyphastore::server::ReadMutation expiring_mutation{
        .key = hashed, .record = expiring, .segment = segment, .opcode = glyphastore::Opcode::put};
    generation = glyphastore::server::PairReadGeneration::publish(std::move(*generation),
                                                                  std::span{&expiring_mutation, 1}, 8);
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE((*generation)->get(hashed, 9).has_value());
    GLYPHA_REQUIRE(!(*generation)->get(hashed, 10).has_value());
    GLYPHA_REQUIRE((*generation)->visible_through() == 3);
}

GLYPHA_TEST("paired read generation owns the exact Segment generation pin") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{9}, glyphastore::WorkerId{},
                                                          glyphastore::GenerationId{4});
    std::weak_ptr<glyphastore::Segment> lifetime = segment;
    auto generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(generation.has_value());
    const std::string key{"pinned-key"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    const auto record = append(*segment, routing, key, "pinned-value", 11, glyphastore::Opcode::put);
    std::vector<glyphastore::server::ReadMutation> mutations;
    mutations.push_back(
        {.key = hashed, .record = record, .segment = segment, .opcode = glyphastore::Opcode::put});
    generation = glyphastore::server::PairReadGeneration::publish(std::move(*generation), mutations, 8);
    GLYPHA_REQUIRE(generation.has_value());
    mutations.clear();
    segment.reset();
    GLYPHA_REQUIRE(!lifetime.expired());
    const auto found = (*generation)->get(hashed, 0);
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "pinned-value");
    generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(lifetime.expired());
}
