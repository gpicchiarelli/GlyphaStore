#include "glyphastore/server/pair_read_generation.hpp"
#include "test.hpp"

#include <array>
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

GLYPHA_TEST("paired compact base preserves inline boundary and multi-block keys") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{10});
    auto generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(generation.has_value());

    const std::array keys{
        std::string(16, 'i'),
        std::string(17, 'e'),
        std::string((64U * 1024U) + 1U, 'l'),
    };
    std::array<glyphastore::server::ReadMutation, keys.size()> mutations{};
    for (std::size_t index = 0; index < keys.size(); ++index) {
        const auto& key = keys[index];
        mutations[index] = {
            .key = {key, glyphastore::hash_key_routing(key, routing)},
            .record = append(*segment, routing, key, "value", index + 1U, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
    }
    generation =
        glyphastore::server::PairReadGeneration::publish(std::move(*generation), mutations, mutations.size());
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE((*generation)->base_entries() == keys.size());
    GLYPHA_REQUIRE((*generation)->delta_entries() == 0);
    for (const auto& key : keys) {
        const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
        const auto found = (*generation)->get(hashed, 0);
        GLYPHA_REQUIRE(found.has_value());
        GLYPHA_REQUIRE(text(*found) == "value");
    }
}

GLYPHA_TEST("paired incremental merge preserves cut and post-cut visibility with two levels") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{12});
    auto generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(generation.has_value());
    const auto hashed = [&](const std::string_view key) {
        return glyphastore::HashedKey{key, glyphastore::hash_key_routing(key, routing)};
    };

    std::array<glyphastore::server::ReadMutation, 3> base_mutations{
        glyphastore::server::ReadMutation{
            .key = hashed("a"),
            .record = append(*segment, routing, "a", "a1", 1, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
        glyphastore::server::ReadMutation{
            .key = hashed("b"),
            .record = append(*segment, routing, "b", "b1", 2, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
        glyphastore::server::ReadMutation{
            .key = hashed("c"),
            .record = append(*segment, routing, "c", "c1", 3, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
    };
    generation = glyphastore::server::PairReadGeneration::publish(std::move(*generation), base_mutations, 3);
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE((*generation)->base_entries() == 3);
    GLYPHA_REQUIRE((*generation)->delta_entries() == 0);

    std::array<glyphastore::server::ReadMutation, 3> cut_mutations{
        glyphastore::server::ReadMutation{
            .key = hashed("a"),
            .record = append(*segment, routing, "a", "a2", 4, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
        glyphastore::server::ReadMutation{
            .key = hashed("b"),
            .record = append(*segment, routing, "b", {}, 5, glyphastore::Opcode::erase),
            .segment = segment,
            .opcode = glyphastore::Opcode::erase},
        glyphastore::server::ReadMutation{
            .key = hashed("d"),
            .record = append(*segment, routing, "d", "d1", 6, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
    };
    generation =
        glyphastore::server::PairReadGeneration::publish_incremental(std::move(*generation), cut_mutations);
    GLYPHA_REQUIRE(generation.has_value());
    auto merge = glyphastore::server::PairReadGeneration::start_incremental_merge(*generation, 100);
    GLYPHA_REQUIRE(merge.has_value());

    auto first_quantum = glyphastore::server::PairReadGeneration::advance_incremental_merge(**merge, 4);
    GLYPHA_REQUIRE(first_quantum.has_value());
    GLYPHA_REQUIRE(*first_quantum <= 4);
    GLYPHA_REQUIRE(!glyphastore::server::PairReadGeneration::merge_ready(**merge));
    GLYPHA_REQUIRE(text(*(*generation)->get(hashed("a"), 0)) == "a2");
    GLYPHA_REQUIRE(!(*generation)->get(hashed("b"), 0).has_value());
    GLYPHA_REQUIRE(text(*(*generation)->get(hashed("d"), 0)) == "d1");

    std::array<glyphastore::server::ReadMutation, 3> post_mutations{
        glyphastore::server::ReadMutation{
            .key = hashed("a"),
            .record = append(*segment, routing, "a", "a3", 7, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
        glyphastore::server::ReadMutation{
            .key = hashed("d"),
            .record = append(*segment, routing, "d", {}, 8, glyphastore::Opcode::erase),
            .segment = segment,
            .opcode = glyphastore::Opcode::erase},
        glyphastore::server::ReadMutation{
            .key = hashed("e"),
            .record = append(*segment, routing, "e", "e1", 9, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put},
    };
    generation = glyphastore::server::PairReadGeneration::publish_incremental(std::move(*generation),
                                                                              post_mutations, merge->get());
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE(glyphastore::server::PairReadGeneration::merge_post_entries(**merge) == 3);

    std::size_t quanta{1};
    while (!glyphastore::server::PairReadGeneration::merge_ready(**merge)) {
        auto advanced = glyphastore::server::PairReadGeneration::advance_incremental_merge(**merge, 4'096);
        GLYPHA_REQUIRE(advanced.has_value());
        GLYPHA_REQUIRE(*advanced <= 4'096);
        ++quanta;
    }
    GLYPHA_REQUIRE(quanta > 2);
    generation =
        glyphastore::server::PairReadGeneration::finish_incremental_merge(std::move(*generation), **merge);
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE((*generation)->base_entries() == 3);
    GLYPHA_REQUIRE((*generation)->delta_entries() == 3);
    GLYPHA_REQUIRE((*generation)->visible_through() == 9);
    GLYPHA_REQUIRE(text(*(*generation)->get(hashed("a"), 0)) == "a3");
    GLYPHA_REQUIRE(!(*generation)->get(hashed("b"), 0).has_value());
    GLYPHA_REQUIRE(text(*(*generation)->get(hashed("c"), 0)) == "c1");
    GLYPHA_REQUIRE(!(*generation)->get(hashed("d"), 0).has_value());
    GLYPHA_REQUIRE(text(*(*generation)->get(hashed("e"), 0)) == "e1");
}

GLYPHA_TEST("paired incremental merge applies bounded post-cut backpressure before publication") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{13});
    auto generation = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(generation.has_value());
    const auto mutation = [&](const std::string_view key, const std::uint64_t sequence) {
        return glyphastore::server::ReadMutation{
            .key = {key, glyphastore::hash_key_routing(key, routing)},
            .record = append(*segment, routing, key, "value", sequence, glyphastore::Opcode::put),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
    };
    auto cut = mutation("cut", 1);
    generation = glyphastore::server::PairReadGeneration::publish_incremental(std::move(*generation),
                                                                              std::span{&cut, 1});
    GLYPHA_REQUIRE(generation.has_value());
    auto merge = glyphastore::server::PairReadGeneration::start_incremental_merge(*generation, 2);
    GLYPHA_REQUIRE(merge.has_value());
    std::array post{mutation("post-a", 2), mutation("post-b", 3)};
    GLYPHA_REQUIRE(glyphastore::server::PairReadGeneration::can_publish_incremental(
        **generation, merge->get(), post.size()));
    generation = glyphastore::server::PairReadGeneration::publish_incremental(std::move(*generation), post,
                                                                              merge->get());
    GLYPHA_REQUIRE(generation.has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::PairReadGeneration::can_publish_incremental(**generation, merge->get(), 1));
    GLYPHA_REQUIRE(glyphastore::server::PairReadGeneration::merge_post_entries(**merge) == 2);
}

GLYPHA_TEST("paired incremental merge rejects publication from another generation lineage") {
    const glyphastore::WorkerRoutingState routing{};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{14});
    auto cut = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(cut.has_value());
    const std::string cut_key{"lineage-cut"};
    const glyphastore::server::ReadMutation cut_mutation{
        .key = {cut_key, glyphastore::hash_key_routing(cut_key, routing)},
        .record = append(*segment, routing, cut_key, "cut", 1, glyphastore::Opcode::put),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    cut = glyphastore::server::PairReadGeneration::publish_incremental(std::move(*cut),
                                                                       std::span{&cut_mutation, 1});
    GLYPHA_REQUIRE(cut.has_value());
    auto merge = glyphastore::server::PairReadGeneration::start_incremental_merge(*cut, 8);
    GLYPHA_REQUIRE(merge.has_value());

    auto unrelated = glyphastore::server::PairReadGeneration::empty(routing);
    GLYPHA_REQUIRE(unrelated.has_value());
    const std::string post_key{"wrong-lineage"};
    const glyphastore::server::ReadMutation post_mutation{
        .key = {post_key, glyphastore::hash_key_routing(post_key, routing)},
        .record = append(*segment, routing, post_key, "post", 2, glyphastore::Opcode::put),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    auto rejected = glyphastore::server::PairReadGeneration::publish_incremental(
        std::move(*unrelated), std::span{&post_mutation, 1}, merge->get());
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_argument);
    GLYPHA_REQUIRE(glyphastore::server::PairReadGeneration::merge_post_entries(**merge) == 0);
}
