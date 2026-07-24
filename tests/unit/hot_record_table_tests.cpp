#include "persistence/hot_record_table.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_control_group.hpp"
#include "test.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

auto make_entry(const std::uint64_t sequence, const std::string_view value)
    -> glyphastore::detail::HotRecordEntry {
    glyphastore::detail::HotRecordEntry entry;
    entry.reference = {glyphastore::SegmentId{1}, glyphastore::RecordOffset{100},
                       glyphastore::RecordSize{static_cast<std::uint32_t>(value.size())},
                       glyphastore::SequenceNumber{sequence}, glyphastore::GenerationId{1}};
    entry.sequence = glyphastore::SequenceNumber{sequence};
    entry.value_size = value.size();
    entry.value_inline = value.size() <= glyphastore::detail::HotRecordEntry::kInlineValueBytes;
    if (entry.value_inline) {
        std::copy_n(reinterpret_cast<const std::byte*>(value.data()), value.size(), entry.inline_value);
    }
    entry.accounted_bytes =
        *glyphastore::detail::hot_record_accounted_bytes(/*key_bytes=*/8, value.size());
    return entry;
}

} // namespace

GLYPHA_TEST("hot record table inserts finds replaces and erases with control fingerprints") {
    glyphastore::detail::HotRecordTable table;
    const auto key = std::string{"alpha"};
    const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(1, "one")).has_value());
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.find(key, hash) != nullptr);
    GLYPHA_REQUIRE(table.find(key, hash)->sequence == glyphastore::SequenceNumber{1});

    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(2, "two")).has_value());
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.find(key, hash)->sequence == glyphastore::SequenceNumber{2});

    GLYPHA_REQUIRE(table.erase(key, hash));
    GLYPHA_REQUIRE(table.size() == 0);
    GLYPHA_REQUIRE(table.tombstone_count() == 1);
    GLYPHA_REQUIRE(table.find(key, hash) == nullptr);
    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(3, "three")).has_value());
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.tombstone_count() == 0);
    GLYPHA_REQUIRE(table.find(key, hash)->sequence == glyphastore::SequenceNumber{3});
}

GLYPHA_TEST("hot record table resolves same-H2 collisions by full key bytes") {
    glyphastore::detail::HotRecordTable table;
    // Force many inserts so probe groups fill; identity stays full key compare.
    for (std::uint64_t i = 0; i < 96; ++i) {
        const auto key = std::string{"k"} + std::to_string(i);
        const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
        GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(i + 1, key)).has_value());
    }
    for (std::uint64_t i = 0; i < 96; ++i) {
        const auto key = std::string{"k"} + std::to_string(i);
        const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
        const auto* found = table.find(key, hash);
        GLYPHA_REQUIRE(found != nullptr);
        GLYPHA_REQUIRE(found->sequence == glyphastore::SequenceNumber{i + 1});
        const auto span = found->value_span();
        GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(span.data()), span.size()) == key);
    }
    GLYPHA_REQUIRE(table.size() == 96);
    GLYPHA_REQUIRE(table.capacity() >= 192);
}

GLYPHA_TEST("hot record table erase_if and clear drop resident entries") {
    glyphastore::detail::HotRecordTable table;
    for (std::uint64_t i = 0; i < 16; ++i) {
        const auto key = std::string{"e"} + std::to_string(i);
        const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
        GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(i + 1, "v")).has_value());
    }
    table.erase_if([](const std::string& key, std::uint64_t, const auto&) {
        return key == "e0" || key == "e1";
    });
    GLYPHA_REQUIRE(table.size() == 14);
    table.clear();
    GLYPHA_REQUIRE(table.size() == 0);
    GLYPHA_REQUIRE(table.tombstone_count() == 0);
    const auto probe = std::string{"e2"};
    GLYPHA_REQUIRE(table.find(probe, glyphastore::hash_key(probe)) == nullptr);
}

GLYPHA_TEST("hot record table control matcher agrees with scalar masks") {
    std::array<std::uint8_t, glyphastore::kSwissGroupSize> control{
        glyphastore::kSwissEmpty,
        0x11,
        glyphastore::kSwissDeleted,
        0x22,
        0x11,
        glyphastore::kSwissEmpty,
        0x7F,
        0x01,
    };
    for (unsigned byte = 0; byte < 256; ++byte) {
        GLYPHA_REQUIRE(
            glyphastore::detail::equal_byte_mask(control.data(), static_cast<std::uint8_t>(byte)) ==
            glyphastore::detail::equal_byte_mask_scalar(control.data(), static_cast<std::uint8_t>(byte)));
    }
}

GLYPHA_TEST("hot record reserve plan doubles capacity at load 0.5") {
    const auto idle = glyphastore::detail::plan_hot_record_reserve(10, 0, 64);
    GLYPHA_REQUIRE(!idle.overflow);
    GLYPHA_REQUIRE(idle.target == 0);

    const auto grow = glyphastore::detail::plan_hot_record_reserve(30, 10, 64);
    GLYPHA_REQUIRE(!grow.overflow);
    GLYPHA_REQUIRE(grow.target >= 80);
}
