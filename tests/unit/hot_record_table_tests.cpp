#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_control_group.hpp"
#include "persistence/hot_record_table.hpp"
#include "test.hpp"

#include <array>
#include <cstdint>
#include <memory>
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
    entry.value_size = value.size();
    if (entry.is_inline()) {
        std::copy_n(reinterpret_cast<const std::byte*>(value.data()), value.size(), entry.inline_value);
    } else {
        auto mutable_value = std::make_shared<std::byte[]>(value.size());
        std::copy_n(reinterpret_cast<const std::byte*>(value.data()), value.size(), mutable_value.get());
        entry.heap_value = std::move(mutable_value);
    }
    return entry;
}

GLYPHA_TEST("hot record inline representation is derived from value size") {
    const auto inline_value = std::string(glyphastore::detail::HotRecordEntry::kInlineValueBytes, 'i');
    const auto heap_value = std::string(glyphastore::detail::HotRecordEntry::kInlineValueBytes + 1U, 'h');

    const auto inline_entry = make_entry(11, inline_value);
    GLYPHA_REQUIRE(inline_entry.is_inline());
    GLYPHA_REQUIRE(inline_entry.reference.sequence == glyphastore::SequenceNumber{11});
    GLYPHA_REQUIRE(inline_entry.value_span().size() == inline_value.size());

    const auto heap_entry = make_entry(12, heap_value);
    GLYPHA_REQUIRE(!heap_entry.is_inline());
    GLYPHA_REQUIRE(heap_entry.reference.sequence == glyphastore::SequenceNumber{12});
    GLYPHA_REQUIRE(heap_entry.value_span().size() == heap_value.size());

    const auto snapshot = glyphastore::detail::HotRecordSnapshot::from_entry(heap_entry);
    GLYPHA_REQUIRE(!snapshot.is_inline());
    GLYPHA_REQUIRE(snapshot.sequence == heap_entry.reference.sequence);
    GLYPHA_REQUIRE(snapshot.value_span().data() == heap_entry.value_span().data());
}

} // namespace

GLYPHA_TEST("hot record table inserts finds replaces and erases with control fingerprints") {
    glyphastore::detail::HotRecordTable table;
    GLYPHA_REQUIRE(table.capacity() == 0);
    const auto key = std::string{"alpha"};
    const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(1, "one")).has_value());
    GLYPHA_REQUIRE(table.capacity() == glyphastore::detail::HotRecordTable::kMinimumCapacity);
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.find(key, hash) != nullptr);
    GLYPHA_REQUIRE(table.find(key, hash)->reference.sequence == glyphastore::SequenceNumber{1});

    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(2, "two")).has_value());
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.find(key, hash)->reference.sequence == glyphastore::SequenceNumber{2});

    GLYPHA_REQUIRE(table.erase(key, hash));
    GLYPHA_REQUIRE(table.size() == 0);
    GLYPHA_REQUIRE(table.tombstone_count() == 1);
    GLYPHA_REQUIRE(table.find(key, hash) == nullptr);
    GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(3, "three")).has_value());
    GLYPHA_REQUIRE(table.size() == 1);
    GLYPHA_REQUIRE(table.tombstone_count() == 0);
    GLYPHA_REQUIRE(table.find(key, hash)->reference.sequence == glyphastore::SequenceNumber{3});
}

GLYPHA_TEST("hot record accounting charges bucket storage exactly once") {
    const auto inline_payload = glyphastore::detail::hot_record_accounted_bytes(8, 16);
    GLYPHA_REQUIRE(inline_payload.has_value());
    GLYPHA_REQUIRE(*inline_payload == 8);

    const auto heap_payload = glyphastore::detail::hot_record_accounted_bytes(8, 64);
    GLYPHA_REQUIRE(heap_payload.has_value());
    GLYPHA_REQUIRE(*heap_payload == 72);
    GLYPHA_REQUIRE(glyphastore::detail::hot_record_slot_bytes() > 0);
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
        GLYPHA_REQUIRE(found->reference.sequence == glyphastore::SequenceNumber{i + 1});
        const auto span = found->value_span();
        GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(span.data()), span.size()) == key);
    }
    GLYPHA_REQUIRE(table.size() == 96);
    GLYPHA_REQUIRE(table.capacity() == 128);
}

GLYPHA_TEST("hot record table erase_if and clear drop resident entries") {
    glyphastore::detail::HotRecordTable table;
    for (std::uint64_t i = 0; i < 16; ++i) {
        const auto key = std::string{"e"} + std::to_string(i);
        const auto hash = glyphastore::hash_key(std::as_bytes(std::span{key}));
        GLYPHA_REQUIRE(table.insert_or_assign(key, hash, make_entry(i + 1, "v")).has_value());
    }
    table.erase_if(
        [](const std::string& key, std::uint64_t, const auto&) { return key == "e0" || key == "e1"; });
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

GLYPHA_TEST("hot record reserve plan grows geometrically at load 0.75") {
    const auto idle = glyphastore::detail::plan_hot_record_reserve(10, 0, 64);
    GLYPHA_REQUIRE(!idle.overflow);
    GLYPHA_REQUIRE(idle.target == 0);

    const auto within = glyphastore::detail::plan_hot_record_reserve(47, 1, 64);
    GLYPHA_REQUIRE(!within.overflow);
    GLYPHA_REQUIRE(within.target == 0);

    const auto grow = glyphastore::detail::plan_hot_record_reserve(48, 1, 64);
    GLYPHA_REQUIRE(!grow.overflow);
    GLYPHA_REQUIRE(grow.target == 128);
}

GLYPHA_TEST("hot record replacement at occupancy boundary does not grow") {
    glyphastore::detail::HotRecordTable table;
    for (std::uint64_t index = 0; index < 48; ++index) {
        const auto key = std::string{"boundary-"} + std::to_string(index);
        GLYPHA_REQUIRE(
            table.insert_or_assign(key, glyphastore::hash_key(key), make_entry(index + 1, "v")).has_value());
    }
    GLYPHA_REQUIRE(table.capacity() == 64);
    GLYPHA_REQUIRE(table
                       .insert_or_assign("boundary-0", glyphastore::hash_key("boundary-0"),
                                         make_entry(100, "replacement"))
                       .has_value());
    GLYPHA_REQUIRE(table.capacity() == 64);

    GLYPHA_REQUIRE(
        table.insert_or_assign("boundary-48", glyphastore::hash_key("boundary-48"), make_entry(101, "v"))
            .has_value());
    GLYPHA_REQUIRE(table.capacity() == 128);
}
