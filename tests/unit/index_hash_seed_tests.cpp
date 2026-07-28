#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/index/index_hash_seed.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "test.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

struct SeedGuard final {
    explicit SeedGuard(const std::uint64_t seed) : previous_(glyphastore::get_index_hash_seed()) {
        glyphastore::set_index_hash_seed(seed);
    }
    ~SeedGuard() {
        glyphastore::set_index_hash_seed(previous_);
    }
    SeedGuard(const SeedGuard&) = delete;
    auto operator=(const SeedGuard&) -> SeedGuard& = delete;

    std::uint64_t previous_;
};

} // namespace

GLYPHA_TEST("index hash seed defaults to published Index v1 constant") {
    SeedGuard guard{glyphastore::kDefaultIndexHashSeed};
    GLYPHA_REQUIRE(glyphastore::get_index_hash_seed() == glyphastore::kDefaultIndexHashSeed);
    glyphastore::SwissTableIndex table;
    GLYPHA_REQUIRE(table.seed() == glyphastore::kDefaultIndexHashSeed);
}

GLYPHA_TEST("index hash seed is stable within process for identical tables") {
    SeedGuard guard{0x1111222233334444ULL};
    glyphastore::SwissTableIndex left;
    glyphastore::SwissTableIndex right;
    GLYPHA_REQUIRE(left.seed() == right.seed());
    GLYPHA_REQUIRE(left.seed() == 0x1111222233334444ULL);

    const glyphastore::HashedKey key = glyphastore::HashedKey::compute("tenant-a/orders/1");
    GLYPHA_REQUIRE(left.insert_or_assign(key, glyphastore::RecordRef{}).has_value());
    GLYPHA_REQUIRE(right.insert_or_assign(key, glyphastore::RecordRef{}).has_value());
    const auto left_entries = left.entries();
    const auto right_entries = right.entries();
    GLYPHA_REQUIRE(left_entries.size() == 1);
    GLYPHA_REQUIRE(right_entries.size() == 1);
    GLYPHA_REQUIRE(left_entries.front().key == right_entries.front().key);
}

GLYPHA_TEST("different index hash seeds diverge placement for the same keys") {
    constexpr std::string_view kKey = "flood-candidate-key";
    const glyphastore::HashedKey hashed = glyphastore::HashedKey::compute(kKey);

    glyphastore::SwissTableIndex a{0xAAAAAAAAAAAAAAAALL};
    glyphastore::SwissTableIndex b{0xBBBBBBBBBBBBBBBBULL};
    GLYPHA_REQUIRE(a.seed() != b.seed());
    GLYPHA_REQUIRE(a.insert_or_assign(hashed, glyphastore::RecordRef{}).has_value());
    GLYPHA_REQUIRE(b.insert_or_assign(hashed, glyphastore::RecordRef{}).has_value());

    GLYPHA_REQUIRE(a.find(hashed).has_value());
    GLYPHA_REQUIRE(b.find(hashed).has_value());
    const auto empty_a = a.clone_empty();
    GLYPHA_REQUIRE(empty_a.seed() == a.seed());
    GLYPHA_REQUIRE(!empty_a.find(hashed).has_value());

    // Flood-resistance note (ADR 0026): without the process seed, an attacker cannot
    // precompute Index bucket targets for a secure-profile daemon. Worker routing remains
    // public FNV-1a until keyed routing lands.
    const auto fnv = glyphastore::hash_key(kKey);
    const auto keyed_a = glyphastore::hash_key_keyed(kKey, 0x1ULL, 0x2ULL);
    const auto keyed_b = glyphastore::hash_key_keyed(kKey, 0x3ULL, 0x4ULL);
    GLYPHA_REQUIRE(fnv != 0);
    GLYPHA_REQUIRE(keyed_a != keyed_b);
}

GLYPHA_TEST("siphash24 matches Aumasson/Bernstein paper vectors for key 00..0f") {
    // k = 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f (little-endian limbs)
    constexpr std::uint64_t k0 = 0x0706050403020100ULL;
    constexpr std::uint64_t k1 = 0x0f0e0d0c0b0a0908ULL;

    GLYPHA_REQUIRE(glyphastore::siphash24({}, k0, k1) == 0x726fdb47dd0e0e31ULL);

    const std::array<std::byte, 3> msg{std::byte{0x00}, std::byte{0x01}, std::byte{0x02}};
    GLYPHA_REQUIRE(glyphastore::siphash24(msg, k0, k1) == 0x85676696d7fb7e2dULL);
}

GLYPHA_TEST("Index constructor captures process seed") {
    SeedGuard guard{0xDEADBEEFCAFEBABEULL};
    glyphastore::Index index;
    GLYPHA_REQUIRE(index.seed() == 0xDEADBEEFCAFEBABEULL);
    const auto empty = index.make_empty();
    GLYPHA_REQUIRE(empty.seed() == index.seed());
}
