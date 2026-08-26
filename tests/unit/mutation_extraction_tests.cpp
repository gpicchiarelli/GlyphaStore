#include "glyphastore/store/paired/fail_closed_state.hpp"
#include "glyphastore/store/paired/mutation_batch.hpp"
#include "glyphastore/store/paired/mutation_execution.hpp"
#include "test.hpp"

#include "glyphastore/core/key_hash.hpp"

#include <array>
#include <atomic>

using glyphastore::Error;
using glyphastore::ErrorCode;
using glyphastore::HashedKey;
using glyphastore::store::paired::classify_volatile_mutation_error;
using glyphastore::store::paired::durable_subbatch_end;
using glyphastore::store::paired::FailClosedLaneWake;
using glyphastore::store::paired::FailClosedScope;
using glyphastore::store::paired::kMaximumPublicationBatch;
using glyphastore::store::paired::rewrite_known_not_committed_wire_error;
using glyphastore::store::paired::sync_publication_chunk_cap;

GLYPHA_TEST("mutation_batch durable_subbatch_end stops before duplicate keys") {
    const std::array keys{
        HashedKey{.key = "a", .hash = 1},
        HashedKey{.key = "b", .hash = 2},
        HashedKey{.key = "a", .hash = 1},
        HashedKey{.key = "c", .hash = 3},
    };
    const auto end = durable_subbatch_end(0, keys.size(), [&](const std::size_t i) -> const HashedKey& {
        return keys[i];
    });
    GLYPHA_REQUIRE(end == 2);
    const auto next = durable_subbatch_end(2, keys.size(), [&](const std::size_t i) -> const HashedKey& {
        return keys[i];
    });
    GLYPHA_REQUIRE(next == 4);
    GLYPHA_REQUIRE(sync_publication_chunk_cap(100) == kMaximumPublicationBatch);
    GLYPHA_REQUIRE(sync_publication_chunk_cap(7) == 7);
}

GLYPHA_TEST("mutation_execution rewrite preserves reject polarity and maps INTERNAL_ERROR bucket") {
    Error exhausted{ErrorCode::resource_exhausted, "keep"};
    rewrite_known_not_committed_wire_error(exhausted);
    GLYPHA_REQUIRE(exhausted.code == ErrorCode::resource_exhausted);

    Error io{ErrorCode::io_error, "map"};
    rewrite_known_not_committed_wire_error(io);
    GLYPHA_REQUIRE(io.code == ErrorCode::resource_exhausted);

    bool sticky = false;
    auto classified = classify_volatile_mutation_error(Error{ErrorCode::unavailable, "sticky"}, sticky);
    GLYPHA_REQUIRE(sticky);
    GLYPHA_REQUIRE(classified.code == ErrorCode::unavailable);

    sticky = false;
    classified = classify_volatile_mutation_error(Error{ErrorCode::corrupted_data, "reject"}, sticky);
    GLYPHA_REQUIRE(!sticky);
    GLYPHA_REQUIRE(classified.code == ErrorCode::resource_exhausted);
}

GLYPHA_TEST("FailClosedScope and lane wake views are distinct") {
    std::atomic_uint64_t signal{0};
    const std::array wakes{FailClosedLaneWake{.signal = &signal}, FailClosedLaneWake{}};
    GLYPHA_REQUIRE(wakes[0].signal != nullptr);
    GLYPHA_REQUIRE(wakes[1].signal == nullptr);
    static_assert(static_cast<std::uint8_t>(FailClosedScope::pair_only) !=
                  static_cast<std::uint8_t>(FailClosedScope::pair_and_store));
}
