#include "experimental/paired_shard.hpp"
#include "experimental/spsc_ring.hpp"
#include "test.hpp"

// ADR 0036 (proposed) — prototype evidence for a future generation slot-pool.
// These tests exercise src/experimental/paired_shard.cpp only. They do not authorize
// production ShardPairRuntime landing; see docs/adr/0036-generation-slot-pool-publish.md.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

[[nodiscard]] auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return std::as_bytes(std::span{value.data(), value.size()});
}

[[nodiscard]] auto text(const glyphastore::experimental::PrototypeRead& read) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(read.value.data()), read.value.size()};
}

[[nodiscard]] auto wait_completion(glyphastore::experimental::VolatileShardPairPrototype& pair)
    -> glyphastore::experimental::PrototypeCompletion {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        if (auto completion = pair.try_pop_completion()) {
            return *completion;
        }
        std::this_thread::yield();
    }
    throw std::runtime_error{"paired prototype completion timeout"};
}

} // namespace

GLYPHA_TEST("paired SPSC ring preserves order through concurrent wraparound") {
    glyphastore::experimental::SpscRing<std::uint64_t, 64> ring;
    constexpr std::uint64_t kCount = 100'000;
    std::atomic_bool failed{};
    std::thread consumer([&] {
        for (std::uint64_t expected = 0; expected < kCount; ++expected) {
            std::uint64_t value = 0;
            while (!ring.try_pop(value)) {
                std::this_thread::yield();
            }
            if (value != expected) {
                failed.store(true, std::memory_order_relaxed);
            }
        }
    });
    for (std::uint64_t value = 0; value < kCount; ++value) {
        while (!ring.try_push(value)) {
            std::this_thread::yield();
        }
    }
    consumer.join();
    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(ring.empty());
}

GLYPHA_TEST("paired volatile publication makes acknowledged PUT visible without GET queue traffic") {
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(1024, 16);
    GLYPHA_REQUIRE(pair.has_value());
    GLYPHA_REQUIRE(!(*pair)->get("alpha").has_value());

    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "alpha", bytes("one")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    const auto completion = wait_completion(**pair);
    GLYPHA_REQUIRE(!completion.error.has_value());
    (*pair)->adopt_publication();
    const auto found = (*pair)->get("alpha");
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "one");

    const auto before = (*pair)->stats();
    for (std::size_t index = 0; index < 1'000; ++index) {
        GLYPHA_REQUIRE((*pair)->get("alpha").has_value());
    }
    const auto after = (*pair)->stats();
    GLYPHA_REQUIRE(after.reader_gets == before.reader_gets + 1'000U);
    GLYPHA_REQUIRE(after.mutation_pushes == before.mutation_pushes);
    GLYPHA_REQUIRE(after.mutation_pops == before.mutation_pops);
    GLYPHA_REQUIRE(after.completion_pushes == before.completion_pushes);
    GLYPHA_REQUIRE(after.completion_pops == before.completion_pops);

    GLYPHA_REQUIRE((*pair)->try_submit_erase(2, "alpha") ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();
    GLYPHA_REQUIRE(!(*pair)->get("alpha").has_value());
}

GLYPHA_TEST("paired volatile admission reserves bounded completion capacity") {
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(64, 512);
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t request = 0;
         request < glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity; ++request) {
        GLYPHA_REQUIRE((*pair)->try_submit_put(request, "key", bytes("value")) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
    }
    GLYPHA_REQUIRE((*pair)->try_submit_put(999, "overflow", bytes("value")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::queue_full);

    std::unordered_set<std::uint64_t> completed;
    while (completed.size() < glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity) {
        const auto completion = wait_completion(**pair);
        GLYPHA_REQUIRE(!completion.error.has_value());
        completed.insert(completion.request_id);
    }
    GLYPHA_REQUIRE(completed.size() == glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity);
    GLYPHA_REQUIRE((*pair)->try_submit_put(1'000, "reused", bytes("slot")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.queue_full == 1);
    GLYPHA_REQUIRE(stats.mutation_queue_high_watermark > 0);
    GLYPHA_REQUIRE(stats.completion_queue_high_watermark > 0);
    GLYPHA_REQUIRE(stats.completion_queue_depth == 0);
    GLYPHA_REQUIRE(stats.maximum_writer_batch_size > 0);
}

GLYPHA_TEST("paired volatile merge keeps two-level visibility and TTL semantics") {
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(128, 8);
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t index = 0; index < 40; ++index) {
        const auto key = std::string{"key-"} + std::to_string(index);
        const auto value = std::string{"value-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, key, bytes(value), index == 7 ? 100 : 0) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    }
    (*pair)->adopt_publication();
    for (std::uint64_t index = 0; index < 40; ++index) {
        const auto key = std::string{"key-"} + std::to_string(index);
        const auto found = (*pair)->get(key, 99);
        GLYPHA_REQUIRE(found.has_value());
        GLYPHA_REQUIRE(text(*found) == std::string{"value-"} + std::to_string(index));
    }
    GLYPHA_REQUIRE(!(*pair)->get("key-7", 100).has_value());
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.visible_through == 40);
    GLYPHA_REQUIRE(stats.reader_epoch == stats.writer_epoch);
    GLYPHA_REQUIRE(stats.delta_merges > 0);
    GLYPHA_REQUIRE(stats.payload_allocations == 40);
}

GLYPHA_TEST("paired delta capacity covers one full batch beyond a small merge threshold") {
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(128, 1);
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t index = 0; index < 64; ++index) {
        const auto key = std::string{"batch-key-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, key, bytes("batch-value")) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
    }
    for (std::uint64_t index = 0; index < 64; ++index) {
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    }
    (*pair)->adopt_publication();
    for (std::uint64_t index = 0; index < 64; ++index) {
        const auto key = std::string{"batch-key-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->get(key).has_value());
    }
    GLYPHA_REQUIRE((*pair)->stats().delta_merges > 0);
}

GLYPHA_TEST("paired QSBR retires generations only across Reader turn boundaries") {
    // ADR 0036 prototype gate analogue: V2/V3 (turn-based quiescence).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(128, 32);
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t index = 0; index < 512; ++index) {
        const auto value = std::string{"value-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, "stable-key", bytes(value)) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    (*pair)->adopt_publication();
    const auto found = (*pair)->get("stable-key");
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "value-511");
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.generation_retire_count > 0);
    GLYPHA_REQUIRE(stats.generation_high_watermark <= 4);
    GLYPHA_REQUIRE(stats.generation_live <= 3);
    GLYPHA_REQUIRE(stats.publication_backpressure == 0);
    GLYPHA_REQUIRE(stats.reader_turns >= 1'025);
    GLYPHA_REQUIRE(stats.payload_allocations == 512);
    GLYPHA_REQUIRE(stats.delta_pages_allocated == 512);
    GLYPHA_REQUIRE(stats.delta_pages_copied == 511);
    GLYPHA_REQUIRE(stats.delta_directory_entries_copied == 4'096);
    GLYPHA_REQUIRE(stats.delta_page_view_entries_copied == 0);
}

GLYPHA_TEST("paired large delta copies only touched persistent directory blocks") {
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(128, 4'096);
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t index = 0; index < 64; ++index) {
        const auto value = std::string{"hierarchical-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, "same-directory-page", bytes(value)) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    (*pair)->adopt_publication();
    const auto found = (*pair)->get("same-directory-page");
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "hierarchical-63");
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.delta_pages_allocated == 64);
    GLYPHA_REQUIRE(stats.delta_pages_copied == 63);
    // Root: 32 handles/publication. The first publication creates an empty
    // block; the remaining 63 copy only that block's 16 page handles.
    GLYPHA_REQUIRE(stats.delta_directory_entries_copied == 3'056);
    GLYPHA_REQUIRE(stats.delta_page_view_entries_copied == 64U * 512U);
    GLYPHA_REQUIRE(stats.delta_directory_entries_copied < 64U * 512U);
}

GLYPHA_TEST("paired Writer batch policy validates bounds and supports zero-wait batch one") {
    using glyphastore::experimental::PrototypeWriterBatchConfig;
    GLYPHA_REQUIRE(!glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32, PrototypeWriterBatchConfig{.max_records = 0}));
    GLYPHA_REQUIRE(!glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32,
        PrototypeWriterBatchConfig{.max_records = 32, .max_wait = std::chrono::microseconds{1'001}}));

    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32, PrototypeWriterBatchConfig{.max_records = 1, .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());
    for (std::uint64_t index = 0; index < 4; ++index) {
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, "batch-policy", bytes("value")) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
    }
    for (std::uint64_t index = 0; index < 4; ++index) {
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    }
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.publications == 4);
    GLYPHA_REQUIRE(stats.maximum_writer_batch_size == 1);
    GLYPHA_REQUIRE(stats.writer_batch_deadline_closes == 0);
}

GLYPHA_TEST("ADR 0036 V1 prototype: adopt publishes one immutable generation atomically") {
    // Publish release ↔ Reader acquire: GET stays on the adopted generation until the
    // next adopt_publication; epoch/visible_through move together (no torn pair).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "atomic-key", bytes("first")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();
    const auto epoch_first = (*pair)->stats().reader_epoch;
    const auto visible_first = (*pair)->stats().visible_through;
    const auto first = (*pair)->get("atomic-key");
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(text(*first) == "first");
    GLYPHA_REQUIRE(epoch_first == (*pair)->stats().writer_epoch);
    GLYPHA_REQUIRE(visible_first == first->sequence);

    GLYPHA_REQUIRE((*pair)->try_submit_put(2, "atomic-key", bytes("second")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    // Writer has advanced; Reader still observes the previously adopted generation.
    GLYPHA_REQUIRE((*pair)->stats().writer_epoch > epoch_first);
    GLYPHA_REQUIRE((*pair)->stats().reader_epoch == epoch_first);
    const auto stale = (*pair)->get("atomic-key");
    GLYPHA_REQUIRE(stale.has_value());
    GLYPHA_REQUIRE(text(*stale) == "first");
    GLYPHA_REQUIRE((*pair)->stats().visible_through == visible_first);

    (*pair)->adopt_publication();
    const auto epoch_second = (*pair)->stats().reader_epoch;
    const auto visible_second = (*pair)->stats().visible_through;
    const auto second = (*pair)->get("atomic-key");
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(text(*second) == "second");
    GLYPHA_REQUIRE(epoch_second == (*pair)->stats().writer_epoch);
    GLYPHA_REQUIRE(epoch_second > epoch_first);
    GLYPHA_REQUIRE(visible_second == second->sequence);
    GLYPHA_REQUIRE(visible_second > visible_first);
}

GLYPHA_TEST("ADR 0036 V3 prototype: pinned generation bytes survive publish and retire races") {
    // Two-boundary retire while a pin holds the pre-storm generation: spans from that
    // generation remain readable; reclaim is blocked (pin_blocks) until reset.
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());
    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "pin-race", bytes("pinned-value")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();
    auto pin = (*pair)->pin_read_generation();
    GLYPHA_REQUIRE(static_cast<bool>(pin));
    const auto pinned = (*pair)->get("pin-race");
    GLYPHA_REQUIRE(pinned.has_value());
    GLYPHA_REQUIRE(text(*pinned) == "pinned-value");
    const auto pinned_copy = *pinned;

    for (std::uint64_t index = 0; index < 64; ++index) {
        const auto value = std::string{"storm-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index + 2U, "pin-race", bytes(value)) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }

    // Span into the pinned generation must remain valid across the storm.
    GLYPHA_REQUIRE(text(pinned_copy) == "pinned-value");
    const auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.generation_output_pins == 1);
    GLYPHA_REQUIRE(stats.generation_retire_pin_blocks > 0);
    GLYPHA_REQUIRE(stats.publications >= 65);

    (*pair)->adopt_publication();
    const auto latest = (*pair)->get("pin-race");
    GLYPHA_REQUIRE(latest.has_value());
    GLYPHA_REQUIRE(text(*latest) == "storm-63");
    // Old span still valid until pin drop.
    GLYPHA_REQUIRE(text(pinned_copy) == "pinned-value");
    pin.reset();
    GLYPHA_REQUIRE((*pair)->stats().generation_output_pins == 0);
}

GLYPHA_TEST("ADR 0036 V9 prototype: slot-pool starvation increments publication backpressure") {
    // Starve QSBR (no adopt) with one-publication-per-mutation so the fixed generation
    // pool cannot reclaim. Writer signals publication_backpressure, then fail-closes with
    // resource_exhausted after a bounded spin (never overwrites a live slot).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        64, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    std::uint64_t submitted = 0;
    std::uint64_t completed = 0;
    std::uint64_t rejected = 0;
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           (*pair)->stats().publication_backpressure == 0 &&
           (*pair)->stats().generation_slot_exhaustions == 0) {
        if (submitted < 1'024) {
            const auto status = (*pair)->try_submit_put(submitted, "pool-pressure", bytes("x"));
            if (status == glyphastore::experimental::PrototypeSubmitStatus::submitted) {
                ++submitted;
            }
        }
        if (auto completion = (*pair)->try_pop_completion()) {
            if (completion->error.has_value()) {
                GLYPHA_REQUIRE(*completion->error == glyphastore::ErrorCode::resource_exhausted);
                ++rejected;
            } else {
                ++completed;
            }
        } else {
            std::this_thread::yield();
        }
    }

    GLYPHA_REQUIRE((*pair)->stats().publication_backpressure > 0);
    GLYPHA_REQUIRE(submitted > completed + rejected);

    // Advance Reader turns so retired slots can free; drain remaining work.
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < drain_deadline && completed + rejected < submitted) {
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
        if (auto completion = (*pair)->try_pop_completion()) {
            if (completion->error.has_value()) {
                GLYPHA_REQUIRE(*completion->error == glyphastore::ErrorCode::resource_exhausted);
                ++rejected;
            } else {
                ++completed;
            }
        } else {
            std::this_thread::yield();
        }
    }
    GLYPHA_REQUIRE(completed + rejected == submitted);
    GLYPHA_REQUIRE((*pair)->stats().mutation_queue_depth == 0);
    GLYPHA_REQUIRE((*pair)->stats().generation_live >= 1);
    GLYPHA_REQUIRE((*pair)->stats().generation_high_watermark <=
                   glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity + 2U);

    // Recovery after reclaim: a new publish must succeed (pool not wedged).
    GLYPHA_REQUIRE((*pair)->try_submit_put(submitted + 1U, "pool-recovery", bytes("ok")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    const auto recovered = wait_completion(**pair);
    GLYPHA_REQUIRE(!recovered.error.has_value());
    (*pair)->adopt_publication();
    const auto found = (*pair)->get("pool-recovery");
    GLYPHA_REQUIRE(found.has_value());
    GLYPHA_REQUIRE(text(*found) == "ok");
    (*pair)->stop_and_drain();
}

GLYPHA_TEST("ADR 0036 V6 prototype: rejected publication never makes mutations visible") {
    // Prototype applies Writer delta + publish atomically: slot-pool exhaustion fails the
    // completion before release-store. Rejected keys must never appear on GET (RAW).
    // Failed-batch completions report the pre-failure Writer epoch / visible_through.
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        64, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "v6-seed", bytes("seed")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();

    std::vector<std::string> rejected_keys;
    rejected_keys.reserve(64);
    std::uint64_t submitted = 0;
    std::uint64_t completed = 0;
    std::uint64_t rejected = 0;
    std::optional<std::uint64_t> epoch_at_reject;
    std::optional<std::uint64_t> visible_at_reject;
    std::uint64_t request_id = 2;
    // ASan/TSan make the Writer's bounded acquire spin (≤1e6 yields) much slower than
    // Release; wait for backpressure first, then for the fail-closed completion itself.
    const auto pressure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{90};
    while (std::chrono::steady_clock::now() < pressure_deadline &&
           (*pair)->stats().generation_slot_exhaustions == 0) {
        // No adopt_publication: QSBR cannot free slots → eventual acquire failure.
        if (submitted < 1'024) {
            auto key = "v6-key-" + std::to_string(request_id);
            const auto status = (*pair)->try_submit_put(request_id, key, bytes("ghost"));
            if (status == glyphastore::experimental::PrototypeSubmitStatus::submitted) {
                ++submitted;
                ++request_id;
            }
        }
        if (auto completion = (*pair)->try_pop_completion()) {
            if (completion->error.has_value()) {
                GLYPHA_REQUIRE(*completion->error == glyphastore::ErrorCode::resource_exhausted);
                if (!epoch_at_reject.has_value()) {
                    epoch_at_reject = completion->epoch;
                    visible_at_reject = completion->visible_through;
                    GLYPHA_REQUIRE((*pair)->stats().writer_epoch == *epoch_at_reject);
                    // stats().visible_through is the Reader-local adopted frontier,
                    // intentionally stale here because this test withholds adoption.
                    // The rejected completion carries the Writer frontier instead.
                }
                GLYPHA_REQUIRE(completion->epoch == *epoch_at_reject);
                GLYPHA_REQUIRE(completion->visible_through == *visible_at_reject);
                rejected_keys.push_back("v6-key-" + std::to_string(completion->request_id));
                ++rejected;
            } else {
                ++completed;
            }
        } else {
            std::this_thread::yield();
        }
    }
    GLYPHA_REQUIRE((*pair)->stats().generation_slot_exhaustions > 0);
    GLYPHA_REQUIRE((*pair)->stats().publication_backpressure > 0);

    // Unblock QSBR so remaining queue work can finish (same reclaim path as V9). Rejected
    // keys collected above must still miss after recovery; successful keys may be visible.
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{90};
    while (std::chrono::steady_clock::now() < drain_deadline && completed + rejected < submitted) {
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
        if (auto completion = (*pair)->try_pop_completion()) {
            if (completion->error.has_value()) {
                GLYPHA_REQUIRE(*completion->error == glyphastore::ErrorCode::resource_exhausted);
                if (!epoch_at_reject.has_value()) {
                    epoch_at_reject = completion->epoch;
                    visible_at_reject = completion->visible_through;
                }
                // After adopt, later rejects may observe a newer Writer frontier if an
                // intervening publish succeeded; still record the key as never-ACKed.
                rejected_keys.push_back("v6-key-" + std::to_string(completion->request_id));
                ++rejected;
            } else {
                ++completed;
            }
        } else {
            std::this_thread::yield();
        }
    }
    GLYPHA_REQUIRE(completed + rejected == submitted);
    GLYPHA_REQUIRE(epoch_at_reject.has_value());
    GLYPHA_REQUIRE(!rejected_keys.empty());
    GLYPHA_REQUIRE((*pair)->stats().mutation_queue_depth == 0);

    GLYPHA_REQUIRE((*pair)->try_submit_put(request_id, "v6-alive", bytes("ok")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    const auto alive = wait_completion(**pair);
    GLYPHA_REQUIRE(!alive.error.has_value());
    (*pair)->adopt_publication();
    GLYPHA_REQUIRE((*pair)->get("v6-alive").has_value());
    GLYPHA_REQUIRE((*pair)->get("v6-seed").has_value());
    for (const auto& key : rejected_keys) {
        GLYPHA_REQUIRE(!(*pair)->get(key).has_value());
    }
    (*pair)->stop_and_drain();
}

GLYPHA_TEST("ADR 0036 V2 prototype: reclaim never frees a pinned or pre-quiescent generation") {
    // Retire waits for two Reader turns; a pin blocks free even after the turn frontier.
    // Spans into the pinned generation remain readable until pin reset.
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "v2-key", bytes("keep-alive")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();
    auto pin = (*pair)->pin_read_generation();
    GLYPHA_REQUIRE(static_cast<bool>(pin));
    const auto pinned = *(*pair)->get("v2-key");
    GLYPHA_REQUIRE(text(pinned) == "keep-alive");

    const auto retires_before = (*pair)->stats().generation_retire_count;
    // Publish a successor without advancing Reader turns: previous is retired but not free.
    GLYPHA_REQUIRE((*pair)->try_submit_put(2, "v2-key", bytes("next")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    GLYPHA_REQUIRE((*pair)->stats().generation_retire_count == retires_before);
    GLYPHA_REQUIRE(text(pinned) == "keep-alive");

    // Two turns satisfy the QSBR frontier; Writer reclaim runs on the next publish.
    (*pair)->adopt_publication();
    (*pair)->adopt_publication();
    GLYPHA_REQUIRE((*pair)->try_submit_put(3, "v2-key", bytes("force-reclaim")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    const auto blocks_after_turns = (*pair)->stats().generation_retire_pin_blocks;
    GLYPHA_REQUIRE(blocks_after_turns > 0);
    GLYPHA_REQUIRE(text(pinned) == "keep-alive");
    GLYPHA_REQUIRE((*pair)->stats().generation_output_pins == 1);

    // Further publishes with full turn advance still cannot reclaim the pinned slot.
    for (std::uint64_t index = 0; index < 8; ++index) {
        GLYPHA_REQUIRE((*pair)->try_submit_put(index + 4U, "v2-key", bytes("storm")) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    GLYPHA_REQUIRE((*pair)->stats().generation_retire_pin_blocks >= blocks_after_turns);
    GLYPHA_REQUIRE(text(pinned) == "keep-alive");

    pin.reset();
    GLYPHA_REQUIRE((*pair)->stats().generation_output_pins == 0);
    // After unpin, additional turns allow the blocked retire to complete.
    const auto retires_at_unpin = (*pair)->stats().generation_retire_count;
    for (int turn = 0; turn < 8; ++turn) {
        (*pair)->adopt_publication();
        GLYPHA_REQUIRE(
            (*pair)->try_submit_put(static_cast<std::uint64_t>(100 + turn), "v2-key", bytes("post-unpin")) ==
            glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
    }
    GLYPHA_REQUIRE((*pair)->stats().generation_retire_count > retires_at_unpin);
    (*pair)->adopt_publication();
    const auto latest = (*pair)->get("v2-key");
    GLYPHA_REQUIRE(latest.has_value());
    GLYPHA_REQUIRE(text(*latest) == "post-unpin");
}

GLYPHA_TEST("ADR 0036 V7 prototype: delta merge under pin slot pressure keeps committed keys") {
    // Low merge threshold + pin holds one generation slot while more keys publish and merge.
    // Successful completions must remain visible; merge must run under pin_blocks pressure.
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        64, 8,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    constexpr std::uint64_t kWarmKeys = 24;
    for (std::uint64_t index = 0; index < kWarmKeys; ++index) {
        const auto key = std::string{"merge-"} + std::to_string(index);
        const auto value = std::string{"warm-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(index, key, bytes(value)) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    (*pair)->adopt_publication();
    GLYPHA_REQUIRE((*pair)->stats().delta_merges > 0);

    auto pin = (*pair)->pin_read_generation();
    GLYPHA_REQUIRE(static_cast<bool>(pin));
    const auto merges_at_pin = (*pair)->stats().delta_merges;

    constexpr std::uint64_t kPressureKeys = 64;
    std::vector<std::uint64_t> accepted_indices;
    accepted_indices.reserve(kPressureKeys);
    for (std::uint64_t index = 0; index < kPressureKeys; ++index) {
        const auto key = std::string{"merge-"} + std::to_string(kWarmKeys + index);
        const auto value = std::string{"press-"} + std::to_string(index);
        GLYPHA_REQUIRE((*pair)->try_submit_put(kWarmKeys + index, key, bytes(value)) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        const auto completion = wait_completion(**pair);
        if (completion.error.has_value()) {
            GLYPHA_REQUIRE(*completion.error == glyphastore::ErrorCode::resource_exhausted);
            continue;
        }
        accepted_indices.push_back(index);
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    GLYPHA_REQUIRE(!accepted_indices.empty());
    GLYPHA_REQUIRE((*pair)->stats().generation_retire_pin_blocks > 0);
    GLYPHA_REQUIRE((*pair)->stats().delta_merges > merges_at_pin);

    (*pair)->adopt_publication();
    for (std::uint64_t index = 0; index < kWarmKeys; ++index) {
        const auto key = std::string{"merge-"} + std::to_string(index);
        const auto found = (*pair)->get(key);
        GLYPHA_REQUIRE(found.has_value());
        GLYPHA_REQUIRE(text(*found) == std::string{"warm-"} + std::to_string(index));
    }
    for (const auto index : accepted_indices) {
        const auto key = std::string{"merge-"} + std::to_string(kWarmKeys + index);
        const auto found = (*pair)->get(key);
        GLYPHA_REQUIRE(found.has_value());
        GLYPHA_REQUIRE(text(*found) == std::string{"press-"} + std::to_string(index));
    }

    pin.reset();
    for (int turn = 0; turn < 4; ++turn) {
        (*pair)->adopt_publication();
    }
    // Post-unpin visibility unchanged for committed keys.
    for (std::uint64_t index = 0; index < kWarmKeys; ++index) {
        GLYPHA_REQUIRE((*pair)->get(std::string{"merge-"} + std::to_string(index)).has_value());
    }
    for (const auto index : accepted_indices) {
        GLYPHA_REQUIRE((*pair)->get(std::string{"merge-"} + std::to_string(kWarmKeys + index)).has_value());
    }
}

GLYPHA_TEST("ADR 0036 V13 prototype: pin adopt merge reclaim stress") {
    // Single Reader-owner thread + internal Writer (prototype contract). Hammers
    // publish/retire/pin/merge/adopt ordering for ThreadSanitizer (macos-tsan).
    // Does not authorize production landing — see ADR 0036 V13.
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        128, 16,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    constexpr std::uint64_t kRounds = 128;
    constexpr std::uint64_t kKeysPerRound = 8;
    std::uint64_t submitted = 0;
    std::uint64_t accepted = 0;
    std::uint64_t rejected = 0;

    for (std::uint64_t round = 0; round < kRounds; ++round) {
        (*pair)->adopt_publication();
        auto pin = (*pair)->pin_read_generation();
        GLYPHA_REQUIRE(static_cast<bool>(pin));

        for (std::uint64_t index = 0; index < kKeysPerRound; ++index) {
            const auto key = std::string{"stress-"} + std::to_string(round) + '-' + std::to_string(index);
            const auto value = std::string{"v-"} + std::to_string(submitted);
            auto status = (*pair)->try_submit_put(submitted, key, bytes(value));
            while (status == glyphastore::experimental::PrototypeSubmitStatus::queue_full) {
                std::this_thread::yield();
                status = (*pair)->try_submit_put(submitted, key, bytes(value));
            }
            GLYPHA_REQUIRE(status == glyphastore::experimental::PrototypeSubmitStatus::submitted);
            ++submitted;
            const auto completion = wait_completion(**pair);
            if (completion.error.has_value()) {
                GLYPHA_REQUIRE(*completion.error == glyphastore::ErrorCode::resource_exhausted);
                ++rejected;
                continue;
            }
            ++accepted;
            (*pair)->adopt_publication();
            const auto found = (*pair)->get(key);
            GLYPHA_REQUIRE(found.has_value());
            GLYPHA_REQUIRE(text(*found) == value);
            (*pair)->adopt_publication();
        }

        // Hold the pin across an extra publish/adopt pair, then drop it.
        auto hold_status = (*pair)->try_submit_put(submitted, "stress-pin-hold", bytes("hold"));
        while (hold_status == glyphastore::experimental::PrototypeSubmitStatus::queue_full) {
            std::this_thread::yield();
            hold_status = (*pair)->try_submit_put(submitted, "stress-pin-hold", bytes("hold"));
        }
        GLYPHA_REQUIRE(hold_status == glyphastore::experimental::PrototypeSubmitStatus::submitted);
        ++submitted;
        {
            const auto completion = wait_completion(**pair);
            if (completion.error.has_value()) {
                GLYPHA_REQUIRE(*completion.error == glyphastore::ErrorCode::resource_exhausted);
                ++rejected;
            } else {
                ++accepted;
            }
        }
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
        pin.reset();
    }

    (*pair)->adopt_publication();
    GLYPHA_REQUIRE(accepted > 0);
    GLYPHA_REQUIRE(accepted + rejected == submitted);
    GLYPHA_REQUIRE((*pair)->stats().publications > 0);
    GLYPHA_REQUIRE((*pair)->stats().delta_merges > 0);
    GLYPHA_REQUIRE((*pair)->stats().generation_retire_count > 0);
    GLYPHA_REQUIRE((*pair)->stats().generation_live >= 1);
    GLYPHA_REQUIRE((*pair)->stats().generation_high_watermark <=
                   glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity + 2U);
    (*pair)->stop_and_drain();
}

GLYPHA_TEST("paired slow-output pin delays generation retirement across Reader turns") {
    // ADR 0036 prototype gate analogue: V4 (pin blocks reclaim).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(128, 32);
    GLYPHA_REQUIRE(pair.has_value());
    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "pinned", bytes("old")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    (*pair)->adopt_publication();
    auto pin = (*pair)->pin_read_generation();
    GLYPHA_REQUIRE(static_cast<bool>(pin));
    for (std::uint64_t index = 0; index < 16; ++index) {
        GLYPHA_REQUIRE((*pair)->try_submit_put(index + 2U, "pinned", bytes("new")) ==
                       glyphastore::experimental::PrototypeSubmitStatus::submitted);
        GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
        (*pair)->adopt_publication();
        (*pair)->adopt_publication();
    }
    auto stats = (*pair)->stats();
    GLYPHA_REQUIRE(stats.generation_output_pins == 1);
    GLYPHA_REQUIRE(stats.generation_output_pin_high_watermark == 1);
    GLYPHA_REQUIRE(stats.generation_retire_pin_blocks > 0);
    pin.reset();
    GLYPHA_REQUIRE((*pair)->stats().generation_output_pins == 0);
}

GLYPHA_TEST("paired volatile shutdown drains submission and closes admission") {
    // ADR 0036 prototype gate analogue: V5 (shutdown drain).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(64, 16);
    GLYPHA_REQUIRE(pair.has_value());
    GLYPHA_REQUIRE((*pair)->try_submit_put(1, "shutdown", bytes("visible")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::submitted);
    (*pair)->stop_and_drain();
    GLYPHA_REQUIRE(!wait_completion(**pair).error.has_value());
    GLYPHA_REQUIRE((*pair)->get("shutdown").has_value());
    GLYPHA_REQUIRE((*pair)->stats().reader_epoch == (*pair)->stats().writer_epoch);
    GLYPHA_REQUIRE((*pair)->try_submit_put(2, "late", bytes("no")) ==
                   glyphastore::experimental::PrototypeSubmitStatus::stopped);
}
