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
    GLYPHA_REQUIRE(stats.writer_epoch > stats.reader_epoch || stats.publications > 1);

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
    // pool cannot reclaim. Writer must signal publication_backpressure rather than
    // silently overwriting a live slot. Prototype still yields while waiting for a free
    // slot — production landing must bound that wait (ADR 0036 V9 residual).
    auto pair = glyphastore::experimental::VolatileShardPairPrototype::create(
        64, 32,
        glyphastore::experimental::PrototypeWriterBatchConfig{.max_records = 1,
                                                              .max_wait = std::chrono::microseconds{0}});
    GLYPHA_REQUIRE(pair.has_value());

    std::uint64_t submitted = 0;
    std::uint64_t completed = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < deadline &&
           (*pair)->stats().publication_backpressure == 0) {
        if (submitted < 1'024) {
            const auto status =
                (*pair)->try_submit_put(submitted, "pool-pressure", bytes("x"));
            if (status == glyphastore::experimental::PrototypeSubmitStatus::submitted) {
                ++submitted;
            }
        }
        if (auto completion = (*pair)->try_pop_completion()) {
            GLYPHA_REQUIRE(!completion->error.has_value());
            ++completed;
        } else {
            std::this_thread::yield();
        }
    }

    const auto backpressure = (*pair)->stats().publication_backpressure;
    GLYPHA_REQUIRE(backpressure > 0);
    GLYPHA_REQUIRE(submitted > 0);

    // Advance Reader turns so retired slots can free and the Writer can drain.
    for (int turn = 0; turn < 512; ++turn) {
        (*pair)->adopt_publication();
        while (auto completion = (*pair)->try_pop_completion()) {
            GLYPHA_REQUIRE(!completion->error.has_value());
            ++completed;
        }
        if (completed == submitted && (*pair)->stats().mutation_queue_depth == 0) {
            break;
        }
    }
    GLYPHA_REQUIRE(completed == submitted);
    GLYPHA_REQUIRE((*pair)->stats().generation_live >= 1);
    GLYPHA_REQUIRE((*pair)->stats().generation_high_watermark <=
                   glyphastore::experimental::VolatileShardPairPrototype::kQueueCapacity + 2U);
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
