#include "experimental/generation_slot_pool.hpp"
#include "experimental/pair_read_generation_shell.hpp"
#include "glyphastore/store/paired/shard_pair_runtime.hpp"
#include "test.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

struct MockGeneration final {
    std::uint64_t generation_epoch{};
    std::uint64_t visible{};
    std::uint64_t marker{};

    [[nodiscard]] auto epoch() const noexcept -> std::uint64_t {
        return generation_epoch;
    }

    [[nodiscard]] auto visible_through() const noexcept -> std::uint64_t {
        return visible;
    }
};

[[nodiscard]] auto generation(const std::uint64_t epoch) -> std::shared_ptr<const MockGeneration> {
    return std::make_shared<const MockGeneration>(
        MockGeneration{.generation_epoch = epoch, .visible = epoch * 10U, .marker = epoch});
}

[[nodiscard]] auto bytes(const std::string_view text) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

[[nodiscard]] auto append(glyphastore::Segment& segment, const glyphastore::WorkerRoutingState routing,
                          const std::string_view key, const std::string_view value,
                          const std::uint64_t sequence) -> glyphastore::RecordRef {
    auto record = segment.append({.sequence = glyphastore::SequenceNumber{sequence},
                                  .opcode = glyphastore::Opcode::put,
                                  .key_hash = glyphastore::hash_key_routing(key, routing),
                                  .key = bytes(key),
                                  .value = bytes(value)});
    GLYPHA_REQUIRE(record.has_value());
    return *record;
}

template <typename Pool> struct DirectPoolShutdown final {
    Pool* pool{};
    ~DirectPoolShutdown() {
        if (pool != nullptr) {
            pool->stop_admission();
            static_cast<void>(pool->mark_reader_quiescent());
            static_cast<void>(pool->try_finish_shutdown());
        }
    }
};

} // namespace

GLYPHA_TEST("ADR 0036 V8 candidate bounds slots and recovers after Reader adoption") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 3>;
    auto pool = Pool::create(generation(0));
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    GLYPHA_REQUIRE((*pool)->try_publish(generation(1)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(2)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(3)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::pool_exhausted);
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 3);

    const auto* adopted = (*pool)->adopt();
    GLYPHA_REQUIRE(adopted != nullptr);
    GLYPHA_REQUIRE(adopted->epoch() == 2);
    (*pool)->reclaim();
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(3)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt()->marker == 3);
    GLYPHA_REQUIRE((*pool)->stats().slot_reuses > 0);
}

GLYPHA_TEST("ADR 0036 V9 candidate applies the official retire-debt capacity formula") {
    constexpr auto kMaximumRetired =
        glyphastore::store::paired::ShardPairRuntime::kMaximumRetiredReadGenerations;
    constexpr auto kCapacity = glyphastore::experimental::GenerationSlotCapacity<kMaximumRetired>::value;
    static_assert(kMaximumRetired == 64U);
    static_assert(kCapacity == 65U);
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, kCapacity>;

    auto pool = Pool::create(generation(0));
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    // Hold the initial Reader frontier while the Writer consumes the complete
    // normative retire-debt bound.
    for (std::uint64_t epoch = 1; epoch < kCapacity; ++epoch) {
        GLYPHA_REQUIRE((*pool)->try_publish(generation(epoch)) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::published);
    }
    const auto saturated = (*pool)->stats();
    GLYPHA_REQUIRE(saturated.live_slots == kCapacity);
    GLYPHA_REQUIRE(saturated.live_high_watermark == kCapacity);
    GLYPHA_REQUIRE(!(*pool)->try_reserve().has_value());
    GLYPHA_REQUIRE((*pool)->stats().pool_exhaustions == 1);

    // No mutation crossed Store while full. Once the Reader advances, all old
    // epochs become reclaimable and the next reservation/publication succeeds.
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == kCapacity - 1U);
    (*pool)->reclaim();
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(kCapacity)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == kCapacity);
}

GLYPHA_TEST("ADR 0036 V8 candidate cold borrow holds the exact retired epoch") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 4>;
    auto initial = generation(0);
    auto pool = Pool::create(initial);
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    auto borrowed = generation(1);
    std::weak_ptr<const MockGeneration> borrowed_lifetime = borrowed;
    GLYPHA_REQUIRE((*pool)->try_publish(borrowed) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);
    borrowed.reset();

    GLYPHA_REQUIRE((*pool)->try_publish(generation(2)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt(1)->epoch() == 2);
    (*pool)->reclaim();
    GLYPHA_REQUIRE(!borrowed_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().reader_safe_epoch == 1);

    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 2);
    (*pool)->reclaim();
    GLYPHA_REQUIRE(borrowed_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().reader_safe_epoch == 2);
}

GLYPHA_TEST("ADR 0036 V8 candidate rejects a late regressing borrow frontier") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 4>;
    auto pool = Pool::create(generation(0));
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(1)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 1);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(2)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 2);
    GLYPHA_REQUIRE((*pool)->stats().reader_safe_epoch == 2);

    GLYPHA_REQUIRE((*pool)->adopt(1) == nullptr);
    GLYPHA_REQUIRE((*pool)->stats().reader_safe_epoch == 2);
    GLYPHA_REQUIRE((*pool)->stats().reader_epoch == 2);
    GLYPHA_REQUIRE((*pool)->stats().invalid_adoptions == 1);
}

GLYPHA_TEST("ADR 0036 V6 candidate reserves before mutation and fail-closes abandoned authority") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 2>;
    std::atomic_uint64_t fail_closed_calls{};
    auto pool = Pool::create(generation(0),
                             {.context = &fail_closed_calls, .fail_closed = [](void* context) noexcept {
                                  static_cast<std::atomic_uint64_t*>(context)->fetch_add(
                                      1U, std::memory_order_relaxed);
                              }});
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(1)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);

    // Full before Store entry: reject without a fail-closed transition.
    GLYPHA_REQUIRE(!(*pool)->try_reserve().has_value());
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 0);
    GLYPHA_REQUIRE((*pool)->stats().unpublished_linearizations == 0);

    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 1);
    (*pool)->reclaim();
    {
        auto reservation = (*pool)->try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        // Cancellation before Store linearization is an ordinary safe abort.
    }
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 0);

    {
        auto reservation = (*pool)->try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        reservation->mark_store_linearized();
        auto moved = std::move(*reservation);
        GLYPHA_REQUIRE(moved.store_linearized());
        GLYPHA_REQUIRE((*pool)->commit(moved, {}) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::invalid_generation);
        // The moved-to guard owns the transition; both destructors together
        // must invoke fail-closed exactly once.
    }
    const auto failed = (*pool)->stats();
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(failed.unpublished_linearizations == 1);
    GLYPHA_REQUIRE(failed.reserved_slots == 0);

    auto recovery = (*pool)->try_reserve();
    GLYPHA_REQUIRE(recovery.has_value());
    recovery->mark_store_linearized();
    GLYPHA_REQUIRE((*pool)->commit(*recovery, generation(2)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 2);
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
}

GLYPHA_TEST("ADR 0036 V5 candidate drains admitted reservations and rejects late admission") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 4>;
    auto pool = Pool::create(generation(0));
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    auto cancelled = (*pool)->try_reserve();
    auto committed = (*pool)->try_reserve();
    GLYPHA_REQUIRE(cancelled.has_value());
    GLYPHA_REQUIRE(committed.has_value());

    (*pool)->stop_admission();
    GLYPHA_REQUIRE(!(*pool)->accepting());
    GLYPHA_REQUIRE(!(*pool)->try_reserve().has_value());
    GLYPHA_REQUIRE(!(*pool)->mark_reader_quiescent());
    GLYPHA_REQUIRE(!(*pool)->try_finish_shutdown());

    committed->mark_store_linearized();
    GLYPHA_REQUIRE((*pool)->commit(*committed, generation(1)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    cancelled.reset();
    GLYPHA_REQUIRE((*pool)->adopt()->epoch() == 1);
    GLYPHA_REQUIRE(!(*pool)->try_finish_shutdown());
    GLYPHA_REQUIRE((*pool)->mark_reader_quiescent());
    GLYPHA_REQUIRE((*pool)->try_finish_shutdown());
    GLYPHA_REQUIRE((*pool)->adopt() == nullptr);

    const auto stats = (*pool)->stats();
    GLYPHA_REQUIRE(stats.shutdown_starts == 1);
    GLYPHA_REQUIRE(stats.shutdown_reservation_rejections == 1);
    GLYPHA_REQUIRE(stats.reserved_slots == 0);
    GLYPHA_REQUIRE(stats.live_slots == 1);
    GLYPHA_REQUIRE(!stats.accepting);
    GLYPHA_REQUIRE(stats.reader_quiescent);
}

GLYPHA_TEST("ADR 0036 V5 candidate holds a slow borrow until terminal Reader quiescence") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 3>;
    auto initial = generation(0);
    std::weak_ptr<const MockGeneration> initial_lifetime = initial;
    auto pool = Pool::create(std::move(initial));
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);
    GLYPHA_REQUIRE((*pool)->try_publish(generation(1)) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    GLYPHA_REQUIRE((*pool)->adopt(0)->epoch() == 1);

    (*pool)->stop_admission();
    (*pool)->reclaim();
    GLYPHA_REQUIRE(!initial_lifetime.expired());
    GLYPHA_REQUIRE(!(*pool)->try_finish_shutdown());

    // The caller reaches this transition only after the slow output/cold I/O
    // represented by epoch zero has completed.
    GLYPHA_REQUIRE((*pool)->mark_reader_quiescent());
    GLYPHA_REQUIRE((*pool)->try_finish_shutdown());
    GLYPHA_REQUIRE(initial_lifetime.expired());
    GLYPHA_REQUIRE((*pool)->stats().live_slots == 1);
}

GLYPHA_TEST("ADR 0036 V5 candidate fail-closes a linearized reservation abandoned by shutdown") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 2>;
    std::atomic_uint64_t fail_closed_calls{};
    auto pool = Pool::create(generation(0),
                             {.context = &fail_closed_calls, .fail_closed = [](void* context) noexcept {
                                  static_cast<std::atomic_uint64_t*>(context)->fetch_add(
                                      1U, std::memory_order_relaxed);
                              }});
    GLYPHA_REQUIRE(pool.has_value());
    GLYPHA_REQUIRE((*pool)->adopt() != nullptr);

    auto reservation = (*pool)->try_reserve();
    GLYPHA_REQUIRE(reservation.has_value());
    reservation->mark_store_linearized();
    (*pool)->stop_admission();
    GLYPHA_REQUIRE(!(*pool)->mark_reader_quiescent());
    reservation.reset();

    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE((*pool)->stats().unpublished_linearizations == 1);
    GLYPHA_REQUIRE((*pool)->mark_reader_quiescent());
    GLYPHA_REQUIRE((*pool)->try_finish_shutdown());
}

GLYPHA_TEST("ADR 0036 V13 candidate publish adopt reclaim stress is race free") {
    using Pool = glyphastore::experimental::GenerationSlotPool<MockGeneration, 8>;
    auto pool = Pool::create(generation(0));
    GLYPHA_REQUIRE(pool.has_value());

    constexpr std::uint64_t kPublications = 20'000;
    std::atomic_bool writer_done{};
    std::atomic_bool failed{};
    std::thread writer([&] {
        for (std::uint64_t epoch = 1; epoch <= kPublications; ++epoch) {
            const auto next = generation(epoch);
            for (;;) {
                const auto status = (*pool)->try_publish(next);
                if (status == glyphastore::experimental::GenerationSlotPublishStatus::published) {
                    break;
                }
                if (status != glyphastore::experimental::GenerationSlotPublishStatus::pool_exhausted) {
                    failed.store(true, std::memory_order_relaxed);
                    writer_done.store(true, std::memory_order_release);
                    return;
                }
                std::this_thread::yield();
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    while (!writer_done.load(std::memory_order_acquire) || (*pool)->stats().reader_epoch < kPublications) {
        const auto* adopted = (*pool)->adopt();
        if (adopted == nullptr || adopted->marker != adopted->epoch() ||
            adopted->visible_through() != adopted->epoch() * 10U) {
            failed.store(true, std::memory_order_relaxed);
        }
        std::this_thread::yield();
    }
    writer.join();
    (*pool)->reclaim();

    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE((*pool)->stats().writer_epoch == kPublications);
    GLYPHA_REQUIRE((*pool)->stats().reader_epoch == kPublications);
    GLYPHA_REQUIRE((*pool)->stats().slot_reuses > 0);
    GLYPHA_REQUIRE((*pool)->stats().live_high_watermark <= 8);
}

GLYPHA_TEST("ADR 0036 shell slot reuses one real generation allocation after weak retirement") {
    using Access = glyphastore::experimental::PairReadGenerationShellAccess;
    using Generation = glyphastore::store::paired::PairReadGeneration;
    const glyphastore::WorkerRoutingState routing{};
    auto initial_result = Generation::empty(routing);
    auto storage_result = glyphastore::experimental::PairReadGenerationShellStorage::create();
    GLYPHA_REQUIRE(initial_result.has_value());
    GLYPHA_REQUIRE(storage_result.has_value());
    auto initial = *initial_result;
    auto storage = *storage_result;
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{91});
    const std::string key{"fixed-shell"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    const glyphastore::store::paired::ReadMutation mutation{
        .key = hashed,
        .record = append(*segment, routing, key, "one", 1),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };

    const Generation* first_address{};
    std::weak_ptr<const Generation> weak_generation;
    {
        auto first = Access::publish_incremental(initial, std::span{&mutation, 1}, storage);
        GLYPHA_REQUIRE(first.has_value());
        first_address = first->get();
        weak_generation = *first;
        GLYPHA_REQUIRE(storage->occupied());
        GLYPHA_REQUIRE(storage->allocation_count() == 1);
        GLYPHA_REQUIRE((*first)->get(hashed, 0).has_value());

        auto rejected = Access::publish_incremental(*first, std::span{&mutation, 1}, storage);
        GLYPHA_REQUIRE(!rejected.has_value());
        GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(storage->allocation_count() == 1);
    }

    // allocate_shared retains the block until the final weak owner releases
    // its control block, not merely until the final strong owner disappears.
    GLYPHA_REQUIRE(weak_generation.expired());
    GLYPHA_REQUIRE(storage->occupied());
    weak_generation.reset();
    GLYPHA_REQUIRE(!storage->occupied());

    auto second = Access::publish_incremental(initial, std::span{&mutation, 1}, storage);
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(second->get() == first_address);
    GLYPHA_REQUIRE(storage->allocation_count() == 2);
    GLYPHA_REQUIRE(storage->reuse_count() == 1);
}

GLYPHA_TEST("ADR 0036 shell slot backing storage outlives its external owner") {
    using Access = glyphastore::experimental::PairReadGenerationShellAccess;
    using Generation = glyphastore::store::paired::PairReadGeneration;
    const glyphastore::WorkerRoutingState routing{};
    auto initial_result = Generation::empty(routing);
    auto storage_result = glyphastore::experimental::PairReadGenerationShellStorage::create();
    GLYPHA_REQUIRE(initial_result.has_value());
    GLYPHA_REQUIRE(storage_result.has_value());
    auto storage = *storage_result;
    std::weak_ptr<glyphastore::experimental::PairReadGenerationShellStorage> lifetime = storage;
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{92});
    const std::string key{"owned-shell"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    const glyphastore::store::paired::ReadMutation mutation{
        .key = hashed,
        .record = append(*segment, routing, key, "value", 1),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };

    auto published = Access::publish_incremental(*initial_result, std::span{&mutation, 1}, storage);
    GLYPHA_REQUIRE(published.has_value());
    storage_result = glyphastore::fail(glyphastore::ErrorCode::internal_error, "released");
    storage.reset();
    GLYPHA_REQUIRE(!lifetime.expired());
    GLYPHA_REQUIRE((*published)->get(hashed, 0).has_value());

    published = glyphastore::fail(glyphastore::ErrorCode::internal_error, "released");
    GLYPHA_REQUIRE(lifetime.expired());
}

GLYPHA_TEST("ADR 0036 real generation pool reserves and reuses its matching shell slot") {
    using Access = glyphastore::experimental::PairReadGenerationShellAccess;
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Pool = glyphastore::experimental::GenerationSlotPool<Generation, 3>;
    using Bank = glyphastore::experimental::PairReadGenerationShellBank<3>;
    const glyphastore::WorkerRoutingState routing{};
    auto initial_result = Generation::empty(routing);
    auto bank_result = Bank::create();
    GLYPHA_REQUIRE(initial_result.has_value());
    GLYPHA_REQUIRE(bank_result.has_value());
    auto pool_result = Pool::create(*initial_result);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    auto& bank = **bank_result;
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{93});
    const std::string key{"pooled-shell"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    const glyphastore::store::paired::ReadMutation first_mutation{
        .key = hashed,
        .record = append(*segment, routing, key, "one", 1),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };

    auto first_reservation = pool.try_reserve();
    GLYPHA_REQUIRE(first_reservation.has_value());
    const auto first_slot = first_reservation->slot_index();
    auto first_storage = bank.at(first_slot);
    auto first = Access::publish_incremental(*initial_result, std::span{&first_mutation, 1}, first_storage);
    GLYPHA_REQUIRE(first.has_value());
    auto writer_current = *first;
    first_reservation->mark_store_linearized();
    GLYPHA_REQUIRE(pool.commit(*first_reservation, writer_current) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    first = glyphastore::fail(glyphastore::ErrorCode::internal_error, "moved to pool");
    GLYPHA_REQUIRE(first_storage->allocation_count() == 1);
    GLYPHA_REQUIRE(pool.adopt()->epoch() == 1);
    pool.reclaim();

    const glyphastore::store::paired::ReadMutation second_mutation{
        .key = hashed,
        .record = append(*segment, routing, key, "two", 2),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    auto second_reservation = pool.try_reserve();
    GLYPHA_REQUIRE(second_reservation.has_value());
    auto second_storage = bank.at(second_reservation->slot_index());
    auto second = Access::publish_incremental(writer_current, std::span{&second_mutation, 1}, second_storage);
    GLYPHA_REQUIRE(second.has_value());
    writer_current = *second;
    second_reservation->mark_store_linearized();
    GLYPHA_REQUIRE(pool.commit(*second_reservation, writer_current) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    second = glyphastore::fail(glyphastore::ErrorCode::internal_error, "moved to pool");
    GLYPHA_REQUIRE(pool.adopt()->epoch() == 2);
    auto found = pool.reader_generation()->get(hashed, 0);
    GLYPHA_REQUIRE(found.has_value());

    pool.reclaim();
    auto reuse_reservation = pool.try_reserve();
    GLYPHA_REQUIRE(reuse_reservation.has_value());
    const auto reuse_slot = reuse_reservation->slot_index();
    auto reuse_storage = bank.at(reuse_slot);
    GLYPHA_REQUIRE(reuse_slot == first_slot);
    GLYPHA_REQUIRE(!reuse_storage->occupied());
    const glyphastore::store::paired::ReadMutation third_mutation{
        .key = hashed,
        .record = append(*segment, routing, key, "three", 3),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    auto third = Access::publish_incremental(writer_current, std::span{&third_mutation, 1}, reuse_storage);
    GLYPHA_REQUIRE(third.has_value());
    GLYPHA_REQUIRE(reuse_storage->allocation_count() == 2);
    GLYPHA_REQUIRE(reuse_storage->reuse_count() == 1);
}

GLYPHA_TEST("ADR 0036 inline slot owner publishes without shared backing ownership") {
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Pool = glyphastore::experimental::PairReadGenerationInlineSlotPool<2>;
    const glyphastore::WorkerRoutingState routing{};
    auto initial = Generation::empty(routing);
    GLYPHA_REQUIRE(initial.has_value());
    auto pool_result = Pool::create(*initial);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{94});
    const std::string key{"inline-shell"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    for (std::uint64_t sequence = 1; sequence <= 128; ++sequence) {
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, "value", sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        auto reservation = pool.try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        reservation->mark_store_linearized();
        GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&mutation, 1}) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::published);
        const auto* adopted = pool.adopt();
        GLYPHA_REQUIRE(adopted != nullptr);
        GLYPHA_REQUIRE(adopted->epoch() == sequence);
        pool.reclaim();
    }

    const auto stats = pool.stats();
    GLYPHA_REQUIRE(stats.publications == 128);
    GLYPHA_REQUIRE(stats.live_slots == 1);
    GLYPHA_REQUIRE(pool.shell_allocation_count(0) == 64);
    GLYPHA_REQUIRE(pool.shell_allocation_count(1) == 64);
    GLYPHA_REQUIRE(pool.shell_reuse_count(0) == 63);
    GLYPHA_REQUIRE(pool.shell_reuse_count(1) == 63);
    const auto found = pool.adopt()->get(hashed, 0);
    GLYPHA_REQUIRE(found.has_value());
}

GLYPHA_TEST("ADR 0036 inline slot owner fail-closes a rejected post-linearization build") {
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Pool = glyphastore::experimental::PairReadGenerationInlineSlotPool<2>;
    std::atomic_uint64_t fail_closed_calls{};
    auto initial = Generation::empty({});
    GLYPHA_REQUIRE(initial.has_value());
    auto pool_result = Pool::create(
        *initial, {.context = &fail_closed_calls, .fail_closed = [](void* context) noexcept {
                       static_cast<std::atomic_uint64_t*>(context)->fetch_add(1U, std::memory_order_relaxed);
                   }});
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    const std::string key{"invalid-inline-shell"};
    const glyphastore::store::paired::ReadMutation invalid{
        .key = {key, glyphastore::hash_key_routing(key, {})},
        .record = {},
        .segment = {},
        .opcode = glyphastore::Opcode::put,
    };
    auto reservation = pool.try_reserve();
    GLYPHA_REQUIRE(reservation.has_value());
    reservation->mark_store_linearized();
    GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&invalid, 1}) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::invalid_generation);
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(pool.stats().unpublished_linearizations == 1);
    GLYPHA_REQUIRE(pool.stats().reserved_slots == 0);
    GLYPHA_REQUIRE(pool.shell_allocation_count(1) == 0);
}

GLYPHA_TEST("ADR 0036 inline slot owner publish adopt reclaim stress is race free") {
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Pool = glyphastore::experimental::PairReadGenerationInlineSlotPool<8>;
    const glyphastore::WorkerRoutingState routing{};
    auto initial = Generation::empty(routing);
    GLYPHA_REQUIRE(initial.has_value());
    auto pool_result = Pool::create(*initial);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    constexpr std::uint64_t kPublications = 10'000;
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{95});
    const std::string key{"inline-shell-stress"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    std::vector<glyphastore::RecordRef> records;
    records.reserve(kPublications);
    for (std::uint64_t sequence = 1; sequence <= kPublications; ++sequence) {
        records.push_back(append(*segment, routing, key, "value", sequence));
    }

    std::atomic_bool writer_done{};
    std::atomic_bool failed{};
    std::thread writer([&] {
        for (std::uint64_t sequence = 1; sequence <= kPublications; ++sequence) {
            const glyphastore::store::paired::ReadMutation mutation{
                .key = hashed,
                .record = records[sequence - 1U],
                .segment = segment,
                .opcode = glyphastore::Opcode::put,
            };
            for (;;) {
                auto reservation = pool.try_reserve();
                if (!reservation) {
                    std::this_thread::yield();
                    continue;
                }
                reservation->mark_store_linearized();
                if (pool.publish_incremental(*reservation, std::span{&mutation, 1}) !=
                    glyphastore::experimental::GenerationSlotPublishStatus::published) {
                    failed.store(true, std::memory_order_relaxed);
                    writer_done.store(true, std::memory_order_release);
                    return;
                }
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    while (!writer_done.load(std::memory_order_acquire) || pool.stats().reader_epoch < kPublications) {
        const auto* adopted = pool.adopt();
        if (adopted == nullptr || adopted->visible_through() != adopted->epoch()) {
            failed.store(true, std::memory_order_relaxed);
        }
        std::this_thread::yield();
    }
    writer.join();

    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(pool.stats().writer_epoch == kPublications);
    GLYPHA_REQUIRE(pool.stats().reader_epoch == kPublications);
    const auto found = pool.adopt()->get(hashed, 0);
    GLYPHA_REQUIRE(found.has_value());
}

GLYPHA_TEST("ADR 0036 direct generation ring matches official incremental publication") {
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Ring = glyphastore::experimental::PairReadGenerationDirectRing<2>;
    const glyphastore::WorkerRoutingState routing{};
    auto initial = Generation::empty(routing);
    GLYPHA_REQUIRE(initial.has_value());
    auto official = *initial;
    Ring ring{*initial};
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{96});
    const std::string key{"direct-generation"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};

    for (std::uint64_t sequence = 1; sequence <= 256; ++sequence) {
        const auto value = std::to_string(sequence);
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, value, sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        auto next_official = Generation::publish_incremental(official, std::span{&mutation, 1});
        auto next_direct = ring.publish(std::span{&mutation, 1});
        GLYPHA_REQUIRE(next_official.has_value());
        GLYPHA_REQUIRE(next_direct.has_value());
        official = *next_official;
        GLYPHA_REQUIRE((*next_direct)->epoch() == official->epoch());
        GLYPHA_REQUIRE((*next_direct)->visible_through() == official->visible_through());
        GLYPHA_REQUIRE((*next_direct)->delta_entries() == official->delta_entries());
        GLYPHA_REQUIRE((*next_direct)->delta_record_versions() == official->delta_record_versions());
        const auto official_value = official->get(hashed, 0);
        const auto direct_value = (*next_direct)->get(hashed, 0);
        GLYPHA_REQUIRE(official_value.has_value());
        GLYPHA_REQUIRE(direct_value.has_value());
        GLYPHA_REQUIRE(official_value->bytes == direct_value->bytes);
    }

    GLYPHA_REQUIRE(ring.allocation_count(0) == 128);
    GLYPHA_REQUIRE(ring.allocation_count(1) == 128);
    GLYPHA_REQUIRE(ring.reuse_count(0) == 127);
    GLYPHA_REQUIRE(ring.reuse_count(1) == 127);
}

GLYPHA_TEST("ADR 0036 direct generation ring preserves current after rejected build") {
    using Generation = glyphastore::store::paired::PairReadGeneration;
    using Ring = glyphastore::experimental::PairReadGenerationDirectRing<2>;
    const glyphastore::WorkerRoutingState routing{};
    auto initial = Generation::empty(routing);
    GLYPHA_REQUIRE(initial.has_value());
    Ring ring{*initial};
    const auto* before = ring.current();
    const std::string key{"direct-invalid"};
    const glyphastore::store::paired::ReadMutation invalid{
        .key = {key, glyphastore::hash_key_routing(key, routing)},
        .record = {},
        .segment = {},
        .opcode = glyphastore::Opcode::put,
    };
    auto rejected = ring.publish(std::span{&invalid, 1});
    GLYPHA_REQUIRE(!rejected.has_value());
    GLYPHA_REQUIRE(rejected.error().code == glyphastore::ErrorCode::invalid_reference);
    GLYPHA_REQUIRE(ring.current() == before);
    GLYPHA_REQUIRE(ring.allocation_count(0) == 0);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{97});
    const glyphastore::store::paired::ReadMutation valid{
        .key = invalid.key,
        .record = append(*segment, routing, key, "valid", 1),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    auto published = ring.publish(std::span{&valid, 1});
    GLYPHA_REQUIRE(published.has_value());
    GLYPHA_REQUIRE((*published)->get(invalid.key, 0).has_value());
    GLYPHA_REQUIRE(ring.allocation_count(0) == 1);
}

GLYPHA_TEST("ADR 0036 direct slot pool bounds debt and reuses reclaimed object storage") {
    using Pool = glyphastore::experimental::PairReadGenerationDirectSlotPool<3>;
    const glyphastore::WorkerRoutingState routing{};
    auto pool_result = Pool::create(routing);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    DirectPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{98});
    const std::string key{"direct-pool-bounded"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    for (std::uint64_t sequence = 1; sequence <= 2; ++sequence) {
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, "value", sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        auto reservation = pool.try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        reservation->mark_store_linearized();
        GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&mutation, 1}) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::published);
    }
    GLYPHA_REQUIRE(!pool.try_reserve().has_value());
    GLYPHA_REQUIRE(pool.stats().live_slots == 3);
    GLYPHA_REQUIRE(pool.stats().live_high_watermark == 3);

    GLYPHA_REQUIRE(pool.adopt()->epoch() == 2);
    pool.reclaim();
    GLYPHA_REQUIRE(pool.stats().live_slots == 1);
    const glyphastore::store::paired::ReadMutation third{
        .key = hashed,
        .record = append(*segment, routing, key, "three", 3),
        .segment = segment,
        .opcode = glyphastore::Opcode::put,
    };
    auto reservation = pool.try_reserve();
    GLYPHA_REQUIRE(reservation.has_value());
    const auto reused_slot = reservation->slot_index();
    reservation->mark_store_linearized();
    GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&third, 1}) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::published);
    const auto* adopted = pool.adopt();
    GLYPHA_REQUIRE(adopted != nullptr);
    GLYPHA_REQUIRE(adopted->epoch() == 3);
    GLYPHA_REQUIRE(adopted->get(hashed, 0).has_value());
    GLYPHA_REQUIRE(pool.shell_reuse_count(reused_slot) > 0);
    GLYPHA_REQUIRE(pool.stats().slot_reuses > 0);
}

GLYPHA_TEST("ADR 0036 direct slot pool holds a cold borrow frontier") {
    using Pool = glyphastore::experimental::PairReadGenerationDirectSlotPool<4>;
    const glyphastore::WorkerRoutingState routing{};
    auto pool_result = Pool::create(routing);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    DirectPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{99});
    const std::string key{"direct-pool-borrow"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};

    for (std::uint64_t sequence = 1; sequence <= 2; ++sequence) {
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, "value", sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        auto reservation = pool.try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        reservation->mark_store_linearized();
        GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&mutation, 1}) ==
                       glyphastore::experimental::GenerationSlotPublishStatus::published);
        if (sequence == 1) {
            GLYPHA_REQUIRE(pool.adopt()->epoch() == 1);
        }
    }
    GLYPHA_REQUIRE(pool.adopt(1)->epoch() == 2);
    pool.reclaim();
    GLYPHA_REQUIRE(pool.stats().reader_safe_epoch == 1);
    GLYPHA_REQUIRE(pool.stats().live_slots == 2);

    GLYPHA_REQUIRE(pool.adopt()->epoch() == 2);
    pool.reclaim();
    GLYPHA_REQUIRE(pool.stats().reader_safe_epoch == 2);
    GLYPHA_REQUIRE(pool.stats().live_slots == 1);
    GLYPHA_REQUIRE(pool.adopt(1) == nullptr);
}

GLYPHA_TEST("ADR 0036 direct slot pool fail-closes a rejected linearized build") {
    using Pool = glyphastore::experimental::PairReadGenerationDirectSlotPool<2>;
    std::atomic_uint64_t fail_closed_calls{};
    auto pool_result = Pool::create(
        {}, {.context = &fail_closed_calls, .fail_closed = [](void* context) noexcept {
                 static_cast<std::atomic_uint64_t*>(context)->fetch_add(1U, std::memory_order_relaxed);
             }});
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    DirectPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    const std::string key{"direct-pool-invalid"};
    const glyphastore::store::paired::ReadMutation invalid{
        .key = {key, glyphastore::hash_key_routing(key, {})},
        .record = {},
        .segment = {},
        .opcode = glyphastore::Opcode::put,
    };
    auto reservation = pool.try_reserve();
    GLYPHA_REQUIRE(reservation.has_value());
    const auto slot = reservation->slot_index();
    reservation->mark_store_linearized();
    GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&invalid, 1}) ==
                   glyphastore::experimental::GenerationSlotPublishStatus::invalid_generation);
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(pool.stats().unpublished_linearizations == 1);
    GLYPHA_REQUIRE(pool.stats().reserved_slots == 0);
    GLYPHA_REQUIRE(pool.shell_allocation_count(slot) == 0);
}

GLYPHA_TEST("ADR 0036 direct slot pool shutdown rejects late admission and revokes adoption") {
    using Pool = glyphastore::experimental::PairReadGenerationDirectSlotPool<3>;
    auto pool_result = Pool::create({});
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    DirectPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    auto reservation = pool.try_reserve();
    GLYPHA_REQUIRE(reservation.has_value());
    pool.stop_admission();
    GLYPHA_REQUIRE(!pool.accepting());
    GLYPHA_REQUIRE(!pool.try_reserve().has_value());
    GLYPHA_REQUIRE(!pool.mark_reader_quiescent());
    GLYPHA_REQUIRE(!pool.try_finish_shutdown());
    reservation.reset();
    GLYPHA_REQUIRE(pool.mark_reader_quiescent());
    GLYPHA_REQUIRE(pool.try_finish_shutdown());
    GLYPHA_REQUIRE(pool.adopt() == nullptr);

    const auto stats = pool.stats();
    GLYPHA_REQUIRE(stats.shutdown_starts == 1);
    GLYPHA_REQUIRE(stats.shutdown_reservation_rejections == 1);
    GLYPHA_REQUIRE(stats.reservation_cancellations == 1);
    GLYPHA_REQUIRE(stats.live_slots == 1);
    GLYPHA_REQUIRE(stats.reader_quiescent);
}

GLYPHA_TEST("ADR 0036 direct slot pool publish adopt reclaim stress is race free") {
    using Pool = glyphastore::experimental::PairReadGenerationDirectSlotPool<8>;
    const glyphastore::WorkerRoutingState routing{};
    auto pool_result = Pool::create(routing);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    DirectPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    constexpr std::uint64_t kPublications = 10'000;
    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{100});
    const std::string key{"direct-pool-stress"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    std::vector<glyphastore::RecordRef> records;
    records.reserve(kPublications);
    for (std::uint64_t sequence = 1; sequence <= kPublications; ++sequence) {
        records.push_back(append(*segment, routing, key, "value", sequence));
    }

    std::atomic_bool writer_done{};
    std::atomic_bool failed{};
    std::thread writer([&] {
        for (std::uint64_t sequence = 1; sequence <= kPublications; ++sequence) {
            const glyphastore::store::paired::ReadMutation mutation{
                .key = hashed,
                .record = records[sequence - 1U],
                .segment = segment,
                .opcode = glyphastore::Opcode::put,
            };
            for (;;) {
                auto reservation = pool.try_reserve();
                if (!reservation) {
                    std::this_thread::yield();
                    continue;
                }
                reservation->mark_store_linearized();
                if (pool.publish_incremental(*reservation, std::span{&mutation, 1}) !=
                    glyphastore::experimental::GenerationSlotPublishStatus::published) {
                    failed.store(true, std::memory_order_relaxed);
                    writer_done.store(true, std::memory_order_release);
                    return;
                }
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    while (!writer_done.load(std::memory_order_acquire) || pool.stats().reader_epoch < kPublications) {
        const auto* adopted = pool.adopt();
        if (adopted == nullptr || adopted->visible_through() != adopted->epoch()) {
            failed.store(true, std::memory_order_relaxed);
        }
        std::this_thread::yield();
    }
    writer.join();
    pool.reclaim();

    GLYPHA_REQUIRE(!failed.load(std::memory_order_relaxed));
    GLYPHA_REQUIRE(pool.stats().writer_epoch == kPublications);
    GLYPHA_REQUIRE(pool.stats().reader_epoch == kPublications);
    GLYPHA_REQUIRE(pool.stats().live_high_watermark <= 8);
    GLYPHA_REQUIRE(pool.stats().slot_reuses > 0);
    GLYPHA_REQUIRE(pool.adopt()->get(hashed, 0).has_value());
}
