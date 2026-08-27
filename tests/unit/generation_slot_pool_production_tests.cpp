#include "glyphastore/store/paired/generation_slot_pool.hpp"
#include "test.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

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

struct ProductionPoolShutdown final {
    glyphastore::store::paired::GenerationSlotPool* pool{};
    ~ProductionPoolShutdown() {
        if (pool == nullptr) {
            return;
        }
        pool->stop_admission();
        pool->revoke_publication();
        static_cast<void>(pool->mark_reader_quiescent());
        static_cast<void>(pool->try_finish_shutdown());
    }
};

} // namespace

GLYPHA_TEST("ADR 0036 production slot V1 token adopt and reincarnation") {
    using Pool = glyphastore::store::paired::GenerationSlotPool;
    using Status = glyphastore::store::paired::GenerationSlotPublishStatus;
    const glyphastore::WorkerRoutingState routing{};
    auto pool_result = Pool::create(routing);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    ProductionPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);
    GLYPHA_REQUIRE(Pool::kCapacity == 65U);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{201});
    const std::string key{"prod-slot-v1"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    constexpr std::uint64_t kCycles = 10'000;
    for (std::uint64_t sequence = 1; sequence <= kCycles; ++sequence) {
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, "value", sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        for (;;) {
            auto reservation = pool.try_reserve();
            if (!reservation) {
                static_cast<void>(pool.adopt());
                pool.reclaim(pool.stats().reader_safe_epoch == 0 ? 1 : pool.stats().reader_safe_epoch);
                continue;
            }
            reservation->mark_store_linearized();
            GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&mutation, 1}) ==
                           Status::published);
            break;
        }
        const auto token = pool.publication_token();
        GLYPHA_REQUIRE(!token.empty());
        GLYPHA_REQUIRE(token.epoch() == sequence);
        const auto* adopted = pool.adopt();
        GLYPHA_REQUIRE(adopted != nullptr);
        GLYPHA_REQUIRE(adopted->epoch() == sequence);
        GLYPHA_REQUIRE(pool.decode_published(token) == adopted);
        pool.reclaim(sequence);
    }
    GLYPHA_REQUIRE(pool.stats().slot_reuses > 0);
    GLYPHA_REQUIRE(pool.stats().writer_epoch == kCycles);
}

GLYPHA_TEST("ADR 0036 production slot V6 reserve-before-mutate fail-closed") {
    using Pool = glyphastore::store::paired::GenerationSlotPool;
    using Status = glyphastore::store::paired::GenerationSlotPublishStatus;
    std::atomic_uint64_t fail_closed_calls{};
    auto pool_result = Pool::create(
        {}, {}, {.context = &fail_closed_calls, .fail_closed = [](void* context) noexcept {
                     static_cast<std::atomic_uint64_t*>(context)->fetch_add(1U, std::memory_order_relaxed);
                 }});
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    ProductionPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    const std::string key{"prod-slot-v6"};
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
                   Status::invalid_generation);
    GLYPHA_REQUIRE(fail_closed_calls.load(std::memory_order_relaxed) == 1);
    GLYPHA_REQUIRE(pool.stats().unpublished_linearizations == 1);
}

GLYPHA_TEST("ADR 0036 production slot V9 exhaustion backpressure") {
    using Pool = glyphastore::store::paired::GenerationSlotPool;
    using Status = glyphastore::store::paired::GenerationSlotPublishStatus;
    const glyphastore::WorkerRoutingState routing{};
    auto pool_result = Pool::create(routing);
    GLYPHA_REQUIRE(pool_result.has_value());
    auto& pool = **pool_result;
    ProductionPoolShutdown guard{&pool};
    GLYPHA_REQUIRE(pool.adopt() != nullptr);

    auto segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{202});
    const std::string key{"prod-slot-v9"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key_routing(key, routing)};
    // Fill to capacity: 1 current + 64 retired = 65 live.
    for (std::uint64_t sequence = 1; sequence <= 64; ++sequence) {
        const glyphastore::store::paired::ReadMutation mutation{
            .key = hashed,
            .record = append(*segment, routing, key, "value", sequence),
            .segment = segment,
            .opcode = glyphastore::Opcode::put,
        };
        auto reservation = pool.try_reserve();
        GLYPHA_REQUIRE(reservation.has_value());
        reservation->mark_store_linearized();
        GLYPHA_REQUIRE(pool.publish_incremental(*reservation, std::span{&mutation, 1}) == Status::published);
    }
    GLYPHA_REQUIRE(pool.stats().live_slots == 65);
    GLYPHA_REQUIRE(!pool.try_reserve().has_value());
    GLYPHA_REQUIRE(pool.stats().pool_exhaustions >= 1);
    const auto before_epoch = pool.stats().writer_epoch;
    GLYPHA_REQUIRE(pool.adopt()->epoch() == before_epoch);
    pool.reclaim(before_epoch);
    GLYPHA_REQUIRE(pool.stats().live_slots == 1);
    auto recovered = pool.try_reserve();
    GLYPHA_REQUIRE(recovered.has_value());
    recovered->reset();
}

GLYPHA_TEST("ADR 0036 production slot epoch overflow and token width") {
    using Token = glyphastore::store::paired::GenerationPublicationToken;
    static_assert(Token::kSlotBits == 16U);
    static_assert(Token::kMaximumEpoch == (std::numeric_limits<std::uint64_t>::max() >> 16U));

    const auto token = Token::encode(Token::kMaximumEpoch, 64);
    GLYPHA_REQUIRE(token.epoch() == Token::kMaximumEpoch);
    GLYPHA_REQUIRE(token.slot_index() == 64U);
    GLYPHA_REQUIRE(!token.empty());
    GLYPHA_REQUIRE(Token{}.empty());
}
