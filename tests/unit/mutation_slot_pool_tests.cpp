#include "server/mutation_slot_pool.hpp"
#include "test.hpp"

#include <algorithm>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

[[nodiscard]] auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] auto text(const std::span<const std::byte> value) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

} // namespace

GLYPHA_TEST("mutation slot pool copies borrowed payload into one stable preallocated slot") {
    glyphastore::server::internal::MutationSlotPool pool{2, 1024, 512};
    std::string key{"arena-key"};
    std::string value{"arena-value"};
    const auto admission = 128U + key.size() + value.size();
    const auto acquired = pool.try_acquire(bytes(key), bytes(value), admission);
    GLYPHA_REQUIRE(acquired.lease.has_value());
    GLYPHA_REQUIRE(acquired.failure == glyphastore::server::internal::MutationSlotPool::AcquireFailure::none);
    std::ranges::fill(key, 'x');
    std::ranges::fill(value, 'y');

    const auto view = pool.view(acquired.lease->slot);
    GLYPHA_REQUIRE(view.has_value());
    GLYPHA_REQUIRE(view->key == "arena-key");
    GLYPHA_REQUIRE(text(view->value) == "arena-value");
    GLYPHA_REQUIRE(view->admission_bytes == admission);
    GLYPHA_REQUIRE(pool.slots_in_use() == 1);
    GLYPHA_REQUIRE(pool.payload_bytes_in_use() == 20);
    GLYPHA_REQUIRE(pool.admission_bytes_in_use() == admission);
    GLYPHA_REQUIRE(pool.release(acquired.lease->slot));
    GLYPHA_REQUIRE(pool.slots_in_use() == 0);
    GLYPHA_REQUIRE(pool.payload_bytes_in_use() == 0);
    GLYPHA_REQUIRE(pool.admission_bytes_in_use() == 0);
}

GLYPHA_TEST("mutation slot pool keeps wrapped payload contiguous without overwriting live FIFO slots") {
    glyphastore::server::internal::MutationSlotPool pool{3, 512, 128};
    const std::string payload_a(100, 'a');
    const std::string payload_b(100, 'b');
    const std::string payload_c(100, 'c');
    const std::string payload_d(100, 'd');
    const std::string payload_e(100, 'e');
    const std::string payload_f(100, 'f');
    const auto acquire = [&](const std::string& value) {
        return pool.try_acquire({}, bytes(value), 228).lease;
    };

    const auto a = acquire(payload_a);
    const auto b = acquire(payload_b);
    GLYPHA_REQUIRE(a.has_value());
    GLYPHA_REQUIRE(b.has_value());
    GLYPHA_REQUIRE(pool.release(a->slot));
    const auto c = acquire(payload_c);
    GLYPHA_REQUIRE(c.has_value());
    GLYPHA_REQUIRE(pool.release(b->slot));
    const auto d = acquire(payload_d);
    GLYPHA_REQUIRE(d.has_value());
    GLYPHA_REQUIRE(pool.release(c->slot));
    const auto e = acquire(payload_e);
    GLYPHA_REQUIRE(e.has_value());
    GLYPHA_REQUIRE(pool.release(d->slot));
    const auto f = acquire(payload_f);
    GLYPHA_REQUIRE(f.has_value());

    const auto e_view = pool.view(e->slot);
    const auto f_view = pool.view(f->slot);
    GLYPHA_REQUIRE(e_view.has_value());
    GLYPHA_REQUIRE(f_view.has_value());
    GLYPHA_REQUIRE(text(e_view->value) == payload_e);
    GLYPHA_REQUIRE(text(f_view->value) == payload_f);
    GLYPHA_REQUIRE(pool.release(e->slot));
    GLYPHA_REQUIRE(pool.release(f->slot));
    GLYPHA_REQUIRE(pool.payload_bytes_in_use() == 0);
}

GLYPHA_TEST("mutation slot pool rejects out-of-order release and rolls back unpublished tail") {
    using Pool = glyphastore::server::internal::MutationSlotPool;
    Pool pool{2, 256, 128};
    const auto first = pool.try_acquire(bytes("first"), bytes("one"), 136);
    const auto second = pool.try_acquire(bytes("second"), bytes("two"), 137);
    GLYPHA_REQUIRE(first.lease.has_value());
    GLYPHA_REQUIRE(!second.lease.has_value());
    GLYPHA_REQUIRE(second.failure == Pool::AcquireFailure::byte_exhausted);
    GLYPHA_REQUIRE(pool.rollback(*first.lease));
    GLYPHA_REQUIRE(pool.slots_in_use() == 0);

    const auto a = pool.try_acquire(bytes("a"), {}, 128);
    const auto b = pool.try_acquire(bytes("b"), {}, 128);
    GLYPHA_REQUIRE(a.lease.has_value());
    GLYPHA_REQUIRE(b.lease.has_value());
    GLYPHA_REQUIRE(!pool.release(b.lease->slot));
    GLYPHA_REQUIRE(pool.release(a.lease->slot));
    GLYPHA_REQUIRE(pool.release(b.lease->slot));
}

GLYPHA_TEST("mutation slot pool distinguishes slot byte and payload bounds") {
    using Pool = glyphastore::server::internal::MutationSlotPool;
    Pool one_slot{1, 512, 128};
    const auto held = one_slot.try_acquire(bytes("key"), bytes("value"), 136);
    GLYPHA_REQUIRE(held.lease.has_value());
    const auto slot_full = one_slot.try_acquire(bytes("next"), {}, 132);
    GLYPHA_REQUIRE(!slot_full.lease.has_value());
    GLYPHA_REQUIRE(slot_full.failure == Pool::AcquireFailure::slot_exhausted);
    GLYPHA_REQUIRE(one_slot.release(held.lease->slot));

    const std::vector<std::byte> oversized(129, std::byte{0x7A});
    const auto too_large = one_slot.try_acquire({}, oversized, 257);
    GLYPHA_REQUIRE(!too_large.lease.has_value());
    GLYPHA_REQUIRE(too_large.failure == Pool::AcquireFailure::payload_too_large);

    Pool byte_limited{2, 256, 128};
    const auto first = byte_limited.try_acquire(bytes("a"), {}, 200);
    GLYPHA_REQUIRE(first.lease.has_value());
    const auto byte_full = byte_limited.try_acquire(bytes("b"), {}, 200);
    GLYPHA_REQUIRE(!byte_full.lease.has_value());
    GLYPHA_REQUIRE(byte_full.failure == Pool::AcquireFailure::byte_exhausted);
    GLYPHA_REQUIRE(byte_limited.release(first.lease->slot));
}
