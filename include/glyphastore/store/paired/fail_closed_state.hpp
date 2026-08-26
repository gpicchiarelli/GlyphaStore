#pragma once

// Sticky fail-closed arm for the paired Writer (behavior-neutral extraction).
// Normative: docs/spec/mutation-lifecycle.md

#include <atomic>
#include <cstdint>
#include <span>

namespace glyphastore {
class Store;
}

namespace glyphastore::store::paired {

// Narrow view of per-lane wake bits used when arming fail-closed.
struct FailClosedLaneWake final {
    std::atomic_uint64_t* signal{};
};

enum class FailClosedScope : std::uint8_t {
    // Pair healthy_/expire_remaining_ + lane wake only (defer Store mark until snapshot).
    pair_only,
    // Full sticky: pair + StoreAccess::mark_fail_closed.
    pair_and_store,
};

class FailClosedState final {
  public:
    FailClosedState(Store& store, std::atomic_bool& healthy, std::atomic_bool& expire_remaining) noexcept
        : store_(store), healthy_(healthy), expire_remaining_(expire_remaining) {}

    [[nodiscard]] auto healthy() const noexcept -> bool {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] auto expire_remaining_armed() const noexcept -> bool {
        return expire_remaining_.load(std::memory_order_acquire);
    }

    // Arms sticky fail-closed and wakes every lane so Writers observe reject.
    void arm(std::span<const FailClosedLaneWake> lanes,
             FailClosedScope scope = FailClosedScope::pair_and_store) noexcept;

  private:
    Store& store_;
    std::atomic_bool& healthy_;
    std::atomic_bool& expire_remaining_;
};

} // namespace glyphastore::store::paired
