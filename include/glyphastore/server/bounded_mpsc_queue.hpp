#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

namespace glyphastore::server {

// Bounded multi-producer/single-consumer ring. Producers reserve cells with an
// atomic sequence; the sole consumer owns dequeue_position_. No allocation is
// performed after construction.
template <typename T> class BoundedMpscQueue final {
  public:
    explicit BoundedMpscQueue(const std::size_t requested_capacity)
        : capacity_(std::bit_ceil(std::max(std::size_t{2}, requested_capacity))), mask_(capacity_ - 1U),
          cells_(std::make_unique<Cell[]>(capacity_)) {
        for (std::size_t index = 0; index < capacity_; ++index) {
            cells_[index].sequence.store(index, std::memory_order_relaxed);
        }
    }

    BoundedMpscQueue(const BoundedMpscQueue&) = delete;
    auto operator=(const BoundedMpscQueue&) -> BoundedMpscQueue& = delete;
    BoundedMpscQueue(BoundedMpscQueue&&) = delete;
    auto operator=(BoundedMpscQueue&&) -> BoundedMpscQueue& = delete;

    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }

    [[nodiscard]] auto try_push(T&& value) -> bool {
        auto position = enqueue_position_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        while (true) {
            cell = &cells_[position & mask_];
            const auto sequence = cell->sequence.load(std::memory_order_acquire);
            const auto difference =
                static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position);
            if (difference == 0) {
                if (enqueue_position_.compare_exchange_weak(position, position + 1U,
                                                            std::memory_order_relaxed)) {
                    break;
                }
                continue;
            }
            if (difference < 0) {
                return false;
            }
            position = enqueue_position_.load(std::memory_order_relaxed);
        }
        cell->value.emplace(std::move(value));
        cell->sequence.store(position + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] auto try_pop() -> std::optional<T> {
        auto& cell = cells_[dequeue_position_ & mask_];
        const auto sequence = cell.sequence.load(std::memory_order_acquire);
        const auto difference =
            static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(dequeue_position_ + 1U);
        if (difference != 0) {
            return std::nullopt;
        }
        auto value = std::optional<T>{std::move(*cell.value)};
        cell.value.reset();
        cell.sequence.store(dequeue_position_ + capacity_, std::memory_order_release);
        ++dequeue_position_;
        return value;
    }

  private:
    struct Cell {
        std::atomic<std::size_t> sequence{};
        std::optional<T> value;
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_position_{};
    alignas(64) std::size_t dequeue_position_{};
};

} // namespace glyphastore::server
