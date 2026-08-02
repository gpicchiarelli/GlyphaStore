#include "glyphastore/core/fault_injection.hpp"

#if defined(GLYPHASTORE_FAULT_INJECTION)

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace glyphastore::fault {
namespace {

struct Config final {
    std::atomic<std::uint64_t> rng{0xC0FFEEULL};
    std::atomic<std::uint32_t> yield_percent{25};
    std::atomic<std::uint32_t> sleep_us_max{50};
    std::atomic<std::uint8_t> fail_site{0};
    std::atomic<std::uint32_t> fail_remaining{0};
};

Config& config() noexcept {
    static Config instance;
    return instance;
}

[[nodiscard]] auto next_u32() noexcept -> std::uint32_t {
    auto& state = config().rng;
    auto x = state.load(std::memory_order_relaxed);
    for (;;) {
        auto y = x;
        y ^= y >> 12;
        y ^= y << 25;
        y ^= y >> 27;
        const auto next = y * 0x2545F4914F6CDD1DULL;
        if (state.compare_exchange_weak(x, next, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return static_cast<std::uint32_t>(next >> 32);
        }
    }
}

} // namespace

void configure(const std::uint64_t seed, const std::uint32_t yield_percent,
               const std::uint32_t sleep_us_max) noexcept {
    config().rng.store(seed == 0 ? 0xC0FFEEULL : seed, std::memory_order_relaxed);
    config().yield_percent.store(yield_percent > 100 ? 100 : yield_percent, std::memory_order_relaxed);
    config().sleep_us_max.store(sleep_us_max, std::memory_order_relaxed);
}

void reset() noexcept {
    configure(0xC0FFEEULL, 25, 50);
    config().fail_site.store(0, std::memory_order_relaxed);
    config().fail_remaining.store(0, std::memory_order_relaxed);
}

void fail_nth(const Site site, const std::uint32_t n) noexcept {
    if (n == 0) {
        config().fail_remaining.store(0, std::memory_order_relaxed);
        config().fail_site.store(0, std::memory_order_release);
        return;
    }
    config().fail_remaining.store(n, std::memory_order_relaxed);
    config().fail_site.store(static_cast<std::uint8_t>(site), std::memory_order_release);
}

void fail_once(const Site site) noexcept {
    fail_nth(site, 1);
}

auto consume_fail(const Site site) noexcept -> bool {
    if (config().fail_site.load(std::memory_order_acquire) != static_cast<std::uint8_t>(site)) {
        return false;
    }
    auto remaining = config().fail_remaining.load(std::memory_order_relaxed);
    while (remaining > 0) {
        if (config().fail_site.load(std::memory_order_acquire) != static_cast<std::uint8_t>(site)) {
            return false;
        }
        if (config().fail_remaining.compare_exchange_weak(remaining, remaining - 1,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_relaxed)) {
            if (remaining == 1) {
                config().fail_site.store(0, std::memory_order_release);
                return true;
            }
            return false;
        }
    }
    return false;
}

void at(const Site) noexcept {
    const auto roll = next_u32() % 100U;
    const auto yield_percent = config().yield_percent.load(std::memory_order_relaxed);
    if (roll >= yield_percent) {
        return;
    }
    std::this_thread::yield();
    const auto sleep_max = config().sleep_us_max.load(std::memory_order_relaxed);
    if (sleep_max == 0) {
        return;
    }
    const auto delay = next_u32() % (sleep_max + 1U);
    if (delay != 0) {
        std::this_thread::sleep_for(std::chrono::microseconds{delay});
    }
}

} // namespace glyphastore::fault

#endif
