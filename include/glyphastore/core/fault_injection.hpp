#pragma once

// Debug-only adverse scheduling hooks for concurrency stress (Phase B1).
//
// Production / default builds: every GS_FAULT_SITE expands to a no-op.
// Enable only with -DGLYPHASTORE_FAULT_INJECTION=ON (CMake) which defines the
// compile macro GLYPHASTORE_FAULT_INJECTION. Never enable in release packaging.

#include <cstdint>

namespace glyphastore::fault {

enum class Site : std::uint8_t {
    enqueue = 1,
    mutate = 2,
    publish = 3,
    adopt = 4,
    rotate = 5,
    compact = 6,
    close = 7,
    capture = 8,
    deferred_ttl = 9,
    index_account = 10,
};

#if defined(GLYPHASTORE_FAULT_INJECTION)

void at(Site site) noexcept;
void configure(std::uint64_t seed, std::uint32_t yield_percent, std::uint32_t sleep_us_max) noexcept;
void reset() noexcept;
// Deterministic one-shot failure for litmus tests (does not throw from at()).
void fail_once(Site site) noexcept;
// Fail the Nth consume_fail for site (1 = first). Zero clears.
void fail_nth(Site site, std::uint32_t n) noexcept;
[[nodiscard]] auto consume_fail(Site site) noexcept -> bool;

#else

inline void at(Site) noexcept {}
inline void configure(std::uint64_t, std::uint32_t, std::uint32_t) noexcept {}
inline void reset() noexcept {}
inline void fail_once(Site) noexcept {}
inline void fail_nth(Site, std::uint32_t) noexcept {}
inline auto consume_fail(Site) noexcept -> bool {
    return false;
}

#endif

} // namespace glyphastore::fault

#if defined(GLYPHASTORE_FAULT_INJECTION)
#define GS_FAULT_SITE(site) ::glyphastore::fault::at(::glyphastore::fault::Site::site)
#else
#define GS_FAULT_SITE(site) ((void)0)
#endif
