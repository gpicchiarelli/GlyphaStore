#include "harness.hpp"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace glyphastore::bench {

namespace {
bool g_cpu_pin_applied = false;
} // namespace

[[nodiscard]] auto cpu_pin_applied() noexcept -> bool {
    return g_cpu_pin_applied;
}

bool try_cpu_pin(const bool requested) {
    g_cpu_pin_applied = false;
    if (!requested) {
        return false;
    }
#if defined(__linux__)
    cpu_set_t cpuset{};
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == 0) {
        g_cpu_pin_applied = true;
        return true;
    }
    return false;
#else
    (void)requested;
    return false;
#endif
}

} // namespace glyphastore::bench
