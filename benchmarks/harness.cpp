#include "harness.hpp"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace glyphastore::bench {

void apply_cpu_pin(const RunSettings& settings) {
    if (!settings.pin_cpu) {
        return;
    }
#if defined(__linux__)
    cpu_set_t cpuset{};
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#else
    (void)settings;
#endif
}

} // namespace glyphastore::bench
