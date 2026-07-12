#include "harness.hpp"

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <cstdio>

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

auto process_memory_snapshot() noexcept -> ResourceSample {
#if defined(__APPLE__)
    task_vm_info_data_t info{};
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    const auto status =
        task_info(mach_task_self(), TASK_VM_INFO, reinterpret_cast<task_info_t>(&info), &count);
    if (status != KERN_SUCCESS) {
        return {};
    }
    const auto resident = static_cast<std::size_t>(info.resident_size);
    return {.rss_before_bytes = resident,
            .rss_after_bytes = resident,
            .peak_rss_bytes = static_cast<std::size_t>(info.resident_size_peak)};
#elif defined(__linux__)
    std::size_t resident{};
    if (auto* statm = std::fopen("/proc/self/statm", "r"); statm != nullptr) {
        unsigned long ignored_pages{};
        unsigned long resident_pages{};
        if (std::fscanf(statm, "%lu %lu", &ignored_pages, &resident_pages) == 2) {
            const auto page_size = ::sysconf(_SC_PAGESIZE);
            if (page_size > 0) {
                resident = static_cast<std::size_t>(resident_pages) * static_cast<std::size_t>(page_size);
            }
        }
        static_cast<void>(std::fclose(statm));
    }
    rusage usage{};
    const auto peak =
        ::getrusage(RUSAGE_SELF, &usage) == 0 ? static_cast<std::size_t>(usage.ru_maxrss) * 1024U : 0U;
    return {.rss_before_bytes = resident, .rss_after_bytes = resident, .peak_rss_bytes = peak};
#else
    return {};
#endif
}

} // namespace glyphastore::bench
