#pragma once

#include <atomic>
#include <cstddef>
#include <limits>

namespace allocation_fault {

struct Observation {
    std::size_t observed{};
    bool fired{};
};

extern std::atomic_bool forbid_all;

void arm(std::size_t fail_at) noexcept;
void arm_process(std::size_t fail_at) noexcept;
void disarm_process() noexcept;
auto disarm() noexcept -> Observation;
void begin_forbid_all() noexcept;
auto end_forbid_all() noexcept -> bool;

} // namespace allocation_fault
