#pragma once

#include <cstddef>
#include <string_view>

namespace glyphastore::server {

enum class ExecutorAffinityMode { disabled, unavailable, advisory, pinned };

struct ExecutorAffinityResult {
    ExecutorAffinityMode mode{ExecutorAffinityMode::disabled};
    int cpu{-1};
};

[[nodiscard]] auto configure_executor_thread(std::size_t executor_id, bool affinity_requested) noexcept
    -> ExecutorAffinityResult;
[[nodiscard]] auto affinity_mode_name(ExecutorAffinityMode mode) noexcept -> std::string_view;

} // namespace glyphastore::server
