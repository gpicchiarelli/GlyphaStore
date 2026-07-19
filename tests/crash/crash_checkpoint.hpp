#pragma once

#include "glyphastore/persistence/filesystem.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <string>
#include <string_view>
#include <unistd.h>

namespace glyphastore::crash {

struct CheckpointState {
    std::filesystem::path checkpoint_dir;
    std::string kill_at;
    bool signaled{false};
    std::array<std::size_t, static_cast<std::size_t>(FilesystemOperation::remove_compaction_segment) + 1U>
        occurrences{};

    static void after(void* context, const FilesystemOperation operation) {
        auto& state = *static_cast<CheckpointState*>(context);
        const auto name = std::string{filesystem_operation_name(operation)};
        const auto operation_index = static_cast<std::size_t>(operation);
        const auto occurrence = ++state.occurrences[operation_index];
        const auto occurrence_name = name + '#' + std::to_string(occurrence);
        std::ofstream{state.checkpoint_dir / name}.put('1');
        std::ofstream{state.checkpoint_dir / occurrence_name}.put('1');
        if (!state.kill_at.empty() && (state.kill_at == name || state.kill_at == occurrence_name) &&
            !state.signaled) {
            state.signaled = true;
            // Stop exactly at the boundary. The parent observes the marker and
            // then sends SIGKILL; using SIGSTOP here prevents the worker from
            // advancing into the next persistence transition before that kill.
            if (::kill(::getpid(), SIGSTOP) != 0) {
                std::abort();
            }
        }
    }

    [[nodiscard]] auto hooks() const -> FilesystemHooks {
        return FilesystemHooks{.context = const_cast<CheckpointState*>(this),
                               .after = &CheckpointState::after};
    }
};

inline void remove_checkpoint_markers(const std::filesystem::path& checkpoint_dir) {
    std::error_code ignored;
    if (!std::filesystem::exists(checkpoint_dir, ignored)) {
        return;
    }
    for (const auto& entry : std::filesystem::directory_iterator(checkpoint_dir, ignored)) {
        std::filesystem::remove(entry.path(), ignored);
    }
}

inline auto wait_for_checkpoint(const std::filesystem::path& checkpoint_dir, const std::string_view operation,
                                const int timeout_ms = 30'000) -> bool {
    const auto marker = checkpoint_dir / std::string{operation};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        std::error_code ignored;
        if (std::filesystem::exists(marker, ignored)) {
            return true;
        }
        ::usleep(1'000);
    }
    return false;
}

} // namespace glyphastore::crash
