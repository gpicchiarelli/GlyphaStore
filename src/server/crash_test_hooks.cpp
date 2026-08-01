#include "glyphastore/server/crash_test_hooks.hpp"

#include "glyphastore/persistence/filesystem.hpp"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <string>
#include <string_view>
#include <unistd.h>

namespace glyphastore::server {
namespace {

struct CrashHookState {
    std::filesystem::path checkpoint_dir;
    std::string kill_at;
    bool signaled{false};
    std::array<std::size_t, static_cast<std::size_t>(FilesystemOperation::sync_backup_destination) + 1U>
        occurrences{};
};

CrashHookState g_crash_hooks{};

void after_operation(void* context, const FilesystemOperation operation) {
    auto& state = *static_cast<CrashHookState*>(context);
    const auto name = std::string{filesystem_operation_name(operation)};
    const auto operation_index = static_cast<std::size_t>(operation);
    const auto occurrence = ++state.occurrences[operation_index];
    const auto occurrence_name = name + '#' + std::to_string(occurrence);
    std::ofstream{state.checkpoint_dir / name}.put('1');
    std::ofstream{state.checkpoint_dir / occurrence_name}.put('1');
    if (!state.kill_at.empty() && (state.kill_at == name || state.kill_at == occurrence_name) &&
        !state.signaled) {
        state.signaled = true;
        // Self-SIGKILL: see tests/crash/crash_checkpoint.hpp — avoids the
        // multi-threaded marker→SIGSTOP race that let mid-backup / mid-commit
        // work continue on Linux.
        if (::raise(SIGKILL) != 0) {
            std::_Exit(1);
        }
        std::_Exit(1);
    }
}

} // namespace

auto maybe_install_crash_test_hooks(StoreConfig& store) -> Status {
    const char* enabled = std::getenv("GLYPHASTORE_CRASH_TEST");
    if (enabled == nullptr || std::string_view{enabled} != "1") {
        return {};
    }
    const char* kill_at = std::getenv("GLYPHASTORE_CRASH_KILL_AT");
    const char* checkpoint_dir = std::getenv("GLYPHASTORE_CRASH_CHECKPOINT_DIR");
    if (kill_at == nullptr || *kill_at == '\0' || checkpoint_dir == nullptr || *checkpoint_dir == '\0') {
        return fail(ErrorCode::invalid_argument,
                    "GLYPHASTORE_CRASH_TEST=1 requires GLYPHASTORE_CRASH_KILL_AT and "
                    "GLYPHASTORE_CRASH_CHECKPOINT_DIR");
    }
    if (store.filesystem_hooks.after != nullptr || store.filesystem_hooks.before != nullptr ||
        store.filesystem_hooks.context != nullptr) {
        return fail(ErrorCode::invalid_argument,
                    "GLYPHASTORE_CRASH_TEST cannot combine with other filesystem hooks");
    }

    g_crash_hooks = CrashHookState{
        .checkpoint_dir = checkpoint_dir,
        .kill_at = kill_at,
    };
    std::error_code ec;
    std::filesystem::create_directories(g_crash_hooks.checkpoint_dir, ec);
    if (ec) {
        return fail(ErrorCode::io_error, "cannot create GLYPHASTORE_CRASH_CHECKPOINT_DIR");
    }

    store.filesystem_hooks = FilesystemHooks{
        .context = &g_crash_hooks,
        .after = &after_operation,
    };
    std::cerr << "glyphastored: warning: crash-test hooks active kill_at=" << g_crash_hooks.kill_at
              << " checkpoint_dir=" << g_crash_hooks.checkpoint_dir.string() << '\n';
    return {};
}

} // namespace glyphastore::server
