#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/store/config.hpp"

namespace glyphastore::server {

// Lab-only crash seams for process-kill matrices against a real glyphastored exec.
// Activated only when ALL of:
//   GLYPHASTORE_CRASH_TEST=1
//   GLYPHASTORE_CRASH_KILL_AT=<filesystem_operation_name>[#N]
//   GLYPHASTORE_CRASH_CHECKPOINT_DIR=<writable dir>
// are set. Production deployments leave these unset (no-op).
[[nodiscard]] auto maybe_install_crash_test_hooks(StoreConfig& store) -> Status;

} // namespace glyphastore::server
