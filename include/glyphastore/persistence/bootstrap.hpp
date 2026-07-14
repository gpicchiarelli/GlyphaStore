#pragma once

#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/store/config.hpp"

#include <cstddef>
#include <optional>

namespace glyphastore {

// Creates or completes the crash-recoverable initial manifest/Segment set.
// The caller must hold the DataDirectory lock.
[[nodiscard]] auto prepare_durable_store(DataDirectory& directory, DurableOpenMode mode,
                                         std::size_t creation_worker_count,
                                         std::optional<std::size_t> required_worker_count) -> Status;

} // namespace glyphastore
