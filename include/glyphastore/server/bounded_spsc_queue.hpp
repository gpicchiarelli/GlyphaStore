#pragma once

// Compatibility header. The bounded SPSC ring moved into glyphastore_core with
// the paired shard runtime (ADR 0032); the daemon keeps using the same type for
// its Reader-owned completion lanes.
#include "glyphastore/store/paired/bounded_spsc_queue.hpp"

namespace glyphastore::server {

template <typename T> using BoundedSpscQueue = store::paired::BoundedSpscQueue<T>;

} // namespace glyphastore::server
