#pragma once

// Compatibility header. The paired mutation slot pool moved into
// glyphastore_core with ShardPairRuntime (ADR 0032).
#include "glyphastore/store/paired/mutation_slot_pool.hpp"

namespace glyphastore::server::internal {

using MutationSlotPool = store::paired::MutationSlotPool;

} // namespace glyphastore::server::internal
