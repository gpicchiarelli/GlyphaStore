#pragma once

// Compatibility header. The immutable paired read generation moved into
// glyphastore_core (ADR 0032) so the embedded Store and glyphastored share one
// publication authority. The daemon keeps the historical names.
#include "glyphastore/store/paired/read_generation.hpp"

namespace glyphastore::server {

using store::paired::PairReadGeneration;
using store::paired::PairReadMerge;
using store::paired::ReadMutation;

} // namespace glyphastore::server
