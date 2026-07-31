#pragma once

// Compatibility shim: LatencyHistogram lives in glyphastore_core so paired Store
// stats and daemon STATS share one definition (ADR 0032).
#include "glyphastore/core/latency_histogram.hpp"

namespace glyphastore::server {

using ::glyphastore::LatencyHistogram;
using ::glyphastore::append_latency_histogram;
using ::glyphastore::latency_histogram_bound_label;

} // namespace glyphastore::server
