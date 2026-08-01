#pragma once

#include "server/spsc_ring.hpp"

namespace glyphastore::experimental {

template <typename T, std::size_t Capacity> using SpscRing = server::SpscRing<T, Capacity>;

} // namespace glyphastore::experimental
