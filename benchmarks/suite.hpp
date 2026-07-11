#pragma once

#include "harness.hpp"

#include <vector>

namespace glyphastore::bench {

[[nodiscard]] auto run_benchmark(BenchmarkKind kind, const Config& config) -> std::vector<Result>;
[[nodiscard]] auto quick_configs() -> std::vector<Config>;
[[nodiscard]] auto suite_configs() -> std::vector<Config>;

} // namespace glyphastore::bench
