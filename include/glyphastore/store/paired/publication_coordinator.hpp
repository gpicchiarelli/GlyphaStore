#pragma once

// Read-generation publication helpers (behavior-neutral extraction).
// Normative: docs/spec/mutation-lifecycle.md

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/store/paired/read_generation.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace glyphastore::store::paired {

// Release-publish the raw generation pointer Readers acquire.
inline void publish_read_generation(std::atomic<const PairReadGeneration*>& published,
                                    const PairReadGeneration* generation) noexcept {
    GS_FAULT_SITE(publish);
    published.store(generation, std::memory_order_release);
}

// Retire previous writer generation and install next after a successful incremental
// or snapshot publish. Caller owns reclaim / merge flags.
struct GenerationInstall final {
    std::shared_ptr<const PairReadGeneration> previous{};
    std::shared_ptr<const PairReadGeneration> next{};
};

inline void install_writer_generation(std::shared_ptr<const PairReadGeneration>& writer_generation,
                                      std::vector<std::shared_ptr<const PairReadGeneration>>& retired,
                                      std::atomic_size_t& retired_count,
                                      std::atomic<std::uint64_t>& writer_epoch,
                                      std::shared_ptr<const PairReadGeneration> next) noexcept {
    retired.push_back(writer_generation);
    retired_count.store(retired.size(), std::memory_order_relaxed);
    writer_generation = std::move(next);
    writer_epoch.store(writer_generation->epoch(), std::memory_order_relaxed);
}

} // namespace glyphastore::store::paired
