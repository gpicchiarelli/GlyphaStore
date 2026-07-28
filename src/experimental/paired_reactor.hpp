#pragma once

#include "experimental/paired_shard.hpp"
#include "glyphastore/core/error.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

namespace glyphastore::experimental {

struct PairedReactorPrototypeConfig final {
    std::size_t maximum_connections{64};
    std::size_t event_batch_size{128};
    std::size_t maximum_input_bytes{4U * 1024U * 1024U};
    std::size_t maximum_output_bytes{4U * 1024U * 1024U};
    std::size_t output_frames_per_connection{128};
    // Zero preserves the platform default. A small explicit value is useful
    // for deterministic slow-consumer and generation-pin validation.
    std::size_t accepted_socket_send_buffer_bytes{};
    std::size_t maximum_value_bytes{256U * 1024U};
    std::size_t merge_delta_entries{4'096};
    PrototypeWriterBatchConfig writer_batch{};
};

struct PairedReactorPrototypeStats final {
    std::uint64_t accepted_connections{};
    std::uint64_t closed_connections{};
    std::uint64_t requests{};
    std::uint64_t gets{};
    std::uint64_t mutations_submitted{};
    std::uint64_t mutation_completions{};
    std::uint64_t mutation_backpressure{};
    std::uint64_t responses{};
    std::uint64_t response_bytes{};
    std::uint64_t writev_calls{};
    std::uint64_t partial_writes{};
    std::uint64_t slow_output_pins{};
    std::size_t active_connections{};
};

// Experimental, single-pair cleartext Reactor. It is compiled only into tests
// and the paired benchmark; glyphastored cannot select it.
class PairedReactorPrototype final {
  public:
    [[nodiscard]] static auto create(PairedReactorPrototypeConfig config = {})
        -> Result<std::unique_ptr<PairedReactorPrototype>>;
    ~PairedReactorPrototype();

    PairedReactorPrototype(const PairedReactorPrototype&) = delete;
    auto operator=(const PairedReactorPrototype&) -> PairedReactorPrototype& = delete;

    [[nodiscard]] auto run_once(int timeout_ms) -> Status;
    void stop_accepting() noexcept;
    void close_all_connections() noexcept;

    [[nodiscard]] auto port() const noexcept -> std::uint16_t;
    [[nodiscard]] auto idle() const noexcept -> bool;
    [[nodiscard]] auto stats() const noexcept -> PairedReactorPrototypeStats;
    [[nodiscard]] auto pair_stats() const noexcept -> PrototypePairStats;

  private:
    struct Impl;
    explicit PairedReactorPrototype(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
    mutable std::mutex stats_mutex_;
    PairedReactorPrototypeStats published_stats_;
    PrototypePairStats published_pair_stats_;
};

} // namespace glyphastore::experimental
