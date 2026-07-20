#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/store/config.hpp"
#include "glyphastore/store/maintenance_types.hpp"
#include "glyphastore/store/value.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace glyphastore {

namespace detail {
class StoreAccess;
}

struct CompactionResult {
    bool compacted{};
    std::optional<std::size_t> worker_index;
    std::uint64_t source_records_verified{};
    std::uint64_t source_bytes_verified{};
    std::uint64_t records_copied{};
    std::uint64_t bytes_copied{};
    std::uint64_t expired_records_dropped{};
};

class Store final {
  public:
    [[nodiscard]] static auto open(const StoreConfig& config = {}) -> Result<std::unique_ptr<Store>>;
    ~Store();

    Store(const Store&) = delete;
    auto operator=(const Store&) -> Store& = delete;
    Store(Store&&) = delete;
    auto operator=(Store&&) -> Store& = delete;

    [[nodiscard]] auto worker_count() const noexcept -> std::size_t;

    // Thread-safe owning read. Returned bytes and metadata are independent of
    // later Store operations and remain valid after Store destruction.
    [[nodiscard]] auto get(std::string_view key) -> Result<OwnedValue>;
    [[nodiscard]] auto get(std::span<const std::byte> key) -> Result<OwnedValue>;
    [[nodiscard]] auto get_copy(std::string_view key) -> Result<OwnedValue>;
    [[nodiscard]] auto get_copy(std::span<const std::byte> key) -> Result<OwnedValue>;

    [[nodiscard]] auto put(std::string_view key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto put(std::span<const std::byte> key, std::span<const std::byte> value,
                           std::uint64_t expire_at_ns = 0) -> Status;
    [[nodiscard]] auto erase(std::string_view key) -> Status;
    [[nodiscard]] auto erase(std::span<const std::byte> key) -> Status;

    [[nodiscard]] auto flush() -> Status;
    // Runs at most one Worker compaction transaction. Durable mode replaces a
    // whole sealed Worker history; volatile mode vacuums selected sparse sealed
    // Segments. Calls do not queue behind an existing compaction. An empty
    // successful result means no Worker currently offers a physical Segment gain.
    [[nodiscard]] auto compact() -> Result<CompactionResult>;
    // Observability for the optional MaintenanceController (ADR 0023).
    [[nodiscard]] auto maintenance_snapshot() const -> MaintenanceSnapshot;
    // Idempotently prevents new operations, waits for calls already in
    // progress, flushes durable state, stops background executors, and releases
    // Store resources. Destruction performs the same shutdown as a
    // non-observable fallback.
    [[nodiscard]] auto close() -> Status;

    [[nodiscard]] auto verify_index() const -> Status;

  private:
    struct Impl;

    explicit Store(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;

    friend class detail::StoreAccess;
};

} // namespace glyphastore
