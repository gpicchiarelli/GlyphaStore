#pragma once

#include "glyphastore/core/error.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore::client {

struct ClientConfig {
    std::string host{"127.0.0.1"};
    std::uint16_t port{7379};
    std::uint32_t connect_timeout_ms{3'000};
    std::uint32_t request_timeout_ms{5'000};
    std::size_t maximum_frame_bytes{2U * 1024U * 1024U};
};

struct PutOptions {
    // Absolute Unix time in nanoseconds. Zero means no expiry.
    std::uint64_t expire_at_ns{};
};

enum class MutationOutcome {
    committed,
    rejected,
    indeterminate,
};

struct MutationResult {
    MutationOutcome outcome{MutationOutcome::rejected};
    std::optional<Error> error;

    [[nodiscard]] auto committed() const noexcept -> bool {
        return outcome == MutationOutcome::committed;
    }
};

// A synchronous, thread-safe TCP client for wire protocol v2. It keeps one
// connection bound to each server worker, preserving per-key routing while
// allowing independent workers to make progress concurrently.
class Client final {
  public:
    [[nodiscard]] static auto connect(ClientConfig config = {}) -> Result<Client>;
    ~Client();

    Client(Client&&) noexcept;
    auto operator=(Client&&) noexcept -> Client&;
    Client(const Client&) = delete;
    auto operator=(const Client&) -> Client& = delete;

    [[nodiscard]] auto get(std::span<const std::byte> key) -> Result<std::vector<std::byte>>;
    [[nodiscard]] auto get(std::string_view key) -> Result<std::vector<std::byte>>;
    [[nodiscard]] auto ping(std::span<const std::byte> payload = {}) -> Result<std::vector<std::byte>>;

    [[nodiscard]] auto put(std::span<const std::byte> key, std::span<const std::byte> value,
                           PutOptions options = {}) -> MutationResult;
    [[nodiscard]] auto put(std::string_view key, std::string_view value, PutOptions options = {})
        -> MutationResult;
    [[nodiscard]] auto erase(std::span<const std::byte> key) -> MutationResult;
    [[nodiscard]] auto erase(std::string_view key) -> MutationResult;

    [[nodiscard]] auto worker_count() const noexcept -> std::uint32_t;
    [[nodiscard]] auto routing_epoch() const noexcept -> std::uint64_t;
    [[nodiscard]] auto healthy() const noexcept -> bool;
    void close() noexcept;

  private:
    class Impl;
    explicit Client(std::unique_ptr<Impl> implementation) noexcept;

    std::unique_ptr<Impl> implementation_;
};

} // namespace glyphastore::client
