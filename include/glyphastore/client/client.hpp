#pragma once

#include "glyphastore/core/error.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace glyphastore::client {

// Opt-in TLS (ADR 0020). Cleartext remains the default. When enable is true the
// client fails closed (no cleartext fallback). Hostname/SNI verification is on
// unless insecure_skip_verify is set (lab escape only).
struct TlsOptions {
    bool enable{false};
    std::string ca_file{};
    std::string cert_file{};
    std::string key_file{};
    // Empty uses ClientConfig::host for SNI / hostname verification.
    std::string server_name{};
    bool insecure_skip_verify{false};
};

struct ClientConfig {
    std::string host{"127.0.0.1"};
    std::uint16_t port{7379};
    std::uint32_t connect_timeout_ms{3'000};
    std::uint32_t request_timeout_ms{5'000};
    std::size_t maximum_frame_bytes{2U * 1024U * 1024U};
    std::size_t maximum_pipeline_requests{256};
    std::size_t maximum_pipeline_bytes{1024U * 1024U};
    TlsOptions tls{};
};

struct PutOptions {
    // Absolute Unix time in nanoseconds. Zero means no expiry.
    std::uint64_t expire_at_ns{};
};

// Optional per-call request budget. When timeout is unset, ClientConfig::request_timeout_ms
// applies. Connect and bootstrap INIT/BIND always use the configured defaults.
struct RequestOptions {
    std::optional<std::chrono::milliseconds> timeout{};
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

enum class PipelineOpcode {
    get,
    put,
    erase,
};

struct PipelineRequest {
    PipelineOpcode opcode{PipelineOpcode::get};
    std::span<const std::byte> key;
    std::span<const std::byte> value;
    PutOptions put_options{};
};

enum class PipelineOutcome {
    succeeded,
    failed,
    indeterminate,
};

struct PipelineResponse {
    PipelineOutcome outcome{PipelineOutcome::failed};
    std::vector<std::byte> value;
    std::optional<Error> error;

    [[nodiscard]] auto succeeded() const noexcept -> bool {
        return outcome == PipelineOutcome::succeeded;
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

    [[nodiscard]] auto get(std::span<const std::byte> key, RequestOptions options = {})
        -> Result<std::vector<std::byte>>;
    [[nodiscard]] auto get(std::string_view key, RequestOptions options = {})
        -> Result<std::vector<std::byte>>;
    [[nodiscard]] auto ping(std::span<const std::byte> payload = {}, RequestOptions options = {})
        -> Result<std::vector<std::byte>>;

    [[nodiscard]] auto put(std::span<const std::byte> key, std::span<const std::byte> value,
                           PutOptions put_options = {}, RequestOptions options = {}) -> MutationResult;
    [[nodiscard]] auto put(std::string_view key, std::string_view value, PutOptions put_options = {},
                           RequestOptions options = {}) -> MutationResult;
    [[nodiscard]] auto erase(std::span<const std::byte> key, RequestOptions options = {})
        -> MutationResult;
    [[nodiscard]] auto erase(std::string_view key, RequestOptions options = {}) -> MutationResult;

    // Executes an ordered pipeline on one Worker-bound connection. Every key
    // must route to the same Worker. Responses always correspond positionally
    // to requests, including partial transport failures.
    [[nodiscard]] auto execute_pipeline(std::span<const PipelineRequest> requests,
                                        RequestOptions options = {})
        -> Result<std::vector<PipelineResponse>>;

    // Groups requests by Worker, runs one pipeline per Worker (concurrently when
    // more than one Worker is involved), and restores caller order. Not atomic:
    // Workers succeed or fail independently after admission. One shared deadline
    // covers the whole batch call.
    [[nodiscard]] auto execute_batch(std::span<const PipelineRequest> requests,
                                     RequestOptions options = {})
        -> Result<std::vector<PipelineResponse>>;

    [[nodiscard]] auto worker_for(std::span<const std::byte> key) const noexcept -> std::uint32_t;
    [[nodiscard]] auto worker_for(std::string_view key) const noexcept -> std::uint32_t;
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
