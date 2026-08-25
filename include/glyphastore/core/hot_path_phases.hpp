#pragma once

// Optional hot-path phase attribution for lab measurements.
//
// Default / production builds: every GS_PHASE_* macro is a compile-time no-op.
// Enable with -DGLYPHASTORE_HOT_PATH_PHASES=ON (CMake). Counters are preallocated,
// shard/thread-local friendly atomics — no global mutexes and no hot-path text logging.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace glyphastore::hot_path {

enum class GetPhase : std::uint8_t {
    admit = 0,
    hash,
    route,
    lease_adopt,
    index_lookup,
    value_copy,
    release,
    count,
};

enum class PutPhase : std::uint8_t {
    admit = 0,
    encode_copy,
    index_publish,
    enqueue,
    worker_apply,
    publish,
    ack,
    count,
};

enum class TcpPhase : std::uint8_t {
    accept_read = 0,
    decode,
    dispatch,
    store_op,
    encode,
    write,
    wait_inflight,
    count,
};

[[nodiscard]] constexpr auto get_phase_name(const GetPhase phase) noexcept -> const char* {
    switch (phase) {
    case GetPhase::admit:
        return "admit";
    case GetPhase::hash:
        return "hash";
    case GetPhase::route:
        return "route";
    case GetPhase::lease_adopt:
        return "lease_adopt";
    case GetPhase::index_lookup:
        return "index_lookup";
    case GetPhase::value_copy:
        return "value_copy";
    case GetPhase::release:
        return "release";
    case GetPhase::count:
        break;
    }
    return "unknown";
}

[[nodiscard]] constexpr auto put_phase_name(const PutPhase phase) noexcept -> const char* {
    switch (phase) {
    case PutPhase::admit:
        return "admit";
    case PutPhase::encode_copy:
        return "encode_copy";
    case PutPhase::index_publish:
        return "index_publish";
    case PutPhase::enqueue:
        return "enqueue";
    case PutPhase::worker_apply:
        return "worker_apply";
    case PutPhase::publish:
        return "publish";
    case PutPhase::ack:
        return "ack";
    case PutPhase::count:
        break;
    }
    return "unknown";
}

[[nodiscard]] constexpr auto tcp_phase_name(const TcpPhase phase) noexcept -> const char* {
    switch (phase) {
    case TcpPhase::accept_read:
        return "accept_read";
    case TcpPhase::decode:
        return "decode";
    case TcpPhase::dispatch:
        return "dispatch";
    case TcpPhase::store_op:
        return "store_op";
    case TcpPhase::encode:
        return "encode";
    case TcpPhase::write:
        return "write";
    case TcpPhase::wait_inflight:
        return "wait_inflight";
    case TcpPhase::count:
        break;
    }
    return "unknown";
}

struct alignas(64) PhaseBucket final {
    std::atomic<std::uint64_t> total_ns{};
    std::atomic<std::uint64_t> samples{};
};

struct PhaseSnapshot final {
    std::uint64_t total_ns{};
    std::uint64_t samples{};
};

#if defined(GLYPHASTORE_HOT_PATH_PHASES)

[[nodiscard]] auto enabled() noexcept -> bool;
void set_enabled(bool on) noexcept;
void reset() noexcept;
void observe_get(GetPhase phase, std::uint64_t elapsed_ns) noexcept;
void observe_put(PutPhase phase, std::uint64_t elapsed_ns) noexcept;
void observe_tcp(TcpPhase phase, std::uint64_t elapsed_ns) noexcept;
[[nodiscard]] auto snapshot_get(GetPhase phase) noexcept -> PhaseSnapshot;
[[nodiscard]] auto snapshot_put(PutPhase phase) noexcept -> PhaseSnapshot;
[[nodiscard]] auto snapshot_tcp(TcpPhase phase) noexcept -> PhaseSnapshot;
[[nodiscard]] auto format_report() -> std::string;

class PhaseScope final {
  public:
    explicit PhaseScope(void (*observe)(std::uint8_t, std::uint64_t) noexcept,
                        const std::uint8_t phase) noexcept;
    ~PhaseScope();

    PhaseScope(const PhaseScope&) = delete;
    auto operator=(const PhaseScope&) -> PhaseScope& = delete;

  private:
    void (*observe_)(std::uint8_t, std::uint64_t) noexcept {};
    std::uint8_t phase_{};
    std::uint64_t start_ns_{};
};

[[nodiscard]] auto now_ns() noexcept -> std::uint64_t;

inline void observe_get_u8(const std::uint8_t phase, const std::uint64_t elapsed_ns) noexcept {
    observe_get(static_cast<GetPhase>(phase), elapsed_ns);
}
inline void observe_put_u8(const std::uint8_t phase, const std::uint64_t elapsed_ns) noexcept {
    observe_put(static_cast<PutPhase>(phase), elapsed_ns);
}
inline void observe_tcp_u8(const std::uint8_t phase, const std::uint64_t elapsed_ns) noexcept {
    observe_tcp(static_cast<TcpPhase>(phase), elapsed_ns);
}

#else

inline auto enabled() noexcept -> bool {
    return false;
}
inline void set_enabled(bool) noexcept {}
inline void reset() noexcept {}
inline void observe_get(GetPhase, std::uint64_t) noexcept {}
inline void observe_put(PutPhase, std::uint64_t) noexcept {}
inline void observe_tcp(TcpPhase, std::uint64_t) noexcept {}
inline auto snapshot_get(GetPhase) noexcept -> PhaseSnapshot {
    return {};
}
inline auto snapshot_put(PutPhase) noexcept -> PhaseSnapshot {
    return {};
}
inline auto snapshot_tcp(TcpPhase) noexcept -> PhaseSnapshot {
    return {};
}
inline auto format_report() -> std::string {
    return {};
}

#endif

} // namespace glyphastore::hot_path

#if defined(GLYPHASTORE_HOT_PATH_PHASES)
#define GS_PHASE_CONCAT_INNER(a, b) a##b
#define GS_PHASE_CONCAT(a, b) GS_PHASE_CONCAT_INNER(a, b)
#define GS_PHASE_GET(phase)                                                                                  \
    const ::glyphastore::hot_path::PhaseScope GS_PHASE_CONCAT(gs_phase_get_, __COUNTER__) {                  \
        &::glyphastore::hot_path::observe_get_u8,                                                            \
            static_cast<std::uint8_t>(::glyphastore::hot_path::GetPhase::phase)                              \
    }
#define GS_PHASE_PUT(phase)                                                                                  \
    const ::glyphastore::hot_path::PhaseScope GS_PHASE_CONCAT(gs_phase_put_, __COUNTER__) {                  \
        &::glyphastore::hot_path::observe_put_u8,                                                            \
            static_cast<std::uint8_t>(::glyphastore::hot_path::PutPhase::phase)                              \
    }
#define GS_PHASE_TCP(phase)                                                                                  \
    const ::glyphastore::hot_path::PhaseScope GS_PHASE_CONCAT(gs_phase_tcp_, __COUNTER__) {                  \
        &::glyphastore::hot_path::observe_tcp_u8,                                                            \
            static_cast<std::uint8_t>(::glyphastore::hot_path::TcpPhase::phase)                              \
    }
#else
#define GS_PHASE_GET(phase) static_cast<void>(0)
#define GS_PHASE_PUT(phase) static_cast<void>(0)
#define GS_PHASE_TCP(phase) static_cast<void>(0)
#endif
