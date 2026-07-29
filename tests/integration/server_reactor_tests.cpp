#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto text(const std::span<const std::byte> value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

auto owned_bytes(const std::string_view value) -> std::vector<std::byte> {
    const auto view = bytes(value);
    return {view.begin(), view.end()};
}

auto load_u32(const std::span<const std::byte> input) -> std::uint32_t {
    std::uint32_t value{};
    for (std::size_t byte = 0; byte < 4; ++byte) {
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[byte])) << (byte * 8U);
    }
    return value;
}

auto send_all(const int socket, const std::span<const std::byte> data) -> bool {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto written = ::send(socket, data.data() + sent, data.size() - sent, 0);
        if (written <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(written);
    }
    return true;
}

auto receive_exact(const int socket, const std::span<std::byte> output) -> bool {
    std::size_t received = 0;
    while (received < output.size()) {
        const auto count = ::recv(socket, output.data() + received, output.size() - received, 0);
        if (count <= 0) {
            return false;
        }
        received += static_cast<std::size_t>(count);
    }
    return true;
}

auto receive_response(const int socket) -> std::vector<std::byte> {
    std::array<std::byte, glyphastore::server::kResponseHeaderBytes> header{};
    if (!receive_exact(socket, header)) {
        return {};
    }
    const auto frame_size = static_cast<std::size_t>(load_u32(header));
    if (frame_size < header.size() || frame_size > glyphastore::server::kMaxFrameBytes) {
        return {};
    }
    std::vector<std::byte> frame(frame_size);
    std::ranges::copy(header, frame.begin());
    if (!receive_exact(socket, std::span<std::byte>{frame}.subspan(header.size()))) {
        return {};
    }
    return frame;
}

auto connect_to(const std::uint16_t port) -> int {
    const auto socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket < 0) {
        return -1;
    }
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    static_cast<void>(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    static_cast<void>(::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr));
    if (::connect(socket, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        static_cast<void>(::close(socket));
        return -1;
    }
    return socket;
}

auto initialize_and_bind(const int socket, const std::uint32_t worker, const std::uint32_t worker_count)
    -> bool {
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    if (!init || !send_all(socket, *init)) {
        return false;
    }
    const auto init_frame = receive_response(socket);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    if (!initialized || initialized->frame.status != glyphastore::server::ResponseStatus::ok ||
        initialized->frame.worker_count != worker_count || initialized->frame.routing_epoch == 0) {
        return false;
    }
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = worker,
    });
    if (!bind || !send_all(socket, *bind)) {
        return false;
    }
    const auto bind_frame = receive_response(socket);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    return bound && bound->frame.status == glyphastore::server::ResponseStatus::ok &&
           bound->frame.owner_worker == worker && bound->frame.worker_count == worker_count;
}

struct LifecycleProbeResponse final {
    glyphastore::server::DecodedFrame<glyphastore::server::ResponseView> decoded;
    std::vector<std::byte> frame_bytes;
};

auto probe_lifecycle(const int socket, const glyphastore::server::RequestOpcode opcode,
                     const std::uint64_t request_id) -> std::optional<LifecycleProbeResponse> {
    const auto request = glyphastore::server::encode_request({.opcode = opcode, .request_id = request_id});
    if (!request || !send_all(socket, *request)) {
        return std::nullopt;
    }
    LifecycleProbeResponse result{.frame_bytes = receive_response(socket)};
    const auto decoded = glyphastore::server::decode_response(result.frame_bytes);
    if (!decoded) {
        return std::nullopt;
    }
    result.decoded = *decoded;
    return result;
}

class ServerTemporaryDirectory final {
  public:
    ServerTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-server-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        root_ = created;
    }

    ~ServerTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] auto store_path() const -> std::filesystem::path {
        return root_ / "store";
    }

  private:
    std::filesystem::path root_;
};

class BlockingColdRead final {
  public:
    void arm() {
        const std::lock_guard lock{mutex_};
        armed_ = true;
    }

    [[nodiscard]] auto wait_until_blocked() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [this] { return blocked_; });
    }

    void release() {
        {
            const std::lock_guard lock{mutex_};
            released_ = true;
        }
        condition_.notify_all();
    }

    [[nodiscard]] auto wait_until_finished() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [this] { return finished_; });
    }

    static auto read_some_at(void* opaque, const int descriptor, const std::span<std::byte> output,
                             const std::uint64_t offset) -> std::ptrdiff_t {
        auto& state = *static_cast<BlockingColdRead*>(opaque);
        bool claimed_here{};
        if (offset >= glyphastore::kSegmentHeaderReservedBytes) {
            std::unique_lock lock{state.mutex_};
            if (state.armed_ && !state.claimed_) {
                state.claimed_ = true;
                claimed_here = true;
                state.blocked_ = true;
                state.condition_.notify_all();
                state.condition_.wait(lock, [&state] { return state.released_; });
            }
        }
        const auto result = ::pread(descriptor, output.data(), output.size(), static_cast<off_t>(offset));
        if (claimed_here) {
            {
                const std::lock_guard lock{state.mutex_};
                state.finished_ = true;
            }
            state.condition_.notify_all();
        }
        return result;
    }

  private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool armed_{};
    bool claimed_{};
    bool blocked_{};
    bool released_{};
    bool finished_{};
};

class BlockingFileSync final {
  public:
    void arm() {
        const std::lock_guard lock{mutex_};
        armed_ = true;
    }

    [[nodiscard]] auto wait_until_blocked() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [this] { return blocked_; });
    }

    void release() {
        {
            const std::lock_guard lock{mutex_};
            released_ = true;
        }
        condition_.notify_all();
    }

    static auto sync_file(void* opaque, const int descriptor, const glyphastore::FileSyncMode mode) -> int {
        auto& state = *static_cast<BlockingFileSync*>(opaque);
        {
            std::unique_lock lock{state.mutex_};
            if (state.armed_ && !state.claimed_) {
                state.claimed_ = true;
                state.blocked_ = true;
                state.condition_.notify_all();
                state.condition_.wait(lock, [&state] { return state.released_; });
            }
        }
#if defined(__APPLE__)
        if (mode == glyphastore::FileSyncMode::full) {
            return ::fcntl(descriptor, F_FULLFSYNC);
        }
#endif
        return ::fsync(descriptor);
    }

  private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool armed_{};
    bool claimed_{};
    bool blocked_{};
    bool released_{};
};

class SyncReleaseGuard final {
  public:
    explicit SyncReleaseGuard(BlockingFileSync& sync) noexcept : sync_(sync) {}
    ~SyncReleaseGuard() {
        sync_.release();
    }

    SyncReleaseGuard(const SyncReleaseGuard&) = delete;
    auto operator=(const SyncReleaseGuard&) -> SyncReleaseGuard& = delete;

  private:
    BlockingFileSync& sync_;
};

class BlockingCompactionIntent final {
  public:
    ~BlockingCompactionIntent() {
        release();
    }

    void force_next_record_write_full() {
        const std::lock_guard lock{mutex_};
        force_record_full_ = true;
    }

    [[nodiscard]] auto wait_until_blocked() -> bool {
        std::unique_lock lock{mutex_};
        return condition_.wait_for(lock, std::chrono::seconds{2}, [this] { return blocked_; });
    }

    void release() {
        {
            const std::lock_guard lock{mutex_};
            released_ = true;
        }
        condition_.notify_all();
    }

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& state = *static_cast<BlockingCompactionIntent*>(opaque);
        std::unique_lock lock{state.mutex_};
        if (operation == glyphastore::FilesystemOperation::write_record && state.force_record_full_) {
            state.force_record_full_ = false;
            return glyphastore::fail(glyphastore::ErrorCode::segment_full,
                                     "injected full Segment before publication-lease wait");
        }
        if (operation != glyphastore::FilesystemOperation::write_compaction_intent || state.claimed_) {
            return {};
        }
        state.claimed_ = true;
        state.blocked_ = true;
        state.condition_.notify_all();
        state.condition_.wait(lock, [&state] { return state.released_; });
        return {};
    }

  private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool force_record_full_{};
    bool claimed_{};
    bool blocked_{};
    bool released_{};
};

class GroupBatchObserver final {
  public:
    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& state = *static_cast<GroupBatchObserver*>(opaque);
        const std::lock_guard lock{state.mutex_};
        if (operation == glyphastore::FilesystemOperation::write_record) {
            ++state.writes_since_sync_;
        } else if (operation == glyphastore::FilesystemOperation::sync_record) {
            state.maximum_writes_before_sync_ =
                std::max(state.maximum_writes_before_sync_, state.writes_since_sync_);
            state.writes_since_sync_ = 0;
            ++state.sync_count_;
        }
        return {};
    }

    [[nodiscard]] auto maximum_writes_before_sync() const -> std::size_t {
        const std::lock_guard lock{mutex_};
        return maximum_writes_before_sync_;
    }

    [[nodiscard]] auto sync_count() const -> std::size_t {
        const std::lock_guard lock{mutex_};
        return sync_count_;
    }

  private:
    mutable std::mutex mutex_;
    std::size_t writes_since_sync_{};
    std::size_t maximum_writes_before_sync_{};
    std::size_t sync_count_{};
};

} // namespace

GLYPHA_TEST("durable daemon retries only a proven non-committed first-attempt sequence conflict") {
    using glyphastore::DurableMutationOutcome;
    using glyphastore::DurableMutationResult;
    using glyphastore::Error;
    using glyphastore::ErrorCode;
    const auto should_retry = [](const DurableMutationResult& result, const unsigned attempt) {
        return glyphastore::detail::StoreAccess::should_retry_durable_mutation(result, attempt);
    };

    const DurableMutationResult retryable{
        .outcome = DurableMutationOutcome::not_committed,
        .error = Error{ErrorCode::sequence_conflict, "stale rotation snapshot"},
    };
    GLYPHA_REQUIRE(should_retry(retryable, 0));
    GLYPHA_REQUIRE(!should_retry(retryable, 1));

    const DurableMutationResult committed{
        .outcome = DurableMutationOutcome::committed,
        .error = Error{ErrorCode::sequence_conflict, "post-commit diagnostic"},
    };
    const DurableMutationResult indeterminate{
        .outcome = DurableMutationOutcome::indeterminate,
        .error = Error{ErrorCode::sequence_conflict, "authority uncertain"},
    };
    const DurableMutationResult other_error{
        .outcome = DurableMutationOutcome::not_committed,
        .error = Error{ErrorCode::io_error, "pre-commit I/O failure"},
    };
    const DurableMutationResult missing_error{
        .outcome = DurableMutationOutcome::not_committed,
    };
    GLYPHA_REQUIRE(!should_retry(committed, 0));
    GLYPHA_REQUIRE(!should_retry(indeterminate, 0));
    GLYPHA_REQUIRE(!should_retry(other_error, 0));
    GLYPHA_REQUIRE(!should_retry(missing_error, 0));
}

GLYPHA_TEST("paired Writer completes incremental read merge in bounded quanta") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    const glyphastore::server::PairReadMergeConfig merge_config{
        .delta_entries = 4,
        .maximum_post_entries = 8,
        .quantum_slots = 4'096,
    };
    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor =
        glyphastore::server::PairWriterPool::create(store, 1, 8, std::chrono::milliseconds{0}, merge_config);
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());

    std::array<std::string, 4> keys{"merge-a", "merge-b", "merge-c", "merge-d"};
    for (std::size_t index = 0; index < keys.size(); ++index) {
        GLYPHA_REQUIRE((*executor)->try_submit({
            .connection = {.slot = static_cast<std::uint32_t>(index + 1U), .generation = 1},
            .request_id = 800U + index,
            .worker_index = 0,
            .kind = glyphastore::server::MutationKind::put,
            .key = keys[index],
            .key_hash = glyphastore::hash_key(keys[index]),
            .value = owned_bytes("value"),
            .admission_bytes = 1,
            .completions = &completions,
            .wakeup = &*wakeup,
        }));
    }

    std::size_t completed{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (completed != keys.size() && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            GLYPHA_REQUIRE(!completion->error.has_value());
            ++completed;
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completed == keys.size());

    glyphastore::server::PairWriterStats stats;
    const auto merge_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < merge_deadline) {
        static_cast<void>((*executor)->adopt_read_generation(0));
        stats = (*executor)->stats()[0];
        if (stats.read_merge_completions != 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(stats.read_merge_starts == 1);
    GLYPHA_REQUIRE(stats.read_merge_completions == 1);
    GLYPHA_REQUIRE(stats.read_merge_failures == 0);
    GLYPHA_REQUIRE(stats.read_merge_backpressure == 0);
    GLYPHA_REQUIRE(stats.read_merge_slots_processed > 0);
    GLYPHA_REQUIRE(!stats.read_merge_active);
    GLYPHA_REQUIRE(stats.read_merge_post_entries == 0);

    const auto* generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(generation != nullptr);
    GLYPHA_REQUIRE(generation->base_entries() == keys.size());
    GLYPHA_REQUIRE(generation->delta_entries() == 0);
    for (const auto& key : keys) {
        auto value = generation->get({.key = key, .hash = glyphastore::hash_key(key)}, 0);
        GLYPHA_REQUIRE(value.has_value());
        GLYPHA_REQUIRE(text(value->bytes) == "value");
    }

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer rejects invalid incremental read merge bounds") {
    auto opened = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, std::chrono::milliseconds{0},
                        {.delta_entries = 0, .maximum_post_entries = 1, .quantum_slots = 1})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, std::chrono::milliseconds{0},
                        {.delta_entries = 1, .maximum_post_entries = 1, .quantum_slots = 0})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::PairWriterPool::create(
                        store, 1, 8, std::chrono::milliseconds{0},
                        {.delta_entries = 1, .maximum_post_entries = 0, .quantum_slots = 1})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::PairWriterPool::create(
             store, 1, 8, std::chrono::milliseconds{0},
             {.delta_entries = glyphastore::server::PairReadGeneration::kMaximumIncrementalDeltaEntries,
              .maximum_post_entries = 1,
              .quantum_slots = 1})
             .has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer feeds one bounded maintenance latency window") {
    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::background,
                        .min_eval_interval_ms = 60'000,
                        .max_eval_interval_ms = 60'000,
                        .suspend_on_p99_latency_ms = std::numeric_limits<std::uint32_t>::max()},
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    auto* maintenance = glyphastore::detail::StoreAccess::maintenance_controller(store);
    GLYPHA_REQUIRE(maintenance != nullptr);
    const auto initial_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((store.maintenance_snapshot().evaluation_cycles == 0 ||
            store.maintenance_snapshot().state != glyphastore::MaintenanceState::idle) &&
           std::chrono::steady_clock::now() < initial_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const auto initial_cycles = store.maintenance_snapshot().evaluation_cycles;
    GLYPHA_REQUIRE(initial_cycles > 0);

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{2};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 2, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const std::string key{"latency-feedback"};
    GLYPHA_REQUIRE((*executor)->try_submit({
        .connection = {.slot = 1, .generation = 1},
        .request_id = 601,
        .worker_index = 0,
        .kind = glyphastore::server::MutationKind::put,
        .key = key,
        .key_hash = glyphastore::hash_key(key),
        .value = owned_bytes("value"),
        .admission_bytes = 1,
        .completions = &completions,
        .wakeup = &*wakeup,
    }));
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::optional<glyphastore::server::MutationCompletion> completion;
    while (!completion && std::chrono::steady_clock::now() < completion_deadline) {
        completion = completions.try_pop();
        if (!completion) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completion.has_value());
    GLYPHA_REQUIRE(!completion->error.has_value());

    auto snapshot = store.maintenance_snapshot();
    const bool feedback_already_consumed =
        snapshot.evaluation_cycles > initial_cycles && snapshot.foreground_latency_samples == 1;
    if (!feedback_already_consumed) {
        const auto cycle_before_request = snapshot.evaluation_cycles;
        maintenance->request_evaluate();
        const auto feedback_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (store.maintenance_snapshot().evaluation_cycles == cycle_before_request &&
               std::chrono::steady_clock::now() < feedback_deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        snapshot = store.maintenance_snapshot();
    }
    GLYPHA_REQUIRE(snapshot.evaluation_cycles > initial_cycles);
    GLYPHA_REQUIRE(snapshot.foreground_latency_samples == 1);
    GLYPHA_REQUIRE(snapshot.last_foreground_p99_ns >= 1'000'000ULL);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Writer preserves same-shard FIFO while compaction publication is active") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before},
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("retry-seed", bytes("v1")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("retry-seed", bytes("v2")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("active-seed", bytes("active")).has_value());

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());

    const auto submit = [&](const std::uint64_t request_id, std::string key, std::string_view value) {
        const auto hash = glyphastore::hash_key(key);
        return (*executor)->try_submit({
            .connection = {.slot = static_cast<std::uint32_t>(request_id), .generation = 1},
            .request_id = request_id,
            .worker_index = 0,
            .kind = glyphastore::server::MutationKind::put,
            .key = std::move(key),
            .key_hash = hash,
            .value = owned_bytes(value),
            .admission_bytes = 1,
            .completions = &completions,
            .wakeup = &*wakeup,
        });
    };

    const auto baseline_rotations = store.maintenance_snapshot().rotation.attempts;
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(submit(501, "retry-after-lease", "first"));
    const auto rotation_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (store.maintenance_snapshot().rotation.attempts == baseline_rotations &&
           std::chrono::steady_clock::now() < rotation_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const bool rotation_waiting = store.maintenance_snapshot().rotation.attempts > baseline_rotations;
    GLYPHA_REQUIRE(submit(502, "progress-during-lease", "second"));

    std::vector<glyphastore::server::MutationCompletion> observed;
    const auto fifo_probe_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (std::chrono::steady_clock::now() < fifo_probe_deadline) {
        if (auto completion = completions.try_pop()) {
            observed.push_back(std::move(*completion));
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const bool no_completion_before_release = observed.empty();

    blocker.release();
    compactor.join();
    const auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (observed.size() != 2 && std::chrono::steady_clock::now() < retry_deadline) {
        if (auto completion = completions.try_pop()) {
            observed.push_back(std::move(*completion));
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(rotation_waiting);
    GLYPHA_REQUIRE(no_completion_before_release);
    GLYPHA_REQUIRE(observed.size() == 2);
    GLYPHA_REQUIRE(observed[0].request_id == 501);
    GLYPHA_REQUIRE(observed[1].request_id == 502);
    GLYPHA_REQUIRE(!observed[0].error.has_value());
    GLYPHA_REQUIRE(!observed[1].error.has_value());
    const auto stats = (*executor)->stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE(text(store.get("retry-after-lease")->bytes) == "first");
    GLYPHA_REQUIRE(text(store.get("progress-during-lease")->bytes) == "second");

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Reader refreshes compacted durable pins and retires the old generation") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before},
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;

    GLYPHA_REQUIRE(store.put("refresh-key", bytes("old")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("refresh-key", bytes("current")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put("active-key", bytes("active")).has_value());

    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto* initial_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(initial_generation != nullptr);
    const std::string key{"refresh-key"};
    const glyphastore::HashedKey hashed{key, glyphastore::hash_key(key)};
    glyphastore::RecordRef initial_reference;
    {
        auto initial_record = initial_generation->prepare_durable(hashed);
        GLYPHA_REQUIRE(initial_record.has_value());
        initial_reference = initial_record->reference();
    }
    const auto initial_epoch = initial_generation->epoch();
    const auto initial_revision = glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();
    compactor.join();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE((*compacted)->compacted);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   initial_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((*executor)->stats()[0].read_refresh_successes == 0 &&
           std::chrono::steady_clock::now() < refresh_deadline) {
        (*executor)->request_read_refresh(0);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    auto stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.read_refresh_attempts >= 1);
    GLYPHA_REQUIRE(stats.read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats.read_refresh_failures == 0);
    GLYPHA_REQUIRE(stats.read_catalog_revision > initial_revision);

    const auto* refreshed_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(refreshed_generation != nullptr);
    GLYPHA_REQUIRE(refreshed_generation->epoch() > initial_epoch);
    auto refreshed_record = refreshed_generation->prepare_durable(hashed);
    GLYPHA_REQUIRE(refreshed_record.has_value());
    GLYPHA_REQUIRE(refreshed_record->reference().sequence == initial_reference.sequence);
    GLYPHA_REQUIRE(refreshed_record->reference().segment_id != initial_reference.segment_id);

    const auto reclaim_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    do {
        stats = (*executor)->stats()[0];
        if (stats.retired_generation_count == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < reclaim_deadline);
    GLYPHA_REQUIRE(stats.generations_retired >= 1);
    GLYPHA_REQUIRE(stats.retired_generation_count == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("paired Reader refreshes durable pins after a Writer-owned rotation") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before},
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("rotation-base", bytes("base")).has_value());

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    GLYPHA_REQUIRE(wakeup.has_value());
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 4, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto* initial_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(initial_generation != nullptr);
    const auto initial_epoch = initial_generation->epoch();
    const auto initial_revision = glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);

    blocker.force_next_record_write_full();
    const std::string key{"rotation-published"};
    GLYPHA_REQUIRE((*executor)->try_submit({
        .connection = {.slot = 1, .generation = 1},
        .request_id = 701,
        .worker_index = 0,
        .kind = glyphastore::server::MutationKind::put,
        .key = key,
        .key_hash = glyphastore::hash_key(key),
        .value = owned_bytes("rotated"),
        .admission_bytes = 1,
        .completions = &completions,
        .wakeup = &*wakeup,
    }));
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::optional<glyphastore::server::MutationCompletion> completion;
    while (!completion && std::chrono::steady_clock::now() < completion_deadline) {
        completion = completions.try_pop();
        if (!completion) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(completion.has_value());
    GLYPHA_REQUIRE(!completion->error.has_value());
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   initial_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((*executor)->stats()[0].read_refresh_successes == 0 &&
           std::chrono::steady_clock::now() < refresh_deadline) {
        (*executor)->request_read_refresh(0);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    const auto* refreshed_generation = (*executor)->adopt_read_generation(0);
    GLYPHA_REQUIRE(refreshed_generation != nullptr);
    GLYPHA_REQUIRE(refreshed_generation->epoch() >= initial_epoch + 2U);
    auto published = refreshed_generation->prepare_durable({.key = key, .hash = glyphastore::hash_key(key)});
    GLYPHA_REQUIRE(published.has_value());
    auto sealed = refreshed_generation->prepare_durable(
        {.key = "rotation-base", .hash = glyphastore::hash_key("rotation-base")});
    GLYPHA_REQUIRE(sealed.has_value());
    GLYPHA_REQUIRE(refreshed_generation->delta_entries() == 0);
    GLYPHA_REQUIRE(refreshed_generation->base_entries() == 2);
    const auto stats = (*executor)->stats()[0];
    GLYPHA_REQUIRE(stats.read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats.read_refresh_failures == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("durable read catalog refresh is isolated to the compacted shard pair") {
    ServerTemporaryDirectory temporary;
    BlockingCompactionIntent blocker;
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 2},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .filesystem_hooks = {.context = &blocker, .before = &BlockingCompactionIntent::before},
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    const auto key_for = [](const std::string_view prefix, const std::size_t worker) {
        for (std::size_t suffix = 0; suffix < 10'000; ++suffix) {
            auto key = std::string{prefix} + std::to_string(suffix);
            if (glyphastore::route_worker(key, 2) == worker) {
                return key;
            }
        }
        return std::string{};
    };
    const auto compacted_key = key_for("isolated-compact-", 0);
    const auto active_key = key_for("isolated-active-", 0);
    const auto other_key = key_for("isolated-other-", 1);
    GLYPHA_REQUIRE(!compacted_key.empty());
    GLYPHA_REQUIRE(!active_key.empty());
    GLYPHA_REQUIRE(!other_key.empty());

    GLYPHA_REQUIRE(store.put(compacted_key, bytes("v1")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put(compacted_key, bytes("v2")).has_value());
    blocker.force_next_record_write_full();
    GLYPHA_REQUIRE(store.put(active_key, bytes("active")).has_value());
    GLYPHA_REQUIRE(store.put(other_key, bytes("other")).has_value());

    auto executor = glyphastore::server::PairWriterPool::create(store, 2, 8, std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(executor.has_value());
    GLYPHA_REQUIRE((*executor)->start().has_value());
    const auto worker_zero_revision =
        glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0);
    const auto worker_one_revision =
        glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 1);

    std::optional<glyphastore::Result<glyphastore::CompactionResult>> compacted;
    std::thread compactor{[&] { compacted = store.compact(); }};
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();
    compactor.join();
    GLYPHA_REQUIRE(compacted.has_value());
    GLYPHA_REQUIRE(compacted->has_value());
    GLYPHA_REQUIRE((*compacted)->compacted);
    GLYPHA_REQUIRE((*compacted)->worker_index == 0);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 0) >
                   worker_zero_revision);
    GLYPHA_REQUIRE(glyphastore::detail::StoreAccess::durable_read_catalog_revision(store, 1) ==
                   worker_one_revision);

    const auto refresh_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    std::vector<glyphastore::server::PairWriterStats> stats;
    do {
        (*executor)->request_read_refresh(0);
        (*executor)->request_read_refresh(1);
        stats = (*executor)->stats();
        if (stats[0].read_refresh_successes != 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < refresh_deadline);
    GLYPHA_REQUIRE(stats.size() == 2);
    GLYPHA_REQUIRE(stats[0].read_refresh_successes == 1);
    GLYPHA_REQUIRE(stats[1].read_refresh_attempts == 0);
    GLYPHA_REQUIRE(stats[1].read_refresh_successes == 0);

    GLYPHA_REQUIRE((*executor)->stop_and_drain().has_value());
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("server rejects unsupported worker counts and undersized protocol buffers") {
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .worker_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_input_bytes = glyphastore::server::kRequestHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_output_bytes = glyphastore::server::kResponseHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .disk_read_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_bytes = 0}).has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .disk_read_thread_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .worker_count = 2, .disk_read_thread_count = 1})
             .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create({.port = 0, .worker_count = 2},
                                                        {.worker_config = {.explicit_count = 1}})
                        .has_value());
}

GLYPHA_TEST("server StoreConfig persists acknowledged wire writes across restart") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 4},
            {.storage_mode = glyphastore::StorageMode::durable_sync,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(opened.has_value());
        auto& server = **opened;
        GLYPHA_REQUIRE(server.start().has_value());

        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 3,
            .key = bytes("durable-wire-key"),
            .value = bytes("durable-wire-value"),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto put_frame = receive_response(socket);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(socket));
        server.request_stop();
        GLYPHA_REQUIRE(server.join().has_value());
    }

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    auto& server = **reopened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 4,
        .key = bytes("durable-wire-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    const auto get_frame = receive_response(socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "durable-wire-value");
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("blocked durable mutation leaves its Reactor responsive with bounded FIFO admission") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .durable_mutation_queue_capacity = 2},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    const auto responsive_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(responsive_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(responsive_socket, 0, 1));

    blocker.arm();
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 70,
        .key = bytes("async-first"),
        .value = bytes("first"),
    });
    const auto ordered_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 74,
        .key = bytes("async-first"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(ordered_get.has_value());
    std::vector<std::byte> first_pipeline;
    first_pipeline.insert(first_pipeline.end(), first->begin(), first->end());
    first_pipeline.insert(first_pipeline.end(), ordered_get->begin(), ordered_get->end());
    GLYPHA_REQUIRE(send_all(first_socket, first_pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    // A second mutation must be admitted without waiting for the lane's slow
    // I/O, proving that its queue mutex is not an equivalent storage lock.
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 71,
        .key = bytes("async-second"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(send_all(second_socket, *second));

    // The per-Worker admission budget is now exhausted. Rejection and the
    // following non-storage request are both handled while fsync is suspended.
    const auto rejected = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 72,
        .key = bytes("async-rejected"),
        .value = bytes("rejected"),
    });
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 73,
        .value = bytes("reactor-live"),
    });
    GLYPHA_REQUIRE(rejected.has_value());
    GLYPHA_REQUIRE(ping.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), rejected->begin(), rejected->end());
    pipeline.insert(pipeline.end(), ping->begin(), ping->end());
    GLYPHA_REQUIRE(send_all(responsive_socket, pipeline));
    const auto rejected_frame = receive_response(responsive_socket);
    const auto ping_frame = receive_response(responsive_socket);
    const auto rejected_response = glyphastore::server::decode_response(rejected_frame);
    const auto ping_response = glyphastore::server::decode_response(ping_frame);
    GLYPHA_REQUIRE(rejected_response.has_value());
    GLYPHA_REQUIRE(rejected_response->frame.request_id == 72);
    GLYPHA_REQUIRE(rejected_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(ping_response->frame.request_id == 73);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "reactor-live");

    blocker.release();
    const auto first_frame = receive_response(first_socket);
    const auto ordered_get_frame = receive_response(first_socket);
    const auto second_frame = receive_response(second_socket);
    const auto first_response = glyphastore::server::decode_response(first_frame);
    const auto ordered_get_response = glyphastore::server::decode_response(ordered_get_frame);
    const auto second_response = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first_response.has_value());
    GLYPHA_REQUIRE(ordered_get_response.has_value());
    GLYPHA_REQUIRE(second_response.has_value());
    GLYPHA_REQUIRE(first_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(ordered_get_response->frame.request_id == 74);
    GLYPHA_REQUIRE(ordered_get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ordered_get_response->frame.value) == "first");
    GLYPHA_REQUIRE(second_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto mutation_stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(mutation_stats.size() == 1);
    GLYPHA_REQUIRE(mutation_stats[0].queue_depth == 0);
    GLYPHA_REQUIRE(mutation_stats[0].queued_bytes == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queue_depth >= 1);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queued_bytes > 0);
    GLYPHA_REQUIRE(mutation_stats[0].admitted == 2);
    GLYPHA_REQUIRE(mutation_stats[0].rejected == 1);
    GLYPHA_REQUIRE(mutation_stats[0].expired_before_store == 0);
    GLYPHA_REQUIRE(mutation_stats[0].completed == 2);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retries == 0);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retry_commits == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_service_ns > 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    static_cast<void>(::close(responsive_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable mutation queue deadline rejects only before Store execution") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 2,
         .durable_mutation_queue_wait_ms = 10},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());
    const auto blocked_socket = connect_to(server.port());
    const auto expiring_socket = connect_to(server.port());
    GLYPHA_REQUIRE(blocked_socket >= 0);
    GLYPHA_REQUIRE(expiring_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(blocked_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(expiring_socket, 0, 1));

    blocker.arm();
    const auto blocked = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 75,
        .key = bytes("deadline-blocker"),
        .value = bytes("committed"),
    });
    const auto expiring = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 76,
        .key = bytes("deadline-expired"),
        .value = bytes("must-not-commit"),
    });
    GLYPHA_REQUIRE(blocked.has_value());
    GLYPHA_REQUIRE(expiring.has_value());
    GLYPHA_REQUIRE(send_all(blocked_socket, *blocked));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(expiring_socket, *expiring));
    const auto expiry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{30};
    while (std::chrono::steady_clock::now() < expiry_deadline) {
        std::this_thread::yield();
    }
    blocker.release();

    const auto blocked_frame = receive_response(blocked_socket);
    const auto expired_frame = receive_response(expiring_socket);
    const auto blocked_response = glyphastore::server::decode_response(blocked_frame);
    const auto expired_response = glyphastore::server::decode_response(expired_frame);
    GLYPHA_REQUIRE(blocked_response.has_value());
    GLYPHA_REQUIRE(expired_response.has_value());
    GLYPHA_REQUIRE(blocked_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(expired_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    const auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 2);
    GLYPHA_REQUIRE(stats[0].expired_before_store == 1);
    GLYPHA_REQUIRE(stats[0].completed == 2);
    GLYPHA_REQUIRE(stats[0].maximum_queue_wait_ns >= 10'000'000U);

    static_cast<void>(::close(blocked_socket));
    static_cast<void>(::close(expiring_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->get("deadline-blocker").has_value());
    const auto absent = (*recovered)->get("deadline-expired");
    GLYPHA_REQUIRE(!absent.has_value());
    GLYPHA_REQUIRE(absent.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server shutdown drains an admitted durable mutation before Store close") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .durable_mutation_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 80,
        .key = bytes("drained-mutation"),
        .value = bytes("survives"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    std::atomic_bool join_finished{};
    bool join_succeeded{};
    server.request_stop();
    std::thread joiner{[&] {
        join_succeeded = server.join().has_value();
        join_finished.store(true, std::memory_order_release);
    }};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (!join_finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    GLYPHA_REQUIRE(!join_finished.load(std::memory_order_acquire));
    blocker.release();
    joiner.join();
    GLYPHA_REQUIRE(join_succeeded);
    static_cast<void>(::close(socket));

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    const auto value = (*recovered)->get("drained-mutation");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(text(value->bytes) == "survives");
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server shutdown drain deadline abandons queued durable mutations") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .durable_mutation_queue_capacity = 4, .shutdown_drain_ms = 50},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    blocker.arm();
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 200,
        .key = bytes("drain-committed"),
        .value = bytes("kept"),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 201,
        .key = bytes("drain-abandoned"),
        .value = bytes("dropped"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(second_socket, *second));
    const auto queued_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1};
    while (std::chrono::steady_clock::now() < queued_deadline) {
        const auto stats = server.pair_writer_stats();
        if (!stats.empty() && stats[0].queue_depth >= 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    {
        const auto stats = server.pair_writer_stats();
        GLYPHA_REQUIRE(!stats.empty());
        GLYPHA_REQUIRE(stats[0].queue_depth >= 1);
    }

    server.request_stop();
    std::optional<glyphastore::Status> joined;
    std::thread joiner{[&] { joined = server.join(); }};
    // Past shutdown_drain_ms the executor arms expire_remaining_; in-flight Store work is
    // still blocked until we release the sync hook.
    std::this_thread::sleep_for(std::chrono::milliseconds{120});
    blocker.release();
    joiner.join();
    GLYPHA_REQUIRE(joined.has_value());
    GLYPHA_REQUIRE(!joined->has_value());
    GLYPHA_REQUIRE(joined->error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(joined->error().message.find("shutdown drain deadline") != std::string::npos);
    {
        const auto stats = server.pair_writer_stats();
        GLYPHA_REQUIRE(!stats.empty());
        GLYPHA_REQUIRE(stats[0].expired_before_store >= 1);
    }
    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->get("drain-committed").has_value());
    const auto abandoned = (*recovered)->get("drain-abandoned");
    GLYPHA_REQUIRE(!abandoned.has_value());
    GLYPHA_REQUIRE(abandoned.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server HEALTH and READY succeed while operational") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 2});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.ready());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 401);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(health->decoded.frame.value) == "GlyphaStore/live");
    const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 402);
    GLYPHA_REQUIRE(ready.has_value());
    GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ready->decoded.frame.value) == "GlyphaStore/ready");
    const auto stats = probe_lifecycle(socket, glyphastore::server::RequestOpcode::stats, 403);
    GLYPHA_REQUIRE(stats.has_value());
    GLYPHA_REQUIRE(stats->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto stats_text = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(stats_text.starts_with("GlyphaStore/stats\n"));
    GLYPHA_REQUIRE(stats_text.find("live=1\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("ready=1\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("version=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("connections_active=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_state=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("useful_compactions=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_skips=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_consecutive_no_gain=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_skip_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_activation_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_no_gain_source_records_verified=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_total_no_gain_source_bytes_verified=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_attempts=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_publication_wait_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_seal_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_create_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_manifest_publication_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_final_record_commit_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_maximum_total_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_candidate_dead_byte_ratio_bp=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_foreground_latency_samples=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_foreground_p99_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_suspends=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_guard_active=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_deferral_age_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_debt_overrides=") != std::string_view::npos);
    static_cast<void>(::close(socket));

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server READY fails during shutdown while live stays true") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 2});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.ready());

    {
        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 411);
        GLYPHA_REQUIRE(health.has_value());
        GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(health->decoded.frame.value) == "GlyphaStore/live");
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 412);
        GLYPHA_REQUIRE(ready.has_value());
        GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(ready->decoded.frame.value) == "GlyphaStore/ready");
        static_cast<void>(::close(socket));
    }

    server.request_stop();
    // Accept stops and idle peers are closed; readiness is fail-closed on the API immediately.
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(!server.ready());
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server READY fails under maintenance emergency") {
    ServerTemporaryDirectory temporary;
    glyphastore::DurableResourceLimits limits{};
    limits.max_segment_count = 1;
    limits.max_store_bytes = 4ULL * glyphastore::kSegmentSizeBytes;
    limits.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    {
        auto seeded = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = temporary.store_path(),
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
            .durable_limits = limits,
            .maintenance = {.mode = glyphastore::MaintenanceMode::cooperative},
        });
        GLYPHA_REQUIRE(seeded.has_value());
        GLYPHA_REQUIRE((*seeded)->put("seed", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seeded)->close().has_value());
    }
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
                                             .durable_limits = limits,
                                             .maintenance = {
                                                 .mode = glyphastore::MaintenanceMode::background,
                                                 .min_eval_interval_ms = 60'000,
                                                 .max_eval_interval_ms = 60'000,
                                             }});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto emergency_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < emergency_deadline) {
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 422);
        GLYPHA_REQUIRE(ready.has_value());
        if (ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error) {
            GLYPHA_REQUIRE(!server.ready());
            const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 421);
            GLYPHA_REQUIRE(health.has_value());
            GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
            static_cast<void>(::close(socket));
            server.request_stop();
            GLYPHA_REQUIRE(server.join().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("server rejects durable PUT and ERASE under maintenance emergency on wire") {
    ServerTemporaryDirectory temporary;
    glyphastore::DurableResourceLimits limits{};
    limits.max_segment_count = 1;
    limits.max_store_bytes = 4ULL * glyphastore::kSegmentSizeBytes;
    limits.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    {
        auto seeded = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = temporary.store_path(),
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
            .durable_limits = limits,
            .maintenance = {.mode = glyphastore::MaintenanceMode::cooperative},
        });
        GLYPHA_REQUIRE(seeded.has_value());
        GLYPHA_REQUIRE((*seeded)->put("seed", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seeded)->close().has_value());
    }
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
                                             .durable_limits = limits,
                                             .maintenance = {
                                                 .mode = glyphastore::MaintenanceMode::background,
                                                 .min_eval_interval_ms = 60'000,
                                                 .max_eval_interval_ms = 60'000,
                                             }});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto emergency_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < emergency_deadline) {
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 422);
        GLYPHA_REQUIRE(ready.has_value());
        if (ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(!server.ready());

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 423,
        .key = bytes("blocked-put"),
        .value = bytes("blocked"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 424,
        .key = bytes("seed"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable wire ERASE persists through reopen") {
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto port = server.port();

    const auto socket = connect_to(port);
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 10,
        .key = bytes("erase-me"),
        .value = bytes("gone"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 11,
        .key = bytes("erase-me"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2},
        {.worker_config = {.explicit_count = 1},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->start().has_value());
    const auto probe_socket = connect_to((*reopened)->port());
    GLYPHA_REQUIRE(probe_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe_socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 12,
        .key = bytes("erase-me"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(probe_socket, *get));
    const auto get_frame = receive_response(probe_socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::not_found);
    static_cast<void>(::close(probe_socket));
    (*reopened)->request_stop();
    GLYPHA_REQUIRE((*reopened)->join().has_value());
}

GLYPHA_TEST("server shutdown stops accepting and closes idle connections") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 4});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto port = server.port();

    const auto idle_socket = connect_to(port);
    GLYPHA_REQUIRE(idle_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(idle_socket, 0, 1));

    server.request_stop();
    const auto refuse_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool refused = false;
    while (std::chrono::steady_clock::now() < refuse_deadline) {
        const auto probe = connect_to(port);
        if (probe < 0) {
            refused = true;
            break;
        }
        static_cast<void>(::close(probe));
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(refused);

    const auto closed_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool peer_closed = false;
    while (std::chrono::steady_clock::now() < closed_deadline) {
        char byte{};
        const auto received = ::recv(idle_socket, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(peer_closed);
    GLYPHA_REQUIRE(server.join().has_value());
    static_cast<void>(::close(idle_socket));
}

GLYPHA_TEST("server shutdown drains in-flight durable response before closing connection") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .shutdown_drain_ms = 5'000},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 310,
        .key = bytes("connection-drain-key"),
        .value = bytes("flushed"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    server.request_stop();
    std::optional<glyphastore::Status> joined;
    std::thread joiner{[&] { joined = server.join(); }};
    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    blocker.release();

    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 310);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);

    char byte{};
    const auto closed_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool peer_closed = false;
    while (std::chrono::steady_clock::now() < closed_deadline) {
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(peer_closed);
    joiner.join();
    GLYPHA_REQUIRE(joined.has_value());
    GLYPHA_REQUIRE(joined->has_value());
    static_cast<void>(::close(socket));
}

GLYPHA_TEST("paired Writer closes strict durable groups without concurrent shard mutators") {
    ServerTemporaryDirectory temporary;
    GroupBatchObserver observer;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .durable_mutation_queue_capacity = 4},
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'000, .min_records = 2},
         .filesystem_hooks = {.context = &observer,
                              .before = &GroupBatchObserver::before,
                              .file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 90,
        .key = bytes("group-first"),
        .value = bytes("first"),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 91,
        .key = bytes("group-second"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    blocker.arm();
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(send_all(second_socket, *second));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    const auto in_flight_batch_stats = server.durable_batch_stats();
    GLYPHA_REQUIRE(in_flight_batch_stats.size() == 1);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].pending_records == 2);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].flush_attempts == 1);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].committed_batches == 0);
    blocker.release();

    const auto first_frame = receive_response(first_socket);
    const auto second_frame = receive_response(second_socket);
    const auto first_response = glyphastore::server::decode_response(first_frame);
    const auto second_response = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first_response.has_value());
    GLYPHA_REQUIRE(second_response.has_value());
    GLYPHA_REQUIRE(first_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(second_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(observer.maximum_writes_before_sync() == 2);
    GLYPHA_REQUIRE(observer.sync_count() == 1);
    const auto batch_stats = server.durable_batch_stats();
    GLYPHA_REQUIRE(batch_stats.size() == 1);
    GLYPHA_REQUIRE(batch_stats[0].enabled);
    GLYPHA_REQUIRE(batch_stats[0].pending_records == 0);
    GLYPHA_REQUIRE(batch_stats[0].pending_bytes == 0);
    GLYPHA_REQUIRE(batch_stats[0].current_record_target == 2);
    GLYPHA_REQUIRE(batch_stats[0].flush_attempts == 1);
    GLYPHA_REQUIRE(batch_stats[0].committed_batches == 1);
    GLYPHA_REQUIRE(batch_stats[0].failed_batches == 0);
    GLYPHA_REQUIRE(batch_stats[0].committed_records == 2);
    GLYPHA_REQUIRE(batch_stats[0].committed_bytes > 0);
    GLYPHA_REQUIRE(batch_stats[0].maximum_batch_records == 2);
    GLYPHA_REQUIRE(batch_stats[0].maximum_batch_bytes == batch_stats[0].committed_bytes);
    GLYPHA_REQUIRE(batch_stats[0].total_commit_duration_ns > 0);
    GLYPHA_REQUIRE(batch_stats[0].maximum_commit_duration_ns == batch_stats[0].total_commit_duration_ns);
    GLYPHA_REQUIRE(batch_stats[0].deadline_closes == 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("blocked durable cold GET leaves its Reactor responsive and applies bounded admission") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("cold-a", bytes("value-a")).has_value());
        GLYPHA_REQUIRE((*seed)->put("cold-b", bytes("value-b")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .disk_read_thread_count = 1, .disk_read_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto blocked_socket = connect_to(server.port());
    GLYPHA_REQUIRE(blocked_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(blocked_socket, 0, 1));
    blocker.arm();
    const auto cold_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 40,
        .key = bytes("cold-a"),
    });
    const auto ordered_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 43,
        .key = bytes("ordered-after-cold"),
        .value = bytes("ordered-value"),
    });
    GLYPHA_REQUIRE(cold_a.has_value());
    GLYPHA_REQUIRE(ordered_put.has_value());
    std::vector<std::byte> blocked_pipeline;
    blocked_pipeline.insert(blocked_pipeline.end(), cold_a->begin(), cold_a->end());
    blocked_pipeline.insert(blocked_pipeline.end(), ordered_put->begin(), ordered_put->end());
    GLYPHA_REQUIRE(send_all(blocked_socket, blocked_pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    // The only disk-read admission is occupied, but the owner-affine Reactor
    // must still accept, initialize, mutate, and respond on another socket.
    const auto responsive_socket = connect_to(server.port());
    GLYPHA_REQUIRE(responsive_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(responsive_socket, 0, 1));
    const auto ordered_not_yet_visible = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 44,
        .key = bytes("ordered-after-cold"),
    });
    GLYPHA_REQUIRE(ordered_not_yet_visible.has_value());
    GLYPHA_REQUIRE(send_all(responsive_socket, *ordered_not_yet_visible));
    const auto not_yet_visible_frame = receive_response(responsive_socket);
    const auto not_yet_visible = glyphastore::server::decode_response(not_yet_visible_frame);
    GLYPHA_REQUIRE(not_yet_visible.has_value());
    GLYPHA_REQUIRE(not_yet_visible->frame.status == glyphastore::server::ResponseStatus::not_found);
    const auto saturated_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 41,
        .key = bytes("cold-b"),
    });
    const auto same_worker_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 42,
        .key = bytes("reactor-remains-live"),
        .value = bytes("stored"),
    });
    GLYPHA_REQUIRE(saturated_get.has_value());
    GLYPHA_REQUIRE(same_worker_put.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), saturated_get->begin(), saturated_get->end());
    pipeline.insert(pipeline.end(), same_worker_put->begin(), same_worker_put->end());
    GLYPHA_REQUIRE(send_all(responsive_socket, pipeline));

    const auto overload_frame = receive_response(responsive_socket);
    const auto put_frame = receive_response(responsive_socket);
    const auto overload = glyphastore::server::decode_response(overload_frame);
    const auto put = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(overload.has_value());
    GLYPHA_REQUIRE(overload->frame.request_id == 41);
    GLYPHA_REQUIRE(overload->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(put->frame.request_id == 42);
    GLYPHA_REQUIRE(put->frame.status == glyphastore::server::ResponseStatus::ok);

    blocker.release();
    const auto cold_frame = receive_response(blocked_socket);
    const auto cold = glyphastore::server::decode_response(cold_frame);
    GLYPHA_REQUIRE(cold.has_value());
    GLYPHA_REQUIRE(cold->frame.request_id == 40);
    GLYPHA_REQUIRE(cold->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(cold->frame.value) == "value-a");
    const auto ordered_put_frame = receive_response(blocked_socket);
    const auto ordered_put_response = glyphastore::server::decode_response(ordered_put_frame);
    GLYPHA_REQUIRE(ordered_put_response.has_value());
    GLYPHA_REQUIRE(ordered_put_response->frame.request_id == 43);
    GLYPHA_REQUIRE(ordered_put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    // Both ACKs must follow immutable durable-generation publication. These
    // GETs exercise the captured active-file pins directly; neither key was in
    // the recovery bootstrap generation.
    const auto visible_after_ack = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 45,
        .key = bytes("reactor-remains-live"),
    });
    const auto ordered_after_ack = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 46,
        .key = bytes("ordered-after-cold"),
    });
    GLYPHA_REQUIRE(visible_after_ack.has_value());
    GLYPHA_REQUIRE(ordered_after_ack.has_value());
    GLYPHA_REQUIRE(send_all(responsive_socket, *visible_after_ack));
    const auto visible_frame = receive_response(responsive_socket);
    const auto visible = glyphastore::server::decode_response(visible_frame);
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(visible->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(visible->frame.value) == "stored");
    GLYPHA_REQUIRE(send_all(blocked_socket, *ordered_after_ack));
    const auto ordered_frame = receive_response(blocked_socket);
    const auto ordered = glyphastore::server::decode_response(ordered_frame);
    GLYPHA_REQUIRE(ordered.has_value());
    GLYPHA_REQUIRE(ordered->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ordered->frame.value) == "ordered-value");

    static_cast<void>(::close(blocked_socket));
    static_cast<void>(::close(responsive_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("late cold-read completion cannot target a reused connection slot") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("stale-read", bytes("old-value")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .disk_read_thread_count = 1, .disk_read_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto old_socket = connect_to(server.port());
    GLYPHA_REQUIRE(old_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(old_socket, 0, 1));
    blocker.arm();
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 50,
        .key = bytes("stale-read"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(old_socket, *get));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    linger reset_on_close{.l_onoff = 1, .l_linger = 0};
    GLYPHA_REQUIRE(::setsockopt(old_socket, SOL_SOCKET, SO_LINGER, &reset_on_close, sizeof(reset_on_close)) ==
                   0);
    static_cast<void>(::close(old_socket));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (server.active_connections_per_executor()[0] != 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const auto old_connection_closed = server.active_connections_per_executor()[0] == 0;
    if (!old_connection_closed) {
        blocker.release();
    }
    GLYPHA_REQUIRE(old_connection_closed);

    // maximum_connections=1 forces the next connection to reuse the same slot
    // with a new generation while the old pinned read is still in flight.
    const auto reused_socket = connect_to(server.port());
    if (reused_socket < 0) {
        blocker.release();
    }
    GLYPHA_REQUIRE(reused_socket >= 0);
    const auto reused_initialized = initialize_and_bind(reused_socket, 0, 1);
    if (!reused_initialized) {
        blocker.release();
    }
    GLYPHA_REQUIRE(reused_initialized);
    blocker.release();
    GLYPHA_REQUIRE(blocker.wait_until_finished());

    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 51,
        .value = bytes("new-generation"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(reused_socket, *ping));
    const auto frame = receive_response(reused_socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 51);
    GLYPHA_REQUIRE(text(response->frame.value) == "new-generation");

    static_cast<void>(::close(reused_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server shutdown drains an in-flight pinned cold read before Store close") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("shutdown-read", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 60,
        .key = bytes("shutdown-read"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    std::atomic_bool join_finished{};
    bool join_succeeded{};
    server.request_stop();
    std::thread joiner{[&] {
        join_succeeded = server.join().has_value();
        join_finished.store(true, std::memory_order_release);
    }};
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (!join_finished.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < drain_deadline) {
        std::this_thread::yield();
    }
    const auto waited_for_read = !join_finished.load(std::memory_order_acquire);
    blocker.release();
    joiner.join();
    GLYPHA_REQUIRE(waited_for_read);
    GLYPHA_REQUIRE(join_succeeded);
    static_cast<void>(::close(socket));
}

GLYPHA_TEST("TCP reactor handles partial and pipelined protocol frames") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 16});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 10,
        .value = bytes("first"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data(), 3}));
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data() + 3, ping->size() - 3}));

    const auto first_frame = receive_response(socket);
    GLYPHA_REQUIRE(!first_frame.empty());
    const auto first = glyphastore::server::decode_response(first_frame);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(first->frame.request_id == 10);
    GLYPHA_REQUIRE(text(first->frame.value) == "first");

    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 11,
    });
    const auto second_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 12,
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(second_ping.has_value());
    std::vector<std::byte> pipelined;
    pipelined.insert(pipelined.end(), init->begin(), init->end());
    pipelined.insert(pipelined.end(), second_ping->begin(), second_ping->end());
    GLYPHA_REQUIRE(send_all(socket, pipelined));

    const auto init_frame = receive_response(socket);
    const auto second_ping_frame = receive_response(socket);
    const auto init_response = glyphastore::server::decode_response(init_frame);
    const auto ping_response = glyphastore::server::decode_response(second_ping_frame);
    GLYPHA_REQUIRE(init_response.has_value());
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(init_response->frame.request_id == 11);
    GLYPHA_REQUIRE(text(init_response->frame.value) == "GlyphaStore/2");
    GLYPHA_REQUIRE(init_response->frame.worker_count == 1);
    GLYPHA_REQUIRE(ping_response->frame.request_id == 12);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "second");
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 14,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(socket, *bind));
    const auto bind_frame = receive_response(socket);
    const auto bind_response = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bind_response.has_value());
    GLYPHA_REQUIRE(bind_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(bind_response->frame.owner_worker == 0);

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 20,
        .key = bytes("network-key"),
        .value = bytes("network-value"),
    });
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 21,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(get.has_value());
    std::vector<std::byte> store_pipeline;
    store_pipeline.insert(store_pipeline.end(), put->begin(), put->end());
    store_pipeline.insert(store_pipeline.end(), get->begin(), get->end());
    GLYPHA_REQUIRE(send_all(socket, store_pipeline));
    const auto put_frame = receive_response(socket);
    const auto get_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "network-value");

    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 22,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 23,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(missing_get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *missing_get));
    const auto missing_frame = receive_response(socket);
    const auto missing_response = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(missing_response.has_value());
    GLYPHA_REQUIRE(missing_response->frame.status == glyphastore::server::ResponseStatus::not_found);

    std::vector<std::byte> large_payload(1536U * 1024U, std::byte{0x5A});
    const auto large_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 13,
        .value = large_payload,
    });
    GLYPHA_REQUIRE(large_ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *large_ping));
    const auto large_response_frame = receive_response(socket);
    const auto large_response = glyphastore::server::decode_response(large_response_frame);
    GLYPHA_REQUIRE(large_response.has_value());
    GLYPHA_REQUIRE(large_response->frame.request_id == 13);
    GLYPHA_REQUIRE(large_response->frame.value.size() == large_payload.size());
    GLYPHA_REQUIRE(std::ranges::equal(large_response->frame.value, large_payload));

    static_cast<void>(::close(socket));

    const auto half_closed_socket = connect_to(server.port());
    GLYPHA_REQUIRE(half_closed_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(half_closed_socket, 0, 1));
    const auto final_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 30,
        .key = bytes("half-close-key"),
        .value = bytes("half-close-value"),
    });
    GLYPHA_REQUIRE(final_put.has_value());
    GLYPHA_REQUIRE(send_all(half_closed_socket, *final_put));
    GLYPHA_REQUIRE(::shutdown(half_closed_socket, SHUT_WR) == 0);
    const auto final_frame = receive_response(half_closed_socket);
    const auto final_response = glyphastore::server::decode_response(final_frame);
    GLYPHA_REQUIRE(final_response.has_value());
    GLYPHA_REQUIRE(final_response->frame.request_id == 30);
    GLYPHA_REQUIRE(final_response->frame.status == glyphastore::server::ResponseStatus::ok);
    static_cast<void>(::close(half_closed_socket));

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("bound Reactor redirects wrong owners without forwarding") {
    GLYPHA_REQUIRE(glyphastore::route_worker("bounded-key-0", 2) == 1);
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 2, .reuse_port = false});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto wrong_socket = connect_to(server.port());
    GLYPHA_REQUIRE(wrong_socket >= 0);
    const auto premature_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 98,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(premature_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *premature_bind));
    const auto premature_bind_frame = receive_response(wrong_socket);
    const auto premature = glyphastore::server::decode_response(premature_bind_frame);
    GLYPHA_REQUIRE(premature.has_value());
    GLYPHA_REQUIRE(premature->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(premature->frame.owner_worker == glyphastore::server::kNoWorker);

    const auto unbound_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 99,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(unbound_get.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *unbound_get));
    const auto unbound_frame = receive_response(wrong_socket);
    const auto unbound = glyphastore::server::decode_response(unbound_frame);
    GLYPHA_REQUIRE(unbound.has_value());
    GLYPHA_REQUIRE(unbound->frame.status == glyphastore::server::ResponseStatus::not_bound);
    GLYPHA_REQUIRE(unbound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(initialize_and_bind(wrong_socket, 0, 2));
    const auto repeated_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 100,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(repeated_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *repeated_bind));
    const auto repeated_bind_frame = receive_response(wrong_socket);
    const auto repeated = glyphastore::server::decode_response(repeated_bind_frame);
    GLYPHA_REQUIRE(repeated.has_value());
    GLYPHA_REQUIRE(repeated->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(repeated->frame.owner_worker == 0);

    const auto misplaced_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 101,
        .key = bytes("bounded-key-0"),
        .value = bytes("stored"),
    });
    GLYPHA_REQUIRE(misplaced_put.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *misplaced_put));
    const auto redirect_frame = receive_response(wrong_socket);
    const auto redirect = glyphastore::server::decode_response(redirect_frame);
    GLYPHA_REQUIRE(redirect.has_value());
    GLYPHA_REQUIRE(redirect->frame.status == glyphastore::server::ResponseStatus::wrong_owner);
    GLYPHA_REQUIRE(redirect->frame.owner_worker == 1);
    static_cast<void>(::close(wrong_socket));

    const auto owner_socket = connect_to(server.port());
    GLYPHA_REQUIRE(owner_socket >= 0);
    const auto owner_init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 101,
    });
    const auto owner_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 102,
        .target_worker = 1,
    });
    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 103,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(owner_init.has_value());
    GLYPHA_REQUIRE(owner_bind.has_value());
    GLYPHA_REQUIRE(missing_get.has_value());
    std::vector<std::byte> bind_pipeline;
    bind_pipeline.insert(bind_pipeline.end(), owner_init->begin(), owner_init->end());
    bind_pipeline.insert(bind_pipeline.end(), owner_bind->begin(), owner_bind->end());
    bind_pipeline.insert(bind_pipeline.end(), missing_get->begin(), missing_get->end());
    GLYPHA_REQUIRE(send_all(owner_socket, bind_pipeline));
    const auto owner_init_frame = receive_response(owner_socket);
    const auto owner_bind_frame = receive_response(owner_socket);
    const auto missing_frame = receive_response(owner_socket);
    const auto owner_initialized = glyphastore::server::decode_response(owner_init_frame);
    const auto owner_bound = glyphastore::server::decode_response(owner_bind_frame);
    const auto missing = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(owner_initialized.has_value());
    GLYPHA_REQUIRE(owner_initialized->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound.has_value());
    GLYPHA_REQUIRE(owner_bound->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(missing.has_value());
    GLYPHA_REQUIRE(missing->frame.status == glyphastore::server::ResponseStatus::not_found);
    static_cast<void>(::close(owner_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("multi-Reactor executors distribute connections and share one Store") {
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 16, .worker_count = 2, .executor_affinity = true});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.executor_count() == 2);
    GLYPHA_REQUIRE(server.start().has_value());

    for (std::uint64_t request = 0; request < 16; ++request) {
        const auto key = std::string{"reuse-key-"} + std::to_string(request);
        const auto value = std::string{"reuse-value-"} + std::to_string(request);
        const auto owner = static_cast<std::uint32_t>(glyphastore::route_worker(key, 2));
        const auto writer = connect_to(server.port());
        GLYPHA_REQUIRE(writer >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(writer, owner, 2));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = request * 2U,
            .key = bytes(key),
            .value = bytes(value),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(writer, *put));
        const auto put_frame = receive_response(writer);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(writer));

        const auto reader = connect_to(server.port());
        GLYPHA_REQUIRE(reader >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(reader, owner, 2));
        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = request * 2U + 1U,
            .key = bytes(key),
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(reader, *get));
        const auto get_frame = receive_response(reader);
        const auto get_response = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(get_response.has_value());
        GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(get_response->frame.value) == value);
        static_cast<void>(::close(reader));
    }

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    const auto adopted = server.adopted_connections_per_executor();
    GLYPHA_REQUIRE(adopted.size() == 2);
    GLYPHA_REQUIRE(adopted[0] > 0);
    GLYPHA_REQUIRE(adopted[1] > 0);
    const auto affinity = server.executor_affinity_results();
    GLYPHA_REQUIRE(affinity.size() == 2);
    GLYPHA_REQUIRE(affinity[0].mode != glyphastore::server::ExecutorAffinityMode::disabled);
    GLYPHA_REQUIRE(affinity[1].mode != glyphastore::server::ExecutorAffinityMode::disabled);
}

GLYPHA_TEST("server connection rate limit returns overloaded") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 8,
        .worker_count = 1,
        .abuse =
            {
                .connection_max_requests_per_sec = 2,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    // INIT + BIND consume the two-request budget; the next PING must be overloaded.
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 99,
        .value = bytes("x"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *ping));
    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    const auto stats = probe_lifecycle(socket, glyphastore::server::RequestOpcode::stats, 100);
    GLYPHA_REQUIRE(stats.has_value());
    GLYPHA_REQUIRE(stats->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(report.find("abuse_connection_rate_rejected=") != std::string_view::npos);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server idle timeout closes quiet connections") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
        .abuse =
            {
                .idle_timeout_ms = 50,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    bool closed = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        char byte{};
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(closed);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server accept rate limit drops excess handshakes") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 32,
        .worker_count = 1,
        .abuse =
            {
                .max_accepts_per_sec = 1,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first = connect_to(server.port());
    GLYPHA_REQUIRE(first >= 0);
    // Give the reactor time to accept the first connection against the budget.
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    const auto second = connect_to(server.port());
    GLYPHA_REQUIRE(second >= 0);

    // First peer can still speak; the second accept is dropped so INIT should fail.
    GLYPHA_REQUIRE(initialize_and_bind(first, 0, 1));
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    static_cast<void>(send_all(second, *init));
    const auto frame = receive_response(second);
    GLYPHA_REQUIRE(frame.empty());

    const auto stats = probe_lifecycle(first, glyphastore::server::RequestOpcode::stats, 7);
    GLYPHA_REQUIRE(stats.has_value());
    const auto report = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(report.find("abuse_accepts_rejected=") != std::string_view::npos);

    static_cast<void>(::close(first));
    static_cast<void>(::close(second));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
