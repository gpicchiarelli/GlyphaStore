#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace allocation_fault {

struct ThreadState {
    std::size_t fail_at{std::numeric_limits<std::size_t>::max()};
    std::size_t observed{};
    bool armed{};
    bool fired{};
};

struct Observation {
    std::size_t observed{};
    bool fired{};
};

constinit thread_local ThreadState thread_state;
constinit std::atomic_bool forbid_all{};
constinit std::atomic_bool forbidden_allocation_observed{};
// Cross-thread arm for paired Writer work (Writer runs on a dedicated thread).
constinit std::atomic_size_t process_fail_at{std::numeric_limits<std::size_t>::max()};
constinit std::atomic_size_t process_observed{};

void arm(const std::size_t fail_at) noexcept {
    thread_state = {.fail_at = fail_at, .observed = 0, .armed = true, .fired = false};
}

void arm_process(const std::size_t fail_at) noexcept {
    process_observed.store(0, std::memory_order_relaxed);
    process_fail_at.store(fail_at, std::memory_order_release);
}

void disarm_process() noexcept {
    process_fail_at.store(std::numeric_limits<std::size_t>::max(), std::memory_order_release);
}

auto disarm() noexcept -> Observation {
    const Observation observation{.observed = thread_state.observed, .fired = thread_state.fired};
    thread_state = {};
    return observation;
}

void begin_forbid_all() noexcept {
    forbidden_allocation_observed.store(false, std::memory_order_relaxed);
    forbid_all.store(true, std::memory_order_release);
}

auto end_forbid_all() noexcept -> bool {
    forbid_all.store(false, std::memory_order_release);
    return forbidden_allocation_observed.load(std::memory_order_acquire);
}

auto should_fail() noexcept -> bool {
    if (forbid_all.load(std::memory_order_acquire)) {
        forbidden_allocation_observed.store(true, std::memory_order_release);
        return true;
    }
    const auto process_at = process_fail_at.load(std::memory_order_acquire);
    if (process_at != std::numeric_limits<std::size_t>::max()) {
        const auto allocation = process_observed.fetch_add(1, std::memory_order_relaxed);
        if (allocation == process_at) {
            return true;
        }
    }
    if (!thread_state.armed) {
        return false;
    }
    const auto allocation = thread_state.observed++;
    if (!thread_state.fired && allocation == thread_state.fail_at) {
        thread_state.fired = true;
        return true;
    }
    return false;
}

auto allocate(const std::size_t requested) -> void* {
    if (should_fail()) {
        throw std::bad_alloc{};
    }
    const auto size = requested == 0 ? std::size_t{1} : requested;
    if (void* memory = std::malloc(size); memory != nullptr) {
        return memory;
    }
    throw std::bad_alloc{};
}

auto allocate_aligned(const std::size_t requested, const std::size_t alignment) -> void* {
    if (should_fail()) {
        throw std::bad_alloc{};
    }
    void* memory{};
    const auto size = requested == 0 ? std::size_t{1} : requested;
    if (::posix_memalign(&memory, alignment, size) == 0 && memory != nullptr) {
        return memory;
    }
    throw std::bad_alloc{};
}

} // namespace allocation_fault

void* operator new(const std::size_t size) {
    return allocation_fault::allocate(size);
}

void* operator new[](const std::size_t size) {
    return allocation_fault::allocate(size);
}

void* operator new(const std::size_t size, const std::align_val_t alignment) {
    return allocation_fault::allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void* operator new[](const std::size_t size, const std::align_val_t alignment) {
    return allocation_fault::allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void* operator new(const std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return allocation_fault::allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void* operator new[](const std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return allocation_fault::allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void* operator new(const std::size_t size, const std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try {
        return allocation_fault::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}

void* operator new[](const std::size_t size, const std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
    try {
        return allocation_fault::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* memory) noexcept {
    std::free(memory);
}

void operator delete[](void* memory) noexcept {
    std::free(memory);
}

void operator delete(void* memory, const std::size_t) noexcept {
    std::free(memory);
}

void operator delete[](void* memory, const std::size_t) noexcept {
    std::free(memory);
}

void operator delete(void* memory, const std::align_val_t) noexcept {
    std::free(memory);
}

void operator delete[](void* memory, const std::align_val_t) noexcept {
    std::free(memory);
}

void operator delete(void* memory, const std::size_t, const std::align_val_t) noexcept {
    std::free(memory);
}

void operator delete[](void* memory, const std::size_t, const std::align_val_t) noexcept {
    std::free(memory);
}

void operator delete(void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}

void operator delete[](void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}

void operator delete(void* memory, const std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(memory);
}

void operator delete[](void* memory, const std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(memory);
}

namespace {

class TemporaryDirectory final {
  public:
    TemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-allocation-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        root_ = created;
        path_ = root_ / "store";
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path root_;
    std::filesystem::path path_;
};

void require(const bool condition, const std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string{message});
    }
}

[[nodiscard]] auto bytes(const std::string_view value) noexcept -> std::span<const std::byte> {
    return std::as_bytes(std::span{value});
}

[[nodiscard]] auto value_text(const glyphastore::OwnedValue& value) -> std::string {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

inline const std::string kLongKey(96, 'k');
inline const std::string kOriginalValue(257, 'o');
inline const std::string kReplacementValue(513, 'r');

enum class MutationKind { put_new, put_update, erase };

struct Scenario {
    std::string_view name;
    MutationKind kind{MutationKind::put_new};
    bool seed{};
    bool force_rotation{};
    glyphastore::DurableRuntimeOptions options{};
};

struct WriteBoundaryObserver {
    std::atomic_bool reached{};
    std::atomic_bool forced_full{};
    bool forbid_allocations{};
    bool force_rotation{};

    [[nodiscard]] static auto
    starts_persistent_change(const glyphastore::FilesystemOperation operation) noexcept -> bool {
        switch (operation) {
        case glyphastore::FilesystemOperation::create_data_directory:
        case glyphastore::FilesystemOperation::write_manifest:
        case glyphastore::FilesystemOperation::rename_manifest:
        case glyphastore::FilesystemOperation::preallocate_segment:
        case glyphastore::FilesystemOperation::write_segment_header:
        case glyphastore::FilesystemOperation::rename_segment:
        case glyphastore::FilesystemOperation::write_record:
        case glyphastore::FilesystemOperation::write_commit_slot:
        case glyphastore::FilesystemOperation::write_bootstrap:
        case glyphastore::FilesystemOperation::rename_bootstrap:
        case glyphastore::FilesystemOperation::remove_bootstrap:
        case glyphastore::FilesystemOperation::write_compaction_intent:
        case glyphastore::FilesystemOperation::rename_compaction_intent:
        case glyphastore::FilesystemOperation::remove_compaction_intent:
        case glyphastore::FilesystemOperation::remove_compaction_segment:
        case glyphastore::FilesystemOperation::copy_backup_segment:
        case glyphastore::FilesystemOperation::copy_backup_manifest:
            return true;
        case glyphastore::FilesystemOperation::sync_parent_directory:
        case glyphastore::FilesystemOperation::sync_manifest:
        case glyphastore::FilesystemOperation::sync_directory:
        case glyphastore::FilesystemOperation::sync_segment_file:
        case glyphastore::FilesystemOperation::sync_record:
        case glyphastore::FilesystemOperation::sync_commit_slot:
        case glyphastore::FilesystemOperation::sync_bootstrap:
        case glyphastore::FilesystemOperation::sync_compaction_intent:
        case glyphastore::FilesystemOperation::sync_backup_destination:
            return false;
        }
        return false;
    }

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& observer = *static_cast<WriteBoundaryObserver*>(opaque);
        if (observer.force_rotation && operation == glyphastore::FilesystemOperation::write_record &&
            !observer.forced_full.exchange(true, std::memory_order_acq_rel)) {
            return glyphastore::unexpected(glyphastore::Error{glyphastore::ErrorCode::segment_full, {}});
        }
        if (starts_persistent_change(operation)) {
            const bool first_boundary = !observer.reached.exchange(true, std::memory_order_acq_rel);
            if (observer.forbid_allocations && first_boundary) {
                allocation_fault::begin_forbid_all();
            }
        }
        return {};
    }
};

void initialize_store(const std::filesystem::path& path, const bool seed) {
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    require(opened.has_value(), "failed to initialize durable allocation test Store");
    if (seed) {
        const auto stored = (*opened)->put(kLongKey, bytes(kOriginalValue));
        require(stored.has_value(), "failed to seed durable allocation test Store");
    }
}

void append_compaction_record(glyphastore::DurableSegmentFile& segment, const std::uint64_t sequence,
                              const std::string_view key, const std::string_view value) {
    const auto encoded = glyphastore::encode_record({
        .sequence = glyphastore::SequenceNumber{sequence},
        .opcode = glyphastore::Opcode::put,
        .type = glyphastore::ValueType::bytes,
        .flags = 0,
        .key_hash = glyphastore::hash_key(key),
        .expire_at_ns = 0,
        .key = bytes(key),
        .value = bytes(value),
    });
    require(encoded.has_value(), "failed to encode allocation compaction Record");
    require(segment.append(*encoded).committed(), "failed to append allocation compaction Record");
}

void initialize_compaction_store(const std::filesystem::path& path) {
    initialize_store(path, false);
    auto directory = glyphastore::DataDirectory::open_and_lock(path);
    require(directory.has_value(), "failed to lock allocation compaction Store");
    auto manifest = directory->read_manifest();
    require(manifest.has_value() && manifest->segments.size() == 1,
            "allocation compaction Store has an unexpected initial manifest");

    auto first_entry = manifest->segments.front();
    const glyphastore::SegmentHeaderIdentity first_identity{
        .store_id = manifest->store_id,
        .segment_id = first_entry.segment_id,
        .generation = first_entry.generation,
        .owner_worker = first_entry.owner_worker,
    };
    auto first = glyphastore::DurableSegmentFile::open(*directory, first_identity,
                                                       glyphastore::SegmentFileOpenMode::read_write);
    require(first.has_value(), "failed to open first allocation compaction Segment");
    append_compaction_record(*first, 1, "first", "first-value");
    require(first->seal().committed(), "failed to seal first allocation compaction Segment");
    first_entry.role = glyphastore::ManifestSegmentRole::sealed;

    const glyphastore::ManifestSegmentEntry second_entry{
        .segment_id = glyphastore::SegmentId{2},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::sealed,
    };
    const glyphastore::SegmentHeaderIdentity second_identity{
        .store_id = manifest->store_id,
        .segment_id = second_entry.segment_id,
        .generation = second_entry.generation,
        .owner_worker = second_entry.owner_worker,
    };
    auto second_created = glyphastore::DurableSegmentFile::create(*directory, second_identity);
    require(second_created.durable() && second_created.file.has_value(),
            "failed to create second allocation compaction Segment");
    append_compaction_record(*second_created.file, 2, "second", "second-value");
    require(second_created.file->seal().committed(), "failed to seal second allocation compaction Segment");

    const glyphastore::ManifestSegmentEntry active_entry{
        .segment_id = glyphastore::SegmentId{3},
        .generation = glyphastore::GenerationId{1},
        .owner_worker = glyphastore::WorkerId{0},
        .role = glyphastore::ManifestSegmentRole::active,
    };
    const glyphastore::SegmentHeaderIdentity active_identity{
        .store_id = manifest->store_id,
        .segment_id = active_entry.segment_id,
        .generation = active_entry.generation,
        .owner_worker = active_entry.owner_worker,
    };
    auto active_created = glyphastore::DurableSegmentFile::create(*directory, active_identity);
    require(active_created.durable(), "failed to create active allocation compaction Segment");

    ++manifest->manifest_generation;
    manifest->next_segment_id = glyphastore::SegmentId{4};
    manifest->segments = {first_entry, second_entry, active_entry};
    require(directory->publish_manifest(*manifest).durable(),
            "failed to publish allocation compaction manifest");
}

[[nodiscard]] auto open_runtime(const std::filesystem::path& path,
                                const glyphastore::DurableRuntimeOptions options,
                                WriteBoundaryObserver* observer = nullptr)
    -> std::unique_ptr<glyphastore::DurableRuntimeCatalog> {
    glyphastore::FilesystemHooks hooks{};
    if (observer != nullptr) {
        hooks = {.context = observer, .before = &WriteBoundaryObserver::before};
    }
    auto directory = glyphastore::DataDirectory::open_and_lock(path, hooks);
    require(directory.has_value(), "failed to lock durable allocation test Store");
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(std::move(*directory), 0, options);
    require(runtime.has_value(), "failed to recover durable allocation test Store");
    return std::move(*runtime);
}

[[nodiscard]] auto mutate(glyphastore::DurableRuntimeCatalog& runtime, const MutationKind kind)
    -> glyphastore::DurableMutationResult {
    switch (kind) {
    case MutationKind::put_new:
    case MutationKind::put_update:
        return runtime.put(bytes(kLongKey), bytes(kReplacementValue));
    case MutationKind::erase:
        return runtime.erase(bytes(kLongKey));
    }
    throw std::runtime_error("unsupported allocation test mutation");
}

void require_recovered_prewrite_state(const std::filesystem::path& path, const bool seeded) {
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    require(reopened.has_value(), "failed to reopen allocation test Store");
    const auto recovered = (*reopened)->get(kLongKey);
    if (!seeded) {
        require(!recovered.has_value(), "pre-write allocation failure recovered a new value");
        require(recovered.error().code == glyphastore::ErrorCode::not_found,
                "pre-write allocation failure returned an unexpected read error");
        return;
    }
    require(recovered.has_value(), "pre-write allocation failure removed the original value");
    require(value_text(*recovered) == kOriginalValue,
            "pre-write allocation failure changed the original value");
}

void run_exhaustive_allocation_failures(const Scenario& scenario) {
    constexpr std::size_t kMaximumExpectedAllocations = 128;
    bool completed{};
    for (std::size_t fail_at = 0; fail_at < kMaximumExpectedAllocations; ++fail_at) {
        TemporaryDirectory temporary;
        initialize_store(temporary.path(), scenario.seed);
        WriteBoundaryObserver observer{.force_rotation = scenario.force_rotation};
        auto runtime = open_runtime(temporary.path(), scenario.options, &observer);

        glyphastore::DurableMutationResult result;
        allocation_fault::arm(fail_at);
        try {
            result = mutate(*runtime, scenario.kind);
        } catch (...) {
            static_cast<void>(allocation_fault::disarm());
            throw;
        }
        const auto allocation = allocation_fault::disarm();
        if (!allocation.fired) {
            require(result.committed(), "allocation baseline mutation did not commit");
            require(fail_at == allocation.observed,
                    "allocation enumeration changed before reaching its terminal count");
            completed = true;
            break;
        }

        require(result.error.has_value(), "injected allocation failure returned no error");
        require(result.error->code == glyphastore::ErrorCode::resource_exhausted,
                "injected allocation failure was not translated to resource_exhausted");
        const bool write_started = observer.reached.load(std::memory_order_acquire);
        if (!write_started) {
            require(result.outcome == glyphastore::DurableMutationOutcome::not_committed ||
                        result.outcome == glyphastore::DurableMutationOutcome::indeterminate,
                    "pre-write allocation failure returned an invalid outcome");
            require(runtime->healthy() ==
                        (result.outcome == glyphastore::DurableMutationOutcome::not_committed),
                    "pre-write allocation failure health disagrees with its outcome");
        } else {
            if (result.outcome != glyphastore::DurableMutationOutcome::indeterminate) {
                throw std::runtime_error(std::string{scenario.name} + " allocation " +
                                         std::to_string(fail_at) +
                                         " crossed the write boundary without an indeterminate outcome");
            }
            require(!runtime->healthy(), "post-write allocation failure did not fail closed");
            const auto blocked = mutate(*runtime, scenario.kind);
            // Pre-I/O reject when already fail-closed is not_committed (no write boundary).
            require(blocked.outcome == glyphastore::DurableMutationOutcome::not_committed,
                    "failed-closed runtime accepted another mutation");
            require(blocked.error.has_value() &&
                        blocked.error->code == glyphastore::ErrorCode::unavailable,
                    "failed-closed reject was not unavailable");
        }
        runtime.reset();
        if (!write_started || scenario.force_rotation) {
            require_recovered_prewrite_state(temporary.path(), scenario.seed);
        }
    }
    require(completed, std::string{scenario.name} + " exceeded allocation enumeration limit");
}

void run_no_post_write_allocation(const glyphastore::DurableRuntimeOptions options) {
    TemporaryDirectory temporary;
    initialize_store(temporary.path(), false);
    WriteBoundaryObserver observer{.forbid_allocations = true};
    auto runtime = open_runtime(temporary.path(), options, &observer);

    glyphastore::DurableMutationResult result;
    try {
        result = mutate(*runtime, MutationKind::put_new);
    } catch (...) {
        static_cast<void>(allocation_fault::end_forbid_all());
        throw;
    }
    const bool forbidden_allocation = allocation_fault::end_forbid_all();
    require(observer.reached.load(std::memory_order_acquire),
            "post-write allocation guard never reached the persistent write boundary");
    require(!forbidden_allocation, "durable mutation allocated after its persistent write boundary");
    require(result.committed(), "post-write allocation guard prevented a durable commit");
    require(runtime->healthy(), "post-write allocation guard failed the runtime closed");
}

void run_exhaustive_read_failures() {
    constexpr std::size_t kMaximumExpectedAllocations = 32;
    bool completed{};
    for (std::size_t fail_at = 0; fail_at < kMaximumExpectedAllocations; ++fail_at) {
        TemporaryDirectory temporary;
        initialize_store(temporary.path(), true);
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = temporary.path(),
            .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        });
        require(opened.has_value(), "failed to open durable read allocation test Store");

        glyphastore::Result<glyphastore::OwnedValue> result =
            glyphastore::unexpected(glyphastore::Error{glyphastore::ErrorCode::internal_error, {}});
        allocation_fault::arm(fail_at);
        try {
            result = (*opened)->get(kLongKey);
        } catch (...) {
            static_cast<void>(allocation_fault::disarm());
            throw;
        }
        const auto allocation = allocation_fault::disarm();
        if (!allocation.fired) {
            require(result.has_value(), "allocation baseline read failed");
            require(value_text(*result) == kOriginalValue, "allocation baseline read returned wrong value");
            require(fail_at == allocation.observed,
                    "read allocation enumeration changed before its terminal count");
            completed = true;
            break;
        }
        require(!result.has_value(), "injected read allocation failure returned a value");
        require(result.error().code == glyphastore::ErrorCode::resource_exhausted,
                "injected read allocation failure was not translated to resource_exhausted");
        const auto repeated = (*opened)->get(kLongKey);
        require(repeated.has_value(), "read allocation failure poisoned an otherwise healthy Store");
        require(value_text(*repeated) == kOriginalValue, "read allocation failure changed the stored value");
    }
    require(completed, "durable read exceeded allocation enumeration limit");
}

struct ThrowingSyncHook {
    std::atomic_bool fired{};

    static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& hook = *static_cast<ThrowingSyncHook*>(opaque);
        if (operation == glyphastore::FilesystemOperation::sync_record &&
            !hook.fired.exchange(true, std::memory_order_acq_rel)) {
            throw std::bad_alloc{};
        }
        return {};
    }
};

void run_background_allocation_failure_waiters() {
    TemporaryDirectory temporary;
    initialize_store(temporary.path(), false);
    ThrowingSyncHook hook;
    auto directory = glyphastore::DataDirectory::open_and_lock(
        temporary.path(),
        glyphastore::FilesystemHooks{.context = &hook, .before = &ThrowingSyncHook::before});
    require(directory.has_value(), "failed to lock background allocation test Store");
    auto runtime = glyphastore::DurableRuntimeCatalog::open_locked(
        std::move(*directory), 0,
        {.commit_sync = glyphastore::SegmentCommitSync::immediate,
         .sync_interval_ms = 60'000,
         .batch =
             glyphastore::DurableGroupConfig{.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 60'000},
         .strict_ack = true});
    require(runtime.has_value(), "failed to recover background allocation test Store");

    const std::array<std::string, 2> keys{"first-allocation-waiter", "second-allocation-waiter"};
    const std::string value{"value"};
    std::array<glyphastore::DurableMutationResult, 2> results;
    std::array<std::thread, 2> producers{
        std::thread{[&] { results[0] = (*runtime)->put(bytes(keys[0]), bytes(value)); }},
        std::thread{[&] { results[1] = (*runtime)->put(bytes(keys[1]), bytes(value)); }},
    };
    for (auto& producer : producers) {
        producer.join();
    }

    require(hook.fired.load(std::memory_order_acquire), "background allocation failure hook did not fire");
    for (const auto& result : results) {
        require(result.outcome == glyphastore::DurableMutationOutcome::indeterminate,
                "background allocation failure did not release a waiter as indeterminate");
    }
    require(!(*runtime)->healthy(), "background allocation failure did not fail closed");
}

void require_recovered_compaction_state(const std::filesystem::path& path) {
    auto runtime = open_runtime(path, {});
    const auto first = runtime->get("first");
    const auto second = runtime->get("second");
    require(first.has_value() && value_text(*first) == "first-value",
            "allocation compaction recovery lost the first value");
    require(second.has_value() && value_text(*second) == "second-value",
            "allocation compaction recovery lost the second value");
    require(runtime->namespace_audit().clean(), "allocation compaction recovery left a dirty namespace");
}

void run_exhaustive_compaction_allocation_failures() {
    constexpr std::size_t kMaximumExpectedAllocations = 512;
    bool completed{};
    for (std::size_t fail_at = 0; fail_at < kMaximumExpectedAllocations; ++fail_at) {
        TemporaryDirectory temporary;
        initialize_compaction_store(temporary.path());
        auto runtime = open_runtime(temporary.path(), {});

        glyphastore::DurableCompactionResult result;
        allocation_fault::arm(fail_at);
        try {
            result = runtime->compact_worker(0, 0);
        } catch (...) {
            static_cast<void>(allocation_fault::disarm());
            throw;
        }
        const auto allocation = allocation_fault::disarm();
        if (!allocation.fired) {
            require(result.compacted(), "allocation compaction baseline did not compact");
            require(fail_at == allocation.observed,
                    "compaction allocation enumeration changed before its terminal count");
            runtime.reset();
            require_recovered_compaction_state(temporary.path());
            completed = true;
            break;
        }

        require(result.error.has_value(), "injected compaction allocation failure returned no error");
        require(result.error->code == glyphastore::ErrorCode::resource_exhausted,
                "injected compaction allocation failure was not resource_exhausted");
        require(result.outcome == glyphastore::DurableCompactionOutcome::not_compacted ||
                    result.outcome == glyphastore::DurableCompactionOutcome::recovery_required,
                "injected compaction allocation failure returned an invalid outcome");
        require(runtime->healthy() ==
                    (result.outcome == glyphastore::DurableCompactionOutcome::not_compacted),
                "compaction allocation failure health disagrees with recovery requirement");
        runtime.reset();
        require_recovered_compaction_state(temporary.path());
    }
    require(completed, "durable compaction exceeded its allocation enumeration limit");
}

void run_volatile_rotation_allocation_failures() {
    glyphastore::GlobalSegmentManager manager;
    const auto active = manager.allocate_active(glyphastore::WorkerId{0});
    glyphastore::SegmentPtr replacement;

    constexpr std::size_t kMaximumExpectedAllocations = 8;
    for (std::size_t fail_at = 0; fail_at < kMaximumExpectedAllocations; ++fail_at) {
        bool threw{};
        allocation_fault::arm(fail_at);
        try {
            const auto prepared = manager.prepare_rotation(active, glyphastore::WorkerId{0});
            require(prepared.has_value(), "volatile rotation preparation returned an error");
            replacement = *prepared;
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        const auto allocation = allocation_fault::disarm();
        require(active->state() == glyphastore::SegmentState::active,
                "failed rotation preparation sealed the old active Segment");
        require(manager.segments().size() == 1,
                "failed rotation preparation published a replacement Segment");
        if (!threw) {
            require(!allocation.fired, "rotation preparation swallowed an allocation failure");
            require(fail_at == allocation.observed,
                    "rotation preparation allocation enumeration changed unexpectedly");
            break;
        }
        require(allocation.fired, "rotation preparation threw before the injected allocation");
    }
    require(replacement != nullptr, "rotation preparation exceeded its allocation bound");
    require(manager.find(replacement->id()) == nullptr,
            "prepared replacement became visible before rotation commit");

    bool commit_threw{};
    allocation_fault::arm(0);
    try {
        static_cast<void>(manager.commit_rotation(active, replacement));
    } catch (const std::bad_alloc&) {
        commit_threw = true;
    }
    const auto commit_allocation = allocation_fault::disarm();
    require(commit_threw && commit_allocation.fired,
            "rotation commit did not expose its first catalog allocation");
    require(active->state() == glyphastore::SegmentState::active,
            "failed rotation commit sealed the old active Segment");
    require(manager.find(replacement->id()) == nullptr,
            "failed rotation commit published the replacement Segment");

    require(manager.commit_rotation(active, replacement).has_value(),
            "rotation commit did not recover after allocation failure");
    require(active->state() == glyphastore::SegmentState::sealed,
            "successful rotation did not seal the previous active Segment");
    require(manager.find(replacement->id()) == replacement,
            "successful rotation did not publish the replacement Segment");
}

void run_volatile_vacuum_publication_allocation_failures() {
    constexpr std::size_t kMaximumExpectedAllocations = 8;
    bool completed{};
    for (std::size_t fail_at = 0; fail_at < kMaximumExpectedAllocations; ++fail_at) {
        glyphastore::GlobalSegmentManager manager;
        const auto first = manager.allocate_active(glyphastore::WorkerId{0});
        const auto second = manager.prepare_rotation(first, glyphastore::WorkerId{0});
        require(second.has_value(), "failed to prepare the second vacuum source Segment");
        require(manager.commit_rotation(first, *second).has_value(),
                "failed to publish the second vacuum source Segment");
        const auto active = manager.prepare_rotation(*second, glyphastore::WorkerId{0});
        require(active.has_value(), "failed to prepare the post-vacuum active Segment");
        require(manager.commit_rotation(*second, *active).has_value(),
                "failed to publish the post-vacuum active Segment");

        const auto first_replacement = manager.prepare_segment(glyphastore::WorkerId{0});
        const auto second_replacement = manager.prepare_segment(glyphastore::WorkerId{0});
        require(first_replacement.has_value() && second_replacement.has_value(),
                "failed to prepare vacuum replacement Segments");
        require((*first_replacement)->seal().has_value() && (*second_replacement)->seal().has_value(),
                "failed to seal vacuum replacement Segments");
        const std::array sources{first->id(), (*second)->id()};
        const std::array replacements{*first_replacement, *second_replacement};

        bool threw{};
        glyphastore::Status result;
        allocation_fault::arm(fail_at);
        try {
            result = manager.replace_sealed(sources, replacements);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        const auto allocation = allocation_fault::disarm();
        if (threw) {
            require(allocation.fired, "vacuum publication threw before the injected allocation");
            require(manager.find(first->id()) == first && manager.find((*second)->id()) == *second,
                    "failed vacuum publication retired a source Segment");
            require(manager.find((*first_replacement)->id()) == nullptr &&
                        manager.find((*second_replacement)->id()) == nullptr,
                    "failed vacuum publication left a partial replacement catalog");
            continue;
        }

        require(result.has_value(), "vacuum publication baseline returned an error");
        require(!allocation.fired, "vacuum publication swallowed an allocation failure");
        require(fail_at == allocation.observed,
                "vacuum publication allocation enumeration changed unexpectedly");
        require(manager.find(first->id()) == nullptr && manager.find((*second)->id()) == nullptr,
                "successful vacuum publication retained a source Segment");
        require(manager.find((*first_replacement)->id()) == *first_replacement &&
                    manager.find((*second_replacement)->id()) == *second_replacement,
                "successful vacuum publication omitted a replacement Segment");
        completed = true;
        break;
    }
    require(completed, "vacuum publication exceeded its allocation bound");
}

void run_index_tombstone_rebuild_allocation_failures() {
    bool completed{};
    for (std::size_t fail_at = 0; fail_at < 128; ++fail_at) {
        glyphastore::Index index;
        std::vector<std::string> keys;
        keys.reserve(80);
        for (std::uint64_t value = 0; value < 80; ++value) {
            auto key = std::string(64, 't');
            key.replace(key.size() - std::to_string(value).size(), std::to_string(value).size(),
                        std::to_string(value));
            const glyphastore::RecordRef ref{
                glyphastore::SegmentId{1}, glyphastore::RecordOffset{4096}, glyphastore::RecordSize{64},
                glyphastore::SequenceNumber{value + 1}, glyphastore::GenerationId{1}};
            require(index.insert_or_assign(key, ref).has_value(), "failed to seed tombstone rebuild");
            keys.push_back(std::move(key));
        }
        for (std::size_t value = 0; value < 60; ++value) {
            const glyphastore::HashedKey key{keys[value], glyphastore::hash_key(keys[value])};
            require(index.erase_no_compact(key).previous.has_value(), "failed to seed deleted slot");
        }
        const auto before = index.stats();
        const glyphastore::HashedKey next{"inline-next", glyphastore::hash_key("inline-next")};

        bool threw{};
        glyphastore::Status prepared;
        allocation_fault::arm(fail_at);
        try {
            prepared = index.prepare_insert(next);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        const auto allocation = allocation_fault::disarm();
        if (allocation.fired) {
            require(threw, "tombstone rebuild swallowed an injected allocation failure");
            const auto after = index.stats();
            require(after.size == before.size && after.deleted_count == before.deleted_count &&
                        after.bucket_count == before.bucket_count &&
                        after.arena_allocated_bytes == before.arena_allocated_bytes &&
                        after.arena_live_bytes == before.arena_live_bytes,
                    "failed tombstone rebuild changed authoritative table state");
            for (std::size_t value = 60; value < keys.size(); ++value) {
                require(index.find(keys[value]).has_value(), "failed tombstone rebuild lost a live key");
            }
            continue;
        }
        require(!threw && prepared.has_value(), "tombstone rebuild baseline failed");
        require(fail_at == allocation.observed,
                "tombstone rebuild allocation enumeration changed unexpectedly");
        require(index.stats().deleted_count == 0, "successful tombstone rebuild retained deleted slots");
        completed = true;
        break;
    }
    require(completed, "tombstone rebuild exceeded its allocation bound");
}

void run_paired_async_durable_coalesced_fail_closed() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    // Capture-fail litmus needs the debug fault seam (durable stays healthy).
    return;
#else
    // Two same-key async puts coalesce (min_records=2) then split into durable
    // sub-batches. First commit + capture fail → drain-snapshot + success ACK;
    // Writer must not Store-mutate the later sub-batch.
    auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-async-fc-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    struct WriteCounter final {
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<WriteCounter*>(context);
            if (operation == glyphastore::FilesystemOperation::write_record) {
                self->writes.fetch_add(1, std::memory_order_relaxed);
            }
            return {};
        }
    } counter;

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 32,
                          .max_bytes = 65'536,
                          .max_wait_ms = 100,
                          .min_records = 2},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
        .filesystem_hooks = {.context = &counter, .before = &WriteCounter::before},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key = "async-fc";
    const auto key_hash = glyphastore::hash_key(key);
    const auto baseline_writes = counter.writes.load(std::memory_order_relaxed);

    glyphastore::fault::fail_once(glyphastore::fault::Site::capture);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = key_hash,
                    .value = bytes("first"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "first async put submit failed");
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 2, .generation = 1},
                    .request_id = 2,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = key_hash,
                    .value = bytes("second"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "second async put submit failed");

    std::array<std::optional<glyphastore::server::MutationCompletion>, 2> done{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while ((!done[0] || !done[1]) && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            const auto slot = completion->request_id == 1 ? 0U : 1U;
            require(!done[slot].has_value(), "duplicate completion");
            done[slot] = std::move(*completion);
            require((*executor)->release_payload(0, done[slot]->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done[0].has_value() && done[1].has_value(), "async completions timed out");
    require(!runtime->healthy(), "capture publication failure did not sticky-fail the pair");
    // First: ACK-after-drain. Second: never mutated after sticky (or reject).
    require(!done[0]->error.has_value(),
            "first clean commit kept error ACK after successful drain (inverted RAW)");
    require(done[1]->error.has_value(), "later same-key sub-batch must not success-ACK after fail-closed");
    require(done[1]->error->code == glyphastore::ErrorCode::resource_exhausted,
            "pre-Store sibling fail-closed must be resource_exhausted (not unavailable/reconcile)");
    const auto armed_writes = counter.writes.load(std::memory_order_relaxed) - baseline_writes;
    require(armed_writes <= 1U,
            "async durable Writer mutated a later sub-batch after post-commit fail-closed");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed drain-snapshotted first value");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "first",
            "drain-snapshotted value mismatch");

    const auto late = store.put("async-fc-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    require(late.error().message.find("fail-closed") != std::string::npos,
            "late put did not hit pair fail-closed reject");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_async_durable_sibling_publish_after_capture_fail() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Two distinct same-shard keys coalesce into one Writer batch. Key A stages
    // successfully; key B's capture fails. Both clean commits must success-ACK and
    // become GET-visible via durable snapshot publish; pair sticky-fails.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-sib-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 32,
                          .max_bytes = 65'536,
                          .max_wait_ms = 100,
                          .min_records = 2},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{8};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key_a = "async-sib-a";
    const std::string key_b = "async-sib-b";
    glyphastore::fault::fail_nth(glyphastore::fault::Site::capture, 2);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key_a),
                    .key_hash = glyphastore::hash_key(key_a),
                    .value = bytes("alpha"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "first async put submit failed");
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 2, .generation = 1},
                    .request_id = 2,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key_b),
                    .key_hash = glyphastore::hash_key(key_b),
                    .value = bytes("beta"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "second async put submit failed");

    std::array<std::optional<glyphastore::server::MutationCompletion>, 2> done{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while ((!done[0] || !done[1]) && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            const auto slot = completion->request_id == 1 ? 0U : 1U;
            require(!done[slot].has_value(), "duplicate completion");
            done[slot] = std::move(*completion);
            require((*executor)->release_payload(0, done[slot]->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done[0].has_value() && done[1].has_value(), "async completions timed out");
    require(!runtime->healthy(), "capture failure did not sticky-fail the pair");
    require(!done[0]->error.has_value(),
            "earlier committed sibling was aborted instead of snapshot-published");
    // ACK-after-publish: capture-failed sibling is drain-snapshotted — success ACK + GET.
    require(!done[1]->error.has_value(),
            "capture-failed committed sibling kept error ACK after successful drain (inverted RAW)");

    const auto got_a = store.get(key_a);
    require(got_a.has_value(), "Store::get rejected published sibling A after fail-closed");
    require(std::string_view(reinterpret_cast<const char*>(got_a->bytes.data()), got_a->bytes.size()) ==
                "alpha",
            "published sibling A value mismatch");
    const auto got_b = store.get(key_b);
    require(got_b.has_value(), "Store::get rejected drain-snapshotted sibling B after fail-closed");
    require(std::string_view(reinterpret_cast<const char*>(got_b->bytes.data()), got_b->bytes.size()) ==
                "beta",
            "published sibling B value mismatch");

    const auto late = store.put("async-sib-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    require(late.error().message.find("fail-closed") != std::string::npos,
            "late put did not hit pair fail-closed reject");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_durable_batch_stops_after_indeterminate_ttl() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Deferred TTL drain failure on the first mutate must sticky-fail durable and
    // reject later siblings in the same Writer batch (no further appends).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-ttl-stop-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    class ManualStoreClock final : public glyphastore::StoreClock {
      public:
        explicit ManualStoreClock(const std::uint64_t initial_now_ns) : now_ns_(initial_now_ns) {}
        [[nodiscard]] auto now_ns() const noexcept -> std::uint64_t override {
            return now_ns_.load(std::memory_order_relaxed);
        }
        void set(const std::uint64_t now_ns) noexcept {
            now_ns_.store(now_ns, std::memory_order_relaxed);
        }

      private:
        std::atomic<std::uint64_t> now_ns_;
    };
    const auto clock = std::make_shared<ManualStoreClock>(50);

    struct WriteCounter final {
        std::atomic_uint64_t writes{0};

        static auto before(void* context, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto* self = static_cast<WriteCounter*>(context);
            if (operation == glyphastore::FilesystemOperation::write_record) {
                self->writes.fetch_add(1, std::memory_order_relaxed);
            }
            return {};
        }
    } counter;

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 32,
                          .max_bytes = 65'536,
                          .max_wait_ms = 10,
                          .min_records = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
        .clock = clock,
        .filesystem_hooks = {.context = &counter, .before = &WriteCounter::before},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    // Seed a deferred TTL reclaim via expired GET (same path as production drain).
    require(store.put("ttl-seed", bytes("stale"), 100).has_value(), "seed put failed");
    clock->set(100);
    const auto expired = store.get("ttl-seed");
    require(!expired.has_value() && expired.error().code == glyphastore::ErrorCode::not_found,
            "expired seed GET did not return not_found");

    const auto baseline_writes = counter.writes.load(std::memory_order_relaxed);
    glyphastore::fault::fail_once(glyphastore::fault::Site::deferred_ttl);
    const std::string key_a = "ttl-stop-a";
    const std::string key_b = "ttl-stop-b";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes("alpha")},
        {.key = key_b, .value = bytes("beta")},
    };
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    require(statuses.size() == 2, "put_batch size mismatch");
    require(!statuses[0].has_value() && !statuses[1].has_value(),
            "indeterminate TTL drain left a successful ACK");
    require(!runtime->healthy(), "TTL drain failure did not sticky-fail the pair");
    require(counter.writes.load(std::memory_order_relaxed) == baseline_writes,
            "later sibling appended after sticky TTL drain failure");

    const auto late = store.put("ttl-stop-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_volatile_multichunk_fail_closed() {
    // put_batch of >32 same-shard keys drains as multiple ≤32 sync publish chunks.
    // After the first chunk sticky-fail-closes, later chunks must not mutate/publish/ACK.
    constexpr std::size_t kBatch = 40;
    constexpr std::size_t kMaximumFailAt = 256;
    bool closed{};
    for (std::size_t fail_at = 0; fail_at < kMaximumFailAt; ++fail_at) {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .concurrency = glyphastore::StoreConcurrencyMode::paired,
            .paired = {.async_lane_capacity = 8,
                       .async_lane_payload_bytes = 1U * 1024U * 1024U,
                       .reader_epoch_lease = true},
        });
        require(opened.has_value(), "failed to open paired volatile Store");
        auto& store = **opened;
        auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
        require(runtime != nullptr, "missing paired runtime");
        require(store.put("seed", bytes("ok")).has_value(), "seed put failed");

        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<glyphastore::Store::PutItem> items;
        keys.reserve(kBatch);
        values.reserve(kBatch);
        items.reserve(kBatch);
        for (std::size_t index = 0; index < kBatch; ++index) {
            keys.push_back("mc-" + std::to_string(index));
            values.push_back("v-" + std::to_string(index));
            items.push_back(glyphastore::Store::PutItem{.key = keys.back(), .value = bytes(values.back())});
        }

        allocation_fault::arm_process(fail_at);
        const auto statuses = store.put_batch(items);
        allocation_fault::disarm_process();
        require(statuses.size() == kBatch, "put_batch status size mismatch");

        if (runtime->healthy()) {
            static_cast<void>(store.close());
            continue;
        }

        closed = true;
        bool saw_failure = false;
        for (std::size_t index = 0; index < statuses.size(); ++index) {
            if (!statuses[index].has_value()) {
                saw_failure = true;
                continue;
            }
            // No success is allowed after the first failure in FIFO batch order.
            require(!saw_failure, "put_batch succeeded after an earlier sticky failure");
        }
        require(saw_failure, "fail-closed without any failed batch status");

        for (std::size_t index = 0; index < statuses.size(); ++index) {
            if (statuses[index].has_value()) {
                continue;
            }
            const auto got = store.get(keys[index]);
            require(!got.has_value(), "failed batch key became GET-visible after sticky fail-closed");
        }

        const auto late = store.put("mc-late", bytes("no"));
        require(!late.has_value(), "late put accepted after sticky fail-closed");
        require(late.error().code == glyphastore::ErrorCode::unavailable,
                "late put was not unavailable after sticky fail-closed");
        require(late.error().message.find("fail-closed") != std::string::npos,
                "late put did not hit pair fail-closed reject");
        static_cast<void>(store.close());
        break;
    }
    require(closed, "paired volatile multichunk fail-closed never tripped");
}

void run_paired_volatile_sync_midchunk_fail_closed_resource_exhausted() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Sync volatile put_batch: after the first same-shard item mutates, Site::mutate
    // sticky-closes the pair. The later sibling never enters Store and must be
    // resource_exhausted (rejected), not unavailable (reconcile / INTERNAL_ERROR).
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key_a = "mid-a";
    const std::string key_b = "mid-b";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes("alpha")},
        {.key = key_b, .value = bytes("beta")},
    };

    glyphastore::fault::fail_once(glyphastore::fault::Site::mutate);
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    require(statuses.size() == 2, "put_batch size mismatch");
    require(statuses[0].has_value(), "first mid-chunk put lost success ACK after publication");
    require(!statuses[1].has_value(), "later mid-chunk sibling must not success-ACK after sticky");
    require(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted,
            "mid-chunk never-Store-entered sibling must be resource_exhausted");
    require(statuses[1].error().message.find("fail-closed") != std::string::npos,
            "mid-chunk sibling did not hit fail-closed reject");
    require(!runtime->healthy(), "Site::mutate sticky did not fail-close the pair");

    const auto got_a = store.get(key_a);
    require(got_a.has_value(), "Store::get missed published first mid-chunk key");
    require(std::string_view(reinterpret_cast<const char*>(got_a->bytes.data()), got_a->bytes.size()) ==
                "alpha",
            "published mid-chunk value mismatch");
    const auto got_b = store.get(key_b);
    require(!got_b.has_value(), "never-Store-entered mid-chunk sibling became GET-visible");

    const auto late = store.put("mid-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_volatile_sync_midchunk_catch_preserves_resource_exhausted() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Mid-chunk sticky stamps the never-entered sibling resource_exhausted; a later
    // Site::publish catch must not upgrade that to unavailable (false indeterminate).
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key_a = "mid-catch-a";
    const std::string key_b = "mid-catch-b";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key_a, .value = bytes("alpha")},
        {.key = key_b, .value = bytes("beta")},
    };

    glyphastore::fault::fail_once(glyphastore::fault::Site::mutate);
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    require(statuses.size() == 2, "put_batch size mismatch");
    require(statuses[0].has_value(),
            "first mid-chunk put lost success ACK after publish-then-catch");
    require(!statuses[1].has_value(), "later mid-chunk sibling must not success-ACK after sticky");
    require(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted,
            "catch must not upgrade never-Store-entered sibling to unavailable");
    require(statuses[1].error().message.find("fail-closed") != std::string::npos,
            "mid-chunk sibling did not keep fail-closed reject through catch");
    require(!runtime->healthy(), "mutate+publish sticky did not fail-close the pair");

    const auto got_a = store.get(key_a);
    require(got_a.has_value(), "Store::get missed published first mid-chunk key after catch");
    require(std::string_view(reinterpret_cast<const char*>(got_a->bytes.data()), got_a->bytes.size()) ==
                "alpha",
            "published mid-chunk value mismatch after catch");
    const auto got_b = store.get(key_b);
    require(!got_b.has_value(), "never-Store-entered mid-chunk sibling became GET-visible");

    const auto late = store.put("mid-catch-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_sync_durable_group_catch_preserves_resource_exhausted() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Sync durable_group put_batch: same-key splits into sub-batches. First Index
    // publish + Site::index_account sticky-closes; second is never Store-entered
    // (resource_exhausted). Site::publish catch must not upgrade that sibling to
    // unavailable.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-grp-catch-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 1,
                          .max_bytes = 65'536,
                          .max_wait_ms = 60'000,
                          .min_records = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "grp-catch";
    const std::vector<glyphastore::Store::PutItem> items{
        {.key = key, .value = bytes("first")},
        {.key = key, .value = bytes("second")},
    };

    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto statuses = store.put_batch(items);
    glyphastore::fault::reset();
    require(statuses.size() == 2, "put_batch size mismatch");
    require(statuses[0].has_value(),
            "first durable-group put lost success ACK after index_account+publish catch");
    require(!statuses[1].has_value(), "later same-key sub-batch must not success-ACK after sticky");
    require(statuses[1].error().code == glyphastore::ErrorCode::resource_exhausted,
            "durable-group catch must not upgrade never-Store-entered sibling to unavailable");
    require(statuses[1].error().message.find("fail-closed") != std::string::npos,
            "later sibling did not keep fail-closed reject through catch");
    require(!runtime->healthy(), "index_account+publish sticky did not fail-close the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed drain-snapshotted first value after catch");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "first",
            "drain-snapshotted value mismatch after catch");

    const auto late = store.put("grp-catch-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_sync_drain_after_capture_fail() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Sync durable_sync (single-op Writer path): commit then capture fail must
    // drain-snapshot before sticky close so Store::get keeps RAW (async already did).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-cap-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::fault::fail_once(glyphastore::fault::Site::capture);
    const std::string key = "sync-cap-a";
    const auto put = store.put(key, bytes("alpha"));
    glyphastore::fault::reset();
    require(put.has_value(), "drain after capture fail did not success-ACK published commit");
    require(!runtime->healthy(), "capture failure did not sticky-fail the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed drain-snapshotted key after sync capture fail");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "drain-snapshotted value mismatch");

    const auto late = store.put("sync-cap-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_sync_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Sync durable_sync: after publish_read_generation, Site::publish fault throws
    // before reclaim. Catch must keep success ACK (authority already published).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-pub-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const std::string key = "sync-pub-a";
    const auto put = store.put(key, bytes("alpha"));
    glyphastore::fault::reset();
    require(put.has_value(), "catch after publish inverted RAW with error ACK");
    require(!runtime->healthy(), "publish-path fault did not sticky-fail the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed published key after catch");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "published value mismatch after catch");

    const auto late = store.put("sync-pub-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_sync_erase_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Sync durable_sync erase: post-publish Site::publish fault must keep success ACK
    // and GET miss (tombstone already in the published generation).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-pube-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "sync-pub-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto erased = store.erase(key);
    glyphastore::fault::reset();
    require(erased.has_value(), "erase catch after publish inverted RAW with error ACK");
    require(!runtime->healthy(), "erase publish-path fault did not sticky-fail the pair");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after published erase catch");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("sync-pube-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_sync_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Index insert succeeds; secondary accounting fails (committed+error). Drain must
    // still success-ACK when the published generation shows the put (no inverted RAW).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-idx-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    const std::string key = "sync-idx-a";
    const auto put = store.put(key, bytes("alpha"));
    glyphastore::fault::reset();
    require(put.has_value(), "Index-visible committed+error kept error ACK after drain");
    require(!runtime->healthy(), "Index accounting failure did not sticky-fail the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed Index-visible key after index_account fail");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "Index-visible value mismatch after index_account fail");

    const auto late = store.put("sync-idx-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_sync_erase_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Seed then erase: Index erase succeeds; accounting fails. Drain must success-ACK
    // when published generation shows absence (erase miss).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-sync-idxe-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "sync-idx-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");
    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    const auto erased = store.erase(key);
    glyphastore::fault::reset();
    require(erased.has_value(), "Index-visible erase committed+error kept error ACK after drain");
    require(!runtime->healthy(), "erase Index accounting failure did not sticky-fail the pair");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after Index erase + accounting fail");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("sync-idxe-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_async_durable_sync_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async durable_sync single-op: Index-visible committed+error must success-ACK
    // after drain-snapshot (mirrors sync ACK-after-visibility).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-idx-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key = "async-idx-a";
    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = bytes("alpha"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async put submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async completion timed out");
    require(!runtime->healthy(), "async Index accounting failure did not sticky-fail the pair");
    require(!done->error.has_value(),
            "async Index-visible committed+error kept error ACK after drain");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed async Index-visible key after index_account fail");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "async Index-visible value mismatch");

    const auto late = store.put("async-idx-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_sync_durable_group_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // durable_group flush: Index publish then accounting fail advances durable_through
    // before sticky close so finalize keeps success ACK + drain-snapshot (no RAW lie).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-grp-idx-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 1,
                          .max_bytes = 65'536,
                          .max_wait_ms = 60'000,
                          .min_records = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    const std::string key = "grp-idx-a";
    const auto put = store.put(key, bytes("alpha"));
    glyphastore::fault::reset();
    require(put.has_value(), "durable_group Index-visible flush kept error ACK after durable_through");
    require(!runtime->healthy(), "group Index accounting failure did not sticky-fail the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed group Index-visible key after index_account fail");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "group Index-visible value mismatch");

    const auto late = store.put("grp-idx-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_volatile_sync_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Volatile sync: after publish_read_generation, Site::publish fault throws before
    // reclaim. Catch must keep success ACK (authority already published) — mirrors
    // durable sync single-op; without this, GET-visible + error ACK inverts RAW.
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const std::string key = "vol-pub-a";
    const auto put = store.put(key, bytes("alpha"));
    glyphastore::fault::reset();
    require(put.has_value(), "volatile catch after publish inverted RAW with error ACK");
    require(!runtime->healthy(), "volatile publish-path fault did not sticky-fail the pair");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed published volatile key after catch");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "published volatile value mismatch after catch");

    const auto late = store.put("vol-pub-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_volatile_sync_erase_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Volatile sync erase: post-publish Site::publish fault must keep success ACK + miss.
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "vol-pub-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto erased = store.erase(key);
    glyphastore::fault::reset();
    require(erased.has_value(), "volatile erase catch after publish inverted RAW with error ACK");
    require(!runtime->healthy(), "volatile erase publish-path fault did not sticky-fail the pair");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after published volatile erase catch");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("vol-pube-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_async_volatile_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async volatile: post-publish Site::publish fault must keep success completion
    // (staged indices ACK-after-publish) while sticky-failing the pair.
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key = "async-vol-pub-a";
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = bytes("alpha"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async volatile put submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async volatile completion timed out");
    require(!runtime->healthy(), "async volatile publish-path fault did not sticky-fail the pair");
    require(!done->error.has_value(),
            "async volatile catch after publish inverted RAW with error ACK");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed async published volatile key after catch");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "async published volatile value mismatch");

    const auto late = store.put("async-vol-pub-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_async_durable_sync_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async durable_sync: post-publish Site::publish fault must keep success completion.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-pub-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key = "async-dur-pub-a";
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = bytes("alpha"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async durable put submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async durable completion timed out");
    require(!runtime->healthy(), "async durable publish-path fault did not sticky-fail the pair");
    require(!done->error.has_value(),
            "async durable catch after publish inverted RAW with error ACK");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed async published durable key after catch");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "async published durable value mismatch");

    const auto late = store.put("async-dur-pub-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_async_durable_sync_erase_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async durable_sync erase: post-publish fault must success-ACK + GET miss.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-pube-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_sync Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "async-dur-pub-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::erase,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = {},
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async durable erase submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async durable erase completion timed out");
    require(!runtime->healthy(), "async durable erase publish-path fault did not sticky-fail");
    require(!done->error.has_value(),
            "async durable erase catch after publish inverted RAW with error ACK");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after async published erase catch");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("async-dur-pube-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_async_volatile_erase_ack_after_publish_catch() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async volatile erase: post-publish fault must success-ACK + GET miss.
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
    });
    require(opened.has_value(), "failed to open paired volatile Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "async-vol-pub-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::erase,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = {},
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async volatile erase submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async volatile erase completion timed out");
    require(!runtime->healthy(), "async volatile erase publish-path fault did not sticky-fail");
    require(!done->error.has_value(),
            "async volatile erase catch after publish inverted RAW with error ACK");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after async volatile erase catch");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("async-vol-pube-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
#endif
}

void run_paired_async_durable_group_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async durable_group: Index publish then accounting fail must success-ACK after
    // mutate_durable_batch finalize + drain (distinct from durable_sync single-op).
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-grp-idx-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 1,
                          .max_bytes = 65'536,
                          .max_wait_ms = 60'000,
                          .min_records = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    const std::string key = "async-grp-idx-a";
    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::put,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = bytes("alpha"),
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async group put submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async group completion timed out");
    require(!runtime->healthy(), "async group Index accounting failure did not sticky-fail the pair");
    require(!done->error.has_value(),
            "async group Index-visible commit kept error ACK after drain");

    const auto got = store.get(key);
    require(got.has_value(), "Store::get missed async group Index-visible key");
    require(std::string_view(reinterpret_cast<const char*>(got->bytes.data()), got->bytes.size()) ==
                "alpha",
            "async group Index-visible value mismatch");

    const auto late = store.put("async-grp-idx-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_paired_async_durable_group_erase_ack_after_index_account() {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Async durable_group erase: Index erase then accounting fail → success ACK + miss.
    auto pattern =
        (std::filesystem::temp_directory_path() / "glyphastore-async-grp-idxe-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    require(::mkdtemp(writable.data()) != nullptr, "mkdtemp failed");
    const std::filesystem::path root{writable.data()};
    const auto store_path = root / "store";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .concurrency = glyphastore::StoreConcurrencyMode::paired,
        .paired = {.async_lane_capacity = 8,
                   .async_lane_payload_bytes = 1U * 1024U * 1024U,
                   .reader_epoch_lease = true},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = store_path,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        .durable_group = {.max_records = 1,
                          .max_bytes = 65'536,
                          .max_wait_ms = 60'000,
                          .min_records = 1},
        .maintenance = {.mode = glyphastore::MaintenanceMode::disabled},
    });
    require(opened.has_value(), "failed to open paired durable_group Store");
    auto& store = **opened;
    auto* runtime = glyphastore::detail::StoreAccess::shard_pair_runtime(store);
    require(runtime != nullptr, "missing paired runtime");

    const std::string key = "async-grp-idx-erase";
    require(store.put(key, bytes("seed")).has_value(), "seed put failed");

    glyphastore::server::BoundedSpscQueue<glyphastore::server::MutationCompletion> completions{4};
    auto wakeup = glyphastore::server::Wakeup::create();
    require(wakeup.has_value(), "Wakeup::create failed");
    auto executor = glyphastore::server::PairWriterPool::create(store, 1, 8, 1U * 1024U * 1024U,
                                                                std::chrono::milliseconds{0});
    require(executor.has_value(), "PairWriterPool::create failed");
    require((*executor)->start().has_value(), "PairWriterPool::start failed");

    glyphastore::fault::fail_once(glyphastore::fault::Site::index_account);
    require((*executor)
                ->try_submit({
                    .connection = {.slot = 1, .generation = 1},
                    .request_id = 1,
                    .worker_index = 0,
                    .kind = glyphastore::server::MutationKind::erase,
                    .key = bytes(key),
                    .key_hash = glyphastore::hash_key(key),
                    .value = {},
                    .completions = &completions,
                    .wakeup = &*wakeup,
                })
                .has_value(),
            "async group erase submit failed");

    std::optional<glyphastore::server::MutationCompletion> done;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!done && std::chrono::steady_clock::now() < deadline) {
        if (auto completion = completions.try_pop()) {
            done = std::move(*completion);
            require((*executor)->release_payload(0, done->payload_slot), "release_payload failed");
        } else {
            static_cast<void>((*executor)->adopt_read_generation(0));
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    glyphastore::fault::reset();
    require(done.has_value(), "async group erase completion timed out");
    require(!runtime->healthy(), "async group erase Index accounting did not sticky-fail the pair");
    require(!done->error.has_value(),
            "async group Index-visible erase kept error ACK after drain");

    const auto got = store.get(key);
    require(!got.has_value(), "Store::get still saw key after async group Index erase");
    require(got.error().code == glyphastore::ErrorCode::not_found,
            "post-erase get was not not_found");

    const auto late = store.put("async-grp-idxe-late", bytes("no"));
    require(!late.has_value(), "late put accepted after sticky fail-closed");
    require(late.error().code == glyphastore::ErrorCode::unavailable,
            "late put was not unavailable after sticky fail-closed");
    static_cast<void>(store.close());
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
#endif
}

void run_all_tests() {
    const glyphastore::DurableRuntimeOptions synchronous{};
    const glyphastore::DurableRuntimeOptions strict_group{
        .commit_sync = glyphastore::SegmentCommitSync::immediate,
        .sync_interval_ms = 60'000,
        .batch =
            glyphastore::DurableGroupConfig{.max_records = 1, .max_bytes = 65'536, .max_wait_ms = 60'000},
        .strict_ack = true,
    };

    run_exhaustive_allocation_failures(
        {.name = "synchronous new put", .kind = MutationKind::put_new, .options = synchronous});
    run_exhaustive_allocation_failures({.name = "synchronous update",
                                        .kind = MutationKind::put_update,
                                        .seed = true,
                                        .options = synchronous});
    run_exhaustive_allocation_failures(
        {.name = "synchronous erase", .kind = MutationKind::erase, .seed = true, .options = synchronous});
    run_exhaustive_allocation_failures(
        {.name = "strict group put", .kind = MutationKind::put_new, .options = strict_group});
    run_exhaustive_allocation_failures({.name = "Segment rotation put",
                                        .kind = MutationKind::put_new,
                                        .force_rotation = true,
                                        .options = synchronous});
    run_exhaustive_read_failures();
    run_no_post_write_allocation(synchronous);
    run_no_post_write_allocation(strict_group);
    run_background_allocation_failure_waiters();
    run_exhaustive_compaction_allocation_failures();
    run_volatile_rotation_allocation_failures();
    run_volatile_vacuum_publication_allocation_failures();
    run_index_tombstone_rebuild_allocation_failures();
    run_paired_volatile_multichunk_fail_closed();
    run_paired_volatile_sync_midchunk_fail_closed_resource_exhausted();
    run_paired_volatile_sync_midchunk_catch_preserves_resource_exhausted();
    run_paired_sync_durable_group_catch_preserves_resource_exhausted();
    run_paired_async_durable_coalesced_fail_closed();
    run_paired_async_durable_sibling_publish_after_capture_fail();
    run_paired_durable_batch_stops_after_indeterminate_ttl();
    run_paired_sync_durable_sync_drain_after_capture_fail();
    run_paired_sync_durable_sync_ack_after_publish_catch();
    run_paired_sync_durable_sync_erase_ack_after_publish_catch();
    run_paired_sync_durable_sync_ack_after_index_account();
    run_paired_sync_durable_sync_erase_ack_after_index_account();
    run_paired_async_durable_sync_ack_after_index_account();
    run_paired_sync_durable_group_ack_after_index_account();
    run_paired_volatile_sync_ack_after_publish_catch();
    run_paired_volatile_sync_erase_ack_after_publish_catch();
    run_paired_async_volatile_ack_after_publish_catch();
    run_paired_async_durable_sync_ack_after_publish_catch();
    run_paired_async_durable_sync_erase_ack_after_publish_catch();
    run_paired_async_volatile_erase_ack_after_publish_catch();
    run_paired_async_durable_group_ack_after_index_account();
    run_paired_async_durable_group_erase_ack_after_index_account();
}

} // namespace

int main() {
    try {
        run_all_tests();
        std::cout << "allocation fault injection passed\n";
        return 0;
    } catch (const std::exception& error) {
        allocation_fault::forbid_all.store(false, std::memory_order_release);
        allocation_fault::disarm_process();
        static_cast<void>(allocation_fault::disarm());
        std::cerr << "allocation fault injection failed: " << error.what() << '\n';
        return 1;
    }
}
