#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/runtime_catalog.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/global_manager.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/store/store.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <new>
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

void arm(const std::size_t fail_at) noexcept {
    thread_state = {.fail_at = fail_at, .observed = 0, .armed = true, .fired = false};
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
            return true;
        case glyphastore::FilesystemOperation::sync_parent_directory:
        case glyphastore::FilesystemOperation::sync_manifest:
        case glyphastore::FilesystemOperation::sync_directory:
        case glyphastore::FilesystemOperation::sync_segment_file:
        case glyphastore::FilesystemOperation::sync_record:
        case glyphastore::FilesystemOperation::sync_commit_slot:
        case glyphastore::FilesystemOperation::sync_bootstrap:
        case glyphastore::FilesystemOperation::sync_compaction_intent:
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
            require(blocked.outcome == glyphastore::DurableMutationOutcome::indeterminate,
                    "failed-closed runtime accepted another mutation");
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
}

} // namespace

int main() {
    try {
        run_all_tests();
        std::cout << "allocation fault injection passed\n";
        return 0;
    } catch (const std::exception& error) {
        allocation_fault::forbid_all.store(false, std::memory_order_release);
        static_cast<void>(allocation_fault::disarm());
        std::cerr << "allocation fault injection failed: " << error.what() << '\n';
        return 1;
    }
}
