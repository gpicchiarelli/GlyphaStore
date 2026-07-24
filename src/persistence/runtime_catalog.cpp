#include "glyphastore/persistence/runtime_catalog.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "persistence/adaptive_batch_sizer.hpp"
#include "persistence/hot_record_table.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>

namespace glyphastore {
namespace {

#if defined(NDEBUG) && !defined(GLYPHASTORE_GET_PATH_TIMING)
inline constexpr bool kGetPathTimingEnabled = false;
#else
inline constexpr bool kGetPathTimingEnabled = true;
#endif

using detail::HotRecordEntry;
using detail::HotRecordSnapshot;
using detail::HotRecordTable;

[[nodiscard]] auto as_string_view(const std::span<const std::byte> bytes) noexcept -> std::string_view {
    if (bytes.empty()) {
        return {};
    }
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

[[nodiscard]] auto steady_duration_ns(const std::chrono::steady_clock::time_point start,
                                      const std::chrono::steady_clock::time_point end) noexcept
    -> std::uint64_t {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return count <= 0 ? 0U : static_cast<std::uint64_t>(count);
}

[[nodiscard]] auto steady_elapsed_ns(const std::chrono::steady_clock::time_point start) noexcept
    -> std::uint64_t {
    return steady_duration_ns(start, std::chrono::steady_clock::now());
}

[[nodiscard]] auto timing_now() noexcept -> std::optional<std::chrono::steady_clock::time_point> {
    if constexpr (!kGetPathTimingEnabled) {
        return std::nullopt;
    }
    return std::chrono::steady_clock::now();
}

[[nodiscard]] auto timing_elapsed_ns(const std::optional<std::chrono::steady_clock::time_point> start) noexcept
    -> std::uint64_t {
    if constexpr (!kGetPathTimingEnabled) {
        return 0;
    }
    if (!start) {
        return 0;
    }
    return steady_elapsed_ns(*start);
}

[[nodiscard]] auto timing_duration_ns(const std::optional<std::chrono::steady_clock::time_point> start,
                                      const std::optional<std::chrono::steady_clock::time_point> end) noexcept
    -> std::uint64_t {
    if constexpr (!kGetPathTimingEnabled) {
        return 0;
    }
    if (!start || !end) {
        return 0;
    }
    return steady_duration_ns(*start, *end);
}

struct ReadContext {
    std::span<const std::byte> expected_key;
    std::uint64_t expected_hash{};
    std::uint64_t now_ns{};
    OwnedValue value;
    std::uint64_t crc_value_copy_ns{};
};

auto copy_verified_value(void* opaque, const RecordView& record) -> Status {
    auto& context = *static_cast<ReadContext*>(opaque);
    const auto copy_started = timing_now();
    if (record.opcode != Opcode::put) {
        context.crc_value_copy_ns = timing_elapsed_ns(copy_started);
        return fail(ErrorCode::corrupted_data, "durable Index references a non-value Record");
    }
    if (record.key_hash != context.expected_hash || !std::ranges::equal(record.key, context.expected_key)) {
        context.crc_value_copy_ns = timing_elapsed_ns(copy_started);
        return fail(ErrorCode::corrupted_data, "durable Index key does not match its referenced Record");
    }
    if (record.expired(context.now_ns)) {
        context.crc_value_copy_ns = timing_elapsed_ns(copy_started);
        return fail(ErrorCode::not_found, "key has expired");
    }
    context.value = {
        .bytes = std::vector<std::byte>{record.value.begin(), record.value.end()},
        .sequence = record.sequence.value,
        .expire_at_ns = record.expire_at_ns,
    };
    context.crc_value_copy_ns = timing_elapsed_ns(copy_started);
    return {};
}

auto mutation_failure(const DurableMutationOutcome outcome, Error error) -> DurableMutationResult {
    return {.outcome = outcome, .sequence = std::nullopt, .error = std::move(error)};
}

void atomic_saturating_add(std::atomic_uint64_t& destination, const std::uint64_t value) noexcept {
    auto current = destination.load(std::memory_order_relaxed);
    while (true) {
        const auto next = value > std::numeric_limits<std::uint64_t>::max() - current
                              ? std::numeric_limits<std::uint64_t>::max()
                              : current + value;
        if (destination.compare_exchange_weak(current, next, std::memory_order_relaxed)) {
            return;
        }
    }
}

template <typename T> void atomic_observe_max(std::atomic<T>& destination, const T value) noexcept {
    auto current = destination.load(std::memory_order_relaxed);
    while (current < value && !destination.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
    }
}

[[nodiscard]] auto begin_atomic_stats_publication(std::atomic_uint64_t& version) noexcept
    -> std::uint64_t {
    auto current = version.load(std::memory_order_relaxed);
    while (true) {
        if ((current & 1U) != 0) {
            current = version.load(std::memory_order_acquire);
            continue;
        }
        if (version.compare_exchange_weak(current, current + 1U, std::memory_order_acquire,
                                          std::memory_order_relaxed)) {
            return current;
        }
    }
}

void end_atomic_stats_publication(std::atomic_uint64_t& version, const std::uint64_t previous) noexcept {
    version.store(previous + 2U, std::memory_order_release);
}

template <typename Callback> class ScopeExit final {
  public:
    explicit ScopeExit(Callback callback) noexcept : callback_(std::move(callback)) {}
    ~ScopeExit() noexcept {
        callback_();
    }
    ScopeExit(const ScopeExit&) = delete;
    auto operator=(const ScopeExit&) -> ScopeExit& = delete;

  private:
    Callback callback_;
};

template <typename Callback> ScopeExit(Callback) -> ScopeExit<Callback>;

// Staged hot-cache admission held outside the resident flat table until Index
// publication. RAII releases staged byte/entry charges if publication aborts.
class PreparedHotRecord final {
  public:
    PreparedHotRecord() = default;
    PreparedHotRecord(std::string key, const std::uint64_t key_hash, HotRecordEntry entry,
                      std::uint64_t* staged_bytes, std::size_t* staged_entries,
                      const std::uint64_t staged_charge) noexcept
        : key_(std::move(key)), key_hash_(key_hash), entry_(std::move(entry)), staged_bytes_(staged_bytes),
          staged_entries_(staged_entries), staged_charge_(staged_charge), active_(true) {}
    ~PreparedHotRecord() {
        release_stage();
    }
    PreparedHotRecord(PreparedHotRecord&& other) noexcept
        : key_(std::move(other.key_)), key_hash_(std::exchange(other.key_hash_, 0)),
          entry_(std::move(other.entry_)), staged_bytes_(std::exchange(other.staged_bytes_, nullptr)),
          staged_entries_(std::exchange(other.staged_entries_, nullptr)),
          staged_charge_(std::exchange(other.staged_charge_, 0)),
          active_(std::exchange(other.active_, false)) {}
    auto operator=(PreparedHotRecord&& other) noexcept -> PreparedHotRecord& {
        if (this != &other) {
            release_stage();
            key_ = std::move(other.key_);
            key_hash_ = std::exchange(other.key_hash_, 0);
            entry_ = std::move(other.entry_);
            staged_bytes_ = std::exchange(other.staged_bytes_, nullptr);
            staged_entries_ = std::exchange(other.staged_entries_, nullptr);
            staged_charge_ = std::exchange(other.staged_charge_, 0);
            active_ = std::exchange(other.active_, false);
        }
        return *this;
    }
    PreparedHotRecord(const PreparedHotRecord&) = delete;
    auto operator=(const PreparedHotRecord&) -> PreparedHotRecord& = delete;

    [[nodiscard]] auto empty() const noexcept -> bool {
        return !active_;
    }
    [[nodiscard]] auto key() const -> const std::string& {
        return key_;
    }
    [[nodiscard]] auto key_hash() const noexcept -> std::uint64_t {
        return key_hash_;
    }
    [[nodiscard]] auto mapped() -> HotRecordEntry& {
        return entry_;
    }
    [[nodiscard]] auto take_entry() -> HotRecordEntry {
        release_stage();
        active_ = false;
        return std::move(entry_);
    }

  private:
    void release_stage() noexcept {
        if (staged_bytes_ == nullptr || staged_entries_ == nullptr) {
            return;
        }
        *staged_bytes_ -= staged_charge_;
        --*staged_entries_;
        staged_bytes_ = nullptr;
        staged_entries_ = nullptr;
        staged_charge_ = 0;
    }

    std::string key_;
    std::uint64_t key_hash_{};
    HotRecordEntry entry_{};
    std::uint64_t* staged_bytes_{};
    std::size_t* staged_entries_{};
    std::uint64_t staged_charge_{};
    bool active_{};
};

[[nodiscard]] auto hot_cache_table_bytes(const std::size_t capacity) noexcept -> std::uint64_t {
    const auto per_slot = detail::hot_record_slot_bytes();
    if (capacity > std::numeric_limits<std::uint64_t>::max() / per_slot) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return static_cast<std::uint64_t>(capacity) * per_slot;
}

[[nodiscard]] auto hot_record_accounted_bytes(const std::size_t key_bytes, const std::size_t value_bytes)
    -> Result<std::uint64_t> {
    return detail::hot_record_accounted_bytes(key_bytes, value_bytes);
}

[[nodiscard]] auto hot_cache_worker_budget(const std::size_t worker_index, const std::size_t worker_count,
                                           const DurableResourceLimits& limits) noexcept -> std::uint64_t {
    if (worker_count == 0) {
        return 0;
    }
    const auto count = static_cast<std::uint64_t>(worker_count);
    const auto base = limits.max_hot_cache_bytes / count;
    const auto remainder = limits.max_hot_cache_bytes % count;
    const auto partition = base + (static_cast<std::uint64_t>(worker_index) < remainder ? 1U : 0U);
    return std::min(partition, limits.max_hot_cache_bytes_per_worker);
}

[[nodiscard]] auto hot_record_matches(const HotRecordEntry& entry, const RecordRef& reference) noexcept
    -> bool {
    return entry.reference == reference;
}

[[nodiscard]] auto owned_value_from_hot(const HotRecordSnapshot& entry) -> OwnedValue {
    std::vector<std::byte> value(entry.value_size);
    if (entry.value_size != 0) {
        const auto bytes = entry.value_span();
        std::copy_n(bytes.begin(), entry.value_size, value.begin());
    }
    return {.bytes = std::move(value), .sequence = entry.sequence.value, .expire_at_ns = entry.expire_at_ns};
}

auto rotation_manifest(const Manifest& current, const ManifestSegmentEntry& old_active,
                       const DurableResourceLimits& limits) -> Result<Manifest> {
    if (current.manifest_generation == std::numeric_limits<std::uint64_t>::max() ||
        current.next_segment_id.value == std::numeric_limits<std::uint64_t>::max() ||
        current.segments.size() == kMaximumManifestSegmentCount) {
        return fail(ErrorCode::arithmetic_overflow, "durable rotation catalog space is exhausted");
    }
    if (auto resources = validate_durable_rotation_resources(current.segments.size(), limits); !resources) {
        return unexpected(resources.error());
    }
    Manifest next = current;
    auto found = std::lower_bound(next.segments.begin(), next.segments.end(), old_active.segment_id,
                                  [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                      return entry.segment_id.value < id.value;
                                  });
    if (found == next.segments.end() || found->segment_id != old_active.segment_id ||
        found->role != ManifestSegmentRole::active) {
        return fail(ErrorCode::corrupted_data, "rotation source is not the catalog active Segment");
    }
    found->role = ManifestSegmentRole::sealed;
    next.segments.push_back({.segment_id = current.next_segment_id,
                             .generation = current.next_segment_generation,
                             .owner_worker = old_active.owner_worker,
                             .role = ManifestSegmentRole::active});
    ++next.manifest_generation;
    ++next.next_segment_id.value;
    return next;
}

auto complete_interrupted_rotation(DataDirectory& directory, const DurableResourceLimits& limits) -> Status {
    auto manifest = directory.read_manifest(limits.max_manifest_bytes);
    if (!manifest) {
        return unexpected(manifest.error());
    }
    if (auto resources = validate_durable_manifest_resources(*manifest, limits); !resources) {
        return resources;
    }
    auto audit = audit_data_directory(directory, *manifest);
    if (!audit) {
        return unexpected(audit.error());
    }

    const NamespaceIssue* prepared_orphan{};
    for (const auto& issue : audit->issues) {
        if (issue.kind == NamespaceIssueKind::stale_manifest_temporary ||
            issue.kind == NamespaceIssueKind::stale_segment_temporary) {
            continue;
        }
        const bool exact_prepared = issue.kind == NamespaceIssueKind::unlisted_segment &&
                                    issue.segment_id == manifest->next_segment_id &&
                                    issue.generation == manifest->next_segment_generation;
        if (!exact_prepared || prepared_orphan) {
            return validate_namespace_for_recovery(*audit);
        }
        prepared_orphan = &issue;
    }

    const ManifestSegmentEntry* sealed_active{};
    for (const auto& entry : manifest->segments) {
        if (entry.role != ManifestSegmentRole::active) {
            continue;
        }
        const SegmentHeaderIdentity identity{.store_id = manifest->store_id,
                                             .segment_id = entry.segment_id,
                                             .generation = entry.generation,
                                             .owner_worker = entry.owner_worker};
        auto file = DurableSegmentFile::open(directory, identity, SegmentFileOpenMode::read_only);
        if (!file) {
            return unexpected(file.error());
        }
        if (file->selected_commit().commit.state == PersistedSegmentState::sealed) {
            if (sealed_active) {
                return fail(ErrorCode::corrupted_data,
                            "multiple interrupted Worker rotations require operator recovery");
            }
            sealed_active = &entry;
        }
    }
    if (!sealed_active) {
        if (prepared_orphan) {
            return fail(ErrorCode::corrupted_data,
                        "unlisted next Segment has no sealed-active rotation intent");
        }
        return {};
    }

    const SegmentHeaderIdentity replacement_identity{
        .store_id = manifest->store_id,
        .segment_id = manifest->next_segment_id,
        .generation = manifest->next_segment_generation,
        .owner_worker = sealed_active->owner_worker,
    };
    auto next = rotation_manifest(*manifest, *sealed_active, limits);
    if (!next) {
        return unexpected(next.error());
    }
    const auto next_manifest_bytes = durable_manifest_bytes(next->segments.size());
    if (!next_manifest_bytes) {
        return unexpected(next_manifest_bytes.error());
    }
    auto additional = *next_manifest_bytes;
    if (!prepared_orphan) {
        if (additional > std::numeric_limits<std::uint64_t>::max() - kSegmentSizeBytes) {
            return fail(ErrorCode::arithmetic_overflow,
                        "interrupted rotation free-space requirement overflow");
        }
        additional += kSegmentSizeBytes;
    }
    if (auto available = require_durable_available_space(directory, additional, limits); !available) {
        return available;
    }
    std::optional<DurableSegmentFile> replacement;
    if (prepared_orphan) {
        auto opened =
            DurableSegmentFile::open(directory, replacement_identity, SegmentFileOpenMode::read_write);
        if (!opened) {
            return unexpected(opened.error());
        }
        const auto& commit = opened->selected_commit().commit;
        if (commit.commit_generation != 1 || commit.record_count != 0 ||
            commit.committed_end != kSegmentHeaderReservedBytes ||
            commit.state != PersistedSegmentState::active) {
            return fail(ErrorCode::corrupted_data,
                        "prepared rotation replacement is not a pristine active Segment");
        }
        replacement.emplace(std::move(*opened));
    } else {
        auto created = DurableSegmentFile::create(directory, replacement_identity);
        if (!created.durable()) {
            return unexpected(
                created.error.value_or(Error{ErrorCode::io_error, "rotation replacement creation failed"}));
        }
        replacement.emplace(std::move(*created.file));
    }

    const auto published = directory.publish_manifest(*next, limits.max_manifest_bytes);
    if (!published.durable()) {
        return unexpected(
            published.error.value_or(Error{ErrorCode::io_error, "rotation manifest publication failed"}));
    }
    return {};
}

auto resolve_interrupted_compaction(DataDirectory& directory, const std::uint64_t recovery_now_ns,
                                    const DurableResourceLimits& limits)
    -> Result<std::optional<DurableRecoveryState>> {
    auto intent = directory.read_compaction_intent(limits.max_manifest_bytes);
    if (!intent) {
        if (intent.error().code == ErrorCode::not_found) {
            return std::optional<DurableRecoveryState>{};
        }
        return unexpected(intent.error());
    }
    auto authority = directory.read_manifest(limits.max_manifest_bytes);
    if (!authority) {
        return unexpected(authority.error());
    }
    const bool old_authority = *authority == intent->old_manifest;
    const bool next_authority = *authority == intent->next_manifest;
    if (!old_authority && !next_authority) {
        return fail(ErrorCode::corrupted_data, "compaction intent matches neither authoritative manifest");
    }

    auto recovered = recover_durable_state(directory, recovery_now_ns, limits, &*intent);
    if (!recovered) {
        return unexpected(recovered.error());
    }
    const auto replacement_count = validate_durable_compaction_transition(
        intent->old_manifest, intent->next_manifest, intent->worker_id);
    if (!replacement_count) {
        return unexpected(replacement_count.error());
    }
    std::vector<ManifestSegmentEntry> sources;
    std::vector<ManifestSegmentEntry> replacements;
    for (const auto& entry : intent->old_manifest.segments) {
        if (entry.owner_worker != intent->worker_id || entry.role != ManifestSegmentRole::sealed) {
            continue;
        }
        sources.push_back(entry);
        if (replacements.size() < *replacement_count) {
            auto replacement = entry;
            ++replacement.generation.value;
            replacements.push_back(replacement);
        }
    }
    const auto& obsolete = old_authority ? replacements : sources;
    const auto& obsolete_manifest = old_authority ? intent->next_manifest : intent->old_manifest;
    for (const auto& entry : obsolete) {
        const SegmentHeaderIdentity identity{.store_id = obsolete_manifest.store_id,
                                             .segment_id = entry.segment_id,
                                             .generation = entry.generation,
                                             .owner_worker = entry.owner_worker};
        auto file = DurableSegmentFile::open(directory, identity, SegmentFileOpenMode::read_only);
        if (!file && file.error().code != ErrorCode::not_found) {
            return unexpected(file.error());
        }
    }
    const auto retired = directory.retire_compaction_segments(obsolete_manifest.store_id, obsolete);
    if (!retired.durable()) {
        return unexpected(
            retired.error.value_or(Error{ErrorCode::io_error, "compaction Segment retirement failed"}));
    }
    const auto removed = directory.remove_compaction_intent();
    if (!removed.durable()) {
        return unexpected(
            removed.error.value_or(Error{ErrorCode::io_error, "compaction intent removal failed"}));
    }
    auto final_audit = audit_data_directory(directory, recovered->manifest);
    if (!final_audit) {
        return unexpected(final_audit.error());
    }
    if (auto safe = validate_namespace_for_recovery(*final_audit); !safe) {
        return unexpected(safe.error());
    }
    recovered->namespace_audit = std::move(*final_audit);
    return std::optional<DurableRecoveryState>{std::move(*recovered)};
}

auto rollback_prepared_compaction(DataDirectory& directory, const Manifest& old_manifest,
                                  const std::span<const ManifestSegmentEntry> replacements,
                                  const DurableResourceLimits& limits) -> Result<NamespaceAuditReport> {
    auto authority = directory.read_manifest(limits.max_manifest_bytes);
    if (!authority) {
        return unexpected(authority.error());
    }
    if (*authority != old_manifest) {
        return fail(ErrorCode::sequence_conflict,
                    "cannot roll back compaction after its old manifest lost authority");
    }
    const auto retired = directory.retire_compaction_segments(old_manifest.store_id, replacements);
    if (!retired.durable()) {
        return unexpected(
            retired.error.value_or(Error{ErrorCode::io_error, "compaction replacement rollback failed"}));
    }
    const auto removed = directory.remove_compaction_intent();
    if (!removed.durable()) {
        return unexpected(
            removed.error.value_or(Error{ErrorCode::io_error, "compaction intent rollback failed"}));
    }
    auto audit = audit_data_directory(directory, old_manifest);
    if (!audit) {
        return unexpected(audit.error());
    }
    if (auto safe = validate_namespace_for_recovery(*audit); !safe) {
        return unexpected(safe.error());
    }
    return audit;
}

} // namespace

struct DurableRuntimeCatalog::PendingGroupMutation {
    std::string key;
    PreparedHotRecord hot_record;
    RecordRef reference{};
    Opcode opcode{Opcode::put};
    std::uint64_t key_hash{};
    std::uint64_t expire_at_ns{};
    bool completed{};
};

// One immutable handle for one exact on-disk Segment generation. A GET may
// carry a RecordRef outside the Worker mutex only as part of a shared ownership
// of this object. POSIX unlink then retires the catalog name without
// invalidating an already linearized reader's descriptor.
struct DurableRuntimeCatalog::RuntimeSegmentGeneration {
    SegmentHeaderIdentity identity;
    SelectedSegmentCommit selected;
    DurableSegmentFile file;
};

struct DeferredTtlReclaim {
    std::string key;
    std::uint64_t key_hash{};
    RecordRef reference{};
};

struct DurableRuntimeCatalog::RuntimeWorker {
    struct alignas(64) BatchMetrics final {
        std::atomic_size_t pending_records{};
        std::atomic_uint64_t pending_bytes{};
        std::atomic_size_t current_record_target{1};
        std::atomic_uint64_t flush_attempts{};
        std::atomic_uint64_t committed_batches{};
        std::atomic_uint64_t failed_batches{};
        std::atomic_uint64_t committed_records{};
        std::atomic_uint64_t committed_bytes{};
        std::atomic_size_t maximum_batch_records{};
        std::atomic_uint64_t maximum_batch_bytes{};
        std::atomic_uint64_t total_commit_duration_ns{};
        std::atomic_uint64_t maximum_commit_duration_ns{};
        std::atomic_uint64_t record_limit_closes{};
        std::atomic_uint64_t byte_limit_closes{};
        std::atomic_uint64_t adaptive_target_closes{};
        std::atomic_uint64_t deadline_closes{};
    };

    // GET path timing + bookkeeping. Timing atomics are only updated when
    // kGetPathTimingEnabled (Debug / explicit define); counters always use
    // relaxed atomics published outside or at the edge of the critical section.
    struct alignas(64) GetPathMetrics final {
        std::atomic_uint64_t prepare_calls{};
        std::atomic_uint64_t complete_calls{};
        std::atomic_uint64_t mutex_wait_ns{};
        std::atomic_uint64_t prepare_hold_ns{};
        std::atomic_uint64_t complete_revalidate_hold_ns{};
        std::atomic_uint64_t index_lookup_ns{};
        std::atomic_uint64_t hot_cache_lookup_ns{};
        std::atomic_uint64_t generation_pin_lookup_ns{};
        std::atomic_uint64_t cold_read_ns{};
        std::atomic_uint64_t crc_value_copy_ns{};
        std::atomic_uint64_t relinearization_retries{};
        std::atomic_uint64_t hot_hits{};
        std::atomic_uint64_t hot_misses{};
        std::atomic_uint64_t hot_stale_hits{};
        std::atomic_uint64_t hot_evictions{};
        std::atomic_uint64_t admission_bypasses{};
        std::atomic_uint64_t size_rejected{};
        std::atomic_uint64_t expired_ttl_gets{};
    };

    explicit RuntimeWorker(RecoveredWorkerState recovered)
        : worker_id(recovered.worker_id), index(std::move(recovered.index)),
          next_sequence(recovered.next_sequence), active_segment(recovered.active_segment),
          active_live_record_bytes(recovered.active_live_record_bytes),
          sealed_live_record_bytes(recovered.sealed_live_record_bytes) {}

    [[nodiscard]] auto update_live_record_bytes(const std::optional<RecordRef>& previous,
                                                const std::optional<RecordRef>& current) -> Status {
        auto active = active_live_record_bytes.load(std::memory_order_relaxed);
        auto sealed = sealed_live_record_bytes.load(std::memory_order_relaxed);
        if (previous) {
            auto& bytes = previous->segment_id == active_segment ? active : sealed;
            if (bytes < previous->size.value) {
                return fail(ErrorCode::corrupted_data, "durable Worker live Record byte counter underflow");
            }
            bytes -= previous->size.value;
        }
        if (current) {
            auto& bytes = current->segment_id == active_segment ? active : sealed;
            if (current->size.value > std::numeric_limits<std::uint64_t>::max() - bytes) {
                return fail(ErrorCode::arithmetic_overflow,
                            "durable Worker live Record byte counter overflow");
            }
            bytes += current->size.value;
        }
        active_live_record_bytes.store(active, std::memory_order_release);
        sealed_live_record_bytes.store(sealed, std::memory_order_release);
        return {};
    }

    WorkerId worker_id;
    Index index;
    SequenceNumber next_sequence;
    SegmentId active_segment;
    std::atomic_uint64_t active_live_record_bytes{};
    std::atomic_uint64_t sealed_live_record_bytes{};
    std::mutex mutex;
    std::optional<DurableSegmentFile> cached_file;
    bool cached_writable{};
    std::vector<std::byte> encode_scratch;
    HotRecordTable hot_records;
    std::uint64_t hot_record_resident_bytes{};
    std::uint64_t hot_record_staged_bytes{};
    std::size_t hot_record_staged_entries{};
    std::vector<DeferredTtlReclaim> deferred_ttl_reclaims;
    std::uint64_t deferred_ttl_enqueued{};
    std::uint64_t deferred_ttl_applied{};
    std::uint64_t deferred_ttl_skipped{};
    std::vector<PendingGroupMutation*> pending_group_mutations;
    std::size_t pending_group_insertions{};
    std::size_t pending_group_heap_key_bytes{};
    std::chrono::steady_clock::time_point batch_started{};
    std::atomic_size_t active_group_mutations{};
    detail::AdaptiveBatchSizer batch_sizer;
    BatchMetrics batch_metrics;
    GetPathMetrics get_path_metrics;
    bool batch_closing{};
    std::condition_variable batch_closed;
    bool compaction_commit_active{};
    std::condition_variable compaction_commit_finished;

    [[nodiscard]] auto hot_cache_total_bytes() const noexcept -> std::uint64_t;
    void erase_hot_record(std::string_view key, std::uint64_t key_hash) noexcept;
    void erase_hot_record(const HashedKey& key) noexcept { erase_hot_record(key.key, key.hash); }
    // Drain up to `limit` deferred TTL reclamations. Verifies exact RecordRef
    // before erase so a reinsert/update is never deleted. Caller holds mutex.
    [[nodiscard]] auto drain_deferred_ttl(std::size_t limit) -> Status;
    // Queue Index reclaim for an expired GET. May drain a bounded prefix when
    // the backlog is full. Caller holds mutex. Zero backlog limit reclaim sync.
    [[nodiscard]] auto defer_or_reclaim_expired(const HashedKey& key, const RecordRef& reference,
                                                std::size_t backlog_limit) -> Status;
    [[nodiscard]] auto prepare_hot_record(std::size_t worker_index, std::size_t worker_count,
                                          const DurableResourceLimits& limits, std::string_view key,
                                          std::uint64_t key_hash, std::span<const std::byte> value,
                                          std::uint64_t expire_at_ns, std::uint64_t publication_staging_bytes)
        -> Result<PreparedHotRecord>;
    [[nodiscard]] auto publish_hot_record(PreparedHotRecord& prepared, const RecordRef& reference,
                                          SequenceNumber sequence) -> Status;
};

[[nodiscard]] auto DurableRuntimeCatalog::RuntimeWorker::hot_cache_total_bytes() const noexcept
    -> std::uint64_t {
    const auto table_bytes = hot_cache_table_bytes(hot_records.capacity());
    const auto first =
        hot_record_resident_bytes > std::numeric_limits<std::uint64_t>::max() - hot_record_staged_bytes
            ? std::numeric_limits<std::uint64_t>::max()
            : hot_record_resident_bytes + hot_record_staged_bytes;
    return first > std::numeric_limits<std::uint64_t>::max() - table_bytes
               ? std::numeric_limits<std::uint64_t>::max()
               : first + table_bytes;
}

void DurableRuntimeCatalog::RuntimeWorker::erase_hot_record(const std::string_view key,
                                                             const std::uint64_t key_hash) noexcept {
    if (auto* existing = hot_records.find(key, key_hash); existing != nullptr) {
        hot_record_resident_bytes -= existing->accounted_bytes;
        static_cast<void>(hot_records.erase(key, key_hash));
        get_path_metrics.hot_evictions.fetch_add(1U, std::memory_order_relaxed);
    }
}

auto DurableRuntimeCatalog::RuntimeWorker::drain_deferred_ttl(const std::size_t limit) -> Status {
    std::size_t processed = 0;
    while (processed < limit && !deferred_ttl_reclaims.empty()) {
        auto pending = std::move(deferred_ttl_reclaims.front());
        deferred_ttl_reclaims.erase(deferred_ttl_reclaims.begin());
        ++processed;
        const HashedKey key{.key = pending.key, .hash = pending.key_hash};
        const auto current = index.find(key);
        if (!current || *current != pending.reference) {
            ++deferred_ttl_skipped;
            continue;
        }
        const auto erased = index.erase_no_compact(key);
        if (auto counted = update_live_record_bytes(erased.previous, std::nullopt); !counted) {
            return counted;
        }
        erase_hot_record(key);
        ++deferred_ttl_applied;
    }
    return {};
}

auto DurableRuntimeCatalog::RuntimeWorker::defer_or_reclaim_expired(const HashedKey& key,
                                                                   const RecordRef& reference,
                                                                   const std::size_t backlog_limit)
    -> Status {
    // Drop any matching hot row immediately so the expired value cannot be
    // served; Index removal may wait for a bounded Worker drain.
    erase_hot_record(key);
    get_path_metrics.expired_ttl_gets.fetch_add(1U, std::memory_order_relaxed);

    if (backlog_limit == 0) {
        const auto current = index.find(key);
        if (!current || *current != reference) {
            ++deferred_ttl_skipped;
            return {};
        }
        const auto erased = index.erase_no_compact(key);
        if (auto counted = update_live_record_bytes(erased.previous, std::nullopt); !counted) {
            return counted;
        }
        ++deferred_ttl_applied;
        return {};
    }

    if (deferred_ttl_reclaims.size() >= backlog_limit) {
        if (auto drained = drain_deferred_ttl(std::max<std::size_t>(1, backlog_limit / 8U)); !drained) {
            return drained;
        }
    }
    if (deferred_ttl_reclaims.size() >= backlog_limit) {
        // Backlog still full: reclaim this exact reference synchronously.
        const auto current = index.find(key);
        if (!current || *current != reference) {
            ++deferred_ttl_skipped;
            return {};
        }
        const auto erased = index.erase_no_compact(key);
        if (auto counted = update_live_record_bytes(erased.previous, std::nullopt); !counted) {
            return counted;
        }
        ++deferred_ttl_applied;
        return {};
    }

    deferred_ttl_reclaims.push_back(
        DeferredTtlReclaim{.key = std::string{key.key}, .key_hash = key.hash, .reference = reference});
    ++deferred_ttl_enqueued;
    return {};
}

[[nodiscard]] auto DurableRuntimeCatalog::RuntimeWorker::prepare_hot_record(
    const std::size_t worker_index, const std::size_t worker_count, const DurableResourceLimits& limits,
    const std::string_view key, const std::uint64_t key_hash, const std::span<const std::byte> value,
    const std::uint64_t expire_at_ns, const std::uint64_t publication_staging_bytes)
    -> Result<PreparedHotRecord> {
    if (!limits.hot_cache_enabled) {
        get_path_metrics.admission_bypasses.fetch_add(1U, std::memory_order_relaxed);
        return PreparedHotRecord{};
    }
    if (limits.max_hot_cache_value_bytes == 0 ||
        value.size() > static_cast<std::size_t>(limits.max_hot_cache_value_bytes)) {
        get_path_metrics.size_rejected.fetch_add(1U, std::memory_order_relaxed);
        get_path_metrics.admission_bypasses.fetch_add(1U, std::memory_order_relaxed);
        return PreparedHotRecord{};
    }
    auto charge = hot_record_accounted_bytes(key.size(), value.size());
    if (!charge) {
        return unexpected(charge.error());
    }
    if (publication_staging_bytes > std::numeric_limits<std::uint64_t>::max() - *charge) {
        return fail(ErrorCode::arithmetic_overflow, "hot-cache publication staging overflow");
    }
    const auto staged_charge = *charge + publication_staging_bytes;
    const auto budget = hot_cache_worker_budget(worker_index, worker_count, limits);
    if (hot_records.size() > std::numeric_limits<std::size_t>::max() - hot_record_staged_entries) {
        return fail(ErrorCode::arithmetic_overflow, "hot-cache entry accounting overflow");
    }
    const auto projected_entries = hot_records.size() + hot_record_staged_entries;
    const bool entry_exhausted = limits.max_hot_cache_entries_per_worker == 0 ||
                                 projected_entries >= limits.max_hot_cache_entries_per_worker;
    const bool staging_exhausted =
        limits.max_hot_cache_staging_bytes_per_worker == 0 ||
        hot_record_staged_bytes > limits.max_hot_cache_staging_bytes_per_worker ||
        staged_charge > limits.max_hot_cache_staging_bytes_per_worker - hot_record_staged_bytes;
    if (budget == 0 || entry_exhausted || staging_exhausted) {
        get_path_metrics.admission_bypasses.fetch_add(1U, std::memory_order_relaxed);
        return PreparedHotRecord{};
    }

    if (hot_record_staged_entries == std::numeric_limits<std::size_t>::max() ||
        projected_entries == std::numeric_limits<std::size_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "hot-cache staged entry accounting overflow");
    }
    const auto additional = hot_record_staged_entries + 1U;
    const auto plan =
        detail::plan_hot_record_reserve(hot_records.size(), additional, hot_records.capacity());
    if (plan.overflow) {
        return fail(ErrorCode::arithmetic_overflow, "hot Record publication capacity overflow");
    }
    const auto projected_capacity = plan.target == 0 ? hot_records.capacity() : plan.target;
    const auto projected_table = hot_cache_table_bytes(projected_capacity);
    const auto fixed =
        hot_record_resident_bytes > std::numeric_limits<std::uint64_t>::max() - hot_record_staged_bytes
            ? std::numeric_limits<std::uint64_t>::max()
            : hot_record_resident_bytes + hot_record_staged_bytes;
    const auto available = fixed >= budget ? 0 : budget - fixed;
    if (projected_table > available || staged_charge > available - projected_table) {
        get_path_metrics.admission_bypasses.fetch_add(1U, std::memory_order_relaxed);
        return PreparedHotRecord{};
    }
    if (plan.target != 0) {
        if (auto reserved = hot_records.reserve(plan.target / 2U); !reserved) {
            return unexpected(reserved.error());
        }
    }
    const auto after_reserve = hot_cache_total_bytes();
    if (after_reserve >= budget || staged_charge > budget - after_reserve) {
        get_path_metrics.admission_bypasses.fetch_add(1U, std::memory_order_relaxed);
        return PreparedHotRecord{};
    }

    HotRecordEntry staged_entry{.value_size = value.size(),
                                .expire_at_ns = expire_at_ns,
                                .accounted_bytes = *charge};
    if (!value.empty() && value.size() <= HotRecordEntry::kInlineValueBytes) {
        std::copy(value.begin(), value.end(), staged_entry.inline_value);
        staged_entry.value_inline = true;
    } else if (!value.empty()) {
        auto mutable_value = std::make_shared<std::byte[]>(value.size());
        std::copy(value.begin(), value.end(), mutable_value.get());
        staged_entry.heap_value = std::move(mutable_value);
        staged_entry.value_inline = false;
    }
    hot_record_staged_bytes += staged_charge;
    ++hot_record_staged_entries;
    return PreparedHotRecord{std::string{key}, key_hash, std::move(staged_entry), &hot_record_staged_bytes,
                             &hot_record_staged_entries, staged_charge};
}

[[nodiscard]] auto DurableRuntimeCatalog::RuntimeWorker::publish_hot_record(PreparedHotRecord& prepared,
                                                                            const RecordRef& reference,
                                                                            const SequenceNumber sequence)
    -> Status {
    if (prepared.empty()) {
        return {};
    }
    prepared.mapped().reference = reference;
    prepared.mapped().sequence = sequence;
    const auto charge = prepared.mapped().accounted_bytes;
    const auto key_hash = prepared.key_hash();
    auto key = prepared.key();
    if (auto* existing = hot_records.find(key, key_hash); existing != nullptr) {
        hot_record_resident_bytes -= existing->accounted_bytes;
        *existing = prepared.take_entry();
    } else {
        auto entry = prepared.take_entry();
        if (auto inserted = hot_records.insert_or_assign(std::move(key), key_hash, std::move(entry));
            !inserted) {
            return unexpected(inserted.error());
        }
    }
    hot_record_resident_bytes += charge;
    return {};
}

DurableRuntimeCatalog::DurableRuntimeCatalog(DataDirectory directory, DurableRecoveryState recovered,
                                             DurableRuntimeOptions options)
    : directory_(std::move(directory)), manifest_(std::move(recovered.manifest)),
      namespace_audit_(std::move(recovered.namespace_audit)), segments_(std::move(recovered.segments)),
      recovery_stats_(recovered.stats), options_(options) {
    workers_.reserve(recovered.workers.size());
    for (auto& worker : recovered.workers) {
        auto runtime_worker = std::make_unique<RuntimeWorker>(std::move(worker));
        if (options_.batch) {
            runtime_worker->batch_sizer.reset(options_.batch->min_records, options_.batch->max_records);
            runtime_worker->batch_metrics.current_record_target.store(runtime_worker->batch_sizer.target(),
                                                                      std::memory_order_relaxed);
        }
        workers_.push_back(std::move(runtime_worker));
    }
    // A single Worker has one file-backed commit domain, so its dedicated
    // executor can own batch closure without cross-Worker coordination.
    dedicated_commit_executor_ = options_.strict_ack && options_.batch.has_value() && workers_.size() == 1;
    if (options_.commit_sync == SegmentCommitSync::deferred || options_.batch) {
        const bool periodic_sync = options_.commit_sync == SegmentCommitSync::deferred;
        const bool batch_timer = options_.batch.has_value();
        const auto batch_wait = options_.batch ? options_.batch->max_wait_ms : 0U;
        flusher_ = std::make_unique<DurableFlushCoordinator>(
            options_.sync_interval_ms, batch_wait, periodic_sync, batch_timer,
            [this](const bool force_all) -> Status {
                try {
                    if (force_all) {
                        const auto flushed = flush_pending_batches(SegmentCommitSync::immediate);
                        if (!flushed) {
                            return flushed;
                        }
                        return flush_dirty_segments();
                    }
                    if (options_.strict_ack) {
                        return flush_due_batches(SegmentCommitSync::immediate);
                    }
                    const auto flushed = options_.batch ? flush_due_batches(SegmentCommitSync::deferred)
                                                        : flush_pending_batches(SegmentCommitSync::deferred);
                    if (!flushed) {
                        return flushed;
                    }
                    return flush_dirty_segments();
                } catch (...) {
                    abandon_pending_batches();
                    throw;
                }
            });
    }
}

DurableRuntimeCatalog::~DurableRuntimeCatalog() {
    try {
        static_cast<void>(close());
    } catch (...) {
        healthy_.store(false, std::memory_order_release);
    }
}

auto DurableRuntimeCatalog::should_flush_batch(RuntimeWorker& worker) const noexcept -> bool {
    if (!options_.batch || !worker.cached_file || !worker.cached_file->has_pending_commit()) {
        return false;
    }
    if (worker.batch_closing) {
        return true;
    }
    const auto& config = *options_.batch;
    const auto pending_records = worker.cached_file->pending_record_count();
    if (pending_records >= config.max_records) {
        atomic_saturating_add(worker.batch_metrics.record_limit_closes, 1U);
        return true;
    }
    if (worker.cached_file->pending_bytes() >= config.max_bytes) {
        atomic_saturating_add(worker.batch_metrics.byte_limit_closes, 1U);
        return true;
    }
    const auto record_target = worker.batch_sizer.target();
    if (options_.strict_ack && pending_records >= record_target) {
        // If more producers are already admitted than fit in this batch, grow
        // the next target up to the observed burst. A relaxed sample is enough:
        // this is a tuning hint and never affects acknowledgement correctness.
        const auto admissions = worker.active_group_mutations.load(std::memory_order_relaxed);
        worker.batch_sizer.observe_target_reached(pending_records, admissions);
        worker.batch_metrics.current_record_target.store(worker.batch_sizer.target(),
                                                         std::memory_order_relaxed);
        atomic_saturating_add(worker.batch_metrics.adaptive_target_closes, 1U);
        return true;
    }
    if (worker.batch_started != std::chrono::steady_clock::time_point{}) {
        const auto elapsed = std::chrono::steady_clock::now() - worker.batch_started;
        if (elapsed >= std::chrono::milliseconds{config.max_wait_ms}) {
            // A deadline means the current target exceeded available
            // concurrency. Contract the next batch to the occupancy actually
            // achieved, within the caller's explicit bounds.
            if (options_.strict_ack) {
                worker.batch_sizer.observe_deadline(pending_records);
                worker.batch_metrics.current_record_target.store(worker.batch_sizer.target(),
                                                                 std::memory_order_relaxed);
            }
            atomic_saturating_add(worker.batch_metrics.deadline_closes, 1U);
            return true;
        }
    }
    return false;
}

void DurableRuntimeCatalog::abandon_pending_batches() noexcept {
    healthy_.store(false, std::memory_order_release);
    for (auto& worker : workers_) {
        try {
            const std::lock_guard lock{worker->mutex};
            for (auto* mutation : worker->pending_group_mutations) {
                mutation->completed = true;
            }
            worker->pending_group_mutations.clear();
            worker->pending_group_insertions = 0;
            worker->pending_group_heap_key_bytes = 0;
            worker->batch_started = {};
            worker->batch_closing = false;
            worker->batch_closed.notify_all();
        } catch (...) {
            worker->batch_closed.notify_all();
        }
    }
}

auto DurableRuntimeCatalog::flush_worker_batch(RuntimeWorker& worker, const SegmentCommitSync sync)
    -> Status {
    if (!worker.cached_file || !worker.cached_writable || !worker.cached_file->has_pending_commit()) {
        return {};
    }
    const auto pending_records = worker.cached_file->pending_record_count();
    const auto pending_bytes = worker.cached_file->pending_bytes();
    atomic_saturating_add(worker.batch_metrics.flush_attempts, 1U);
    const auto commit_started = std::chrono::steady_clock::now();
    const auto flushed = worker.cached_file->flush_pending_commit(sync);
    const auto commit_duration_ns = steady_elapsed_ns(commit_started);
    atomic_saturating_add(worker.batch_metrics.total_commit_duration_ns, commit_duration_ns);
    atomic_observe_max(worker.batch_metrics.maximum_commit_duration_ns, commit_duration_ns);
    if (!flushed.committed()) {
        atomic_saturating_add(worker.batch_metrics.failed_batches, 1U);
        healthy_.store(false, std::memory_order_release);
        for (auto* mutation : worker.pending_group_mutations) {
            mutation->completed = true;
        }
        worker.pending_group_mutations.clear();
        worker.pending_group_insertions = 0;
        worker.pending_group_heap_key_bytes = 0;
        worker.batch_closing = false;
        worker.batch_closed.notify_all();
        return unexpected(flushed.error.value_or(Error{ErrorCode::io_error, "batch flush failed"}));
    }
    atomic_saturating_add(worker.batch_metrics.committed_batches, 1U);
    atomic_saturating_add(worker.batch_metrics.committed_records, pending_records);
    atomic_saturating_add(worker.batch_metrics.committed_bytes, pending_bytes);
    atomic_observe_max(worker.batch_metrics.maximum_batch_records, static_cast<std::size_t>(pending_records));
    atomic_observe_max(worker.batch_metrics.maximum_batch_bytes, pending_bytes);
    worker.batch_metrics.pending_records.store(0, std::memory_order_relaxed);
    worker.batch_metrics.pending_bytes.store(0, std::memory_order_relaxed);
    const auto& identity = worker.cached_file->identity();
    const auto position =
        std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), identity.segment_id,
                         [](const ManifestSegmentEntry& entry, const SegmentId id) {
                             return entry.segment_id.value < id.value;
                         });
    if (position == manifest_.segments.end() || position->segment_id != identity.segment_id ||
        position->generation != identity.generation || position->owner_worker != identity.owner_worker) {
        return fail(ErrorCode::corrupted_data,
                    "batched Segment cache identity is absent from the runtime catalog");
    }
    const auto catalog_index = static_cast<std::size_t>(position - manifest_.segments.begin());
    segments_[catalog_index].selected = worker.cached_file->selected_commit();
    const auto publication_failed = [&](Error error) -> Status {
        healthy_.store(false, std::memory_order_release);
        for (auto* pending : worker.pending_group_mutations) {
            pending->completed = true;
        }
        worker.pending_group_mutations.clear();
        worker.pending_group_insertions = 0;
        worker.pending_group_heap_key_bytes = 0;
        worker.batch_started = {};
        worker.batch_closing = false;
        worker.batch_closed.notify_all();
        return unexpected(std::move(error));
    };
    for (auto* mutation : worker.pending_group_mutations) {
        const HashedKey hashed{.key = mutation->key, .hash = mutation->key_hash};
        if (mutation->opcode == Opcode::put) {
            const auto published = worker.index.insert_or_assign(hashed, mutation->reference);
            if (!published) {
                return publication_failed(published.error());
            }
            if (auto counted = worker.update_live_record_bytes(published->previous, mutation->reference);
                !counted) {
                return publication_failed(counted.error());
            }
            if (mutation->hot_record.empty()) {
                worker.erase_hot_record(mutation->key, mutation->key_hash);
            } else if (auto hot_published = worker.publish_hot_record(
                           mutation->hot_record, mutation->reference, mutation->reference.sequence);
                       !hot_published) {
                return publication_failed(hot_published.error());
            }
        } else {
            const auto erased = worker.index.erase_no_compact(hashed);
            if (auto counted = worker.update_live_record_bytes(erased.previous, std::nullopt); !counted) {
                return publication_failed(counted.error());
            }
            worker.erase_hot_record(mutation->key, mutation->key_hash);
        }
        mutation->completed = true;
    }
    worker.pending_group_mutations.clear();
    worker.pending_group_insertions = 0;
    worker.pending_group_heap_key_bytes = 0;
    worker.batch_started = {};
    worker.batch_closing = false;
    worker.batch_closed.notify_all();
    return {};
}

void DurableRuntimeCatalog::wait_for_batch_close(RuntimeWorker& worker, PendingGroupMutation& mutation,
                                                 std::unique_lock<std::mutex>& lock) {
    worker.batch_closed.wait(lock, [&] { return mutation.completed || !healthy(); });
}

auto DurableRuntimeCatalog::flush_pending_batches(const SegmentCommitSync sync) -> Status {
    for (auto& worker : workers_) {
        std::unique_lock lock{worker->mutex};
        worker->compaction_commit_finished.wait(lock, [&] { return !worker->compaction_commit_active; });
        const std::shared_lock catalog_lock{catalog_mutex_};
        if (auto flushed = flush_worker_batch(*worker, sync); !flushed) {
            return flushed;
        }
    }
    return {};
}

auto DurableRuntimeCatalog::flush_due_batches(const SegmentCommitSync sync) -> Status {
    for (auto& worker : workers_) {
        std::unique_lock lock{worker->mutex};
        worker->compaction_commit_finished.wait(lock, [&] { return !worker->compaction_commit_active; });
        if (!should_flush_batch(*worker)) {
            continue;
        }
        const std::shared_lock catalog_lock{catalog_mutex_};
        if (auto flushed = flush_worker_batch(*worker, sync); !flushed) {
            return flushed;
        }
    }
    return {};
}

auto DurableRuntimeCatalog::open_existing(const std::filesystem::path& path,
                                          const std::uint64_t recovery_now_ns, const FilesystemHooks hooks)
    -> Result<std::unique_ptr<DurableRuntimeCatalog>> {
    auto directory = DataDirectory::open_and_lock(path, hooks);
    if (!directory) {
        return unexpected(directory.error());
    }
    return open_locked(std::move(*directory), recovery_now_ns);
}

auto DurableRuntimeCatalog::open_locked(DataDirectory directory, const std::uint64_t recovery_now_ns,
                                        const DurableRuntimeOptions options)
    -> Result<std::unique_ptr<DurableRuntimeCatalog>> {
    if (auto valid = validate_durable_resource_limits(options.limits); !valid) {
        return unexpected(valid.error());
    }
    auto compaction = resolve_interrupted_compaction(directory, recovery_now_ns, options.limits);
    if (!compaction) {
        return unexpected(compaction.error());
    }
    std::optional<DurableRecoveryState> recovered = std::move(*compaction);
    if (!recovered) {
        if (auto completed = complete_interrupted_rotation(directory, options.limits); !completed) {
            return unexpected(completed.error());
        }
        auto ordinary = recover_durable_state(directory, recovery_now_ns, options.limits);
        if (!ordinary) {
            return unexpected(ordinary.error());
        }
        recovered.emplace(std::move(*ordinary));
    }
    if (recovered->segments.size() != recovered->manifest.segments.size() ||
        recovered->workers.size() != recovered->manifest.worker_count) {
        return fail(ErrorCode::corrupted_data, "durable recovery result is not aligned with its manifest");
    }
    if (std::ranges::any_of(recovered->workers, [](const RecoveredWorkerState& worker) {
            return worker.active_requires_rotation;
        })) {
        return fail(ErrorCode::unavailable,
                    "durable Store requires interrupted rotation completion before runtime service");
    }
    auto runtime = std::unique_ptr<DurableRuntimeCatalog>(
        new DurableRuntimeCatalog(std::move(directory), std::move(*recovered), options));
    if (auto pinned = runtime->initialize_generation_pins(); !pinned) {
        return unexpected(pinned.error());
    }
    return runtime;
}

auto DurableRuntimeCatalog::initialize_generation_pins() -> Status {
    if (manifest_.segments.size() != segments_.size()) {
        return fail(ErrorCode::corrupted_data, "runtime Segment catalog is not aligned");
    }
    std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> pins;
    pins.reserve(manifest_.segments.size());
    for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
        const auto& entry = manifest_.segments[index];
        const SegmentHeaderIdentity identity{.store_id = manifest_.store_id,
                                             .segment_id = entry.segment_id,
                                             .generation = entry.generation,
                                             .owner_worker = entry.owner_worker};
        auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_only);
        if (!opened) {
            return unexpected(opened.error());
        }
        if (opened->selected_commit() != segments_[index].selected) {
            return fail(ErrorCode::corrupted_data,
                        "runtime Segment commit boundary changed while generation pins were built");
        }
        pins.push_back(std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
            .identity = identity, .selected = segments_[index].selected, .file = std::move(*opened)}));
    }
    generation_pins_ = std::move(pins);
    rebuild_pin_slot_index();
    return {};
}

void DurableRuntimeCatalog::rebuild_pin_slot_index() noexcept {
    constexpr auto kAbsent = std::numeric_limits<std::uint32_t>::max();
    const auto needed = std::max(manifest_.next_segment_id.value, static_cast<std::uint64_t>(1));
    pin_slot_by_segment_id_.assign(static_cast<std::size_t>(needed), kAbsent);
    for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
        const auto segment_id = manifest_.segments[index].segment_id.value;
        if (segment_id >= pin_slot_by_segment_id_.size()) {
            pin_slot_by_segment_id_.resize(static_cast<std::size_t>(segment_id) + 1U, kAbsent);
        }
        pin_slot_by_segment_id_[static_cast<std::size_t>(segment_id)] = static_cast<std::uint32_t>(index);
    }
}

auto DurableRuntimeCatalog::catalog_index_for_segment(const SegmentId segment_id) const noexcept
    -> std::optional<std::size_t> {
    constexpr auto kAbsent = std::numeric_limits<std::uint32_t>::max();
    if (segment_id.value >= pin_slot_by_segment_id_.size()) {
        return std::nullopt;
    }
    const auto slot = pin_slot_by_segment_id_[static_cast<std::size_t>(segment_id.value)];
    if (slot == kAbsent || slot >= manifest_.segments.size() || slot >= generation_pins_.size()) {
        return std::nullopt;
    }
    if (manifest_.segments[slot].segment_id != segment_id) {
        return std::nullopt;
    }
    return slot;
}

auto DurableRuntimeCatalog::flush_dirty_segments() -> Status {
    for (auto& worker : workers_) {
        std::unique_lock lock{worker->mutex};
        worker->compaction_commit_finished.wait(lock, [&] { return !worker->compaction_commit_active; });
        if (!worker->cached_file || !worker->cached_writable || !worker->cached_file->is_dirty()) {
            continue;
        }
        const auto synced = worker->cached_file->sync_file();
        if (!synced.committed()) {
            healthy_.store(false, std::memory_order_release);
            return unexpected(synced.error.value_or(Error{ErrorCode::io_error, "Segment flush failed"}));
        }
    }
    return {};
}

auto DurableRuntimeCatalog::flush() -> Status {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    if (dedicated_commit_executor_ && flusher_) {
        return flusher_->flush_all_blocking();
    }
    if (options_.batch || options_.commit_sync == SegmentCommitSync::deferred) {
        if (auto flushed = flush_pending_batches(SegmentCommitSync::immediate); !flushed) {
            return flushed;
        }
    }
    return flush_dirty_segments();
}

void DurableRuntimeCatalog::request_close_flush() {
    if (flusher_) {
        flusher_->request_flush_all();
    }
}

auto DurableRuntimeCatalog::close() -> Status {
    const std::lock_guard close_lock{close_mutex_};
    const auto cached_status = [&]() -> Status {
        if (!close_error_) {
            return {};
        }
        try {
            return unexpected(*close_error_);
        } catch (const std::bad_alloc&) {
            return unexpected(Error{ErrorCode::resource_exhausted, {}});
        } catch (...) {
            return unexpected(Error{ErrorCode::internal_error, {}});
        }
    };
    if (closed_.exchange(true, std::memory_order_acq_rel)) {
        return cached_status();
    }

    Status result;
    try {
        if (flusher_) {
            result = flusher_->flush_all_blocking();
            flusher_->stop();
        } else if (!healthy_.load(std::memory_order_acquire) || !directory_.healthy()) {
            result = unexpected(Error{ErrorCode::unavailable, {}});
        }
    } catch (const std::bad_alloc&) {
        result = unexpected(Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        result = unexpected(Error{ErrorCode::internal_error, {}});
    }
    if (!result) {
        healthy_.store(false, std::memory_order_release);
        close_error_.emplace(std::move(result.error()));
    }
    return cached_status();
}

auto DurableRuntimeCatalog::fail_closed(Error error) -> Unexpected {
    healthy_.store(false, std::memory_order_release);
    return unexpected(std::move(error));
}

auto DurableRuntimeCatalog::get(const std::string_view key, const std::uint64_t now_ns)
    -> Result<OwnedValue> {
    return get(std::as_bytes(std::span{key}), now_ns);
}

auto DurableRuntimeCatalog::get(const std::span<const std::byte> key, const std::uint64_t now_ns)
    -> Result<OwnedValue> {
    return get(HashedKey{.key = as_string_view(key), .hash = hash_key(key)}, now_ns);
}

auto DurableRuntimeCatalog::get(const HashedKey& key, const std::uint64_t now_ns) -> Result<OwnedValue> {
    auto prepared = prepare_get(key, now_ns);
    if (!prepared) {
        return unexpected(prepared.error());
    }
    if (prepared->value) {
        return std::move(*prepared->value);
    }
    if (!prepared->cold) {
        return fail(ErrorCode::internal_error, "durable GET preparation produced no result");
    }
    return complete_get(std::move(*prepared->cold));
}

auto DurableRuntimeCatalog::prepare_get(const HashedKey& key, const std::uint64_t now_ns)
    -> Result<PreparedRead> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const auto worker_index = route_worker(key.hash, workers_.size());
    auto& worker = *workers_[worker_index];
    auto& metrics = worker.get_path_metrics;
    RecordRef cold_reference;
    std::shared_ptr<const RuntimeSegmentGeneration> cold_pin;
    std::optional<HotRecordSnapshot> hot;
    std::uint64_t mutex_wait_ns = 0;
    std::uint64_t prepare_hold_ns = 0;
    std::uint64_t index_lookup_ns = 0;
    std::uint64_t hot_cache_lookup_ns = 0;
    std::uint64_t generation_pin_lookup_ns = 0;
    std::uint64_t local_hits = 0;
    std::uint64_t local_misses = 0;
    std::uint64_t local_stale = 0;
    ScopeExit publish_prepare_metrics{[&]() noexcept {
        metrics.prepare_calls.fetch_add(1U, std::memory_order_relaxed);
        if (local_hits != 0) {
            metrics.hot_hits.fetch_add(local_hits, std::memory_order_relaxed);
        }
        if (local_misses != 0) {
            metrics.hot_misses.fetch_add(local_misses, std::memory_order_relaxed);
        }
        if (local_stale != 0) {
            metrics.hot_stale_hits.fetch_add(local_stale, std::memory_order_relaxed);
        }
        if constexpr (kGetPathTimingEnabled) {
            atomic_saturating_add(metrics.mutex_wait_ns, mutex_wait_ns);
            atomic_saturating_add(metrics.prepare_hold_ns, prepare_hold_ns);
            atomic_saturating_add(metrics.index_lookup_ns, index_lookup_ns);
            atomic_saturating_add(metrics.hot_cache_lookup_ns, hot_cache_lookup_ns);
            atomic_saturating_add(metrics.generation_pin_lookup_ns, generation_pin_lookup_ns);
        }
    }};
    {
        const auto wait_started = timing_now();
        const std::lock_guard worker_lock{worker.mutex};
        const auto locked_at = timing_now();
        mutex_wait_ns = timing_duration_ns(wait_started, locked_at);
        ScopeExit observe_hold{[&]() noexcept { prepare_hold_ns = timing_elapsed_ns(locked_at); }};
        // Hot path only needs the Worker mutex + atomic health. Catalog shared lock
        // is taken below solely for cold-miss generation pin / manifest identity.
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }

        // Drain deferred TTL only when the backlog is non-empty, and before Index
        // lookup so a reclaimed key becomes an Index miss (not a second cold read).
        // Empty-backlog hot GETs skip this entirely.
        if (!worker.deferred_ttl_reclaims.empty()) {
            if (auto drained = worker.drain_deferred_ttl(
                    std::min<std::size_t>(8, worker.deferred_ttl_reclaims.size()));
                !drained) {
                return fail_closed(drained.error());
            }
        }

        // Ordinary hot path under mutex: Index lookup, hot match + snapshot, unlock.
        // No I/O/CRC/value copy under the lock.
        const auto index_started = timing_now();
        const auto reference = worker.index.find(key);
        index_lookup_ns = timing_elapsed_ns(index_started);
        if (!reference) {
            return fail(ErrorCode::not_found, "key is not present");
        }

        const auto hot_started = timing_now();
        if (const auto* cached = worker.hot_records.find(key); cached != nullptr) {
            if (hot_record_matches(*cached, *reference)) {
                ++local_hits;
                if (cached->expire_at_ns != 0 && now_ns != 0 && cached->expire_at_ns <= now_ns) {
                    if (auto reclaimed = worker.defer_or_reclaim_expired(
                            key, *reference, options_.limits.max_deferred_ttl_reclaims_per_worker);
                        !reclaimed) {
                        hot_cache_lookup_ns = timing_elapsed_ns(hot_started);
                        return fail_closed(reclaimed.error());
                    }
                    hot_cache_lookup_ns = timing_elapsed_ns(hot_started);
                    return fail(ErrorCode::not_found, "key has expired");
                }
                hot.emplace(HotRecordSnapshot::from_entry(*cached));
            } else {
                ++local_stale;
                worker.erase_hot_record(key);
            }
        }
        hot_cache_lookup_ns = timing_elapsed_ns(hot_started);

        if (!hot) {
            ++local_misses;

            const auto pin_started = timing_now();
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }
            const auto catalog_index = catalog_index_for_segment(reference->segment_id);
            if (!catalog_index) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Index references a Segment absent from the catalog"});
            }
            const auto& found = manifest_.segments[*catalog_index];
            if (found.generation != reference->generation || found.owner_worker != worker.worker_id) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(
                    Error{ErrorCode::corrupted_data,
                          "durable Index reference disagrees with catalog identity or ownership"});
            }
            const auto& pin = generation_pins_[*catalog_index];
            if (!pin || pin->identity.segment_id != reference->segment_id ||
                pin->identity.generation != reference->generation ||
                pin->identity.owner_worker != worker.worker_id) {
                generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Segment generation pin disagrees with the Index"});
            }
            generation_pin_lookup_ns = timing_elapsed_ns(pin_started);
            cold_reference = *reference;
            cold_pin = pin;
        }
    }
    if (hot) {
        return PreparedRead{.value = owned_value_from_hot(*hot)};
    }
    return PreparedRead{.cold = PinnedRead{std::string{key.key}, key.hash, now_ns, worker_index,
                                           cold_reference, std::move(cold_pin)}};
}

auto DurableRuntimeCatalog::complete_get(PinnedRead read, const std::atomic_bool* cancelled)
    -> Result<OwnedValue> {
    constexpr std::size_t kMaximumRelinearizationAttempts = 8;
    for (std::size_t attempt = 0; attempt < kMaximumRelinearizationAttempts; ++attempt) {
        if (cancelled != nullptr && cancelled->load(std::memory_order_acquire)) {
            return fail(ErrorCode::unavailable, "durable cold read was cancelled");
        }
        auto& worker = *workers_[read.worker_index_];
        auto& metrics = worker.get_path_metrics;
        metrics.complete_calls.fetch_add(1U, std::memory_order_relaxed);
        const auto key_bytes = std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(read.key_.data()), read.key_.size()};
        ReadContext context{
            .expected_key = key_bytes, .expected_hash = read.key_hash_, .now_ns = read.now_ns_};
        const auto cold_started = timing_now();
        auto visited =
            read.generation_->file.visit_runtime_record(read.reference_, &context, &copy_verified_value);
        if constexpr (kGetPathTimingEnabled) {
            atomic_saturating_add(metrics.cold_read_ns, timing_elapsed_ns(cold_started));
            atomic_saturating_add(metrics.crc_value_copy_ns, context.crc_value_copy_ns);
        }

        // Linearization point for a cold GET: the Worker index must still
        // designate both the exact RecordRef and immutable generation pin
        // captured by prepare_get(). No file I/O or CRC work holds either lock.
        bool still_current{};
        {
            const auto wait_started = timing_now();
            const std::lock_guard worker_lock{worker.mutex};
            const auto locked_at = timing_now();
            if constexpr (kGetPathTimingEnabled) {
                atomic_saturating_add(metrics.mutex_wait_ns, timing_duration_ns(wait_started, locked_at));
            }
            ScopeExit observe_hold{[&]() noexcept {
                if constexpr (kGetPathTimingEnabled) {
                    atomic_saturating_add(metrics.complete_revalidate_hold_ns,
                                          timing_elapsed_ns(locked_at));
                }
            }};
            if (!healthy()) {
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }
            const HashedKey key{.key = read.key_, .hash = read.key_hash_};
            const auto index_started = timing_now();
            const auto current = worker.index.find(key);
            if constexpr (kGetPathTimingEnabled) {
                atomic_saturating_add(metrics.index_lookup_ns, timing_elapsed_ns(index_started));
            }
            // Catalog lock only when the Index still names the captured RecordRef;
            // churned keys skip pin identity work and catalog shared-lock contention.
            if (current && *current == read.reference_) {
                const auto pin_started = timing_now();
                const std::shared_lock catalog_lock{catalog_mutex_};
                if (!healthy()) {
                    return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
                }
                const auto catalog_index = catalog_index_for_segment(current->segment_id);
                still_current = catalog_index.has_value() &&
                                generation_pins_[*catalog_index] == read.generation_;
                if constexpr (kGetPathTimingEnabled) {
                    atomic_saturating_add(metrics.generation_pin_lookup_ns,
                                          timing_elapsed_ns(pin_started));
                }
            }
        }
        if (!still_current) {
            if (attempt + 1U == kMaximumRelinearizationAttempts) {
                return fail(ErrorCode::sequence_conflict,
                            "durable cold read exceeded its relinearization retry budget");
            }
            atomic_saturating_add(metrics.relinearization_retries, 1U);
            const HashedKey key{.key = read.key_, .hash = read.key_hash_};
            auto prepared = prepare_get(key, read.now_ns_);
            if (!prepared) {
                return unexpected(prepared.error());
            }
            if (prepared->value) {
                return std::move(*prepared->value);
            }
            if (!prepared->cold) {
                return fail(ErrorCode::internal_error, "durable GET retry produced no result");
            }
            read = std::move(*prepared->cold);
            continue;
        }
        if (!visited) {
            if (visited.error().code == ErrorCode::not_found) {
                // Visitor not_found is validated expiry only. Defer Index reclaim
                // while verifying the exact RecordRef when the backlog drains.
                {
                    const auto wait_started = timing_now();
                    const std::lock_guard worker_lock{worker.mutex};
                    const auto locked_at = timing_now();
                    if constexpr (kGetPathTimingEnabled) {
                        atomic_saturating_add(metrics.mutex_wait_ns,
                                              timing_duration_ns(wait_started, locked_at));
                    }
                    ScopeExit observe_hold{[&]() noexcept {
                        if constexpr (kGetPathTimingEnabled) {
                            atomic_saturating_add(metrics.complete_revalidate_hold_ns,
                                                  timing_elapsed_ns(locked_at));
                        }
                    }};
                    // Index + deferred TTL reclaim are Worker-local; no catalog pin work.
                    if (healthy()) {
                        const HashedKey key{.key = read.key_, .hash = read.key_hash_};
                        const auto current = worker.index.find(key);
                        if (current && *current == read.reference_) {
                            if (auto reclaimed = worker.defer_or_reclaim_expired(
                                    key, read.reference_,
                                    options_.limits.max_deferred_ttl_reclaims_per_worker);
                                !reclaimed) {
                                return fail_closed(reclaimed.error());
                            }
                        }
                    }
                }
                return unexpected(visited.error());
            }
            return fail_closed(visited.error());
        }
        return std::move(context.value);
    }
    return fail(ErrorCode::internal_error, "durable cold read retry loop escaped its bound");
}

auto DurableRuntimeCatalog::put(const std::span<const std::byte> key, const std::span<const std::byte> value,
                                const std::uint64_t expire_at_ns, const ValueType type,
                                const std::uint32_t flags) -> DurableMutationResult {
    return mutate(key, value, Opcode::put, hash_key(key), expire_at_ns, type, flags);
}

auto DurableRuntimeCatalog::put(const HashedKey& key, const std::span<const std::byte> value,
                                const std::uint64_t expire_at_ns, const ValueType type,
                                const std::uint32_t flags) -> DurableMutationResult {
    return mutate(
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()}, value,
        Opcode::put, key.hash, expire_at_ns, type, flags);
}

auto DurableRuntimeCatalog::erase(const std::span<const std::byte> key) -> DurableMutationResult {
    return mutate(key, {}, Opcode::erase, hash_key(key), 0, ValueType::bytes, 0);
}

auto DurableRuntimeCatalog::erase(const HashedKey& key) -> DurableMutationResult {
    return mutate(
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()}, {},
        Opcode::erase, key.hash, 0, ValueType::bytes, 0);
}

auto DurableRuntimeCatalog::rotate_active(RuntimeWorker& worker) -> DurableMutationResult {
    const auto rotation_started = std::chrono::steady_clock::now();
    rotation_attempts_.fetch_add(1U, std::memory_order_relaxed);
    std::unique_lock publication_lock{manifest_publication_mutex_};
    if (compaction_publication_active_) {
        rotation_compaction_waits_.fetch_add(1U, std::memory_order_relaxed);
    }
    manifest_publication_changed_.wait(publication_lock, [&] { return !compaction_publication_active_; });
    const auto execution_started = std::chrono::steady_clock::now();
    bool rotation_committed{};
    std::uint64_t seal_ns{};
    std::uint64_t create_ns{};
    std::uint64_t manifest_publication_ns{};
    ScopeExit telemetry{[&, this]() noexcept {
        const auto finished = std::chrono::steady_clock::now();
        const auto publication_wait_ns = steady_duration_ns(rotation_started, execution_started);
        const auto execution_ns = steady_duration_ns(execution_started, finished);
        const auto total_ns = steady_duration_ns(rotation_started, finished);
        const auto previous_version = begin_atomic_stats_publication(rotation_stats_version_);
        if (rotation_committed) {
            rotations_committed_.fetch_add(1U, std::memory_order_relaxed);
        }
        last_rotation_publication_wait_ns_.store(publication_wait_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_publication_wait_ns_, publication_wait_ns);
        atomic_observe_max(maximum_rotation_publication_wait_ns_, publication_wait_ns);
        last_rotation_seal_ns_.store(seal_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_seal_ns_, seal_ns);
        atomic_observe_max(maximum_rotation_seal_ns_, seal_ns);
        last_rotation_create_ns_.store(create_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_create_ns_, create_ns);
        atomic_observe_max(maximum_rotation_create_ns_, create_ns);
        last_rotation_manifest_publication_ns_.store(manifest_publication_ns,
                                                     std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_manifest_publication_ns_, manifest_publication_ns);
        atomic_observe_max(maximum_rotation_manifest_publication_ns_, manifest_publication_ns);
        last_rotation_execution_ns_.store(execution_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_execution_ns_, execution_ns);
        atomic_observe_max(maximum_rotation_execution_ns_, execution_ns);
        last_rotation_total_ns_.store(total_ns, std::memory_order_relaxed);
        atomic_saturating_add(total_rotation_ns_, total_ns);
        atomic_observe_max(maximum_rotation_total_ns_, total_ns);
        end_atomic_stats_publication(rotation_stats_version_, previous_version);
    }};
    const std::unique_lock catalog_lock{catalog_mutex_};
    const auto old_position =
        std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), worker.active_segment,
                         [](const ManifestSegmentEntry& entry, const SegmentId id) {
                             return entry.segment_id.value < id.value;
                         });
    if (old_position == manifest_.segments.end() || old_position->segment_id != worker.active_segment ||
        old_position->owner_worker != worker.worker_id || old_position->role != ManifestSegmentRole::active) {
        return mutation_failure(
            DurableMutationOutcome::indeterminate,
            Error{ErrorCode::corrupted_data, "runtime active Segment disagrees with the manifest"});
    }
    const auto old_index = static_cast<std::size_t>(old_position - manifest_.segments.begin());
    const auto old_entry = *old_position;
    const auto active_live_record_bytes = worker.active_live_record_bytes.load(std::memory_order_relaxed);
    const auto sealed_live_record_bytes = worker.sealed_live_record_bytes.load(std::memory_order_relaxed);
    if (active_live_record_bytes > std::numeric_limits<std::uint64_t>::max() - sealed_live_record_bytes) {
        return mutation_failure(DurableMutationOutcome::not_committed,
                                Error{ErrorCode::arithmetic_overflow,
                                      "durable rotation live Record byte count overflows uint64_t"});
    }
    const auto next_sealed_live_record_bytes = sealed_live_record_bytes + active_live_record_bytes;
    auto next_manifest = rotation_manifest(manifest_, old_entry, options_.limits);
    if (!next_manifest) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_manifest.error());
    }
    const auto next_manifest_bytes = durable_manifest_bytes(next_manifest->segments.size());
    if (!next_manifest_bytes) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_manifest_bytes.error());
    }
    if (*next_manifest_bytes > std::numeric_limits<std::uint64_t>::max() - kSegmentSizeBytes) {
        return mutation_failure(
            DurableMutationOutcome::not_committed,
            Error{ErrorCode::arithmetic_overflow, "rotation free-space requirement overflow"});
    }
    if (auto available = require_durable_available_space(directory_, kSegmentSizeBytes + *next_manifest_bytes,
                                                         options_.limits);
        !available) {
        return mutation_failure(DurableMutationOutcome::not_committed, available.error());
    }
    segments_.reserve(segments_.size() + 1U);
    generation_pins_.reserve(generation_pins_.size() + 1U);

    const SegmentHeaderIdentity old_identity{.store_id = manifest_.store_id,
                                             .segment_id = old_entry.segment_id,
                                             .generation = old_entry.generation,
                                             .owner_worker = old_entry.owner_worker};
    if (!worker.cached_file || worker.cached_file->identity() != old_identity || !worker.cached_writable) {
        worker.cached_file.reset();
        worker.cached_writable = false;
        auto opened = DurableSegmentFile::open(directory_, old_identity, SegmentFileOpenMode::read_write);
        if (!opened || opened->selected_commit() != segments_[old_index].selected) {
            auto error = opened ? Error{ErrorCode::corrupted_data, "active Segment changed after recovery"}
                                : opened.error();
            healthy_.store(false, std::memory_order_release);
            return mutation_failure(DurableMutationOutcome::indeterminate, std::move(error));
        }
        worker.cached_file.emplace(std::move(*opened));
        worker.cached_writable = true;
    }

    {
        const auto seal_started = std::chrono::steady_clock::now();
        ScopeExit observe_seal{
            [&]() noexcept { seal_ns = steady_elapsed_ns(seal_started); }};
        if (worker.cached_file->selected_commit().commit.state != PersistedSegmentState::sealed) {
            const auto sealed = worker.cached_file->seal();
            if (!sealed.committed()) {
                if (sealed.outcome == SegmentCommitOutcome::indeterminate) {
                    healthy_.store(false, std::memory_order_release);
                }
                return mutation_failure(
                    sealed.outcome == SegmentCommitOutcome::indeterminate
                        ? DurableMutationOutcome::indeterminate
                        : DurableMutationOutcome::not_committed,
                    sealed.error.value_or(Error{ErrorCode::io_error, "Segment seal failed"}));
            }
            segments_[old_index].selected = worker.cached_file->selected_commit();
        }
    }

    auto sealed_generation = std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
        .identity = old_identity,
        .selected = worker.cached_file->selected_commit(),
        .file = std::move(*worker.cached_file),
    });
    worker.cached_file.reset();
    worker.cached_writable = false;

    const auto& replacement_entry = next_manifest->segments.back();
    const SegmentHeaderIdentity replacement_identity{
        .store_id = manifest_.store_id,
        .segment_id = replacement_entry.segment_id,
        .generation = replacement_entry.generation,
        .owner_worker = replacement_entry.owner_worker,
    };
    SegmentFileCreationResult created;
    {
        const auto create_started = std::chrono::steady_clock::now();
        ScopeExit observe_create{
            [&]() noexcept { create_ns = steady_elapsed_ns(create_started); }};
        created = DurableSegmentFile::create(directory_, replacement_identity);
    }
    if (!created.durable()) {
        if (created.outcome == SegmentFileCreationOutcome::indeterminate) {
            healthy_.store(false, std::memory_order_release);
        }
        return mutation_failure(
            created.outcome == SegmentFileCreationOutcome::indeterminate
                ? DurableMutationOutcome::indeterminate
                : DurableMutationOutcome::not_committed,
            created.error.value_or(Error{ErrorCode::io_error, "replacement Segment creation failed"}));
    }
    const auto replacement_selected = created.file->selected_commit();
    auto replacement_reader =
        DurableSegmentFile::open(directory_, replacement_identity, SegmentFileOpenMode::read_only);
    if (!replacement_reader || replacement_reader->selected_commit() != replacement_selected) {
        auto error = replacement_reader
                         ? Error{ErrorCode::corrupted_data,
                                 "new active Segment changed before generation pin publication"}
                         : replacement_reader.error();
        return mutation_failure(DurableMutationOutcome::not_committed, std::move(error));
    }
    auto replacement_generation = std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
        .identity = replacement_identity,
        .selected = replacement_selected,
        .file = std::move(*replacement_reader),
    });
    ManifestPublicationResult published;
    {
        const auto manifest_started = std::chrono::steady_clock::now();
        ScopeExit observe_manifest{[&]() noexcept {
            manifest_publication_ns = steady_elapsed_ns(manifest_started);
        }};
        published = directory_.publish_manifest(*next_manifest, options_.limits.max_manifest_bytes);
    }
    if (!published.durable()) {
        healthy_.store(false, std::memory_order_release);
        return mutation_failure(
            published.outcome == ManifestPublicationOutcome::indeterminate
                ? DurableMutationOutcome::indeterminate
                : DurableMutationOutcome::not_committed,
            published.error.value_or(Error{ErrorCode::io_error, "rotation manifest publication failed"}));
    }

    const auto replacement_segment_id = replacement_entry.segment_id;
    manifest_ = std::move(*next_manifest);
    segments_.push_back({.selected = replacement_selected});
    generation_pins_[old_index] = std::move(sealed_generation);
    generation_pins_.push_back(std::move(replacement_generation));
    rebuild_pin_slot_index();
    worker.active_segment = replacement_segment_id;
    worker.active_live_record_bytes.store(0, std::memory_order_release);
    worker.sealed_live_record_bytes.store(next_sealed_live_record_bytes, std::memory_order_release);
    worker.cached_file.emplace(std::move(*created.file));
    worker.cached_writable = true;
    worker.hot_records.erase_if([&](const std::string&, std::uint64_t, HotRecordEntry& entry) {
        if (entry.reference.segment_id != old_entry.segment_id) {
            return false;
        }
        worker.hot_record_resident_bytes -= entry.accounted_bytes;
        worker.get_path_metrics.hot_evictions.fetch_add(1U, std::memory_order_relaxed);
        return true;
    });
    rotation_committed = true;
    return {.outcome = DurableMutationOutcome::committed, .sequence = std::nullopt, .error = std::nullopt};
}

auto DurableRuntimeCatalog::mutate(const std::span<const std::byte> key,
                                   const std::span<const std::byte> value, const Opcode opcode,
                                   const std::uint64_t key_hash, const std::uint64_t expire_at_ns,
                                   const ValueType type, const std::uint32_t flags) -> DurableMutationResult {
    auto exception_outcome = DurableMutationOutcome::not_committed;
    std::optional<std::chrono::steady_clock::time_point> final_record_commit_started;
    bool final_record_committed{};
    ScopeExit final_record_telemetry{[&, this]() noexcept {
        if (final_record_commit_started) {
            record_rotation_final_commit(steady_elapsed_ns(*final_record_commit_started),
                                         final_record_committed);
        }
    }};
    try {
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        const auto worker_index = route_worker(key_hash, workers_.size());
        auto& worker = *workers_[worker_index];
        const bool strict_batch = options_.batch.has_value() && options_.strict_ack;
        struct GroupAdmission final {
            std::atomic_size_t* active{};

            explicit GroupAdmission(std::atomic_size_t* counter) noexcept : active(counter) {
                if (active) {
                    active->fetch_add(1U, std::memory_order_relaxed);
                }
            }
            ~GroupAdmission() {
                if (active) {
                    active->fetch_sub(1U, std::memory_order_relaxed);
                }
            }
            GroupAdmission(const GroupAdmission&) = delete;
            auto operator=(const GroupAdmission&) -> GroupAdmission& = delete;
        } admission{strict_batch ? &worker.active_group_mutations : nullptr};
        std::unique_lock worker_lock{worker.mutex};
        if (dedicated_commit_executor_) {
            worker.batch_closed.wait(worker_lock, [&] { return !worker.batch_closing || !healthy(); });
        }
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        if (auto drained = worker.drain_deferred_ttl(worker.deferred_ttl_reclaims.size()); !drained) {
            return mutation_failure(DurableMutationOutcome::indeterminate, drained.error());
        }
        if (worker.compaction_commit_active) {
            return mutation_failure(DurableMutationOutcome::not_committed,
                                    Error{ErrorCode::sequence_conflict,
                                          "durable mutation conflicts with compaction manifest publication"});
        }
        if (worker.next_sequence.value == 0 ||
            worker.next_sequence.value == std::numeric_limits<std::uint64_t>::max()) {
            return mutation_failure(
                DurableMutationOutcome::not_committed,
                Error{ErrorCode::arithmetic_overflow, "Worker sequence space is exhausted"});
        }

        const HashedKey hashed{.key = as_string_view(key), .hash = key_hash};
        bool key_present = worker.index.find(hashed).has_value();
        if (strict_batch) {
            const auto pending = std::find_if(
                worker.pending_group_mutations.rbegin(), worker.pending_group_mutations.rend(),
                [&](const PendingGroupMutation* mutation) { return mutation->key == hashed.key; });
            if (pending != worker.pending_group_mutations.rend()) {
                key_present = (*pending)->opcode == Opcode::put;
            }
        }
        if (opcode == Opcode::erase && !key_present) {
            return mutation_failure(DurableMutationOutcome::not_committed,
                                    Error{ErrorCode::not_found, "key is not present"});
        }
        std::size_t prospective_group_insertions = worker.pending_group_insertions;
        std::size_t prospective_group_heap_key_bytes = worker.pending_group_heap_key_bytes;
        if (opcode == Opcode::put) {
            if (!key_present) {
                const auto live_key_limit = durable_worker_live_key_limit(worker_index, workers_.size(),
                                                                          options_.limits.max_live_keys);
                const auto current_size = worker.index.stats().size;
                if (current_size >= live_key_limit ||
                    prospective_group_insertions >= live_key_limit - current_size) {
                    return mutation_failure(
                        DurableMutationOutcome::not_committed,
                        Error{ErrorCode::resource_exhausted, "durable Worker live-key budget is exhausted"});
                }
            }
            if (auto prepared = worker.index.prepare_insert(hashed); !prepared) {
                return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
            }
            if (strict_batch && !key_present) {
                if (prospective_group_insertions == std::numeric_limits<std::size_t>::max() ||
                    (key.size() > kSwissInlineKeyBytes &&
                     key.size() >
                         std::numeric_limits<std::size_t>::max() - prospective_group_heap_key_bytes)) {
                    return mutation_failure(
                        DurableMutationOutcome::not_committed,
                        Error{ErrorCode::arithmetic_overflow, "group publication capacity overflow"});
                }
                ++prospective_group_insertions;
                if (key.size() > kSwissInlineKeyBytes) {
                    prospective_group_heap_key_bytes += key.size();
                }
                if (auto prepared = worker.index.prepare_batch_insert(prospective_group_insertions,
                                                                      prospective_group_heap_key_bytes);
                    !prepared) {
                    return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
                }
            }
        }
        const RecordInput input{.sequence = worker.next_sequence,
                                .opcode = opcode,
                                .type = type,
                                .flags = flags,
                                .key_hash = key_hash,
                                .expire_at_ns = expire_at_ns,
                                .key = key,
                                .value = value};
        const auto committed_sequence = worker.next_sequence;
        const auto encoded_size = encoded_record_size(input);
        if (!encoded_size) {
            return mutation_failure(DurableMutationOutcome::not_committed, encoded_size.error());
        }
        worker.encode_scratch.resize(*encoded_size);
        if (auto encoded = encode_record(worker.encode_scratch, input); !encoded) {
            return mutation_failure(DurableMutationOutcome::not_committed, encoded.error());
        }
        PendingGroupMutation group_mutation;
        PreparedHotRecord prepared_hot_record;
        if (opcode == Opcode::put) {
            constexpr auto group_publication_fixed_bytes =
                static_cast<std::uint64_t>(sizeof(PendingGroupMutation) + sizeof(PendingGroupMutation*));
            const auto publication_staging_bytes =
                strict_batch
                    ? (key.size() > std::numeric_limits<std::uint64_t>::max() - group_publication_fixed_bytes
                           ? std::numeric_limits<std::uint64_t>::max()
                           : group_publication_fixed_bytes + static_cast<std::uint64_t>(key.size()))
                    : 0U;
            auto prepared =
                worker.prepare_hot_record(worker_index, workers_.size(), options_.limits, hashed.key,
                                          hashed.hash, value, expire_at_ns, publication_staging_bytes);
            if (!prepared) {
                return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
            }
            prepared_hot_record = std::move(*prepared);
        }
        if (strict_batch) {
            group_mutation.key.assign(hashed.key);
            group_mutation.hot_record = std::move(prepared_hot_record);
            group_mutation.opcode = opcode;
            group_mutation.key_hash = key_hash;
            group_mutation.expire_at_ns = expire_at_ns;
            worker.pending_group_mutations.reserve(worker.pending_group_mutations.size() + 1U);
        }

        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            std::shared_lock catalog_lock{catalog_mutex_};
            const auto position =
                std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), worker.active_segment,
                                 [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                     return entry.segment_id.value < id.value;
                                 });
            if (position == manifest_.segments.end() || position->segment_id != worker.active_segment) {
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(
                    DurableMutationOutcome::indeterminate,
                    Error{ErrorCode::corrupted_data, "runtime active Segment is absent from the manifest"});
            }
            const auto catalog_index = static_cast<std::size_t>(position - manifest_.segments.begin());
            const SegmentHeaderIdentity identity{.store_id = manifest_.store_id,
                                                 .segment_id = position->segment_id,
                                                 .generation = position->generation,
                                                 .owner_worker = position->owner_worker};
            if (!worker.cached_file || worker.cached_file->identity() != identity ||
                !worker.cached_writable) {
                worker.cached_file.reset();
                worker.cached_writable = false;
                auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_write);
                if (!opened || opened->selected_commit() != segments_[catalog_index].selected) {
                    auto error =
                        opened ? Error{ErrorCode::corrupted_data, "active Segment changed after recovery"}
                               : opened.error();
                    healthy_.store(false, std::memory_order_release);
                    return mutation_failure(DurableMutationOutcome::indeterminate, std::move(error));
                }
                worker.cached_file.emplace(std::move(*opened));
                worker.cached_writable = true;
            }

            const auto offset = worker.cached_file->selected_commit().commit.committed_end;
            const bool batch_enabled = options_.batch.has_value();
            const bool deferred_commit = options_.commit_sync == SegmentCommitSync::deferred;
            SegmentCommitResult appended{};
            // From the first persistent write until coherent runtime publication,
            // an unexpected exception has a conservative indeterminate outcome.
            exception_outcome = DurableMutationOutcome::indeterminate;
            if (batch_enabled || deferred_commit) {
                appended = worker.cached_file->append_record(worker.encode_scratch);
            } else {
                appended = worker.cached_file->append(worker.encode_scratch, options_.commit_sync);
            }
            if (!appended.committed()) {
                if (appended.error &&
                    (appended.error->code == ErrorCode::segment_full ||
                     appended.error->code == ErrorCode::segment_sealed) &&
                    attempt == 0) {
                    if (strict_batch && !worker.pending_group_mutations.empty()) {
                        if (dedicated_commit_executor_) {
                            worker.batch_closing = true;
                            catalog_lock.unlock();
                            flusher_->request_flush();
                            worker.batch_closed.wait(worker_lock, [&] {
                                return worker.pending_group_mutations.empty() || !healthy();
                            });
                            if (!healthy()) {
                                return mutation_failure(
                                    DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                            }
                        } else if (auto flushed = flush_worker_batch(worker, SegmentCommitSync::immediate);
                                   !flushed) {
                            return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                        }
                        prospective_group_insertions = opcode == Opcode::put && !key_present ? 1U : 0U;
                        prospective_group_heap_key_bytes =
                            prospective_group_insertions != 0 && key.size() > kSwissInlineKeyBytes
                                ? key.size()
                                : 0U;
                    }
                    if (catalog_lock.owns_lock()) {
                        catalog_lock.unlock();
                    }
                    const auto rotated = rotate_active(worker);
                    if (!rotated.committed()) {
                        return rotated;
                    }
                    final_record_commit_started = std::chrono::steady_clock::now();
                    continue;
                }
                if (appended.outcome == SegmentCommitOutcome::indeterminate || !directory_.healthy()) {
                    healthy_.store(false, std::memory_order_release);
                    for (auto* mutation : worker.pending_group_mutations) {
                        mutation->completed = true;
                    }
                    worker.pending_group_mutations.clear();
                    worker.pending_group_insertions = 0;
                    worker.pending_group_heap_key_bytes = 0;
                    worker.batch_closing = false;
                    worker.batch_closed.notify_all();
                }
                return mutation_failure(
                    appended.outcome == SegmentCommitOutcome::indeterminate
                        ? DurableMutationOutcome::indeterminate
                        : DurableMutationOutcome::not_committed,
                    appended.error.value_or(Error{ErrorCode::io_error, "Record append failed"}));
            }

            segments_[catalog_index].selected = worker.cached_file->selected_commit();
            ++worker.next_sequence.value;

            const RecordRef reference{
                .segment_id = position->segment_id,
                .offset = RecordOffset{offset},
                .size = RecordSize{static_cast<std::uint32_t>(worker.encode_scratch.size())},
                .sequence = committed_sequence,
                .generation = position->generation};
            if (strict_batch) {
                group_mutation.reference = reference;
                worker.pending_group_mutations.push_back(&group_mutation);
                worker.pending_group_insertions = prospective_group_insertions;
                worker.pending_group_heap_key_bytes = prospective_group_heap_key_bytes;
            }

            if (batch_enabled) {
                worker.batch_metrics.pending_records.store(
                    static_cast<std::size_t>(worker.cached_file->pending_record_count()),
                    std::memory_order_relaxed);
                worker.batch_metrics.pending_bytes.store(worker.cached_file->pending_bytes(),
                                                         std::memory_order_relaxed);
                if (worker.batch_started == std::chrono::steady_clock::time_point{}) {
                    worker.batch_started = std::chrono::steady_clock::now();
                    if (dedicated_commit_executor_ && flusher_) {
                        flusher_->request_flush_at(worker.batch_started +
                                                   std::chrono::milliseconds{options_.batch->max_wait_ms});
                    }
                }
                if (options_.strict_ack) {
                    if (should_flush_batch(worker)) {
                        if (dedicated_commit_executor_) {
                            worker.batch_closing = true;
                            catalog_lock.unlock();
                            flusher_->request_flush();
                            wait_for_batch_close(worker, group_mutation, worker_lock);
                            if (!healthy() || !group_mutation.completed) {
                                return mutation_failure(
                                    DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                            }
                        } else if (auto flushed = flush_worker_batch(worker, SegmentCommitSync::immediate);
                                   !flushed) {
                            return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                        }
                    } else {
                        catalog_lock.unlock();
                        wait_for_batch_close(worker, group_mutation, worker_lock);
                        if (!healthy() || !group_mutation.completed) {
                            return mutation_failure(
                                DurableMutationOutcome::indeterminate,
                                Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
                        }
                    }
                } else if (should_flush_batch(worker)) {
                    if (auto flushed = flush_worker_batch(worker, SegmentCommitSync::deferred); !flushed) {
                        return mutation_failure(DurableMutationOutcome::indeterminate, flushed.error());
                    }
                } else if (flusher_) {
                    flusher_->notify_batch_activity();
                }
            } else if (deferred_commit && flusher_) {
                flusher_->notify_batch_activity();
            }

            if (strict_batch) {
                final_record_committed = true;
                return {.outcome = DurableMutationOutcome::committed,
                        .sequence = committed_sequence,
                        .error = std::nullopt};
            }
            if (opcode == Opcode::put) {
                const auto published = worker.index.insert_or_assign(hashed, reference);
                if (!published) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = published.error()};
                }
                if (auto counted = worker.update_live_record_bytes(published->previous, reference);
                    !counted) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = counted.error()};
                }
                if (prepared_hot_record.empty()) {
                    worker.erase_hot_record(hashed);
                } else if (auto hot_published =
                               worker.publish_hot_record(prepared_hot_record, reference, committed_sequence);
                           !hot_published) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = hot_published.error()};
                }
            } else {
                const auto erased = worker.index.erase_no_compact(hashed);
                if (auto counted = worker.update_live_record_bytes(erased.previous, std::nullopt); !counted) {
                    healthy_.store(false, std::memory_order_release);
                    return {.outcome = DurableMutationOutcome::committed,
                            .sequence = committed_sequence,
                            .error = counted.error()};
                }
                worker.erase_hot_record(hashed);
            }
            final_record_committed = true;
            return {.outcome = DurableMutationOutcome::committed,
                    .sequence = committed_sequence,
                    .error = std::nullopt};
        }
        return mutation_failure(
            DurableMutationOutcome::not_committed,
            Error{ErrorCode::segment_full, "Record does not fit after one durable rotation"});
    } catch (const std::bad_alloc&) {
        if (exception_outcome != DurableMutationOutcome::not_committed) {
            abandon_pending_batches();
        }
        return mutation_failure(exception_outcome, Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        if (exception_outcome != DurableMutationOutcome::not_committed) {
            abandon_pending_batches();
        }
        return mutation_failure(exception_outcome, Error{ErrorCode::internal_error, {}});
    }
}

auto DurableRuntimeCatalog::healthy() const noexcept -> bool {
    return !closed_.load(std::memory_order_acquire) && healthy_.load(std::memory_order_acquire) &&
           directory_.healthy();
}

void DurableRuntimeCatalog::mark_fail_closed() noexcept {
    abandon_pending_batches();
}

auto DurableRuntimeCatalog::worker_count() const noexcept -> std::size_t {
    return workers_.size();
}

auto DurableRuntimeCatalog::manifest() const -> Manifest {
    const std::shared_lock lock{catalog_mutex_};
    return manifest_;
}

auto DurableRuntimeCatalog::namespace_audit() const -> NamespaceAuditReport {
    const std::shared_lock lock{catalog_mutex_};
    return namespace_audit_;
}

auto DurableRuntimeCatalog::recovery_stats() const noexcept -> const DurableRecoveryStats& {
    return recovery_stats_;
}

auto DurableRuntimeCatalog::hot_cache_stats() const -> std::vector<DurableHotCacheWorkerStats> {
    std::vector<DurableHotCacheWorkerStats> result;
    result.reserve(workers_.size());
    for (std::size_t index = 0; index < workers_.size(); ++index) {
        auto& worker = *workers_[index];
        const auto& metrics = worker.get_path_metrics;
        std::size_t resident_entries{};
        std::uint64_t resident_bytes{};
        std::size_t staged_entries{};
        std::uint64_t staged_bytes{};
        std::uint64_t table_bytes{};
        std::uint64_t total_accounted{};
        {
            const std::lock_guard lock{worker.mutex};
            resident_entries = worker.hot_records.size();
            resident_bytes = worker.hot_record_resident_bytes;
            staged_entries = worker.hot_record_staged_entries;
            staged_bytes = worker.hot_record_staged_bytes;
            table_bytes = hot_cache_table_bytes(worker.hot_records.capacity());
            total_accounted = worker.hot_cache_total_bytes();
        }
        const auto hits = metrics.hot_hits.load(std::memory_order_relaxed);
        const auto misses = metrics.hot_misses.load(std::memory_order_relaxed);
        result.push_back({
            .worker_id = worker.worker_id,
            .resident_entries = resident_entries,
            .resident_bytes = resident_bytes,
            .staged_entries = staged_entries,
            .staged_bytes = staged_bytes,
            .bucket_bytes = table_bytes,
            .total_accounted_bytes = total_accounted,
            .byte_budget = hot_cache_worker_budget(index, workers_.size(), options_.limits),
            .staging_byte_budget = options_.limits.max_hot_cache_staging_bytes_per_worker,
            .entry_budget = options_.limits.max_hot_cache_entries_per_worker,
            .hits = hits,
            .misses = misses,
            .stale_hits = metrics.hot_stale_hits.load(std::memory_order_relaxed),
            .evictions = metrics.hot_evictions.load(std::memory_order_relaxed),
            .admission_bypasses = metrics.admission_bypasses.load(std::memory_order_relaxed),
            .size_rejected = metrics.size_rejected.load(std::memory_order_relaxed),
            .expired_gets = metrics.expired_ttl_gets.load(std::memory_order_relaxed),
            .hit_rate_bp = (hits + misses) == 0 ? 0 : (hits * 10'000ULL) / (hits + misses),
            .enabled = options_.limits.hot_cache_enabled &&
                       hot_cache_worker_budget(index, workers_.size(), options_.limits) > 0 &&
                       options_.limits.max_hot_cache_entries_per_worker > 0 &&
                       options_.limits.max_hot_cache_value_bytes > 0,
            .max_value_bytes = options_.limits.max_hot_cache_value_bytes,
        });
    }
    return result;
}

auto DurableRuntimeCatalog::get_path_stats() const -> std::vector<DurableGetPathWorkerStats> {
    std::vector<DurableGetPathWorkerStats> result;
    result.reserve(workers_.size());
    for (const auto& worker : workers_) {
        const auto& metrics = worker->get_path_metrics;
        std::size_t resident_entries{};
        std::uint64_t resident_bytes{};
        {
            const std::lock_guard lock{worker->mutex};
            resident_entries = worker->hot_records.size();
            resident_bytes = worker->hot_record_resident_bytes;
        }
        result.push_back({
            .worker_id = worker->worker_id,
            .prepare_calls = metrics.prepare_calls.load(std::memory_order_relaxed),
            .complete_calls = metrics.complete_calls.load(std::memory_order_relaxed),
            .mutex_wait_ns = metrics.mutex_wait_ns.load(std::memory_order_relaxed),
            .prepare_hold_ns = metrics.prepare_hold_ns.load(std::memory_order_relaxed),
            .complete_revalidate_hold_ns =
                metrics.complete_revalidate_hold_ns.load(std::memory_order_relaxed),
            .index_lookup_ns = metrics.index_lookup_ns.load(std::memory_order_relaxed),
            .hot_cache_lookup_ns = metrics.hot_cache_lookup_ns.load(std::memory_order_relaxed),
            .generation_pin_lookup_ns = metrics.generation_pin_lookup_ns.load(std::memory_order_relaxed),
            .cold_read_ns = metrics.cold_read_ns.load(std::memory_order_relaxed),
            .crc_value_copy_ns = metrics.crc_value_copy_ns.load(std::memory_order_relaxed),
            .relinearization_retries = metrics.relinearization_retries.load(std::memory_order_relaxed),
            .hot_hits = metrics.hot_hits.load(std::memory_order_relaxed),
            .hot_misses = metrics.hot_misses.load(std::memory_order_relaxed),
            .hot_stale = metrics.hot_stale_hits.load(std::memory_order_relaxed),
            .hot_evictions = metrics.hot_evictions.load(std::memory_order_relaxed),
            .expired_ttl_gets = metrics.expired_ttl_gets.load(std::memory_order_relaxed),
            .hot_resident_entries = resident_entries,
            .hot_resident_bytes = resident_bytes,
        });
    }
    return result;
}

auto DurableRuntimeCatalog::batch_stats() const -> std::vector<DurableBatchWorkerStats> {
    std::vector<DurableBatchWorkerStats> result;
    result.reserve(workers_.size());
    for (const auto& worker : workers_) {
        const auto& metrics = worker->batch_metrics;
        result.push_back({
            .worker_id = worker->worker_id,
            .enabled = options_.batch.has_value(),
            .pending_records = metrics.pending_records.load(std::memory_order_relaxed),
            .pending_bytes = metrics.pending_bytes.load(std::memory_order_relaxed),
            .current_record_target = metrics.current_record_target.load(std::memory_order_relaxed),
            .flush_attempts = metrics.flush_attempts.load(std::memory_order_relaxed),
            .committed_batches = metrics.committed_batches.load(std::memory_order_relaxed),
            .failed_batches = metrics.failed_batches.load(std::memory_order_relaxed),
            .committed_records = metrics.committed_records.load(std::memory_order_relaxed),
            .committed_bytes = metrics.committed_bytes.load(std::memory_order_relaxed),
            .maximum_batch_records = metrics.maximum_batch_records.load(std::memory_order_relaxed),
            .maximum_batch_bytes = metrics.maximum_batch_bytes.load(std::memory_order_relaxed),
            .total_commit_duration_ns = metrics.total_commit_duration_ns.load(std::memory_order_relaxed),
            .maximum_commit_duration_ns = metrics.maximum_commit_duration_ns.load(std::memory_order_relaxed),
            .record_limit_closes = metrics.record_limit_closes.load(std::memory_order_relaxed),
            .byte_limit_closes = metrics.byte_limit_closes.load(std::memory_order_relaxed),
            .adaptive_target_closes = metrics.adaptive_target_closes.load(std::memory_order_relaxed),
            .deadline_closes = metrics.deadline_closes.load(std::memory_order_relaxed),
        });
    }
    return result;
}

void DurableRuntimeCatalog::record_rotation_final_commit(const std::uint64_t duration_ns,
                                                         const bool committed) noexcept {
    const auto previous_version = begin_atomic_stats_publication(rotation_stats_version_);
    rotation_final_record_commit_attempts_.fetch_add(1U, std::memory_order_relaxed);
    if (committed) {
        rotation_final_record_commits_.fetch_add(1U, std::memory_order_relaxed);
    }
    last_rotation_final_record_commit_ns_.store(duration_ns, std::memory_order_relaxed);
    atomic_saturating_add(total_rotation_final_record_commit_ns_, duration_ns);
    atomic_observe_max(maximum_rotation_final_record_commit_ns_, duration_ns);
    end_atomic_stats_publication(rotation_stats_version_, previous_version);
}

auto DurableRuntimeCatalog::rotation_stats() const noexcept -> DurableRotationStats {
    while (true) {
        const auto before = rotation_stats_version_.load(std::memory_order_acquire);
        if ((before & 1U) != 0) {
            continue;
        }
        const DurableRotationStats result{
            .attempts = rotation_attempts_.load(std::memory_order_relaxed),
            .committed = rotations_committed_.load(std::memory_order_relaxed),
            .compaction_waits = rotation_compaction_waits_.load(std::memory_order_relaxed),
            .final_record_commit_attempts =
                rotation_final_record_commit_attempts_.load(std::memory_order_relaxed),
            .final_record_commits = rotation_final_record_commits_.load(std::memory_order_relaxed),
            .last_publication_wait_duration_ns =
                last_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .total_publication_wait_duration_ns =
                total_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .maximum_publication_wait_duration_ns =
                maximum_rotation_publication_wait_ns_.load(std::memory_order_relaxed),
            .last_seal_duration_ns = last_rotation_seal_ns_.load(std::memory_order_relaxed),
            .total_seal_duration_ns = total_rotation_seal_ns_.load(std::memory_order_relaxed),
            .maximum_seal_duration_ns = maximum_rotation_seal_ns_.load(std::memory_order_relaxed),
            .last_create_duration_ns = last_rotation_create_ns_.load(std::memory_order_relaxed),
            .total_create_duration_ns = total_rotation_create_ns_.load(std::memory_order_relaxed),
            .maximum_create_duration_ns = maximum_rotation_create_ns_.load(std::memory_order_relaxed),
            .last_manifest_publication_duration_ns =
                last_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .total_manifest_publication_duration_ns =
                total_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .maximum_manifest_publication_duration_ns =
                maximum_rotation_manifest_publication_ns_.load(std::memory_order_relaxed),
            .last_execution_duration_ns = last_rotation_execution_ns_.load(std::memory_order_relaxed),
            .total_execution_duration_ns = total_rotation_execution_ns_.load(std::memory_order_relaxed),
            .maximum_execution_duration_ns =
                maximum_rotation_execution_ns_.load(std::memory_order_relaxed),
            .last_total_duration_ns = last_rotation_total_ns_.load(std::memory_order_relaxed),
            .total_duration_ns = total_rotation_ns_.load(std::memory_order_relaxed),
            .maximum_total_duration_ns = maximum_rotation_total_ns_.load(std::memory_order_relaxed),
            .last_final_record_commit_duration_ns =
                last_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
            .total_final_record_commit_duration_ns =
                total_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
            .maximum_final_record_commit_duration_ns =
                maximum_rotation_final_record_commit_ns_.load(std::memory_order_relaxed),
        };
        if (before == rotation_stats_version_.load(std::memory_order_acquire)) {
            return result;
        }
    }
}

auto DurableRuntimeCatalog::next_sequence(const std::size_t worker_index) const -> Result<SequenceNumber> {
    if (worker_index >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "Worker index is outside the durable runtime");
    }
    const std::lock_guard lock{workers_[worker_index]->mutex};
    return workers_[worker_index]->next_sequence;
}

auto DurableRuntimeCatalog::active_segment(const std::size_t worker_index) const -> Result<SegmentId> {
    if (worker_index >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "Worker index is outside the durable runtime");
    }
    const std::lock_guard lock{workers_[worker_index]->mutex};
    return workers_[worker_index]->active_segment;
}

auto DurableRuntimeCatalog::next_compaction_worker(const std::size_t start_worker) const
    -> Result<std::optional<std::size_t>> {
    if (start_worker >= workers_.size()) {
        return fail(ErrorCode::invalid_argument, "compaction cursor is outside the durable runtime");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    std::optional<std::size_t> candidate;
    auto best_distance = workers_.size();
    for (const auto& entry : manifest_.segments) {
        if (entry.role != ManifestSegmentRole::sealed) {
            continue;
        }
        const auto owner = static_cast<std::size_t>(entry.owner_worker.value);
        if (owner >= workers_.size()) {
            return fail(ErrorCode::corrupted_data, "sealed Segment owner is outside the durable runtime");
        }
        const auto distance =
            owner >= start_worker ? owner - start_worker : workers_.size() - start_worker + owner;
        if (distance < best_distance) {
            candidate = owner;
            best_distance = distance;
            if (distance == 0) {
                break;
            }
        }
    }
    return candidate;
}

auto DurableRuntimeCatalog::maintenance_observation(const std::size_t start_worker,
                                                      const std::uint64_t now_ns,
                                                      const bool probe_unread_expired_ttl)
    -> Result<MaintenanceObservation> {
    if (start_worker >= workers_.size()) {
        return fail(ErrorCode::invalid_argument,
                    "maintenance compaction cursor is outside the durable runtime");
    }
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const std::shared_lock catalog_lock{catalog_mutex_};
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }

    MaintenanceObservation observation{
        .durable = true,
        .segment_count = manifest_.segments.size(),
        .max_segment_count = options_.limits.max_segment_count,
        .reserved_free_bytes = options_.limits.reserved_free_bytes,
    };
    if (segments_.size() != manifest_.segments.size()) {
        return fail(ErrorCode::corrupted_data, "maintenance observation catalog metadata is not aligned");
    }
    auto best_distance = workers_.size();
    for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
        const auto& entry = manifest_.segments[index];
        if (entry.role == ManifestSegmentRole::sealed) {
            ++observation.sealed_segment_count;
            const auto owner = static_cast<std::size_t>(entry.owner_worker.value);
            if (owner >= workers_.size()) {
                return fail(ErrorCode::corrupted_data, "sealed Segment owner is outside the durable runtime");
            }
            const auto distance =
                owner >= start_worker ? owner - start_worker : workers_.size() - start_worker + owner;
            if (distance < best_distance) {
                observation.compaction_candidate_worker = owner;
                best_distance = distance;
            }
        }
    }
    if (observation.compaction_candidate_worker) {
        const auto candidate = *observation.compaction_candidate_worker;
        for (std::size_t index = 0; index < manifest_.segments.size(); ++index) {
            const auto& entry = manifest_.segments[index];
            if (entry.role != ManifestSegmentRole::sealed || entry.owner_worker.value != candidate) {
                continue;
            }
            const auto committed_end = segments_[index].selected.commit.committed_end;
            if (committed_end < kSegmentHeaderReservedBytes || committed_end > kSegmentSizeBytes) {
                return fail(ErrorCode::corrupted_data,
                            "sealed Segment committed extent is outside v1 bounds");
            }
            const auto record_bytes = static_cast<std::uint64_t>(committed_end - kSegmentHeaderReservedBytes);
            if (record_bytes >
                std::numeric_limits<std::uint64_t>::max() - observation.candidate_sealed_record_bytes) {
                return fail(ErrorCode::arithmetic_overflow,
                            "maintenance sealed Record byte count overflows uint64_t");
            }
            observation.candidate_sealed_record_bytes += record_bytes;
        }
        observation.candidate_live_record_bytes =
            workers_[candidate]->sealed_live_record_bytes.load(std::memory_order_acquire);
        if (observation.candidate_live_record_bytes > observation.candidate_sealed_record_bytes) {
            return fail(ErrorCode::corrupted_data,
                        "maintenance live Record bytes exceed sealed committed bytes");
        }
        observation.candidate_dead_record_bytes =
            observation.candidate_sealed_record_bytes - observation.candidate_live_record_bytes;
        observation.candidate_dead_byte_ratio_bp =
            observation.candidate_sealed_record_bytes == 0
                ? 10'000U
                : static_cast<std::uint32_t>(observation.candidate_dead_record_bytes * 10'000U /
                                             observation.candidate_sealed_record_bytes);
    }
    // Match rotate_active: require_durable_available_space(kSegmentSizeBytes + next_manifest_bytes).
    const auto next_count = observation.segment_count + 1U;
    if (auto next_manifest = durable_manifest_bytes(next_count); next_manifest) {
        if (*next_manifest <= std::numeric_limits<std::uint64_t>::max() - kSegmentSizeBytes) {
            observation.rotate_additional_bytes = kSegmentSizeBytes + *next_manifest;
        } else {
            observation.rotate_additional_bytes = std::numeric_limits<std::uint64_t>::max();
        }
    } else {
        observation.rotate_additional_bytes = std::numeric_limits<std::uint64_t>::max();
    }
    if (auto free = directory_.available_space_bytes(); free) {
        observation.available_free_bytes = *free;
    } else if (free.error().code != ErrorCode::unavailable && free.error().code != ErrorCode::io_error) {
        return unexpected(free.error());
    }

    if (probe_unread_expired_ttl && now_ns != 0 && observation.compaction_candidate_worker) {
        struct ProbeContext {
            std::uint64_t now_ns{};
            std::uint64_t key_hash{};
            bool expired{};
        };
        const RecordVisitor probe_visitor = [](void* opaque, const RecordView& record) -> Status {
            auto& context = *static_cast<ProbeContext*>(opaque);
            if (record.opcode != Opcode::put || record.key_hash != context.key_hash) {
                return fail(ErrorCode::corrupted_data,
                            "unread TTL probe Index entry disagrees with its source Record");
            }
            context.expired = record.expired(context.now_ns);
            return {};
        };

        const auto candidate = *observation.compaction_candidate_worker;
        auto& worker = *workers_[candidate];
        std::lock_guard worker_lock{worker.mutex};
        const std::shared_lock probe_catalog_lock{catalog_mutex_};
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }
        if (generation_pins_.size() != manifest_.segments.size()) {
            return fail(ErrorCode::corrupted_data, "unread TTL probe catalog metadata is not aligned");
        }

        for (const auto& entry : worker.index.entries()) {
            if (entry.record.segment_id == worker.active_segment) {
                continue;
            }
            const auto found = std::lower_bound(
                manifest_.segments.begin(), manifest_.segments.end(), entry.record.segment_id,
                [](const ManifestSegmentEntry& segment, const SegmentId id) {
                    return segment.segment_id.value < id.value;
                });
            if (found == manifest_.segments.end() || found->segment_id != entry.record.segment_id ||
                found->role != ManifestSegmentRole::sealed || found->owner_worker.value != candidate) {
                continue;
            }
            const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
            const auto& pin = generation_pins_[catalog_index];
            if (!pin || pin->identity.segment_id != entry.record.segment_id ||
                pin->identity.generation != entry.record.generation ||
                pin->identity.owner_worker != worker.worker_id) {
                return fail(ErrorCode::corrupted_data,
                            "unread TTL probe generation pin disagrees with the Index");
            }
            ProbeContext context{.now_ns = now_ns, .key_hash = hash_key(entry.key)};
            if (auto visited = pin->file.visit_runtime_record(entry.record, &context, probe_visitor); !visited) {
                return unexpected(visited.error());
            }
            if (!context.expired) {
                continue;
            }
            ++observation.candidate_unread_expired_sealed_record_count;
            if (entry.record.size.value >
                std::numeric_limits<std::uint64_t>::max() -
                    observation.candidate_unread_expired_sealed_record_bytes) {
                return fail(ErrorCode::arithmetic_overflow,
                            "unread TTL probe sealed Record byte count overflows uint64_t");
            }
            observation.candidate_unread_expired_sealed_record_bytes += entry.record.size.value;
        }
        observation.unread_ttl_probe_performed = true;
    }
    return observation;
}

auto DurableRuntimeCatalog::compact_worker(const std::size_t worker_index, const std::uint64_t now_ns,
                                           const std::uint64_t max_copy_bytes) -> DurableCompactionResult {
    bool recovery_required{};
    DurableCompactionCopyStats stats{};
    const auto notify_fail_closed = [&] {
        healthy_.store(false, std::memory_order_release);
        for (const auto& worker : workers_) {
            worker->batch_closed.notify_all();
        }
    };
    const auto failure = [&](Error error) {
        if (recovery_required) {
            notify_fail_closed();
        }
        return DurableCompactionResult{
            .outcome = recovery_required ? DurableCompactionOutcome::recovery_required
                                         : DurableCompactionOutcome::not_compacted,
            .stats = stats,
            .error = std::move(error),
        };
    };

    try {
        if (worker_index >= workers_.size()) {
            return failure(Error{ErrorCode::invalid_argument, "Worker index is outside the durable runtime"});
        }
        if (!healthy()) {
            return failure(Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }

        auto& worker = *workers_[worker_index];
        Manifest snapshot;
        SequenceNumber snapshot_next_sequence{};
        std::vector<IndexEntry> snapshot_entries;
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> source_pins;

        // Phase A: capture only owning state. No file operation is allowed in
        // this scope. The complete Index enumeration is currently necessary
        // because the replacement Index must retain active-Segment references.
        {
            std::unique_lock worker_lock{worker.mutex};
            if (dedicated_commit_executor_) {
                worker.batch_closed.wait(worker_lock, [&] { return !worker.batch_closing || !healthy(); });
            }
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                return failure(Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
            }
            if (!worker.pending_group_mutations.empty() || worker.batch_closing) {
                return failure(Error{ErrorCode::sequence_conflict,
                                     "durable compaction cannot snapshot a pending group publication"});
            }
            const auto sealed_live_record_bytes =
                worker.sealed_live_record_bytes.load(std::memory_order_relaxed);
            if (max_copy_bytes > 0 && sealed_live_record_bytes > max_copy_bytes) {
                return failure(Error{ErrorCode::sequence_conflict,
                                     "durable compaction candidate exceeds its maintenance copy budget"});
            }
            snapshot = manifest_;
            snapshot_next_sequence = worker.next_sequence;
            const auto index_size = worker.index.stats().size;
            snapshot_entries = worker.index.entries();
            if (snapshot_entries.size() != index_size || generation_pins_.size() != segments_.size() ||
                segments_.size() != snapshot.segments.size()) {
                return failure(
                    Error{ErrorCode::corrupted_data, "durable compaction snapshot is not catalog-aligned"});
            }
            for (std::size_t index = 0; index < snapshot.segments.size(); ++index) {
                const auto& entry = snapshot.segments[index];
                if (entry.owner_worker != worker.worker_id || entry.role != ManifestSegmentRole::sealed) {
                    continue;
                }
                const auto& pin = generation_pins_[index];
                if (!pin || pin->identity.segment_id != entry.segment_id ||
                    pin->identity.generation != entry.generation ||
                    pin->identity.owner_worker != entry.owner_worker) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "durable compaction source has no matching generation pin"});
                }
                source_pins.push_back(pin);
            }
        }

        if (source_pins.empty()) {
            return failure(Error{ErrorCode::not_found, "durable Worker has no sealed Segments to compact"});
        }

        // Reserve the right to publish this exact old/next authority pair, but
        // do not retain the serializer while the builder performs file I/O.
        // Rotations observe the lease and fail with a finite conflict instead
        // of waiting behind the complete compaction build.
        {
            const std::lock_guard publication_lock{manifest_publication_mutex_};
            if (compaction_publication_active_) {
                return failure(
                    Error{ErrorCode::sequence_conflict, "another durable compaction publication is active"});
            }
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (manifest_ != snapshot) {
                return failure(
                    Error{ErrorCode::sequence_conflict, "manifest changed before compaction build"});
            }
            compaction_publication_active_ = true;
        }
        struct PublicationLease final {
            DurableRuntimeCatalog& runtime;

            explicit PublicationLease(DurableRuntimeCatalog& owner) noexcept : runtime(owner) {}
            ~PublicationLease() {
                {
                    const std::lock_guard lock{runtime.manifest_publication_mutex_};
                    runtime.compaction_publication_active_ = false;
                }
                runtime.manifest_publication_changed_.notify_all();
            }

            PublicationLease(const PublicationLease&) = delete;
            auto operator=(const PublicationLease&) -> PublicationLease& = delete;
        } publication_lease{*this};

        {
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (manifest_ != snapshot) {
                return failure(
                    Error{ErrorCode::sequence_conflict, "manifest changed before compaction build"});
            }
        }

        // Phase B: scans, CRC checks, allocation, Record copies, file writes,
        // and replacement verification execute without Worker/catalog locks.
        auto built = build_durable_worker_compaction(directory_, snapshot, worker.worker_id,
                                                     std::move(snapshot_entries), now_ns, options_.limits);
        if (!built.succeeded()) {
            if (built.outcome == DurableCompactionBuildOutcome::not_beneficial) {
                return {.outcome = DurableCompactionOutcome::not_beneficial,
                        .stats = built.stats,
                        .error = std::move(built.error)};
            }
            recovery_required = built.outcome == DurableCompactionBuildOutcome::recovery_required;
            return failure(built.error.value_or(
                Error{ErrorCode::io_error, "durable compaction replacement build failed"}));
        }
        recovery_required = true;
        auto prepared = std::move(*built.prepared);
        stats = prepared.stats;
        auto sources = std::move(prepared.plan.sources);
        const auto abort_prepared = [&](Error reason) -> DurableCompactionResult {
            auto rolled_back = rollback_prepared_compaction(directory_, snapshot, prepared.plan.replacements,
                                                            options_.limits);
            if (!rolled_back) {
                return failure(rolled_back.error());
            }
            {
                const std::unique_lock catalog_lock{catalog_mutex_};
                namespace_audit_ = std::move(*rolled_back);
            }
            recovery_required = false;
            return failure(std::move(reason));
        };
        if (prepared.replacement_commits.size() != prepared.plan.replacements.size()) {
            return abort_prepared(
                Error{ErrorCode::internal_error, "compaction replacement commit catalog is incomplete"});
        }
        std::vector<RecoveredSegmentState> next_segments;
        next_segments.reserve(prepared.plan.next_manifest.segments.size());
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> replacement_pins;
        replacement_pins.reserve(prepared.plan.replacements.size());
        for (std::size_t index = 0; index < prepared.plan.replacements.size(); ++index) {
            const auto& entry = prepared.plan.replacements[index];
            const SegmentHeaderIdentity identity{.store_id = snapshot.store_id,
                                                 .segment_id = entry.segment_id,
                                                 .generation = entry.generation,
                                                 .owner_worker = entry.owner_worker};
            auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_only);
            if (!opened) {
                return abort_prepared(opened.error());
            }
            if (opened->selected_commit() != prepared.replacement_commits[index]) {
                return abort_prepared(
                    Error{ErrorCode::corrupted_data,
                          "compaction replacement changed before generation pin publication"});
            }
            replacement_pins.push_back(
                std::make_shared<const RuntimeSegmentGeneration>(RuntimeSegmentGeneration{
                    .identity = identity,
                    .selected = prepared.replacement_commits[index],
                    .file = std::move(*opened),
                }));
        }
        std::vector<std::shared_ptr<const RuntimeSegmentGeneration>> next_generation_pins;
        next_generation_pins.reserve(prepared.plan.next_manifest.segments.size());
        std::vector<std::size_t> retained_old_indices;
        retained_old_indices.reserve(prepared.plan.next_manifest.segments.size());
        auto installed_manifest = prepared.plan.next_manifest;

        // Phase C first validates and prepares a non-allocating publication
        // while locked. The durable manifest write happens only after the
        // target Worker is logically quiesced and every physical mutex has
        // been released.
        std::unique_lock worker_lock{worker.mutex, std::try_to_lock};
        if (!worker_lock.owns_lock()) {
            return abort_prepared(
                Error{ErrorCode::sequence_conflict, "Worker changed while compaction was prepared"});
        }
        std::unique_lock catalog_lock{catalog_mutex_};
        bool sources_still_pinned = source_pins.size() == sources.size();
        for (std::size_t source_index = 0; sources_still_pinned && source_index < sources.size();
             ++source_index) {
            const auto found = std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(),
                                                sources[source_index].segment_id,
                                                [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                                    return entry.segment_id.value < id.value;
                                                });
            if (found == manifest_.segments.end() || *found != sources[source_index]) {
                sources_still_pinned = false;
                break;
            }
            const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
            sources_still_pinned = catalog_index < generation_pins_.size() &&
                                   generation_pins_[catalog_index] == source_pins[source_index];
        }
        if (!healthy() || manifest_ != snapshot || segments_.size() != snapshot.segments.size() ||
            worker.next_sequence != snapshot_next_sequence || !worker.pending_group_mutations.empty() ||
            worker.batch_closing || !sources_still_pinned) {
            catalog_lock.unlock();
            worker_lock.unlock();
            return abort_prepared(
                Error{ErrorCode::sequence_conflict, "runtime state changed during durable compaction"});
        }

        {
            std::size_t replacement_index{};
            for (const auto& entry : prepared.plan.next_manifest.segments) {
                if (replacement_index < prepared.plan.replacements.size() &&
                    entry == prepared.plan.replacements[replacement_index]) {
                    next_segments.push_back({.selected = prepared.replacement_commits[replacement_index]});
                    next_generation_pins.push_back(replacement_pins[replacement_index]);
                    retained_old_indices.push_back(std::numeric_limits<std::size_t>::max());
                    ++replacement_index;
                    continue;
                }
                const auto found =
                    std::lower_bound(snapshot.segments.begin(), snapshot.segments.end(), entry.segment_id,
                                     [](const ManifestSegmentEntry& candidate, const SegmentId id) {
                                         return candidate.segment_id.value < id.value;
                                     });
                if (found == snapshot.segments.end() || *found != entry) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "compaction retained entry is absent from the old catalog"});
                }
                const auto old_index = static_cast<std::size_t>(found - snapshot.segments.begin());
                next_segments.push_back(segments_[old_index]);
                if (old_index >= generation_pins_.size() || !generation_pins_[old_index]) {
                    return failure(Error{ErrorCode::corrupted_data,
                                         "retained compaction Segment has no generation pin"});
                }
                next_generation_pins.push_back(generation_pins_[old_index]);
                retained_old_indices.push_back(old_index);
            }
            if (replacement_index != prepared.plan.replacements.size() ||
                next_segments.size() != prepared.plan.next_manifest.segments.size() ||
                next_generation_pins.size() != prepared.plan.next_manifest.segments.size() ||
                retained_old_indices.size() != prepared.plan.next_manifest.segments.size()) {
                return failure(
                    Error{ErrorCode::internal_error, "compaction runtime catalog preparation is incomplete"});
            }
        }

        worker.compaction_commit_active = true;
        struct WorkerCommitGate final {
            RuntimeWorker& worker;
            bool active{true};

            explicit WorkerCommitGate(RuntimeWorker& owner) noexcept : worker(owner) {}
            ~WorkerCommitGate() {
                if (!active) {
                    return;
                }
                const std::lock_guard lock{worker.mutex};
                worker.compaction_commit_active = false;
                worker.compaction_commit_finished.notify_all();
            }
            void clear_locked() noexcept {
                worker.compaction_commit_active = false;
                active = false;
                worker.compaction_commit_finished.notify_all();
            }

            WorkerCommitGate(const WorkerCommitGate&) = delete;
            auto operator=(const WorkerCommitGate&) -> WorkerCommitGate& = delete;
        } commit_gate{worker};
        catalog_lock.unlock();
        worker_lock.unlock();

        const auto published =
            directory_.publish_manifest(prepared.plan.next_manifest, options_.limits.max_manifest_bytes);
        if (!published.durable()) {
            return failure(published.error.value_or(
                Error{ErrorCode::io_error, "compaction manifest publication failed"}));
        }

        try {
            worker_lock.lock();
            catalog_lock.lock();
            for (std::size_t next_index = 0; next_index < retained_old_indices.size(); ++next_index) {
                const auto old_index = retained_old_indices[next_index];
                if (old_index == std::numeric_limits<std::size_t>::max()) {
                    continue;
                }
                next_segments[next_index] = segments_[old_index];
                next_generation_pins[next_index] = generation_pins_[old_index];
            }
            manifest_ = std::move(prepared.plan.next_manifest);
            segments_ = std::move(next_segments);
            generation_pins_ = std::move(next_generation_pins);
            rebuild_pin_slot_index();
            worker.index = std::move(prepared.index);
            worker.active_live_record_bytes.store(prepared.active_live_record_bytes,
                                                  std::memory_order_release);
            worker.sealed_live_record_bytes.store(stats.bytes_copied, std::memory_order_release);
            commit_gate.clear_locked();
        } catch (...) {
            if (worker_lock.owns_lock()) {
                commit_gate.clear_locked();
            }
            if (catalog_lock.owns_lock()) {
                catalog_lock.unlock();
            }
            if (worker_lock.owns_lock()) {
                worker_lock.unlock();
            }
            throw;
        }
        catalog_lock.unlock();
        worker_lock.unlock();

        const auto retired = directory_.retire_compaction_segments(snapshot.store_id, sources);
        if (!retired.durable()) {
            return failure(
                retired.error.value_or(Error{ErrorCode::io_error, "compaction source retirement failed"}));
        }
        const auto removed = directory_.remove_compaction_intent();
        if (!removed.durable()) {
            return failure(
                removed.error.value_or(Error{ErrorCode::io_error, "compaction intent removal failed"}));
        }
        auto audit = audit_data_directory(directory_, installed_manifest);
        if (!audit) {
            return failure(audit.error());
        }
        if (auto safe = validate_namespace_for_recovery(*audit); !safe) {
            return failure(safe.error());
        }
        {
            const std::unique_lock audit_lock{catalog_mutex_};
            namespace_audit_ = std::move(*audit);
        }
        recovery_required = false;
        return {.outcome = DurableCompactionOutcome::compacted, .stats = stats, .error = std::nullopt};
    } catch (const std::bad_alloc&) {
        return failure(Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        return failure(Error{ErrorCode::internal_error, {}});
    }
}

auto DurableRuntimeCatalog::snapshot_live_keys() -> Result<std::vector<std::string>> {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    std::vector<std::string> keys;
    for (std::size_t worker_index = 0; worker_index < workers_.size(); ++worker_index) {
        auto& worker = *workers_[worker_index];
        const std::lock_guard lock{worker.mutex};
        const std::shared_lock catalog_lock{catalog_mutex_};
        if (!healthy()) {
            return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
        }
        auto entries = worker.index.entries();
        for (auto& entry : entries) {
            if (route_worker(entry.key, workers_.size()) != worker_index) {
                return fail(ErrorCode::corrupted_data, "durable Index entry is routed to the wrong Worker");
            }
            keys.push_back(std::move(entry.key));
        }
    }
    std::sort(keys.begin(), keys.end());
    return keys;
}

auto DurableRuntimeCatalog::verify_index() -> Status {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    auto keys = snapshot_live_keys();
    if (!keys) {
        return unexpected(keys.error());
    }
    for (const auto& key : *keys) {
        if (auto verified = get(key); !verified) {
            return unexpected(verified.error());
        }
    }
    return {};
}

} // namespace glyphastore
