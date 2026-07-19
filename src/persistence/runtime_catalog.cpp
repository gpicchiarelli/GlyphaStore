#include "glyphastore/persistence/runtime_catalog.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "glyphastore/persistence/compaction.hpp"
#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"
#include "persistence/adaptive_batch_sizer.hpp"
#include "persistence/hot_record_capacity.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>

namespace glyphastore {
namespace {

[[nodiscard]] auto as_string_view(const std::span<const std::byte> bytes) noexcept -> std::string_view {
    if (bytes.empty()) {
        return {};
    }
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

struct ReadContext {
    std::span<const std::byte> expected_key;
    std::uint64_t expected_hash{};
    std::uint64_t now_ns{};
    OwnedValue value;
};

auto copy_verified_value(void* opaque, const RecordView& record) -> Status {
    auto& context = *static_cast<ReadContext*>(opaque);
    if (record.opcode != Opcode::put) {
        return fail(ErrorCode::corrupted_data, "durable Index references a non-value Record");
    }
    if (record.key_hash != context.expected_hash || !std::ranges::equal(record.key, context.expected_key)) {
        return fail(ErrorCode::corrupted_data, "durable Index key does not match its referenced Record");
    }
    if (record.expired(context.now_ns)) {
        return fail(ErrorCode::not_found, "key has expired");
    }
    context.value = {
        .bytes = std::vector<std::byte>{record.value.begin(), record.value.end()},
        .sequence = record.sequence.value,
        .expire_at_ns = record.expire_at_ns,
    };
    return {};
}

auto mutation_failure(const DurableMutationOutcome outcome, Error error) -> DurableMutationResult {
    return {.outcome = outcome, .sequence = std::nullopt, .error = std::move(error)};
}

struct HotRecordEntry {
    RecordRef reference{};
    std::vector<std::byte> value_bytes;
    SequenceNumber sequence{};
    std::uint64_t expire_at_ns{};
};

struct TransparentStringHash {
    using is_transparent = void;

    [[nodiscard]] auto operator()(const std::string_view value) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(value);
    }

    [[nodiscard]] auto operator()(const std::string& value) const noexcept -> std::size_t {
        return (*this)(std::string_view{value});
    }
};

struct TransparentStringEqual {
    using is_transparent = void;

    [[nodiscard]] auto operator()(const std::string_view left, const std::string_view right) const noexcept
        -> bool {
        return left == right;
    }
};

using HotRecordMap =
    std::unordered_map<std::string, HotRecordEntry, TransparentStringHash, TransparentStringEqual>;

[[nodiscard]] auto prepare_hot_record_insertions(HotRecordMap& records, const std::size_t additional_records)
    -> Status {
    const auto raw_capacity =
        static_cast<double>(records.bucket_count()) * static_cast<double>(records.max_load_factor());
    const auto current_capacity = raw_capacity >= static_cast<double>(std::numeric_limits<std::size_t>::max())
                                      ? std::numeric_limits<std::size_t>::max()
                                      : static_cast<std::size_t>(raw_capacity);
    const auto plan = detail::plan_hot_record_reserve(records.size(), additional_records, current_capacity);
    if (plan.overflow) {
        return fail(ErrorCode::arithmetic_overflow, "hot Record publication capacity overflow");
    }
    if (plan.target != 0) {
        records.reserve(plan.target);
    }
    return {};
}

[[nodiscard]] auto hot_record_matches(const HotRecordEntry& entry, const RecordRef& reference) noexcept
    -> bool {
    return entry.reference == reference;
}

[[nodiscard]] auto owned_value_from_hot(const HotRecordEntry& entry) -> OwnedValue {
    return {.bytes = entry.value_bytes, .sequence = entry.sequence.value, .expire_at_ns = entry.expire_at_ns};
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
    HotRecordMap::node_type hot_record;
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

struct DurableRuntimeCatalog::RuntimeWorker {
    explicit RuntimeWorker(RecoveredWorkerState recovered)
        : worker_id(recovered.worker_id), index(std::move(recovered.index)),
          next_sequence(recovered.next_sequence), active_segment(recovered.active_segment) {}

    WorkerId worker_id;
    Index index;
    SequenceNumber next_sequence;
    SegmentId active_segment;
    std::mutex mutex;
    std::optional<DurableSegmentFile> cached_file;
    bool cached_writable{};
    std::vector<std::byte> encode_scratch;
    HotRecordMap hot_records;
    HotRecordMap hot_record_staging;
    std::vector<PendingGroupMutation*> pending_group_mutations;
    std::size_t pending_group_insertions{};
    std::size_t pending_group_heap_key_bytes{};
    std::chrono::steady_clock::time_point batch_started{};
    std::atomic_size_t active_group_mutations{};
    detail::AdaptiveBatchSizer batch_sizer;
    bool batch_closing{};
    std::condition_variable batch_closed;
    bool compaction_commit_active{};
    std::condition_variable compaction_commit_finished;
};

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
        return true;
    }
    if (worker.cached_file->pending_bytes() >= config.max_bytes) {
        return true;
    }
    const auto record_target = worker.batch_sizer.target();
    if (options_.strict_ack && pending_records >= record_target) {
        // If more producers are already admitted than fit in this batch, grow
        // the next target up to the observed burst. A relaxed sample is enough:
        // this is a tuning hint and never affects acknowledgement correctness.
        const auto admissions = worker.active_group_mutations.load(std::memory_order_relaxed);
        worker.batch_sizer.observe_target_reached(pending_records, admissions);
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
            }
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
    const auto flushed = worker.cached_file->flush_pending_commit(sync);
    if (!flushed.committed()) {
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
            if (mutation->hot_record.empty()) {
                return publication_failed(
                    Error{ErrorCode::corrupted_data, "prepared hot Record publication is absent"});
            }
            mutation->hot_record.mapped().reference = mutation->reference;
            mutation->hot_record.mapped().sequence = mutation->reference.sequence;
            const auto existing = worker.hot_records.find(mutation->hot_record.key());
            if (existing != worker.hot_records.end()) {
                existing->second = std::move(mutation->hot_record.mapped());
            } else {
                const auto inserted = worker.hot_records.insert(std::move(mutation->hot_record));
                if (!inserted.inserted) {
                    return publication_failed(
                        Error{ErrorCode::corrupted_data, "prepared hot Record publication conflicted"});
                }
            }
        } else {
            static_cast<void>(worker.index.erase_no_compact(hashed));
            if (const auto existing = worker.hot_records.find(mutation->key);
                existing != worker.hot_records.end()) {
                worker.hot_records.erase(existing);
            }
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
    return {};
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
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    const auto key_bytes =
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()};
    const auto worker_index = route_worker(key.hash, workers_.size());
    auto& worker = *workers_[worker_index];

    struct PinnedRead final {
        RecordRef reference;
        std::shared_ptr<const RuntimeSegmentGeneration> generation;
    };

    for (;;) {
        std::optional<PinnedRead> read;
        {
            const std::lock_guard worker_lock{worker.mutex};
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }

            const auto reference = worker.index.find(key);
            if (!reference) {
                return fail(ErrorCode::not_found, "key is not present");
            }
            const auto found =
                std::lower_bound(manifest_.segments.begin(), manifest_.segments.end(), reference->segment_id,
                                 [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                     return entry.segment_id.value < id.value;
                                 });
            if (found == manifest_.segments.end() || found->segment_id != reference->segment_id) {
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Index references a Segment absent from the catalog"});
            }
            const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
            if (found->generation != reference->generation || found->owner_worker != worker.worker_id ||
                catalog_index >= generation_pins_.size()) {
                return fail_closed(
                    Error{ErrorCode::corrupted_data,
                          "durable Index reference disagrees with catalog identity or ownership"});
            }

            if (const auto cached = worker.hot_records.find(key.key); cached != worker.hot_records.end()) {
                if (hot_record_matches(cached->second, *reference)) {
                    if (cached->second.expire_at_ns != 0 && now_ns != 0 &&
                        cached->second.expire_at_ns <= now_ns) {
                        return fail(ErrorCode::not_found, "key has expired");
                    }
                    return Result<OwnedValue>{owned_value_from_hot(cached->second)};
                }
                worker.hot_records.erase(cached);
            }

            const auto& pin = generation_pins_[catalog_index];
            if (!pin || pin->identity.segment_id != reference->segment_id ||
                pin->identity.generation != reference->generation ||
                pin->identity.owner_worker != worker.worker_id) {
                return fail_closed(Error{ErrorCode::corrupted_data,
                                         "durable Segment generation pin disagrees with the Index"});
            }
            read.emplace(PinnedRead{.reference = *reference, .generation = pin});
        }

        ReadContext context{.expected_key = key_bytes, .expected_hash = key.hash, .now_ns = now_ns};
        auto visited = read->generation->file.visit_record(read->reference, &context, &copy_verified_value);

        // Linearization point for a cold GET: while holding the Worker and
        // catalog locks, the key must still designate both the same RecordRef
        // and the same immutable generation pin. I/O and CRC validation happen
        // before this point and never hold either lock.
        bool still_current{};
        {
            const std::lock_guard worker_lock{worker.mutex};
            const std::shared_lock catalog_lock{catalog_mutex_};
            if (!healthy()) {
                return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
            }
            const auto current = worker.index.find(key);
            if (current && *current == read->reference) {
                const auto found = std::lower_bound(
                    manifest_.segments.begin(), manifest_.segments.end(), current->segment_id,
                    [](const ManifestSegmentEntry& entry, const SegmentId id) {
                        return entry.segment_id.value < id.value;
                    });
                if (found != manifest_.segments.end() && found->segment_id == current->segment_id) {
                    const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
                    still_current = catalog_index < generation_pins_.size() &&
                                    generation_pins_[catalog_index] == read->generation;
                }
            }
        }
        if (!still_current) {
            continue;
        }
        if (!visited) {
            if (visited.error().code == ErrorCode::not_found) {
                return unexpected(visited.error());
            }
            return fail_closed(visited.error());
        }
        return Result<OwnedValue>{std::move(context.value)};
    }
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
    const std::lock_guard publication_lock{manifest_publication_mutex_};
    if (compaction_publication_active_) {
        return mutation_failure(
            DurableMutationOutcome::not_committed,
            Error{ErrorCode::sequence_conflict,
                  "durable rotation conflicts with an active compaction publication lease"});
    }
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

    if (worker.cached_file->selected_commit().commit.state != PersistedSegmentState::sealed) {
        const auto sealed = worker.cached_file->seal();
        if (!sealed.committed()) {
            if (sealed.outcome == SegmentCommitOutcome::indeterminate) {
                healthy_.store(false, std::memory_order_release);
            }
            return mutation_failure(sealed.outcome == SegmentCommitOutcome::indeterminate
                                        ? DurableMutationOutcome::indeterminate
                                        : DurableMutationOutcome::not_committed,
                                    sealed.error.value_or(Error{ErrorCode::io_error, "Segment seal failed"}));
        }
        segments_[old_index].selected = worker.cached_file->selected_commit();
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
    auto created = DurableSegmentFile::create(directory_, replacement_identity);
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
    const auto published = directory_.publish_manifest(*next_manifest, options_.limits.max_manifest_bytes);
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
    worker.active_segment = replacement_segment_id;
    worker.cached_file.emplace(std::move(*created.file));
    worker.cached_writable = true;
    std::erase_if(worker.hot_records, [&](const auto& entry) {
        return entry.second.reference.segment_id == old_entry.segment_id;
    });
    return {.outcome = DurableMutationOutcome::committed, .sequence = std::nullopt, .error = std::nullopt};
}

auto DurableRuntimeCatalog::mutate(const std::span<const std::byte> key,
                                   const std::span<const std::byte> value, const Opcode opcode,
                                   const std::uint64_t key_hash, const std::uint64_t expire_at_ns,
                                   const ValueType type, const std::uint32_t flags) -> DurableMutationResult {
    auto exception_outcome = DurableMutationOutcome::not_committed;
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
        HotRecordMap::node_type prepared_hot_record;
        if (opcode == Opcode::put) {
            const auto additional_hot_records =
                strict_batch ? (worker.pending_group_mutations.empty()
                                    ? static_cast<std::size_t>(options_.batch->max_records)
                                    : 0U)
                             : (key_present ? 0U : 1U);
            if (auto prepared = prepare_hot_record_insertions(worker.hot_records, additional_hot_records);
                !prepared) {
                return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
            }
            const auto staged = worker.hot_record_staging.emplace(
                std::string{hashed.key},
                HotRecordEntry{.value_bytes = std::vector<std::byte>{value.begin(), value.end()},
                               .expire_at_ns = expire_at_ns});
            if (!staged.second) {
                return mutation_failure(
                    DurableMutationOutcome::not_committed,
                    Error{ErrorCode::corrupted_data, "hot Record staging map was not empty"});
            }
            prepared_hot_record = worker.hot_record_staging.extract(staged.first);
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
                prepared_hot_record.mapped().reference = reference;
                prepared_hot_record.mapped().sequence = committed_sequence;
                const auto existing = worker.hot_records.find(prepared_hot_record.key());
                if (existing != worker.hot_records.end()) {
                    existing->second = std::move(prepared_hot_record.mapped());
                } else {
                    const auto inserted = worker.hot_records.insert(std::move(prepared_hot_record));
                    if (!inserted.inserted) {
                        healthy_.store(false, std::memory_order_release);
                        return {.outcome = DurableMutationOutcome::committed,
                                .sequence = committed_sequence,
                                .error = Error{ErrorCode::corrupted_data,
                                               "prepared hot Record publication conflicted"}};
                    }
                }
            } else {
                static_cast<void>(worker.index.erase_no_compact(hashed));
                if (const auto existing = worker.hot_records.find(hashed.key);
                    existing != worker.hot_records.end()) {
                    worker.hot_records.erase(existing);
                }
            }
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

auto DurableRuntimeCatalog::compact_worker(const std::size_t worker_index, const std::uint64_t now_ns)
    -> DurableCompactionResult {
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
                const std::lock_guard lock{runtime.manifest_publication_mutex_};
                runtime.compaction_publication_active_ = false;
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
                        .stats = stats,
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
            worker.index = std::move(prepared.index);
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

auto DurableRuntimeCatalog::verify_index() -> Status {
    if (!healthy()) {
        return fail(ErrorCode::unavailable, "durable runtime is fail-closed");
    }
    std::vector<std::string> keys;
    for (std::size_t worker_index = 0; worker_index < workers_.size(); ++worker_index) {
        auto& worker = *workers_[worker_index];
        const std::lock_guard lock{worker.mutex};
        auto entries = worker.index.entries();
        for (auto& entry : entries) {
            if (route_worker(entry.key, workers_.size()) != worker_index) {
                return fail(ErrorCode::corrupted_data, "durable Index entry is routed to the wrong Worker");
            }
            keys.push_back(std::move(entry.key));
        }
    }
    for (const auto& key : keys) {
        if (auto verified = get(key); !verified) {
            return unexpected(verified.error());
        }
    }
    return {};
}

} // namespace glyphastore
