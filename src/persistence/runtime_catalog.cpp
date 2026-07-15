#include "glyphastore/persistence/runtime_catalog.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_table.hpp"
#include "glyphastore/persistence/durable_flush_coordinator.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"

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

[[nodiscard]] auto hot_record_matches(const HotRecordEntry& entry, const RecordRef& reference) noexcept
    -> bool {
    return entry.reference == reference;
}

[[nodiscard]] auto owned_value_from_hot(const HotRecordEntry& entry) -> OwnedValue {
    return {.bytes = entry.value_bytes, .sequence = entry.sequence.value, .expire_at_ns = entry.expire_at_ns};
}

auto rotation_manifest(const Manifest& current, const ManifestSegmentEntry& old_active) -> Result<Manifest> {
    if (current.manifest_generation == std::numeric_limits<std::uint64_t>::max() ||
        current.next_segment_id.value == std::numeric_limits<std::uint64_t>::max() ||
        current.segments.size() == kMaximumManifestSegmentCount) {
        return fail(ErrorCode::arithmetic_overflow, "durable rotation catalog space is exhausted");
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

auto complete_interrupted_rotation(DataDirectory& directory) -> Status {
    auto manifest = directory.read_manifest();
    if (!manifest) {
        return unexpected(manifest.error());
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
    auto next = rotation_manifest(*manifest, *sealed_active);
    if (!next) {
        return unexpected(next.error());
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

    const auto published = directory.publish_manifest(*next);
    if (!published.durable()) {
        return unexpected(
            published.error.value_or(Error{ErrorCode::io_error, "rotation manifest publication failed"}));
    }
    return {};
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

struct DurableRuntimeCatalog::RuntimeWorker {
    explicit RuntimeWorker(RecoveredWorkerState recovered)
        : worker_id(recovered.worker_id), index(std::move(recovered.index)),
          next_sequence(recovered.next_sequence), active_segment(recovered.active_segment) {}

    WorkerId worker_id;
    Index index;
    SequenceNumber next_sequence;
    SegmentId active_segment;
    std::mutex mutex;
    std::optional<std::size_t> cached_catalog_index;
    std::optional<DurableSegmentFile> cached_file;
    bool cached_writable{};
    std::vector<std::byte> encode_scratch;
    HotRecordMap hot_records;
    HotRecordMap hot_record_staging;
    std::vector<PendingGroupMutation*> pending_group_mutations;
    std::size_t pending_group_insertions{};
    std::size_t pending_group_heap_key_bytes{};
    std::chrono::steady_clock::time_point batch_started{};
    bool batch_closing{};
    std::condition_variable batch_closed;
};

DurableRuntimeCatalog::DurableRuntimeCatalog(DataDirectory directory, DurableRecoveryState recovered,
                                             DurableRuntimeOptions options)
    : directory_(std::move(directory)), manifest_(std::move(recovered.manifest)),
      namespace_audit_(std::move(recovered.namespace_audit)), segments_(std::move(recovered.segments)),
      recovery_stats_(recovered.stats), options_(options) {
    workers_.reserve(recovered.workers.size());
    for (auto& worker : recovered.workers) {
        workers_.push_back(std::make_unique<RuntimeWorker>(std::move(worker)));
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
    if (dedicated_commit_executor_ && flusher_) {
        if (healthy() && !flush()) {
            healthy_.store(false, std::memory_order_release);
        }
        flusher_->stop();
        return;
    }
    if (flusher_) {
        flusher_->stop();
    }
    if (healthy() && (options_.batch || options_.commit_sync == SegmentCommitSync::deferred)) {
        if (auto flushed = flush(); !flushed) {
            healthy_.store(false, std::memory_order_release);
        }
    }
}

auto DurableRuntimeCatalog::should_flush_batch(const RuntimeWorker& worker) const noexcept -> bool {
    if (!options_.batch || !worker.cached_file || !worker.cached_file->has_pending_commit()) {
        return false;
    }
    if (worker.batch_closing) {
        return true;
    }
    const auto& config = *options_.batch;
    if (worker.cached_file->pending_record_count() >= config.max_records) {
        return true;
    }
    if (worker.cached_file->pending_bytes() >= config.max_bytes) {
        return true;
    }
    if (worker.batch_started != std::chrono::steady_clock::time_point{}) {
        const auto elapsed = std::chrono::steady_clock::now() - worker.batch_started;
        if (elapsed >= std::chrono::milliseconds{config.max_wait_ms}) {
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
    if (worker.cached_catalog_index) {
        segments_[*worker.cached_catalog_index].selected = worker.cached_file->selected_commit();
    }
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
        std::lock_guard lock{worker->mutex};
        const std::shared_lock catalog_lock{catalog_mutex_};
        if (auto flushed = flush_worker_batch(*worker, sync); !flushed) {
            return flushed;
        }
    }
    return {};
}

auto DurableRuntimeCatalog::flush_due_batches(const SegmentCommitSync sync) -> Status {
    for (auto& worker : workers_) {
        std::lock_guard lock{worker->mutex};
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
    if (auto completed = complete_interrupted_rotation(directory); !completed) {
        return unexpected(completed.error());
    }
    auto recovered = recover_durable_state(directory, recovery_now_ns);
    if (!recovered) {
        return unexpected(recovered.error());
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
    return std::unique_ptr<DurableRuntimeCatalog>(
        new DurableRuntimeCatalog(std::move(directory), std::move(*recovered), options));
}

auto DurableRuntimeCatalog::flush_dirty_segments() -> Status {
    for (auto& worker : workers_) {
        std::lock_guard lock{worker->mutex};
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
    const std::lock_guard lock{worker.mutex};
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
        return fail_closed(
            Error{ErrorCode::corrupted_data, "durable Index references a Segment absent from the catalog"});
    }
    const auto catalog_index = static_cast<std::size_t>(found - manifest_.segments.begin());
    if (found->generation != reference->generation || found->owner_worker != worker.worker_id) {
        return fail_closed(Error{ErrorCode::corrupted_data,
                                 "durable Index reference disagrees with catalog identity or ownership"});
    }

    if (const auto cached = worker.hot_records.find(key.key); cached != worker.hot_records.end()) {
        if (hot_record_matches(cached->second, *reference)) {
            if (cached->second.expire_at_ns != 0 && now_ns != 0 && cached->second.expire_at_ns <= now_ns) {
                return fail(ErrorCode::not_found, "key has expired");
            }
            return Result<OwnedValue>{owned_value_from_hot(cached->second)};
        }
        worker.hot_records.erase(cached);
    }

    if (!worker.cached_catalog_index || *worker.cached_catalog_index != catalog_index) {
        worker.cached_file.reset();
        worker.cached_catalog_index.reset();
        const SegmentHeaderIdentity identity{
            .store_id = manifest_.store_id,
            .segment_id = found->segment_id,
            .generation = found->generation,
            .owner_worker = found->owner_worker,
        };
        auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_only);
        if (!opened) {
            auto error = opened.error();
            if (error.code == ErrorCode::not_found) {
                error.code = ErrorCode::corrupted_data;
            }
            error.message =
                "runtime Segment " + std::to_string(found->segment_id.value) + ": " + error.message;
            return fail_closed(std::move(error));
        }
        if (opened->selected_commit() != segments_[catalog_index].selected) {
            return fail_closed(Error{
                ErrorCode::corrupted_data,
                "runtime Segment commit boundary changed after durable recovery",
            });
        }
        worker.cached_file.emplace(std::move(*opened));
        worker.cached_catalog_index = catalog_index;
        worker.cached_writable = false;
    }

    ReadContext context{.expected_key = key_bytes, .expected_hash = key.hash, .now_ns = now_ns};
    if (auto visited = worker.cached_file->visit_record(*reference, &context, &copy_verified_value);
        !visited) {
        if (visited.error().code == ErrorCode::not_found) {
            return unexpected(visited.error());
        }
        return fail_closed(visited.error());
    }
    return Result<OwnedValue>{std::move(context.value)};
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
    auto next_manifest = rotation_manifest(manifest_, old_entry);
    if (!next_manifest) {
        return mutation_failure(DurableMutationOutcome::not_committed, next_manifest.error());
    }
    segments_.reserve(segments_.size() + 1U);

    if (!worker.cached_catalog_index || *worker.cached_catalog_index != old_index || !worker.cached_file ||
        !worker.cached_writable) {
        worker.cached_file.reset();
        worker.cached_catalog_index.reset();
        worker.cached_writable = false;
        const SegmentHeaderIdentity identity{.store_id = manifest_.store_id,
                                             .segment_id = old_entry.segment_id,
                                             .generation = old_entry.generation,
                                             .owner_worker = old_entry.owner_worker};
        auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_write);
        if (!opened || opened->selected_commit() != segments_[old_index].selected) {
            auto error = opened ? Error{ErrorCode::corrupted_data, "active Segment changed after recovery"}
                                : opened.error();
            healthy_.store(false, std::memory_order_release);
            return mutation_failure(DurableMutationOutcome::indeterminate, std::move(error));
        }
        worker.cached_file.emplace(std::move(*opened));
        worker.cached_catalog_index = old_index;
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
    const auto published = directory_.publish_manifest(*next_manifest);
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
    worker.active_segment = replacement_segment_id;
    worker.cached_file.emplace(std::move(*created.file));
    worker.cached_catalog_index = segments_.size() - 1U;
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
        std::unique_lock worker_lock{worker.mutex};
        if (dedicated_commit_executor_) {
            worker.batch_closed.wait(worker_lock, [&] { return !worker.batch_closing || !healthy(); });
        }
        if (!healthy()) {
            return mutation_failure(DurableMutationOutcome::indeterminate,
                                    Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
        }
        if (worker.next_sequence.value == 0 ||
            worker.next_sequence.value == std::numeric_limits<std::uint64_t>::max()) {
            return mutation_failure(
                DurableMutationOutcome::not_committed,
                Error{ErrorCode::arithmetic_overflow, "Worker sequence space is exhausted"});
        }

        const HashedKey hashed{.key = as_string_view(key), .hash = key_hash};
        const bool strict_batch = options_.batch.has_value() && options_.strict_ack;
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
                             : 1U;
            if (additional_hot_records >
                std::numeric_limits<std::size_t>::max() - worker.hot_records.size()) {
                return mutation_failure(
                    DurableMutationOutcome::not_committed,
                    Error{ErrorCode::arithmetic_overflow, "hot Record publication capacity overflow"});
            }
            if (additional_hot_records != 0) {
                worker.hot_records.reserve(worker.hot_records.size() + additional_hot_records);
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
            if (!worker.cached_catalog_index || *worker.cached_catalog_index != catalog_index ||
                !worker.cached_file || !worker.cached_writable) {
                worker.cached_file.reset();
                worker.cached_catalog_index.reset();
                worker.cached_writable = false;
                const SegmentHeaderIdentity identity{.store_id = manifest_.store_id,
                                                     .segment_id = position->segment_id,
                                                     .generation = position->generation,
                                                     .owner_worker = position->owner_worker};
                auto opened = DurableSegmentFile::open(directory_, identity, SegmentFileOpenMode::read_write);
                if (!opened || opened->selected_commit() != segments_[catalog_index].selected) {
                    auto error =
                        opened ? Error{ErrorCode::corrupted_data, "active Segment changed after recovery"}
                               : opened.error();
                    healthy_.store(false, std::memory_order_release);
                    return mutation_failure(DurableMutationOutcome::indeterminate, std::move(error));
                }
                worker.cached_file.emplace(std::move(*opened));
                worker.cached_catalog_index = catalog_index;
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
    return healthy_.load(std::memory_order_acquire) && directory_.healthy();
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

auto DurableRuntimeCatalog::namespace_audit() const noexcept -> const NamespaceAuditReport& {
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
