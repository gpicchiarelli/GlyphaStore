#include "glyphastore/persistence/runtime_catalog.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/record.hpp"

#include <algorithm>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
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
};

DurableRuntimeCatalog::DurableRuntimeCatalog(DataDirectory directory, DurableRecoveryState recovered)
    : directory_(std::move(directory)), manifest_(std::move(recovered.manifest)),
      namespace_audit_(std::move(recovered.namespace_audit)), segments_(std::move(recovered.segments)),
      recovery_stats_(recovered.stats) {
    workers_.reserve(recovered.workers.size());
    for (auto& worker : recovered.workers) {
        workers_.push_back(std::make_unique<RuntimeWorker>(std::move(worker)));
    }
}

DurableRuntimeCatalog::~DurableRuntimeCatalog() = default;

auto DurableRuntimeCatalog::open_existing(const std::filesystem::path& path,
                                          const std::uint64_t recovery_now_ns, const FilesystemHooks hooks)
    -> Result<std::unique_ptr<DurableRuntimeCatalog>> {
    auto directory = DataDirectory::open_and_lock(path, hooks);
    if (!directory) {
        return unexpected(directory.error());
    }
    return open_locked(std::move(*directory), recovery_now_ns);
}

auto DurableRuntimeCatalog::open_locked(DataDirectory directory, const std::uint64_t recovery_now_ns)
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
        new DurableRuntimeCatalog(std::move(directory), std::move(*recovered)));
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
    auto next_manifest = rotation_manifest(manifest_, *old_position);
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
                                             .segment_id = old_position->segment_id,
                                             .generation = old_position->generation,
                                             .owner_worker = old_position->owner_worker};
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
    return {.outcome = DurableMutationOutcome::committed, .sequence = std::nullopt, .error = std::nullopt};
}

auto DurableRuntimeCatalog::mutate(const std::span<const std::byte> key,
                                   const std::span<const std::byte> value, const Opcode opcode,
                                   const std::uint64_t key_hash, const std::uint64_t expire_at_ns,
                                   const ValueType type, const std::uint32_t flags) -> DurableMutationResult {
    if (!healthy()) {
        return mutation_failure(DurableMutationOutcome::indeterminate,
                                Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
    }
    const auto worker_index = route_worker(key_hash, workers_.size());
    auto& worker = *workers_[worker_index];
    const std::lock_guard worker_lock{worker.mutex};
    if (!healthy()) {
        return mutation_failure(DurableMutationOutcome::indeterminate,
                                Error{ErrorCode::unavailable, "durable runtime is fail-closed"});
    }
    if (worker.next_sequence.value == 0 ||
        worker.next_sequence.value == std::numeric_limits<std::uint64_t>::max()) {
        return mutation_failure(DurableMutationOutcome::not_committed,
                                Error{ErrorCode::arithmetic_overflow, "Worker sequence space is exhausted"});
    }

    const HashedKey hashed{.key = as_string_view(key), .hash = key_hash};
    if (opcode == Opcode::erase && !worker.index.find(hashed)) {
        return mutation_failure(DurableMutationOutcome::not_committed,
                                Error{ErrorCode::not_found, "key is not present"});
    }
    if (opcode == Opcode::put) {
        if (auto prepared = worker.index.prepare_insert(hashed); !prepared) {
            return mutation_failure(DurableMutationOutcome::not_committed, prepared.error());
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
    auto encoded = encode_record(input);
    if (!encoded) {
        return mutation_failure(DurableMutationOutcome::not_committed, encoded.error());
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
                auto error = opened
                                 ? Error{ErrorCode::corrupted_data, "active Segment changed after recovery"}
                                 : opened.error();
                healthy_.store(false, std::memory_order_release);
                return mutation_failure(DurableMutationOutcome::indeterminate, std::move(error));
            }
            worker.cached_file.emplace(std::move(*opened));
            worker.cached_catalog_index = catalog_index;
            worker.cached_writable = true;
        }

        const auto offset = worker.cached_file->selected_commit().commit.committed_end;
        const auto appended = worker.cached_file->append(*encoded);
        if (!appended.committed()) {
            if (appended.error &&
                (appended.error->code == ErrorCode::segment_full ||
                 appended.error->code == ErrorCode::segment_sealed) &&
                attempt == 0) {
                catalog_lock.unlock();
                const auto rotated = rotate_active(worker);
                if (!rotated.committed()) {
                    return rotated;
                }
                continue;
            }
            if (appended.outcome == SegmentCommitOutcome::indeterminate || !directory_.healthy()) {
                healthy_.store(false, std::memory_order_release);
            }
            return mutation_failure(
                appended.outcome == SegmentCommitOutcome::indeterminate
                    ? DurableMutationOutcome::indeterminate
                    : DurableMutationOutcome::not_committed,
                appended.error.value_or(Error{ErrorCode::io_error, "Record append failed"}));
        }

        segments_[catalog_index].selected = worker.cached_file->selected_commit();
        const RecordRef reference{.segment_id = position->segment_id,
                                  .offset = RecordOffset{offset},
                                  .size = RecordSize{static_cast<std::uint32_t>(encoded->size())},
                                  .sequence = worker.next_sequence,
                                  .generation = position->generation};
        if (opcode == Opcode::put) {
            const auto published = worker.index.insert_or_assign(hashed, reference);
            if (!published) {
                healthy_.store(false, std::memory_order_release);
                return {.outcome = DurableMutationOutcome::committed,
                        .sequence = worker.next_sequence,
                        .error = published.error()};
            }
        } else {
            static_cast<void>(worker.index.erase_no_compact(hashed));
        }
        const auto committed_sequence = worker.next_sequence;
        ++worker.next_sequence.value;
        return {.outcome = DurableMutationOutcome::committed,
                .sequence = committed_sequence,
                .error = std::nullopt};
    }
    return mutation_failure(DurableMutationOutcome::not_committed,
                            Error{ErrorCode::segment_full, "Record does not fit after one durable rotation"});
}

auto DurableRuntimeCatalog::healthy() const noexcept -> bool {
    return healthy_.load(std::memory_order_acquire) && directory_.healthy();
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
