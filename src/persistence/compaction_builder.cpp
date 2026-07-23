#include "glyphastore/persistence/compaction_builder.hpp"

#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/segment/record.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore {
namespace {

struct PackedRecord {
    IndexEntry source;
    DurableCompactionPlacement placement;
    RecordRef output;
    std::uint32_t encoded_crc{};
};

struct VerifiedRecordContext {
    std::string_view key;
    std::uint64_t key_hash{};
    std::size_t worker_count{};
    WorkerId worker_id{};
    std::uint64_t now_ns{};
    bool expired{};
};

auto verify_indexed_put(void* opaque, const RecordView& record) -> Status {
    auto& context = *static_cast<VerifiedRecordContext*>(opaque);
    if (record.opcode != Opcode::put || record.key_hash != context.key_hash ||
        record.key_string() != context.key ||
        route_worker(record.key_hash, context.worker_count) != context.worker_id.value) {
        return fail(ErrorCode::corrupted_data,
                    "durable compaction Index entry disagrees with its source Record");
    }
    context.expired = record.expired(context.now_ns);
    return {};
}

auto build_failure(const DurableCompactionBuildOutcome outcome, Error error,
                   DurableCompactionCopyStats stats = {}) -> DurableCompactionBuildResult {
    return {.outcome = outcome,
            .prepared = std::nullopt,
            .stats = std::move(stats),
            .error = std::move(error)};
}

auto checked_add(const std::uint64_t left, const std::uint64_t right, const char* description)
    -> Result<std::uint64_t> {
    if (right > std::numeric_limits<std::uint64_t>::max() - left) {
        return fail(ErrorCode::arithmetic_overflow, description);
    }
    return left + right;
}

auto segment_identity(const Manifest& manifest, const ManifestSegmentEntry& entry) -> SegmentHeaderIdentity {
    return {.store_id = manifest.store_id,
            .segment_id = entry.segment_id,
            .generation = entry.generation,
            .owner_worker = entry.owner_worker};
}

auto find_catalog_entry(const Manifest& manifest, const SegmentId segment_id) -> const ManifestSegmentEntry* {
    const auto found = std::lower_bound(manifest.segments.begin(), manifest.segments.end(), segment_id,
                                        [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                            return entry.segment_id.value < id.value;
                                        });
    return found != manifest.segments.end() && found->segment_id == segment_id ? &*found : nullptr;
}

auto find_source_index(const std::span<const ManifestSegmentEntry> sources, const SegmentId segment_id)
    -> std::optional<std::size_t> {
    const auto found = std::lower_bound(sources.begin(), sources.end(), segment_id,
                                        [](const ManifestSegmentEntry& entry, const SegmentId id) {
                                            return entry.segment_id.value < id.value;
                                        });
    if (found == sources.end() || found->segment_id != segment_id) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(found - sources.begin());
}

} // namespace

auto build_durable_worker_compaction(DataDirectory& directory, const Manifest& current,
                                     const WorkerId worker_id, std::vector<IndexEntry> entries,
                                     const std::uint64_t now_ns, const DurableResourceLimits& limits)
    -> DurableCompactionBuildResult {
    bool recovery_required{};
    const auto failure = [&](Error error) {
        return build_failure(recovery_required ? DurableCompactionBuildOutcome::recovery_required
                                               : DurableCompactionBuildOutcome::not_started,
                             std::move(error));
    };
    try {
        if (!directory.healthy()) {
            return failure(
                Error{ErrorCode::io_error, "cannot build compaction through a poisoned data directory"});
        }
        if (auto valid = validate_durable_manifest_resources(current, limits); !valid) {
            return failure(valid.error());
        }
        if (worker_id.value >= current.worker_count) {
            return failure(
                Error{ErrorCode::invalid_argument, "durable compaction Worker is outside the manifest"});
        }
        auto authority = directory.read_manifest(limits.max_manifest_bytes);
        if (!authority) {
            return failure(authority.error());
        }
        if (*authority != current) {
            return failure(Error{ErrorCode::sequence_conflict,
                                 "durable compaction manifest snapshot is no longer authoritative"});
        }
        auto existing_intent = directory.read_compaction_intent(limits.max_manifest_bytes);
        if (existing_intent) {
            return failure(Error{ErrorCode::sequence_conflict,
                                 "another durable compaction intent is already installed"});
        }
        if (existing_intent.error().code != ErrorCode::not_found) {
            return failure(existing_intent.error());
        }

        std::vector<ManifestSegmentEntry> sources;
        for (const auto& entry : current.segments) {
            if (entry.owner_worker == worker_id && entry.role == ManifestSegmentRole::sealed) {
                sources.push_back(entry);
            }
        }
        if (sources.empty()) {
            return failure(Error{ErrorCode::not_found, "durable Worker has no sealed Segments to compact"});
        }

        std::vector<SelectedSegmentCommit> source_commits;
        source_commits.reserve(sources.size());
        for (const auto& source : sources) {
            auto opened = DurableSegmentFile::open(directory, segment_identity(current, source),
                                                   SegmentFileOpenMode::read_only);
            if (!opened) {
                return failure(opened.error());
            }
            if (opened->selected_commit().commit.state != PersistedSegmentState::sealed) {
                return failure(Error{ErrorCode::corrupted_data, "durable compaction source is not sealed"});
            }
            source_commits.push_back(opened->selected_commit());
        }

        Index prepared_index;
        if (auto reserved = prepared_index.reserve(entries.size()); !reserved) {
            return failure(reserved.error());
        }
        std::vector<IndexEntry> source_entries;
        source_entries.reserve(entries.size());
        std::uint64_t active_live_record_bytes{};
        for (auto& entry : entries) {
            const auto key_hash = hash_key(entry.key);
            const HashedKey hashed{.key = entry.key, .hash = key_hash};
            if (route_worker(key_hash, current.worker_count) != worker_id.value) {
                return failure(Error{ErrorCode::corrupted_data,
                                     "durable compaction Index routing or hash is inconsistent"});
            }
            const auto* catalog_entry = find_catalog_entry(current, entry.record.segment_id);
            if (catalog_entry == nullptr || catalog_entry->generation != entry.record.generation ||
                catalog_entry->owner_worker != worker_id) {
                return failure(Error{ErrorCode::corrupted_data,
                                     "durable compaction Index references an invalid catalog identity"});
            }
            if (catalog_entry->role == ManifestSegmentRole::sealed) {
                source_entries.push_back(std::move(entry));
                continue;
            }
            auto inserted = prepared_index.insert_or_assign(hashed, entry.record);
            if (!inserted || !inserted->inserted) {
                return failure(inserted ? Error{ErrorCode::corrupted_data,
                                                "durable compaction active Index key conflicted"}
                                        : inserted.error());
            }
            if (entry.record.size.value >
                std::numeric_limits<std::uint64_t>::max() - active_live_record_bytes) {
                return failure(Error{ErrorCode::arithmetic_overflow,
                                     "durable compaction active live Record byte count overflows uint64_t"});
            }
            active_live_record_bytes += entry.record.size.value;
        }
        entries = {};

        std::ranges::sort(source_entries, [](const IndexEntry& left, const IndexEntry& right) {
            if (left.record.sequence != right.record.sequence) {
                return left.record.sequence.value < right.record.sequence.value;
            }
            if (left.record.segment_id != right.record.segment_id) {
                return left.record.segment_id.value < right.record.segment_id.value;
            }
            return left.record.offset.value < right.record.offset.value;
        });
        for (std::size_t index = 1; index < source_entries.size(); ++index) {
            const auto& previous = source_entries[index - 1U].record;
            const auto& next = source_entries[index].record;
            if (previous.sequence == next.sequence || previous.segment_id.value > next.segment_id.value ||
                (previous.segment_id == next.segment_id && previous.offset.value >= next.offset.value)) {
                return failure(Error{ErrorCode::corrupted_data,
                                     "durable compaction source Index order is inconsistent"});
            }
        }

        DurableCompactionLayout layout;
        DurableCompactionCopyStats copy_stats{};
        std::vector<PackedRecord> packed;
        packed.reserve(source_entries.size());
        std::optional<std::size_t> open_source_index;
        std::optional<DurableSegmentFile> open_source;
        std::vector<std::byte> scratch;
        for (auto& entry : source_entries) {
            const auto source_index = find_source_index(sources, entry.record.segment_id);
            if (!source_index || sources[*source_index].generation != entry.record.generation) {
                return failure(Error{ErrorCode::corrupted_data,
                                     "durable compaction source reference is not in the sealed set"});
            }
            if (!open_source_index || *open_source_index != *source_index) {
                open_source.reset();
                auto opened =
                    DurableSegmentFile::open(directory, segment_identity(current, sources[*source_index]),
                                             SegmentFileOpenMode::read_only);
                if (!opened || opened->selected_commit() != source_commits[*source_index]) {
                    return failure(opened ? Error{ErrorCode::corrupted_data,
                                                  "durable compaction source boundary changed"}
                                          : opened.error());
                }
                open_source.emplace(std::move(*opened));
                open_source_index = *source_index;
            }
            const auto key_hash = hash_key(entry.key);
            VerifiedRecordContext context{.key = entry.key,
                                          .key_hash = key_hash,
                                          .worker_count = current.worker_count,
                                          .worker_id = worker_id,
                                          .now_ns = now_ns};
            if (auto visited =
                    open_source->visit_record(entry.record, scratch, &context, &verify_indexed_put);
                !visited) {
                return failure(visited.error());
            }
            ++copy_stats.source_index_records_verified;
            copy_stats.source_bytes_verified += entry.record.size.value;
            if (context.expired) {
                ++copy_stats.expired_records_dropped;
                continue;
            }
            auto placement = layout.add_record(entry.record.size.value);
            if (!placement) {
                return failure(placement.error());
            }
            packed.push_back(
                {.source = std::move(entry), .placement = *placement, .encoded_crc = crc32c(scratch)});
        }
        open_source.reset();
        open_source_index.reset();

        if (layout.segment_count() >= sources.size()) {
            return build_failure(
                DurableCompactionBuildOutcome::not_beneficial,
                Error{ErrorCode::not_found, "durable compaction would not reclaim a physical Segment"},
                copy_stats);
        }

        auto plan = plan_durable_worker_compaction(current, worker_id, layout.segment_count(), limits);
        if (!plan) {
            return failure(plan.error());
        }
        if (plan->sources != sources || plan->replacements.size() != layout.segment_count()) {
            return failure(Error{ErrorCode::internal_error,
                                 "durable compaction planner disagrees with the exact Record layout"});
        }
        for (auto& record : packed) {
            const auto& replacement = plan->replacements[record.placement.segment_index];
            record.output = {.segment_id = replacement.segment_id,
                             .offset = record.placement.offset,
                             .size = record.source.record.size,
                             .sequence = record.source.record.sequence,
                             .generation = replacement.generation};
            const HashedKey hashed{.key = record.source.key, .hash = hash_key(record.source.key)};
            auto inserted = prepared_index.insert_or_assign(hashed, record.output);
            if (!inserted || !inserted->inserted) {
                return failure(inserted ? Error{ErrorCode::corrupted_data,
                                                "durable compaction replacement Index key conflicted"}
                                        : inserted.error());
            }
        }

        const DurableCompactionIntent intent{
            .worker_id = worker_id, .old_manifest = current, .next_manifest = plan->next_manifest};
        const auto intent_bytes = encoded_compaction_intent_size(intent);
        const auto next_manifest_bytes = encoded_manifest_size(plan->next_manifest);
        if (!intent_bytes || !next_manifest_bytes) {
            return failure((!intent_bytes ? intent_bytes.error() : next_manifest_bytes.error()));
        }
        auto additional = checked_add(plan->temporary_bytes, static_cast<std::uint64_t>(*intent_bytes),
                                      "durable compaction free-space requirement overflow");
        if (additional) {
            additional = checked_add(*additional, static_cast<std::uint64_t>(*next_manifest_bytes),
                                     "durable compaction free-space requirement overflow");
        }
        if (!additional) {
            return failure(additional.error());
        }
        if (auto available = require_durable_available_space(directory, *additional, limits); !available) {
            return failure(available.error());
        }

        std::vector<SelectedSegmentCommit> replacement_commits;
        replacement_commits.reserve(plan->replacements.size());
        const auto published = directory.publish_compaction_intent(intent, limits.max_manifest_bytes);
        if (!published.durable()) {
            recovery_required = published.outcome == CompactionIntentPublicationOutcome::indeterminate;
            return failure(published.error.value_or(
                Error{ErrorCode::io_error, "durable compaction intent publication failed"}));
        }
        recovery_required = true;

        std::optional<DurableSegmentFile> output;
        std::optional<std::size_t> output_index;
        std::size_t output_group_begin{};
        const auto finalize_output = [&](const std::size_t group_end) -> Status {
            if (!output || !output_index || group_end <= output_group_begin) {
                return fail(ErrorCode::internal_error, "durable compaction output group is incomplete");
            }
            const auto sealed = output->seal();
            if (!sealed.committed()) {
                return unexpected(sealed.error.value_or(
                    Error{ErrorCode::io_error, "durable compaction replacement seal failed"}));
            }
            const auto selected = output->selected_commit();
            output.reset();
            const auto& replacement = plan->replacements[*output_index];
            auto reopened = DurableSegmentFile::open(directory, segment_identity(current, replacement),
                                                     SegmentFileOpenMode::read_only);
            if (!reopened || reopened->selected_commit() != selected) {
                return reopened ? fail(ErrorCode::corrupted_data,
                                       "durable compaction replacement boundary changed on reopen")
                                : unexpected(reopened.error());
            }
            const auto& commit = reopened->selected_commit().commit;
            const auto expected_count = group_end - output_group_begin;
            const auto expected_end = static_cast<std::uint32_t>(packed[group_end - 1U].output.offset.value +
                                                                 packed[group_end - 1U].output.size.value);
            if (commit.state != PersistedSegmentState::sealed || commit.record_count != expected_count ||
                commit.committed_end != expected_end ||
                commit.first_sequence != packed[output_group_begin].output.sequence ||
                commit.last_sequence != packed[group_end - 1U].output.sequence) {
                return fail(ErrorCode::corrupted_data,
                            "durable compaction replacement metadata failed validation");
            }
            for (auto index = output_group_begin; index < group_end; ++index) {
                const auto& record = packed[index];
                VerifiedRecordContext context{.key = record.source.key,
                                              .key_hash = hash_key(record.source.key),
                                              .worker_count = current.worker_count,
                                              .worker_id = worker_id,
                                              .now_ns = now_ns};
                if (auto visited =
                        reopened->visit_record(record.output, scratch, &context, &verify_indexed_put);
                    !visited) {
                    return visited;
                }
                if (context.expired || crc32c(scratch) != record.encoded_crc) {
                    return fail(ErrorCode::checksum_mismatch,
                                "durable compaction replacement differs from its source Record");
                }
            }
            replacement_commits.push_back(selected);
            return {};
        };

        open_source.reset();
        open_source_index.reset();
        for (std::size_t index = 0; index < packed.size(); ++index) {
            const auto& record = packed[index];
            if (!output_index || *output_index != record.placement.segment_index) {
                if (output) {
                    if (auto finalized = finalize_output(index); !finalized) {
                        return failure(finalized.error());
                    }
                }
                output_group_begin = index;
                output_index = record.placement.segment_index;
                const auto& replacement = plan->replacements[*output_index];
                auto created = DurableSegmentFile::create(directory, segment_identity(current, replacement));
                if (!created.durable()) {
                    return failure(created.error.value_or(
                        Error{ErrorCode::io_error, "durable compaction replacement creation failed"}));
                }
                output.emplace(std::move(*created.file));
            }

            const auto source_index = find_source_index(sources, record.source.record.segment_id);
            if (!source_index) {
                return failure(Error{ErrorCode::internal_error, "durable compaction lost a source identity"});
            }
            if (!open_source_index || *open_source_index != *source_index) {
                open_source.reset();
                auto opened =
                    DurableSegmentFile::open(directory, segment_identity(current, sources[*source_index]),
                                             SegmentFileOpenMode::read_only);
                if (!opened || opened->selected_commit() != source_commits[*source_index]) {
                    return failure(opened ? Error{ErrorCode::corrupted_data,
                                                  "durable compaction source changed during copy"}
                                          : opened.error());
                }
                open_source.emplace(std::move(*opened));
                open_source_index = *source_index;
            }
            VerifiedRecordContext context{.key = record.source.key,
                                          .key_hash = hash_key(record.source.key),
                                          .worker_count = current.worker_count,
                                          .worker_id = worker_id,
                                          .now_ns = now_ns};
            if (auto visited =
                    open_source->visit_record(record.source.record, scratch, &context, &verify_indexed_put);
                !visited) {
                return failure(visited.error());
            }
            if (context.expired || crc32c(scratch) != record.encoded_crc ||
                output->selected_commit().commit.committed_end != record.output.offset.value) {
                return failure(Error{ErrorCode::corrupted_data,
                                     "durable compaction source or output layout changed during copy"});
            }
            const auto appended = output->append_record(scratch);
            if (!appended.committed()) {
                return failure(appended.error.value_or(
                    Error{ErrorCode::io_error, "durable compaction Record copy failed"}));
            }
            ++copy_stats.records_copied;
            copy_stats.bytes_copied += scratch.size();
        }
        if (output) {
            if (auto finalized = finalize_output(packed.size()); !finalized) {
                return failure(finalized.error());
            }
        }
        if (replacement_commits.size() != plan->replacements.size() ||
            copy_stats.records_copied != packed.size() || copy_stats.bytes_copied != layout.encoded_bytes()) {
            return failure(
                Error{ErrorCode::internal_error, "durable compaction replacement build is incomplete"});
        }

        return {.outcome = DurableCompactionBuildOutcome::prepared,
                .prepared = PreparedDurableCompaction{.plan = std::move(*plan),
                                                      .index = std::move(prepared_index),
                                                      .active_live_record_bytes = active_live_record_bytes,
                                                      .replacement_commits = std::move(replacement_commits),
                                                      .stats = copy_stats},
                .error = std::nullopt};
    } catch (const std::bad_alloc&) {
        return failure(Error{ErrorCode::resource_exhausted, {}});
    } catch (...) {
        return failure(Error{ErrorCode::internal_error, {}});
    }
}

auto build_durable_worker_compaction(DataDirectory& directory, const Manifest& current,
                                     const WorkerId worker_id, const Index& current_index,
                                     const std::uint64_t now_ns, const DurableResourceLimits& limits)
    -> DurableCompactionBuildResult {
    const auto stats = current_index.stats();
    auto entries = current_index.entries();
    if (entries.size() != stats.size) {
        return build_failure(
            DurableCompactionBuildOutcome::not_started,
            Error{ErrorCode::corrupted_data, "durable compaction Index enumeration changed size"});
    }
    for (const auto& entry : entries) {
        const auto hashed = HashedKey::compute(entry.key);
        if (current_index.find(hashed) != entry.record) {
            return build_failure(
                DurableCompactionBuildOutcome::not_started,
                Error{ErrorCode::corrupted_data, "durable compaction Index snapshot is inconsistent"});
        }
    }
    return build_durable_worker_compaction(directory, current, worker_id, std::move(entries), now_ns, limits);
}

} // namespace glyphastore
