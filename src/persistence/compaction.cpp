#include "glyphastore/persistence/compaction.hpp"

#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/persistence/resource_limits.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment_header.hpp"

#include <limits>

namespace glyphastore {
namespace {

[[nodiscard]] auto checked_add(const std::uint64_t left, const std::uint64_t right, const char* description)
    -> Result<std::uint64_t> {
    if (right > std::numeric_limits<std::uint64_t>::max() - left) {
        return fail(ErrorCode::arithmetic_overflow, description);
    }
    return left + right;
}

[[nodiscard]] auto checked_multiply(const std::uint64_t left, const std::uint64_t right,
                                    const char* description) -> Result<std::uint64_t> {
    if (left != 0 && right > std::numeric_limits<std::uint64_t>::max() / left) {
        return fail(ErrorCode::arithmetic_overflow, description);
    }
    return left * right;
}

[[nodiscard]] auto segment_bytes(const std::size_t count, const char* description) -> Result<std::uint64_t> {
    return checked_multiply(static_cast<std::uint64_t>(count), static_cast<std::uint64_t>(kSegmentSizeBytes),
                            description);
}

[[nodiscard]] auto validate_write_amplification(const std::size_t source_count,
                                                const std::size_t output_count,
                                                const DurableResourceLimits& limits) -> Status {
    const auto temporary = segment_bytes(output_count, "compaction temporary byte count overflow");
    if (!temporary) {
        return unexpected(temporary.error());
    }
    const auto reclaimed =
        segment_bytes(source_count - output_count, "compaction reclaimed byte count overflow");
    if (!reclaimed) {
        return unexpected(reclaimed.error());
    }
    if (*temporary == 0) {
        return {};
    }
    if (*reclaimed == 0) {
        return fail(ErrorCode::storage_exhausted, "durable compaction would not reclaim a physical Segment");
    }
    if (*reclaimed <= std::numeric_limits<std::uint64_t>::max() / limits.max_write_amplification &&
        *temporary > *reclaimed * limits.max_write_amplification) {
        return fail(ErrorCode::storage_exhausted,
                    "durable compaction exceeds the configured write-amplification limit");
    }
    return {};
}

} // namespace

auto DurableCompactionLayout::add_record(const std::uint32_t encoded_size)
    -> Result<DurableCompactionPlacement> {
    constexpr auto kPayloadBytes =
        static_cast<std::uint32_t>(kSegmentSizeBytes - kSegmentHeaderReservedBytes);
    if (encoded_size < kEncodedRecordHeaderSize || encoded_size > kMaxNormalRecordSize ||
        encoded_size % kRecordAlignment != 0 || encoded_size > kPayloadBytes) {
        return fail(ErrorCode::invalid_record,
                    "durable compaction Record extent cannot fit a v1 Segment payload");
    }
    if (encoded_size > std::numeric_limits<std::uint64_t>::max() - encoded_bytes_) {
        return fail(ErrorCode::arithmetic_overflow, "durable compaction live byte count overflows uint64_t");
    }
    if (segment_count_ == 0 || encoded_size > kPayloadBytes - current_payload_bytes_) {
        if (segment_count_ == std::numeric_limits<std::size_t>::max()) {
            return fail(ErrorCode::arithmetic_overflow,
                        "durable compaction output Segment count overflows size_t");
        }
        ++segment_count_;
        current_payload_bytes_ = 0;
    }
    const DurableCompactionPlacement placement{
        .segment_index = segment_count_ - 1U,
        .offset =
            RecordOffset{static_cast<std::uint32_t>(kSegmentHeaderReservedBytes) + current_payload_bytes_},
    };
    current_payload_bytes_ += encoded_size;
    encoded_bytes_ += encoded_size;
    return placement;
}

auto durable_compaction_output_segments(const std::uint64_t live_encoded_bytes) -> Result<std::size_t> {
    static_assert(kSegmentSizeBytes > kSegmentHeaderReservedBytes);
    constexpr auto kPayloadBytes =
        static_cast<std::uint64_t>(kSegmentSizeBytes - kSegmentHeaderReservedBytes);
    if (live_encoded_bytes == 0) {
        return std::size_t{0};
    }
    const auto quotient = live_encoded_bytes / kPayloadBytes;
    const auto remainder = live_encoded_bytes % kPayloadBytes;
    if (quotient > std::numeric_limits<std::size_t>::max() - (remainder == 0 ? 0U : 1U)) {
        return fail(ErrorCode::arithmetic_overflow,
                    "durable compaction output Segment count overflows size_t");
    }
    return static_cast<std::size_t>(quotient) + (remainder == 0 ? 0U : 1U);
}

auto validate_durable_compaction_transition(const Manifest& current, const Manifest& next,
                                            const WorkerId worker_id) -> Result<std::size_t> {
    if (!encoded_manifest_size(current) || !encoded_manifest_size(next)) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition contains an invalid v1 manifest");
    }
    if (worker_id.value >= current.worker_count || current.store_id != next.store_id ||
        current.worker_count != next.worker_count || current.routing_algorithm != next.routing_algorithm ||
        current.routing_epoch != next.routing_epoch || current.next_segment_id != next.next_segment_id ||
        current.next_segment_generation != next.next_segment_generation ||
        current.manifest_generation == std::numeric_limits<std::uint64_t>::max() ||
        next.manifest_generation != current.manifest_generation + 1U) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition changes immutable catalog metadata");
    }

    std::vector<ManifestSegmentEntry> sources;
    for (const auto& entry : current.segments) {
        if (entry.owner_worker == worker_id && entry.role == ManifestSegmentRole::sealed) {
            sources.push_back(entry);
        }
    }
    if (sources.empty() || next.segments.size() > current.segments.size()) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition has no sealed source set or grows the catalog");
    }
    const auto retained_entries = current.segments.size() - sources.size();
    if (next.segments.size() < retained_entries) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction transition removes a non-source catalog entry");
    }
    const auto output_count = next.segments.size() - retained_entries;

    Manifest expected = current;
    expected.manifest_generation = next.manifest_generation;
    expected.segments.clear();
    expected.segments.reserve(next.segments.size());
    std::size_t replacements_remaining = output_count;
    for (const auto& entry : current.segments) {
        if (entry.owner_worker != worker_id || entry.role != ManifestSegmentRole::sealed) {
            expected.segments.push_back(entry);
            continue;
        }
        if (replacements_remaining == 0) {
            continue;
        }
        if (entry.generation.value == std::numeric_limits<std::uint32_t>::max()) {
            return fail(ErrorCode::arithmetic_overflow, "durable compaction Segment generation is exhausted");
        }
        auto replacement = entry;
        ++replacement.generation.value;
        expected.segments.push_back(replacement);
        --replacements_remaining;
    }
    if (expected != next) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction next manifest is not the canonical sealed-set replacement");
    }
    return output_count;
}

auto plan_durable_worker_compaction(const Manifest& current, const WorkerId worker_id,
                                    const std::size_t output_segment_count,
                                    const DurableResourceLimits& limits) -> Result<DurableCompactionPlan> {
    if (auto valid = validate_durable_manifest_resources(current, limits); !valid) {
        return unexpected(valid.error());
    }
    if (worker_id.value >= current.worker_count) {
        return fail(ErrorCode::invalid_argument,
                    "durable compaction Worker is outside the manifest Worker set");
    }
    if (current.manifest_generation == std::numeric_limits<std::uint64_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "durable compaction manifest generation is exhausted");
    }

    DurableCompactionPlan plan{.worker_id = worker_id, .next_manifest = current};
    for (const auto& entry : current.segments) {
        if (entry.owner_worker == worker_id && entry.role == ManifestSegmentRole::sealed) {
            plan.sources.push_back(entry);
        }
    }
    if (plan.sources.empty()) {
        return fail(ErrorCode::not_found, "durable Worker has no sealed Segments to compact");
    }
    if (output_segment_count > plan.sources.size()) {
        return fail(ErrorCode::invalid_argument, "durable compaction output exceeds its sealed source set");
    }
    if (auto amplification = validate_write_amplification(plan.sources.size(), output_segment_count, limits);
        !amplification) {
        return unexpected(amplification.error());
    }

    plan.next_manifest.segments.clear();
    plan.next_manifest.segments.reserve(current.segments.size() - plan.sources.size() + output_segment_count);
    std::size_t replacements_remaining = output_segment_count;
    for (const auto& entry : current.segments) {
        if (entry.owner_worker != worker_id || entry.role != ManifestSegmentRole::sealed) {
            plan.next_manifest.segments.push_back(entry);
            continue;
        }
        if (replacements_remaining == 0) {
            continue;
        }
        if (entry.generation.value == std::numeric_limits<std::uint32_t>::max()) {
            return fail(ErrorCode::arithmetic_overflow, "durable compaction Segment generation is exhausted");
        }
        auto replacement = entry;
        ++replacement.generation.value;
        plan.replacements.push_back(replacement);
        plan.next_manifest.segments.push_back(replacement);
        --replacements_remaining;
    }
    ++plan.next_manifest.manifest_generation;

    const auto temporary = segment_bytes(output_segment_count, "compaction temporary byte count overflow");
    const auto reclaimed =
        segment_bytes(plan.sources.size() - output_segment_count, "compaction reclaimed byte count overflow");
    if (!temporary || !reclaimed) {
        return unexpected((!temporary ? temporary.error() : reclaimed.error()));
    }
    plan.temporary_bytes = *temporary;
    plan.reclaimed_bytes = *reclaimed;
    if (plan.temporary_bytes > limits.max_temporary_compaction_bytes) {
        return fail(ErrorCode::storage_exhausted,
                    "durable compaction exceeds the configured temporary-space budget");
    }

    const auto current_segments =
        segment_bytes(current.segments.size(), "compaction Store byte count overflow");
    const auto current_manifest = durable_manifest_bytes(current.segments.size());
    const auto next_manifest = durable_manifest_bytes(plan.next_manifest.segments.size());
    if (!current_segments || !current_manifest || !next_manifest) {
        if (!current_segments) {
            return unexpected(current_segments.error());
        }
        return unexpected((!current_manifest ? current_manifest.error() : next_manifest.error()));
    }
    auto peak =
        checked_add(*current_segments, plan.temporary_bytes, "compaction peak Store byte count overflow");
    if (peak) {
        peak = checked_add(*peak, *current_manifest, "compaction peak Store byte count overflow");
    }
    if (peak) {
        peak = checked_add(*peak, *next_manifest, "compaction peak Store byte count overflow");
    }
    auto intent_bytes = checked_add(static_cast<std::uint64_t>(kCompactionIntentHeaderBytes),
                                    *current_manifest, "compaction intent byte count overflow");
    if (intent_bytes) {
        intent_bytes = checked_add(*intent_bytes, *next_manifest, "compaction intent byte count overflow");
    }
    if (peak && intent_bytes) {
        peak = checked_add(*peak, *intent_bytes, "compaction peak Store byte count overflow");
    }
    if (!intent_bytes) {
        return unexpected(intent_bytes.error());
    }
    if (!peak) {
        return unexpected(peak.error());
    }
    if (*peak > limits.max_store_bytes) {
        return fail(ErrorCode::storage_exhausted,
                    "durable compaction exceeds the configured peak Store byte budget");
    }
    if (auto encoded = encoded_manifest_size(plan.next_manifest); !encoded) {
        return unexpected(encoded.error());
    }
    const auto transition = validate_durable_compaction_transition(current, plan.next_manifest, worker_id);
    if (!transition) {
        return unexpected(transition.error());
    }
    if (*transition != output_segment_count) {
        return fail(ErrorCode::internal_error,
                    "durable compaction planner produced the wrong replacement count");
    }
    return plan;
}

} // namespace glyphastore
