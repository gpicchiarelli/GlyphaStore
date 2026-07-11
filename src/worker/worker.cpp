#include "glyphastore/worker/worker.hpp"

#include "glyphastore/core/key_hash.hpp"

namespace glyphastore {
namespace {

[[nodiscard]] auto validate_live_record(const Segment& segment, const RecordRef& ref) -> Status {
    if (auto valid = segment.validate_ref_extent(ref); !valid) {
        return valid;
    }
    const auto stats = segment.stats();
    if (stats.live_records == 0 || stats.live_bytes < ref.size.value) {
        return fail(ErrorCode::corrupted_data, "record is not accounted as live");
    }
    return {};
}

} // namespace

Worker::Worker(WorkerId id, GlobalSegmentManager& manager) : id_(id), manager_(manager) {
    active_ = manager_.allocate_active(id_);
    owned_.push_back(active_);
}

auto Worker::next_sequence() -> SequenceNumber {
    const auto current = next_sequence_;
    next_sequence_ = SequenceNumber{next_sequence_.value + 1};
    return current;
}

auto Worker::append_record(const RecordInput& input) -> Result<RecordRef> {
    auto ref = active_->append(input);
    if (!ref && ref.error().code == ErrorCode::segment_full) {
        auto rotated = manager_.rotate_active(active_, id_);
        if (!rotated) {
            return unexpected(rotated.error());
        }
        active_ = *rotated;
        owned_.push_back(active_);
        ref = active_->append(input);
    }
    return ref;
}

auto Worker::read_ref(const RecordRef& ref) const -> Result<RecordView> {
    const auto* segment = manager_.find(ref.segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "record reference targets a missing segment");
    }
    // Store get must always verify CRC: segment bytes are reachable via Store::segments() and
    // Segment::mutable_base(), so skipping checksum would allow in-memory tampering.
    return segment->read(ref);
}

auto Worker::publish(const HashedKey& key, const RecordRef& ref) -> Status {
    auto* segment = manager_.find(ref.segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "new record reference targets a missing segment");
    }

    const auto previous = index_.find(key);
    Segment* previous_segment = nullptr;
    if (previous) {
        previous_segment = manager_.find(previous->segment_id);
        if (!previous_segment) {
            return fail(ErrorCode::invalid_reference, "previous record reference targets a missing segment");
        }
        if (auto valid = validate_live_record(*previous_segment, *previous); !valid) {
            return valid;
        }
    }

    if (auto live = segment->mark_live(ref); !live) {
        return live;
    }
    if (previous) {
        if (auto dead = previous_segment->mark_dead(*previous); !dead) {
            static_cast<void>(segment->mark_dead(ref));
            return dead;
        }
    }

    auto mutation = index_.insert_or_assign(key, ref);
    if (!mutation) {
        if (previous) {
            static_cast<void>(previous_segment->mark_live(*previous));
        }
        static_cast<void>(segment->mark_dead(ref));
        return unexpected(mutation.error());
    }
    if (mutation->previous != previous) {
        return fail(ErrorCode::corrupted_data, "index changed during record publication");
    }
    if (previous) {
        static_cast<void>(manager_.try_retire(previous->segment_id));
    }
    return {};
}

auto Worker::unpublish(const HashedKey& key) -> Status {
    const auto previous = index_.find(key);
    if (!previous) {
        return fail(ErrorCode::not_found, "key is not present in worker index");
    }
    auto* segment = manager_.find(previous->segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "erased record reference targets a missing segment");
    }
    if (auto valid = validate_live_record(*segment, *previous); !valid) {
        return valid;
    }
    if (auto dead = segment->mark_dead(*previous); !dead) {
        return dead;
    }
    const auto mutation = index_.erase(key);
    if (mutation.previous != previous) {
        static_cast<void>(segment->mark_live(*previous));
        return fail(ErrorCode::corrupted_data, "index changed during record removal");
    }
    static_cast<void>(manager_.try_retire(previous->segment_id));
    return {};
}

auto Worker::get(const HashedKey& key, const std::uint64_t now_ns) -> Result<RecordView> {
    const auto ref = index_.find(key);
    if (!ref) {
        return fail(ErrorCode::not_found, "key is not present");
    }
    auto record = read_ref(*ref);
    if (!record) {
        return unexpected(record.error());
    }
    if (record->opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key is not present");
    }
    if (record->expired(now_ns)) {
        if (auto erased = erase(key); !erased) {
            return unexpected(erased.error());
        }
        return fail(ErrorCode::not_found, "key has expired");
    }
    return record;
}

auto Worker::put(const HashedKey& key, const std::span<const std::byte> value,
                 const std::uint64_t expire_at_ns) -> Status {
    const auto existing = index_.find(key);
    if (existing) {
        const auto* previous_segment = manager_.find(existing->segment_id);
        if (!previous_segment) {
            return fail(ErrorCode::invalid_reference, "existing record targets a missing segment");
        }
        if (auto valid = validate_live_record(*previous_segment, *existing); !valid) {
            return valid;
        }
    } else if (auto reserved = index_.reserve(index_.stats().size + 1U); !reserved) {
        return reserved;
    }
    const RecordInput input{
        .sequence = next_sequence(),
        .opcode = Opcode::put,
        .key_hash = key.hash,
        .expire_at_ns = expire_at_ns,
        .key = {reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()},
        .value = value,
    };
    auto ref = append_record(input);
    if (!ref) {
        return unexpected(ref.error());
    }
    return publish(key, *ref);
}

auto Worker::erase(const HashedKey& key) -> Status {
    const auto existing = index_.find(key);
    if (!existing) {
        return fail(ErrorCode::not_found, "key is not present");
    }
    const auto* previous_segment = manager_.find(existing->segment_id);
    if (!previous_segment) {
        return fail(ErrorCode::invalid_reference, "existing record targets a missing segment");
    }
    if (auto valid = validate_live_record(*previous_segment, *existing); !valid) {
        return valid;
    }
    const RecordInput input{
        .sequence = next_sequence(),
        .opcode = Opcode::erase,
        .key_hash = key.hash,
        .key = {reinterpret_cast<const std::byte*>(key.key.data()), key.key.size()},
    };
    auto ref = append_record(input);
    if (!ref) {
        return unexpected(ref.error());
    }
    return unpublish(key);
}

} // namespace glyphastore
