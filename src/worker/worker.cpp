#include "glyphastore/worker/worker.hpp"

#include "glyphastore/core/key_hash.hpp"

namespace glyphastore {

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
    const auto segment = manager_.find(ref.segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "record reference targets a missing segment");
    }
    return segment->read(ref);
}

auto Worker::publish(std::string_view key, const RecordRef& ref) -> Status {
    const auto mutation = index_.insert_or_assign(key, ref);
    if (mutation.previous) {
        const auto previous_segment = manager_.find(mutation.previous->segment_id);
        if (!previous_segment) {
            return fail(ErrorCode::invalid_reference, "previous record reference targets a missing segment");
        }
        if (auto dead = previous_segment->mark_dead(*mutation.previous); !dead) {
            return dead;
        }
        static_cast<void>(manager_.try_retire(mutation.previous->segment_id));
    }
    const auto segment = manager_.find(ref.segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "new record reference targets a missing segment");
    }
    if (auto live = segment->mark_live(ref); !live) {
        return live;
    }
    return {};
}

auto Worker::unpublish(std::string_view key) -> Status {
    const auto mutation = index_.erase(key);
    if (!mutation.previous) {
        return fail(ErrorCode::not_found, "key is not present in worker index");
    }
    const auto segment = manager_.find(mutation.previous->segment_id);
    if (!segment) {
        return fail(ErrorCode::invalid_reference, "erased record reference targets a missing segment");
    }
    if (auto dead = segment->mark_dead(*mutation.previous); !dead) {
        return dead;
    }
    static_cast<void>(manager_.try_retire(mutation.previous->segment_id));
    return {};
}

auto Worker::get(std::string_view key, std::uint64_t now_ns) -> Result<RecordView> {
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

auto Worker::put(std::string_view key, std::span<const std::byte> value, std::uint64_t expire_at_ns)
    -> Status {
    const auto key_hash = hash_key(key);
    const RecordInput input{
        .sequence = next_sequence(),
        .opcode = Opcode::put,
        .key_hash = key_hash,
        .expire_at_ns = expire_at_ns,
        .key = {reinterpret_cast<const std::byte*>(key.data()), key.size()},
        .value = value,
    };
    auto ref = append_record(input);
    if (!ref) {
        return unexpected(ref.error());
    }
    return publish(key, *ref);
}

auto Worker::erase(std::string_view key) -> Status {
    const auto existing = index_.find(key);
    if (!existing) {
        return fail(ErrorCode::not_found, "key is not present");
    }
    const auto key_hash = hash_key(key);
    const RecordInput input{
        .sequence = next_sequence(),
        .opcode = Opcode::erase,
        .key_hash = key_hash,
        .key = {reinterpret_cast<const std::byte*>(key.data()), key.size()},
    };
    auto ref = append_record(input);
    if (!ref) {
        return unexpected(ref.error());
    }
    return unpublish(key);
}

} // namespace glyphastore
