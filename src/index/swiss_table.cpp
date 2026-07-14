#include "glyphastore/index/swiss_table.hpp"

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_control_group.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>

namespace glyphastore {
namespace {

inline constexpr std::uint64_t kSwissSeed = 0x243F6A8885A308D3ULL;
inline constexpr std::uint64_t kSwissMixConstant = 0x9E3779B97F4A7C15ULL;
inline constexpr std::size_t kHeapArenaCompactDeadThreshold = 65'536;
} // namespace

SwissTableIndex::SwissTableIndex() : seed_(kSwissSeed) {
    capacity_ = kSwissGroupSize;
    control_.assign(capacity_, kSwissEmpty);
    slots_.resize(capacity_);
}

auto SwissTableIndex::normalize_capacity(const std::size_t minimum_slots) -> Result<std::size_t> {
    std::size_t capacity = kSwissGroupSize;
    while (capacity < minimum_slots) {
        if (capacity > std::numeric_limits<std::size_t>::max() / 2U) {
            return fail(ErrorCode::arithmetic_overflow, "swiss table capacity overflow");
        }
        capacity <<= 1U;
    }
    return capacity;
}

auto SwissTableIndex::mix_hash(const std::uint64_t key_hash) const noexcept -> std::uint64_t {
    std::uint64_t hash = key_hash;
    hash ^= seed_;
    hash *= kSwissMixConstant;
    hash ^= hash >> 33U;
    hash *= kSwissMixConstant;
    hash ^= hash >> 29U;
    return hash;
}

auto SwissTableIndex::h2(const std::uint64_t hash) noexcept -> std::uint8_t {
    const auto fingerprint = static_cast<std::uint8_t>(hash & 0x7FU);
    return fingerprint == 0 ? static_cast<std::uint8_t>(1) : fingerprint;
}

auto SwissTableIndex::probe_start(const std::uint64_t hash) const noexcept -> std::size_t {
    const auto group_index = (hash >> 7U) & ((capacity_ / kSwissGroupSize) - 1U);
    return group_index * kSwissGroupSize;
}

auto SwissTableIndex::next_group(const std::size_t group_start, const std::size_t capacity) noexcept
    -> std::size_t {
    const auto next = group_start + kSwissGroupSize;
    return next >= capacity ? next - capacity : next;
}

auto SwissTableIndex::slot_key(const Slot& slot) const noexcept -> std::string_view {
    if (slot.key_size == 0) {
        return {};
    }
    if (slot.key_is_inline) {
        return {reinterpret_cast<const char*>(slot.inline_key), slot.key_size};
    }
    return {reinterpret_cast<const char*>(heap_keys_.data(slot.heap_key_offset, slot.key_size).data()),
            slot.key_size};
}

auto SwissTableIndex::key_equals(const Slot& slot, const std::string_view key,
                                 const std::uint64_t key_hash) const noexcept -> bool {
    if (slot.key_size != key.size() || slot.key_hash != key_hash) {
        return false;
    }
    if (key.empty()) {
        return true;
    }
    if (slot.key_is_inline) {
        return std::memcmp(slot.inline_key, key.data(), key.size()) == 0;
    }
    const auto heap_key = heap_keys_.data(slot.heap_key_offset, slot.key_size);
    if (heap_key.size() != key.size()) {
        return false;
    }
    return std::memcmp(heap_key.data(), key.data(), key.size()) == 0;
}

auto SwissTableIndex::find_slot_index(const std::string_view key, const std::uint64_t key_hash) const
    -> std::optional<std::size_t> {
    if (capacity_ == 0) {
        return std::nullopt;
    }
    const auto hash = mix_hash(key_hash);
    const auto fingerprint = h2(hash);
    auto group_start = probe_start(hash);
    for (std::size_t probe = 0; probe < capacity_; probe += kSwissGroupSize) {
        (void)probe;
        const auto control_word = detail::load_control_group64(&control_[group_start]);
        const auto fingerprint_mask = detail::equal_byte_mask(&control_[group_start], fingerprint);
        for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
            const auto control = detail::control_byte_at(control_word, offset);
            if (control == kSwissEmpty) {
                return std::nullopt;
            }
            if ((fingerprint_mask & (1ULL << offset)) != 0 &&
                key_equals(slots_[group_start + offset], key, key_hash)) {
                return group_start + offset;
            }
        }
        group_start = next_group(group_start, capacity_);
    }
    return std::nullopt;
}

auto SwissTableIndex::find_insert_index(const std::string_view key, const std::uint64_t key_hash,
                                        const std::uint64_t mixed_hash) -> Result<std::size_t> {
    if (auto grown = grow_if_needed(); !grown) {
        return unexpected(grown.error());
    }
    const auto fingerprint = h2(mixed_hash);
    auto group_start = probe_start(mixed_hash);
    std::optional<std::size_t> first_deleted;

    for (std::size_t probe = 0; probe < capacity_; probe += kSwissGroupSize) {
        (void)probe;
        const auto control_word = detail::load_control_group64(&control_[group_start]);
        const auto fingerprint_mask = detail::equal_byte_mask(&control_[group_start], fingerprint);
        for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
            const auto index = group_start + offset;
            const auto control = detail::control_byte_at(control_word, offset);
            if (control == kSwissEmpty) {
                return first_deleted.value_or(index);
            }
            if (control == kSwissDeleted && !first_deleted.has_value()) {
                first_deleted = index;
            }
            if ((fingerprint_mask & (1ULL << offset)) != 0 && key_equals(slots_[index], key, key_hash)) {
                return index;
            }
        }
        group_start = next_group(group_start, capacity_);
    }
    return fail(ErrorCode::corrupted_data, "swiss table probe sequence exhausted without insert slot");
}

auto SwissTableIndex::set_key(Slot& slot, const std::string_view key, const std::uint64_t key_hash)
    -> Status {
    slot.key_size = static_cast<std::uint32_t>(key.size());
    slot.key_hash = key_hash;
    if (key.size() <= kSwissInlineKeyBytes) {
        slot.key_is_inline = true;
        slot.heap_key_offset = 0;
        std::memset(slot.inline_key, 0, kSwissInlineKeyBytes);
        if (!key.empty()) {
            std::memcpy(slot.inline_key, key.data(), key.size());
        }
        return {};
    }
    slot.key_is_inline = false;
    auto offset = heap_keys_.allocate(key.size());
    if (!offset) {
        slot = Slot{};
        return unexpected(offset.error());
    }
    slot.heap_key_offset = *offset;
    const auto destination = heap_keys_.data(slot.heap_key_offset, key.size());
    if (destination.size() != key.size()) {
        slot = Slot{};
        return fail(ErrorCode::corrupted_data, "index key arena allocation is out of range");
    }
    std::memcpy(const_cast<std::byte*>(destination.data()), key.data(), key.size());
    heap_live_bytes_ += key.size();
    return {};
}

auto SwissTableIndex::compact_heap_keys(const std::span<Slot> slots,
                                        const std::span<const std::uint8_t> control) -> Status {
    KeyArena compacted;
    std::vector<std::uint32_t> new_offsets(slots.size());
    std::size_t heap_bytes = 0;
    for (std::size_t index = 0; index < slots.size(); ++index) {
        const auto slot_control = control[index];
        if (slot_control == kSwissEmpty || slot_control == kSwissDeleted) {
            continue;
        }
        const auto& slot = slots[index];
        if (!slot.key_is_inline && slot.key_size > 0) {
            heap_bytes += slot.key_size;
        }
    }
    if (auto reserved = compacted.reserve(heap_bytes); !reserved) {
        return reserved;
    }
    for (std::size_t index = 0; index < slots.size(); ++index) {
        auto& slot = slots[index];
        const auto slot_control = control[index];
        if (slot_control == kSwissEmpty || slot_control == kSwissDeleted || slot.key_is_inline ||
            slot.key_size == 0) {
            continue;
        }
        const auto source = heap_keys_.data(slot.heap_key_offset, slot.key_size);
        if (source.size() != slot.key_size) {
            return fail(ErrorCode::corrupted_data, "heap key compaction source is out of range");
        }
        const auto offset = compacted.allocate(slot.key_size);
        if (!offset) {
            return unexpected(offset.error());
        }
        const auto destination = compacted.data(*offset, slot.key_size);
        if (destination.size() != slot.key_size) {
            return fail(ErrorCode::corrupted_data, "heap key compaction destination is out of range");
        }
        std::memcpy(const_cast<std::byte*>(destination.data()), source.data(), slot.key_size);
        new_offsets[index] = *offset;
    }
    for (std::size_t index = 0; index < slots.size(); ++index) {
        if (control[index] != kSwissEmpty && control[index] != kSwissDeleted && !slots[index].key_is_inline &&
            slots[index].key_size > 0) {
            slots[index].heap_key_offset = new_offsets[index];
        }
    }
    heap_keys_ = std::move(compacted);
    heap_dead_bytes_ = 0;
    heap_live_bytes_ = heap_keys_.allocated_bytes();
    return {};
}

auto SwissTableIndex::maybe_compact_heap_keys() -> Status {
    if (heap_dead_bytes_ == 0) {
        return {};
    }
    if (heap_dead_bytes_ < heap_live_bytes_ && heap_dead_bytes_ < kHeapArenaCompactDeadThreshold) {
        return {};
    }
    return compact_heap_keys(slots_, control_);
}

void SwissTableIndex::clear_slot(const std::size_t index) {
    if (!slots_[index].key_is_inline && slots_[index].key_size > 0) {
        if (heap_live_bytes_ >= slots_[index].key_size) {
            heap_live_bytes_ -= slots_[index].key_size;
        } else {
            heap_live_bytes_ = 0;
        }
        heap_dead_bytes_ += slots_[index].key_size;
    }
    control_[index] = kSwissDeleted;
    slots_[index] = Slot{};
}

auto SwissTableIndex::grow_if_needed() -> Status {
    if (capacity_ == 0) {
        return {};
    }
    const auto projected = size_ + 1;
    const auto maximum_size = capacity_ - capacity_ / 8U;
    if (projected <= maximum_size) {
        return {};
    }
    if (capacity_ > std::numeric_limits<std::size_t>::max() / 2U) {
        return fail(ErrorCode::arithmetic_overflow, "swiss table growth overflow");
    }
    return rehash(capacity_ << 1U);
}

auto SwissTableIndex::rehash(const std::size_t new_capacity) -> Status {
    auto normalized = normalize_capacity(new_capacity);
    if (!normalized) {
        return unexpected(normalized.error());
    }
    if (*normalized == capacity_) {
        return {};
    }

    std::vector<std::uint8_t> new_control(*normalized, kSwissEmpty);
    std::vector<Slot> new_slots(*normalized);
    std::vector<std::size_t> targets(capacity_, std::numeric_limits<std::size_t>::max());

    for (std::size_t index = 0; index < capacity_; ++index) {
        if (control_[index] == kSwissEmpty || control_[index] == kSwissDeleted) {
            continue;
        }
        const auto hash = mix_hash(slots_[index].key_hash);
        auto group_start = ((hash >> 7U) & ((*normalized / kSwissGroupSize) - 1U)) * kSwissGroupSize;
        bool placed{};
        for (std::size_t probe = 0; probe < *normalized && !placed; probe += kSwissGroupSize) {
            const auto control_word = detail::load_control_group64(&new_control[group_start]);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                if (detail::control_byte_at(control_word, offset) != kSwissEmpty) {
                    continue;
                }
                const auto target = group_start + offset;
                new_control[target] = control_[index];
                targets[index] = target;
                placed = true;
                break;
            }
            group_start = next_group(group_start, *normalized);
        }
        if (!placed) {
            return fail(ErrorCode::corrupted_data, "swiss table rehash could not place an entry");
        }
    }
    for (std::size_t index = 0; index < capacity_; ++index) {
        if (targets[index] != std::numeric_limits<std::size_t>::max()) {
            new_slots[targets[index]] = std::move(slots_[index]);
        }
    }
    if (auto compacted = compact_heap_keys(new_slots, new_control); !compacted) {
        return compacted;
    }
    capacity_ = *normalized;
    control_ = std::move(new_control);
    slots_ = std::move(new_slots);
    return {};
}

auto SwissTableIndex::find(const std::string_view key) const -> std::optional<RecordRef> {
    return find(HashedKey{key, hash_key(key)});
}

auto SwissTableIndex::find(const HashedKey& key) const -> std::optional<RecordRef> {
    const auto index = find_slot_index(key.key, key.hash);
    if (!index) {
        return std::nullopt;
    }
    return slots_[*index].ref;
}

auto SwissTableIndex::insert_or_assign(const std::string_view key, RecordRef ref)
    -> Result<IndexMutationResult> {
    return insert_or_assign(HashedKey{key, hash_key(key)}, ref);
}

auto SwissTableIndex::insert_or_assign(const HashedKey& key, RecordRef ref) -> Result<IndexMutationResult> {
    if (key.key.size() > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::invalid_argument, "index key exceeds supported size");
    }
    const auto mixed_hash = mix_hash(key.hash);
    auto slot_index = find_insert_index(key.key, key.hash, mixed_hash);
    if (!slot_index) {
        return unexpected(slot_index.error());
    }
    const auto index = *slot_index;
    const auto fingerprint = h2(mixed_hash);
    if (control_[index] == kSwissEmpty || control_[index] == kSwissDeleted) {
        const auto previous_control = control_[index];
        control_[index] = fingerprint;
        if (auto key_set = set_key(slots_[index], key.key, key.hash); !key_set) {
            control_[index] = previous_control;
            return unexpected(key_set.error());
        }
        slots_[index].ref = ref;
        ++size_;
        return IndexMutationResult{.inserted = true, .previous = std::nullopt};
    }
    const auto previous = slots_[index].ref;
    slots_[index].ref = ref;
    return IndexMutationResult{.inserted = false, .previous = previous};
}

auto SwissTableIndex::erase(const std::string_view key) -> IndexMutationResult {
    return erase(HashedKey{key, hash_key(key)});
}

auto SwissTableIndex::erase(const HashedKey& key) -> IndexMutationResult {
    auto result = erase_no_compact(key);
    if (result.previous) {
        (void)maybe_compact_heap_keys();
    }
    return result;
}

auto SwissTableIndex::erase_no_compact(const HashedKey& key) -> IndexMutationResult {
    const auto index = find_slot_index(key.key, key.hash);
    if (!index) {
        return {};
    }
    const auto previous = slots_[*index].ref;
    clear_slot(*index);
    --size_;
    return {.inserted = false, .previous = previous};
}

auto SwissTableIndex::prepare_insert(const HashedKey& key) -> Status {
    if (key.key.size() > std::numeric_limits<std::uint32_t>::max()) {
        return fail(ErrorCode::invalid_argument, "index key exceeds supported size");
    }
    if (find_slot_index(key.key, key.hash)) {
        return {};
    }
    if (size_ == std::numeric_limits<std::size_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "swiss table size overflow");
    }
    if (auto prepared = reserve(size_ + 1U); !prepared) {
        return prepared;
    }
    if (key.key.size() > kSwissInlineKeyBytes) {
        return heap_keys_.prepare_allocate(key.key.size());
    }
    return {};
}

auto SwissTableIndex::reserve(const std::size_t count) -> Status {
    const auto extra = count / 7U + (count % 7U == 0 ? 0U : 1U);
    if (extra > std::numeric_limits<std::size_t>::max() - count) {
        return fail(ErrorCode::arithmetic_overflow, "swiss table reserve overflow");
    }
    const auto minimum = count + extra;
    if (capacity_ == 0) {
        auto normalized = normalize_capacity(std::max(minimum, kSwissGroupSize));
        if (!normalized) {
            return unexpected(normalized.error());
        }
        capacity_ = *normalized;
        control_.assign(capacity_, kSwissEmpty);
        slots_.resize(capacity_);
        return {};
    }
    auto needed = normalize_capacity(std::max(minimum, kSwissGroupSize));
    if (!needed) {
        return unexpected(needed.error());
    }
    if (*needed > capacity_) {
        return rehash(*needed);
    }
    return {};
}

auto SwissTableIndex::entries() const -> std::vector<IndexEntry> {
    std::vector<IndexEntry> result;
    result.reserve(size_);
    for (std::size_t index = 0; index < capacity_; ++index) {
        const auto control = control_[index];
        if (control == kSwissEmpty || control == kSwissDeleted) {
            continue;
        }
        result.push_back({std::string{slot_key(slots_[index])}, slots_[index].ref});
    }
    return result;
}

auto SwissTableIndex::stats() const noexcept -> IndexStats {
    return {size_, capacity_,
            capacity_ == 0 ? 0.0F : static_cast<float>(size_) / static_cast<float>(capacity_),
            heap_keys_.allocated_bytes(), heap_live_bytes_};
}

auto SwissTableIndex::clone_empty() const -> SwissTableIndex {
    SwissTableIndex copy;
    copy.seed_ = seed_;
    return copy;
}

} // namespace glyphastore
