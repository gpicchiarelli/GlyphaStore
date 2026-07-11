#include "glyphastore/index/swiss_table.hpp"

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>

namespace glyphastore {
namespace {

inline constexpr std::uint64_t kSwissSeed = 0x243F6A8885A308D3ULL;
inline constexpr std::uint64_t kSwissMixConstant = 0x9E3779B97F4A7C15ULL;

} // namespace

SwissTableIndex::SwissTableIndex() : seed_(kSwissSeed) {
    capacity_ = normalize_capacity(kSwissGroupSize);
    control_.assign(capacity_, kSwissEmpty);
    slots_.resize(capacity_);
}

auto SwissTableIndex::normalize_capacity(const std::size_t minimum_slots) -> std::size_t {
    std::size_t capacity = kSwissGroupSize;
    while (capacity < minimum_slots) {
        capacity <<= 1U;
    }
    return capacity;
}

auto SwissTableIndex::hash_slot(const std::string_view key) const noexcept -> std::uint64_t {
    std::uint64_t hash = hash_key(key);
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
    return {reinterpret_cast<const char*>(slot.heap_key.get()), slot.key_size};
}

auto SwissTableIndex::key_equals(const Slot& slot, const std::string_view key) const noexcept -> bool {
    const auto stored = slot_key(slot);
    return stored.size() == key.size() && stored == key;
}

auto SwissTableIndex::find_slot_index(const std::string_view key) const -> std::optional<std::size_t> {
    if (capacity_ == 0) {
        return std::nullopt;
    }
    const auto hash = hash_slot(key);
    const auto fingerprint = h2(hash);
    auto group_start = probe_start(hash);
    for (std::size_t probe = 0; probe < capacity_; probe += kSwissGroupSize) {
        (void)probe;
        for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
            const auto index = group_start + offset;
            const auto control = control_[index];
            if (control == kSwissEmpty) {
                return std::nullopt;
            }
            if (control == fingerprint && key_equals(slots_[index], key)) {
                return index;
            }
        }
        group_start = next_group(group_start, capacity_);
    }
    return std::nullopt;
}

auto SwissTableIndex::find_insert_index(const std::string_view key) -> Result<std::size_t> {
    grow_if_needed();
    const auto hash = hash_slot(key);
    const auto fingerprint = h2(hash);
    auto group_start = probe_start(hash);
    std::optional<std::size_t> first_deleted;

    for (std::size_t probe = 0; probe < capacity_; probe += kSwissGroupSize) {
        (void)probe;
        for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
            const auto index = group_start + offset;
            const auto control = control_[index];
            if (control == kSwissEmpty) {
                return first_deleted.value_or(index);
            }
            if (control == kSwissDeleted && !first_deleted.has_value()) {
                first_deleted = index;
            }
            if (control == fingerprint && key_equals(slots_[index], key)) {
                return index;
            }
        }
        group_start = next_group(group_start, capacity_);
    }
    return fail(ErrorCode::corrupted_data, "swiss table probe sequence exhausted without insert slot");
}

void SwissTableIndex::set_key(Slot& slot, const std::string_view key) {
    slot.key_size = static_cast<std::uint16_t>(key.size());
    if (key.size() <= kSwissInlineKeyBytes) {
        slot.key_is_inline = true;
        slot.heap_key.reset();
        std::memset(slot.inline_key, 0, kSwissInlineKeyBytes);
        if (!key.empty()) {
            std::memcpy(slot.inline_key, key.data(), key.size());
        }
        return;
    }
    slot.key_is_inline = false;
    slot.heap_key = std::make_unique_for_overwrite<std::byte[]>(key.size());
    std::memcpy(slot.heap_key.get(), key.data(), key.size());
}

void SwissTableIndex::clear_slot(const std::size_t index) {
    control_[index] = kSwissDeleted;
    slots_[index] = Slot{};
}

void SwissTableIndex::grow_if_needed() {
    if (capacity_ == 0) {
        return;
    }
    const auto projected = size_ + 1;
    if (static_cast<float>(projected) / static_cast<float>(capacity_) <= kSwissMaxLoadFactor) {
        return;
    }
    rehash(capacity_ << 1U);
}

void SwissTableIndex::rehash(const std::size_t new_capacity) {
    const auto normalized = normalize_capacity(new_capacity);
    if (normalized == capacity_) {
        return;
    }

    auto old_control = std::move(control_);
    auto old_slots = std::move(slots_);
    const auto old_capacity = capacity_;

    capacity_ = normalized;
    size_ = 0;
    control_.assign(capacity_, kSwissEmpty);
    slots_.clear();
    slots_.resize(capacity_);

    for (std::size_t index = 0; index < old_capacity; ++index) {
        if (old_control[index] == kSwissEmpty || old_control[index] == kSwissDeleted) {
            continue;
        }
        const auto key = slot_key(old_slots[index]);
        auto target = find_insert_index(key);
        if (!target) {
            capacity_ = old_capacity;
            control_ = std::move(old_control);
            slots_ = std::move(old_slots);
            return;
        }
        const auto slot_index = *target;
        control_[slot_index] = old_control[index];
        slots_[slot_index] = std::move(old_slots[index]);
        ++size_;
    }
}

auto SwissTableIndex::find(const std::string_view key) const -> std::optional<RecordRef> {
    const auto index = find_slot_index(key);
    if (!index) {
        return std::nullopt;
    }
    return slots_[*index].ref;
}

auto SwissTableIndex::insert_or_assign(const std::string_view key, RecordRef ref) -> IndexMutationResult {
    auto slot_index = find_insert_index(key);
    if (!slot_index) {
        return {};
    }
    const auto index = *slot_index;
    const auto fingerprint = h2(hash_slot(key));
    if (control_[index] == kSwissEmpty || control_[index] == kSwissDeleted) {
        control_[index] = fingerprint;
        set_key(slots_[index], key);
        slots_[index].ref = ref;
        ++size_;
        return {.inserted = true, .previous = std::nullopt};
    }
    const auto previous = slots_[index].ref;
    slots_[index].ref = ref;
    return {.inserted = false, .previous = previous};
}

auto SwissTableIndex::erase(const std::string_view key) -> IndexMutationResult {
    const auto index = find_slot_index(key);
    if (!index) {
        return {};
    }
    const auto previous = slots_[*index].ref;
    clear_slot(*index);
    --size_;
    return {.inserted = false, .previous = previous};
}

void SwissTableIndex::reserve(const std::size_t count) {
    if (capacity_ == 0) {
        capacity_ = normalize_capacity(std::max(count, kSwissGroupSize));
        control_.assign(capacity_, kSwissEmpty);
        slots_.resize(capacity_);
        return;
    }
    const auto needed =
        normalize_capacity(static_cast<std::size_t>(static_cast<float>(count) / kSwissMaxLoadFactor) + 1U);
    if (needed > capacity_) {
        rehash(needed);
    }
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
            capacity_ == 0 ? 0.0F : static_cast<float>(size_) / static_cast<float>(capacity_)};
}

auto SwissTableIndex::clone_empty() const -> SwissTableIndex {
    SwissTableIndex copy;
    copy.seed_ = seed_;
    return copy;
}

} // namespace glyphastore
