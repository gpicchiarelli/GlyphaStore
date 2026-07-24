#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/core/types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glyphastore {
namespace detail {

// Per-Worker durable hot-cache table: flat open addressing (power-of-two capacity,
// linear probe, tombstones). Uses the same FNV-1a key hash as Index routing; hash
// alone is never identity — full key bytes are compared on every candidate slot.
// Values ≤ kInlineValueBytes live in the slot; larger values use a shared heap buffer.
struct HotRecordEntry {
    static constexpr std::size_t kInlineValueBytes = 48;

    RecordRef reference{};
    std::shared_ptr<const std::byte[]> heap_value;
    alignas(std::max_align_t) std::byte inline_value[kInlineValueBytes]{};
    std::size_t value_size{};
    SequenceNumber sequence{};
    std::uint64_t expire_at_ns{};
    std::uint64_t accounted_bytes{};
    bool value_inline{};

    [[nodiscard]] auto value_span() const noexcept -> std::span<const std::byte> {
        if (value_size == 0) {
            return {};
        }
        if (value_inline) {
            return {inline_value, value_size};
        }
        return {heap_value.get(), value_size};
    }
};

struct HotRecordSnapshot {
    std::shared_ptr<const std::byte[]> heap_value;
    alignas(std::max_align_t) std::byte inline_value[HotRecordEntry::kInlineValueBytes]{};
    std::size_t value_size{};
    SequenceNumber sequence{};
    std::uint64_t expire_at_ns{};
    bool value_inline{};

    [[nodiscard]] auto value_span() const noexcept -> std::span<const std::byte> {
        if (value_size == 0) {
            return {};
        }
        if (value_inline) {
            return {inline_value, value_size};
        }
        return {heap_value.get(), value_size};
    }

    [[nodiscard]] static auto from_entry(const HotRecordEntry& entry) -> HotRecordSnapshot {
        HotRecordSnapshot snapshot{.value_size = entry.value_size,
                                   .sequence = entry.sequence,
                                   .expire_at_ns = entry.expire_at_ns,
                                   .value_inline = entry.value_inline};
        if (entry.value_inline) {
            std::copy_n(entry.inline_value, entry.value_size, snapshot.inline_value);
        } else {
            snapshot.heap_value = entry.heap_value;
        }
        return snapshot;
    }
};

class HotRecordTable final {
  public:
    static constexpr float kMaxLoadFactor = 0.5f;
    static constexpr std::size_t kMinimumCapacity = 64;

    HotRecordTable() {
        static_cast<void>(rehash(kMinimumCapacity));
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
    }
    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }
    [[nodiscard]] auto bucket_count() const noexcept -> std::size_t {
        return capacity_;
    }
    [[nodiscard]] auto tombstone_count() const noexcept -> std::size_t {
        return tombstones_;
    }

    [[nodiscard]] auto find(const std::string_view key, const std::uint64_t key_hash) noexcept
        -> HotRecordEntry* {
        if (capacity_ == 0) {
            return nullptr;
        }
        const auto mask = capacity_ - 1U;
        auto index = static_cast<std::size_t>(key_hash) & mask;
        for (std::size_t probed = 0; probed < capacity_; ++probed) {
            const auto control = control_[index];
            if (control == kEmpty) {
                return nullptr;
            }
            if (control == kFull && hashes_[index] == key_hash && keys_[index] == key) {
                return &entries_[index];
            }
            index = (index + 1U) & mask;
        }
        return nullptr;
    }

    [[nodiscard]] auto find(const std::string_view key, const std::uint64_t key_hash) const noexcept
        -> const HotRecordEntry* {
        return const_cast<HotRecordTable*>(this)->find(key, key_hash);
    }

    [[nodiscard]] auto find(const HashedKey& key) noexcept -> HotRecordEntry* {
        return find(key.key, key.hash);
    }

    // Insert or replace. Grows when occupancy would exceed the load factor.
    [[nodiscard]] auto insert_or_assign(std::string key, const std::uint64_t key_hash, HotRecordEntry entry)
        -> Status {
        if (auto grown = grow_for_insert(); !grown) {
            return grown;
        }
        const auto mask = capacity_ - 1U;
        auto index = static_cast<std::size_t>(key_hash) & mask;
        std::optional<std::size_t> tomb;
        for (std::size_t probed = 0; probed < capacity_; ++probed) {
            const auto control = control_[index];
            if (control == kFull && hashes_[index] == key_hash && keys_[index] == key) {
                entries_[index] = std::move(entry);
                return {};
            }
            if (control == kEmpty) {
                place_new(tomb.value_or(index), std::move(key), key_hash, std::move(entry));
                return {};
            }
            if (control == kTombstone && !tomb) {
                tomb = index;
            }
            index = (index + 1U) & mask;
        }
        if (!tomb) {
            return fail(ErrorCode::internal_error, "hot-cache insert probed a full table");
        }
        place_new(*tomb, std::move(key), key_hash, std::move(entry));
        return {};
    }

    // Erase by key. Returns true when a resident entry was removed.
    auto erase(const std::string_view key, const std::uint64_t key_hash) noexcept -> bool {
        if (capacity_ == 0) {
            return false;
        }
        const auto mask = capacity_ - 1U;
        auto index = static_cast<std::size_t>(key_hash) & mask;
        for (std::size_t probed = 0; probed < capacity_; ++probed) {
            const auto control = control_[index];
            if (control == kEmpty) {
                return false;
            }
            if (control == kFull && hashes_[index] == key_hash && keys_[index] == key) {
                clear_slot(index);
                return true;
            }
            index = (index + 1U) & mask;
        }
        return false;
    }

    auto erase(const HashedKey& key) noexcept -> bool {
        return erase(key.key, key.hash);
    }

    // Reserve capacity for at least `entries` live records at the configured load factor.
    [[nodiscard]] auto reserve(const std::size_t entries) -> Status {
        const auto needed = minimum_capacity_for(entries);
        if (!needed) {
            return unexpected(needed.error());
        }
        if (*needed <= capacity_) {
            return {};
        }
        return rehash(*needed);
    }

    template <typename Predicate> void erase_if(Predicate&& predicate) {
        for (std::size_t index = 0; index < capacity_; ++index) {
            if (control_[index] != kFull) {
                continue;
            }
            if (predicate(keys_[index], hashes_[index], entries_[index])) {
                clear_slot(index);
            }
        }
    }

    template <typename Callback> void for_each(Callback&& callback) const {
        for (std::size_t index = 0; index < capacity_; ++index) {
            if (control_[index] == kFull) {
                callback(keys_[index], hashes_[index], entries_[index]);
            }
        }
    }

    void clear() noexcept {
        for (std::size_t index = 0; index < capacity_; ++index) {
            if (control_[index] == kFull) {
                keys_[index].clear();
                keys_[index].shrink_to_fit();
                entries_[index] = HotRecordEntry{};
            }
            control_[index] = kEmpty;
            hashes_[index] = 0;
        }
        size_ = 0;
        tombstones_ = 0;
    }

  private:
    static constexpr std::uint8_t kEmpty = 0;
    static constexpr std::uint8_t kFull = 1;
    static constexpr std::uint8_t kTombstone = 2;

    [[nodiscard]] static auto normalize_capacity(std::size_t minimum) -> Result<std::size_t> {
        if (minimum < kMinimumCapacity) {
            minimum = kMinimumCapacity;
        }
        std::size_t capacity = kMinimumCapacity;
        while (capacity < minimum) {
            if (capacity > std::numeric_limits<std::size_t>::max() / 2U) {
                return fail(ErrorCode::arithmetic_overflow, "hot-cache capacity overflow");
            }
            capacity *= 2U;
        }
        return capacity;
    }

    [[nodiscard]] static auto minimum_capacity_for(const std::size_t entries) -> Result<std::size_t> {
        // capacity >= ceil(entries / max_load) == entries * 2 at load 0.5
        if (entries > std::numeric_limits<std::size_t>::max() / 2U) {
            return fail(ErrorCode::arithmetic_overflow, "hot-cache reserve overflow");
        }
        return normalize_capacity(std::max(entries * 2U, kMinimumCapacity));
    }

    [[nodiscard]] auto maximum_occupancy() const noexcept -> std::size_t {
        return capacity_ / 2U;
    }

    [[nodiscard]] auto grow_for_insert() -> Status {
        if (size_ < maximum_occupancy()) {
            // Rebuild when tombstones dominate probes.
            if (tombstones_ > maximum_occupancy() && size_ + tombstones_ > (capacity_ * 3U) / 4U) {
                return rehash(capacity_);
            }
            return {};
        }
        if (capacity_ > std::numeric_limits<std::size_t>::max() / 2U) {
            return fail(ErrorCode::arithmetic_overflow, "hot-cache growth overflow");
        }
        return rehash(capacity_ == 0 ? kMinimumCapacity : capacity_ * 2U);
    }

    [[nodiscard]] auto rehash(const std::size_t new_capacity) -> Status {
        auto normalized = normalize_capacity(new_capacity);
        if (!normalized) {
            return unexpected(normalized.error());
        }
        std::vector<std::uint8_t> control(*normalized, kEmpty);
        std::vector<std::uint64_t> hashes(*normalized, 0);
        std::vector<std::string> keys(*normalized);
        std::vector<HotRecordEntry> entries(*normalized);
        const auto mask = *normalized - 1U;
        for (std::size_t index = 0; index < capacity_; ++index) {
            if (control_[index] != kFull) {
                continue;
            }
            auto slot = static_cast<std::size_t>(hashes_[index]) & mask;
            while (control[slot] == kFull) {
                slot = (slot + 1U) & mask;
            }
            control[slot] = kFull;
            hashes[slot] = hashes_[index];
            keys[slot] = std::move(keys_[index]);
            entries[slot] = std::move(entries_[index]);
        }
        control_ = std::move(control);
        hashes_ = std::move(hashes);
        keys_ = std::move(keys);
        entries_ = std::move(entries);
        capacity_ = *normalized;
        tombstones_ = 0;
        return {};
    }

    void place_new(const std::size_t slot, std::string key, const std::uint64_t key_hash,
                   HotRecordEntry entry) noexcept {
        if (control_[slot] == kTombstone) {
            --tombstones_;
        }
        control_[slot] = kFull;
        hashes_[slot] = key_hash;
        keys_[slot] = std::move(key);
        entries_[slot] = std::move(entry);
        ++size_;
    }

    void clear_slot(const std::size_t index) noexcept {
        control_[index] = kTombstone;
        hashes_[index] = 0;
        keys_[index].clear();
        keys_[index].shrink_to_fit();
        entries_[index] = HotRecordEntry{};
        --size_;
        ++tombstones_;
    }

    std::size_t size_{};
    std::size_t tombstones_{};
    std::size_t capacity_{};
    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::vector<std::string> keys_;
    std::vector<HotRecordEntry> entries_;
};

[[nodiscard]] inline auto hot_record_slot_bytes() noexcept -> std::uint64_t {
    // Control + hash + SSO string + entry. Conservative upper bound used for budgets.
    constexpr auto per_slot = static_cast<std::uint64_t>(sizeof(std::uint8_t) + sizeof(std::uint64_t) +
                                                         sizeof(std::string) + sizeof(HotRecordEntry));
    return per_slot;
}

[[nodiscard]] inline auto hot_record_accounted_bytes(const std::size_t key_bytes,
                                                     const std::size_t value_bytes) -> Result<std::uint64_t> {
    // Flat slot overhead (no separate hash-map node). Keys are owned once in the slot.
    // Values ≤ inline buffer do not charge heap bytes. Hash is never identity.
    const auto fixed = hot_record_slot_bytes();
    if (key_bytes > std::numeric_limits<std::uint64_t>::max() - fixed) {
        return fail(ErrorCode::arithmetic_overflow, "hot-cache key accounting overflow");
    }
    const auto with_key = fixed + static_cast<std::uint64_t>(key_bytes);
    const auto charged_value =
        value_bytes <= HotRecordEntry::kInlineValueBytes ? 0U : static_cast<std::uint64_t>(value_bytes);
    if (charged_value > std::numeric_limits<std::uint64_t>::max() - with_key) {
        return fail(ErrorCode::arithmetic_overflow, "hot-cache value accounting overflow");
    }
    return with_key + charged_value;
}

struct HotRecordReservePlan {
    std::size_t target{};
    bool overflow{};
};

[[nodiscard]] constexpr auto plan_hot_record_reserve(const std::size_t current_size,
                                                     const std::size_t additional_records,
                                                     const std::size_t current_capacity) noexcept
    -> HotRecordReservePlan {
    if (additional_records > std::numeric_limits<std::size_t>::max() - current_size) {
        return {.overflow = true};
    }
    const auto required = current_size + additional_records;
    // Flat table max load 0.5 → need capacity >= 2 * live entries.
    if (additional_records == 0 || required <= current_capacity / 2U) {
        return {};
    }
    if (required > std::numeric_limits<std::size_t>::max() / 2U) {
        return {.overflow = true};
    }
    constexpr std::size_t kMinimumReserve = 64;
    const auto needed = required * 2U;
    const auto geometric = current_capacity > std::numeric_limits<std::size_t>::max() / 2U
                               ? std::numeric_limits<std::size_t>::max()
                               : current_capacity * 2U;
    return {.target = std::max({needed, geometric, kMinimumReserve})};
}

} // namespace detail
} // namespace glyphastore
