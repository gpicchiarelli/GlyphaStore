#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/index_types.hpp"
#include "glyphastore/index/key_arena.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace glyphastore {

inline constexpr std::size_t kSwissGroupSize = 8;
inline constexpr std::uint8_t kSwissEmpty = 0x80;
inline constexpr std::uint8_t kSwissDeleted = 0xFE;
inline constexpr float kSwissMaxLoadFactor = 7.0f / 8.0f;
inline constexpr std::size_t kSwissInlineKeyBytes = 24;

// Production Index partition: flat open-addressing table in the SwissTable layout.
class SwissTableIndex final {
  public:
    SwissTableIndex();

    [[nodiscard]] auto find(std::string_view key) const -> std::optional<RecordRef>;
    [[nodiscard]] auto find(const HashedKey& key) const -> std::optional<RecordRef>;
    [[nodiscard]] auto insert_or_assign(std::string_view key, RecordRef ref) -> Result<IndexMutationResult>;
    [[nodiscard]] auto insert_or_assign(const HashedKey& key, RecordRef ref) -> Result<IndexMutationResult>;
    auto erase(std::string_view key) -> IndexMutationResult;
    auto erase(const HashedKey& key) -> IndexMutationResult;
    auto erase_no_compact(const HashedKey& key) -> IndexMutationResult;
    [[nodiscard]] auto reserve(std::size_t count) -> Status;
    // Prepares every allocation needed to insert this key. With external
    // serialization, the subsequent insert_or_assign cannot grow the table or
    // the heap-key arena.
    [[nodiscard]] auto prepare_insert(const HashedKey& key) -> Status;
    [[nodiscard]] auto entries() const -> std::vector<IndexEntry>;
    [[nodiscard]] auto stats() const noexcept -> IndexStats;
    [[nodiscard]] auto clone_empty() const -> SwissTableIndex;

  private:
    struct Slot {
        RecordRef ref{};
        std::uint64_t key_hash{};
        std::uint32_t key_size{};
        bool key_is_inline{};
        alignas(RecordRef) std::byte inline_key[kSwissInlineKeyBytes]{};
        std::uint32_t heap_key_offset{};

        Slot() = default;
        Slot(Slot&&) = default;
        auto operator=(Slot&&) -> Slot& = default;
        Slot(const Slot&) = delete;
        auto operator=(const Slot&) -> Slot& = delete;
    };

    [[nodiscard]] auto mix_hash(std::uint64_t key_hash) const noexcept -> std::uint64_t;
    [[nodiscard]] static auto h2(std::uint64_t hash) noexcept -> std::uint8_t;
    [[nodiscard]] auto probe_start(std::uint64_t hash) const noexcept -> std::size_t;
    [[nodiscard]] static auto next_group(std::size_t group_start, std::size_t capacity) noexcept
        -> std::size_t;
    [[nodiscard]] auto key_equals(const Slot& slot, std::string_view key,
                                  std::uint64_t key_hash) const noexcept -> bool;
    [[nodiscard]] auto slot_key(const Slot& slot) const noexcept -> std::string_view;
    [[nodiscard]] auto find_slot_index(std::string_view key, std::uint64_t key_hash) const
        -> std::optional<std::size_t>;
    [[nodiscard]] auto find_insert_index(std::string_view key, std::uint64_t key_hash,
                                         std::uint64_t mixed_hash) -> Result<std::size_t>;
    [[nodiscard]] auto rehash(std::size_t new_capacity) -> Status;
    [[nodiscard]] auto set_key(Slot& slot, std::string_view key, std::uint64_t key_hash) -> Status;
    void clear_slot(std::size_t index);
    [[nodiscard]] auto compact_heap_keys(std::span<Slot> slots, std::span<const std::uint8_t> control)
        -> Status;
    [[nodiscard]] auto maybe_compact_heap_keys() -> Status;
    [[nodiscard]] auto grow_if_needed() -> Status;
    [[nodiscard]] static auto normalize_capacity(std::size_t minimum_slots) -> Result<std::size_t>;

    std::uint64_t seed_;
    std::size_t size_{};
    std::size_t capacity_{};
    std::size_t heap_live_bytes_{};
    std::size_t heap_dead_bytes_{};
    std::vector<std::uint8_t> control_;
    std::vector<Slot> slots_;
    KeyArena heap_keys_;
};

} // namespace glyphastore
