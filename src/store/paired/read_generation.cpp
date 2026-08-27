#include "glyphastore/store/paired/read_generation.hpp"

#include "experimental/pair_read_generation_shell.hpp"
#include "glyphastore/core/hot_path_phases.hpp"
#include "glyphastore/store/paired/generation_slot_pool.hpp"
#include "glyphastore/index/swiss_control_group.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <deque>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace glyphastore::store::paired {
namespace {

inline constexpr std::size_t kDeltaPageSlots = 16;
inline constexpr std::size_t kDeltaDirectoryBlockPages = 16;
// Second COW level above directory blocks. A full 40 960-entry Delta uses 256
// blocks; copying that spine on every single-record publication was a mixed-path
// tax. Chunks keep per-publication spine traffic to one 16-block group.
inline constexpr std::size_t kDeltaDirectoryChunkBlocks = 16;
inline constexpr std::size_t kFlatDeltaMaximumPages = 32;
inline constexpr std::size_t kMaximumPublicationBatch = 32;
inline constexpr std::size_t kDeltaArenaBlockRecords = 64;
inline constexpr std::size_t kLargeImmutableArrayMappingThreshold = 1U << 20U;
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
inline constexpr bool kLargeImmutableArrayMappingEnabled = false;
#else
inline constexpr bool kLargeImmutableArrayMappingEnabled = true;
#endif
#elif defined(__SANITIZE_ADDRESS__)
inline constexpr bool kLargeImmutableArrayMappingEnabled = false;
#else
inline constexpr bool kLargeImmutableArrayMappingEnabled = true;
#endif
std::atomic_size_t immutable_base_spare_mapping_payload_bytes{};
static_assert(kDeltaPageSlots % kSwissGroupSize == 0);

#if defined(__unix__) || defined(__APPLE__)
class LargeImmutableMappingPool final {
  public:
    LargeImmutableMappingPool() noexcept = default;
    LargeImmutableMappingPool(const LargeImmutableMappingPool&) = delete;
    auto operator=(const LargeImmutableMappingPool&) -> LargeImmutableMappingPool& = delete;

    ~LargeImmutableMappingPool() {
        std::scoped_lock lock{mutex_};
        release_locked();
    }

    [[nodiscard]] auto take(const std::size_t mapped_bytes) noexcept -> void* {
        std::scoped_lock lock{mutex_};
        if (base_ == nullptr) {
            return nullptr;
        }
        if (mapped_bytes != mapped_bytes_) {
            release_locked();
            return nullptr;
        }
        auto* result = base_;
        immutable_base_spare_mapping_payload_bytes.fetch_sub(payload_bytes_, std::memory_order_relaxed);
        base_ = nullptr;
        mapped_bytes_ = 0;
        payload_bytes_ = 0;
        return result;
    }

    void put(void* base, const std::size_t mapped_bytes, const std::size_t payload_bytes) noexcept {
        std::scoped_lock lock{mutex_};
        release_locked();
        base_ = base;
        mapped_bytes_ = mapped_bytes;
        payload_bytes_ = payload_bytes;
        immutable_base_spare_mapping_payload_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
    }

  private:
    void release_locked() noexcept {
        if (base_ == nullptr) {
            return;
        }
        immutable_base_spare_mapping_payload_bytes.fetch_sub(payload_bytes_, std::memory_order_relaxed);
        static_cast<void>(::munmap(base_, mapped_bytes_));
        base_ = nullptr;
        mapped_bytes_ = 0;
        payload_bytes_ = 0;
    }

    std::mutex mutex_;
    void* base_{};
    std::size_t mapped_bytes_{};
    std::size_t payload_bytes_{};
};
#endif

// Immutable base rebuilds grow by merge quanta. General-purpose allocators may
// retain every previously used large size, turning bounded live generations
// into unbounded resident high-water. Large arrays therefore use their own
// anonymous mappings in geometric size classes. Each immutable-base lineage
// keeps at most one retired mapping: successive same-class rebuilds alternate
// between two bounded buffers instead of paying zero-fill faults every time.
// A class change or thread exit unmaps the spare. Guard pages preserve coarse
// over/underflow detection even outside ASan.
template <typename T> class ReleasingLargeAllocator {
  public:
    using value_type = T;
    using is_always_equal = std::false_type;
    using propagate_on_container_move_assignment = std::true_type;

#if defined(__unix__) || defined(__APPLE__)
    ReleasingLargeAllocator() : pool_(std::make_shared<LargeImmutableMappingPool>()) {}
    explicit ReleasingLargeAllocator(std::shared_ptr<LargeImmutableMappingPool> pool) noexcept
        : pool_(std::move(pool)) {}
    template <typename U>
    ReleasingLargeAllocator(const ReleasingLargeAllocator<U>& other) noexcept : pool_(other.pool_) {}

    [[nodiscard]] auto mapping_pool() const noexcept -> const std::shared_ptr<LargeImmutableMappingPool>& {
        return pool_;
    }
#else
    ReleasingLargeAllocator() noexcept = default;
    template <typename U> ReleasingLargeAllocator(const ReleasingLargeAllocator<U>&) noexcept {}
#endif

    [[nodiscard]] static constexpr auto mapped_storage_bytes(const std::size_t count) noexcept
        -> std::size_t {
#if defined(__unix__) || defined(__APPLE__)
        if (count <= std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            const auto bytes = count * sizeof(T);
            return kLargeImmutableArrayMappingEnabled && bytes >= kLargeImmutableArrayMappingThreshold ? bytes
                                                                                                       : 0U;
        }
#else
        static_cast<void>(count);
#endif
        return 0U;
    }

#if defined(__unix__) || defined(__APPLE__)
    struct MappingLayout final {
        std::size_t page_size{};
        std::size_t payload_bytes{};
        std::size_t mapped_bytes{};
    };

    [[nodiscard]] static auto layout_for(const std::size_t bytes) -> MappingLayout {
        const auto page_size_value = ::sysconf(_SC_PAGESIZE);
        if (page_size_value <= 0) {
            throw std::bad_alloc{};
        }
        const auto page_size = static_cast<std::size_t>(page_size_value);
        if (page_size > std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::bad_array_new_length{};
        }
        auto payload_class = kLargeImmutableArrayMappingThreshold;
        while (payload_class < bytes) {
            if (payload_class > std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::bad_array_new_length{};
            }
            payload_class *= 2U;
        }
        if (payload_class > std::numeric_limits<std::size_t>::max() - (page_size - 1U)) {
            throw std::bad_array_new_length{};
        }
        const auto payload_bytes = ((payload_class + page_size - 1U) / page_size) * page_size;
        if (payload_bytes > std::numeric_limits<std::size_t>::max() - 2U * page_size) {
            throw std::bad_array_new_length{};
        }
        return {.page_size = page_size,
                .payload_bytes = payload_bytes,
                .mapped_bytes = payload_bytes + 2U * page_size};
    }

#endif

    [[nodiscard]] auto allocate(const std::size_t count) -> T* {
        if (count > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length{};
        }
        const auto bytes = count * sizeof(T);
#if defined(__unix__) || defined(__APPLE__)
        if (kLargeImmutableArrayMappingEnabled && bytes >= kLargeImmutableArrayMappingThreshold) {
            const auto layout = layout_for(bytes);
            auto* mapping = pool_->take(layout.mapped_bytes);
            if (mapping == nullptr) {
#if defined(MAP_ANONYMOUS)
                constexpr auto kAnonymousFlag = MAP_ANONYMOUS;
#else
                constexpr auto kAnonymousFlag = MAP_ANON;
#endif
                mapping = ::mmap(nullptr, layout.mapped_bytes, PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | kAnonymousFlag, -1, 0);
                if (mapping == MAP_FAILED) {
                    throw std::bad_alloc{};
                }
                auto* trailing_guard =
                    static_cast<std::byte*>(mapping) + layout.page_size + layout.payload_bytes;
                if (::mprotect(mapping, layout.page_size, PROT_NONE) != 0 ||
                    ::mprotect(trailing_guard, layout.page_size, PROT_NONE) != 0) {
                    static_cast<void>(::munmap(mapping, layout.mapped_bytes));
                    throw std::bad_alloc{};
                }
            }
            return reinterpret_cast<T*>(static_cast<std::byte*>(mapping) + layout.page_size);
        }
#endif
        return std::allocator<T>{}.allocate(count);
    }

    void deallocate(T* pointer, const std::size_t count) noexcept {
        const auto bytes = count * sizeof(T);
#if defined(__unix__) || defined(__APPLE__)
        if (kLargeImmutableArrayMappingEnabled && bytes >= kLargeImmutableArrayMappingThreshold) {
            try {
                const auto layout = layout_for(bytes);
                pool_->put(reinterpret_cast<std::byte*>(pointer) - layout.page_size, layout.mapped_bytes,
                           layout.payload_bytes);
                return;
            } catch (...) {
                std::terminate();
            }
        }
#endif
        std::allocator<T>{}.deallocate(pointer, count);
    }

    template <typename U>
    [[nodiscard]] auto operator==(const ReleasingLargeAllocator<U>& other) const noexcept -> bool {
#if defined(__unix__) || defined(__APPLE__)
        return pool_ == other.pool_;
#else
        static_cast<void>(other);
        return true;
#endif
    }

  private:
#if defined(__unix__) || defined(__APPLE__)
    std::shared_ptr<LargeImmutableMappingPool> pool_;
    template <typename> friend class ReleasingLargeAllocator;
#endif
};

struct ReadRecord final {
    std::uint64_t hash{};
    std::string key;
    RecordRef record;
    SegmentPtr segment;
    std::optional<DurableRuntimeCatalog::PublishedReadRecord> durable;
    Opcode opcode{Opcode::put};
};

using ReadRecordHandle = std::shared_ptr<const ReadRecord>;
using MutableReadIndex = std::vector<ReadRecordHandle>;
using DurableReadPin = DurableRuntimeCatalog::PublishedReadPin;

[[nodiscard]] auto record_key(const ReadRecord& record) noexcept -> std::string_view {
    return record.durable ? record.durable->key() : std::string_view{record.key};
}

struct ReadRecordView final {
    std::uint64_t hash{};
    std::string_view key;
    RecordRef record;
    const SegmentPtr* segment{};
    const DurableReadPin* durable{};
    Opcode opcode{Opcode::put};
};

[[nodiscard]] auto view_of(const ReadRecord& record) noexcept -> ReadRecordView {
    return {.hash = record.hash,
            .key = record_key(record),
            .record = record.record,
            .segment = record.segment ? &record.segment : nullptr,
            .durable = record.durable ? &record.durable->pin() : nullptr,
            .opcode = record.opcode};
}

[[nodiscard]] auto view_of(const ReadMutation& mutation) noexcept -> ReadRecordView {
    return {.hash = mutation.key.hash,
            .key = mutation.key.key,
            .record = mutation.record,
            .segment = mutation.segment ? &mutation.segment : nullptr,
            .durable = mutation.durable ? &mutation.durable->pin() : nullptr,
            .opcode = mutation.opcode};
}

[[nodiscard]] auto materialize(const ReadRecordView& view) -> ReadRecordHandle {
    std::optional<DurableRuntimeCatalog::PublishedReadRecord> durable;
    if (view.durable != nullptr) {
        durable.emplace(DurableRuntimeCatalog::PublishedReadRecord::bind(std::string{view.key}, view.hash,
                                                                         view.record, *view.durable));
    }
    return std::make_shared<const ReadRecord>(ReadRecord{
        .hash = view.hash,
        .key = durable ? std::string{} : std::string{view.key},
        .record = view.record,
        .segment = view.segment == nullptr ? SegmentPtr{} : *view.segment,
        .durable = std::move(durable),
        .opcode = view.opcode,
    });
}

[[nodiscard]] auto record_less(const ReadRecordHandle& left, const ReadRecordHandle& right) noexcept -> bool {
    return left->hash < right->hash || (left->hash == right->hash && record_key(*left) < record_key(*right));
}

void apply_record(MutableReadIndex& index, ReadRecordHandle record) {
    const auto position = std::lower_bound(index.begin(), index.end(), record, record_less);
    const bool present = position != index.end() && (*position)->hash == record->hash &&
                         record_key(**position) == record_key(*record);
    if (record->opcode == Opcode::erase) {
        if (present) {
            index.erase(position);
        }
        return;
    }
    if (present) {
        *position = std::move(record);
    } else {
        index.insert(position, std::move(record));
    }
}

[[nodiscard]] auto fingerprint(const std::uint64_t hash) noexcept -> std::uint8_t {
    const auto result = static_cast<std::uint8_t>(hash & 0x7FU);
    return result == 0 ? static_cast<std::uint8_t>(1) : result;
}

[[nodiscard]] constexpr auto saturating_add(const std::size_t left, const std::size_t right) noexcept
    -> std::size_t {
    return right > std::numeric_limits<std::size_t>::max() - left ? std::numeric_limits<std::size_t>::max()
                                                                  : left + right;
}

[[nodiscard]] constexpr auto saturating_multiply(const std::size_t left, const std::size_t right) noexcept
    -> std::size_t {
    return left != 0 && right > std::numeric_limits<std::size_t>::max() / left
               ? std::numeric_limits<std::size_t>::max()
               : left * right;
}

[[nodiscard]] auto same_segment(const SegmentPtr& segment, const RecordRef& record) noexcept -> bool {
    return segment && segment->id() == record.segment_id && segment->generation() == record.generation;
}

[[nodiscard]] auto decode_pinned(const ReadRecordView& record) -> Result<RecordView> {
    if (record.segment == nullptr || !same_segment(*record.segment, record.record)) {
        return fail(ErrorCode::invalid_reference, "read generation has no exact Segment generation pin");
    }
    const auto offset = static_cast<std::size_t>(record.record.offset.value);
    const auto size = static_cast<std::size_t>(record.record.size.value);
    if (offset > (*record.segment)->capacity() || size > (*record.segment)->capacity() - offset) {
        return fail(ErrorCode::invalid_reference, "read generation RecordRef exceeds pinned Segment");
    }
    auto decoded = decode_record({(*record.segment)->base() + offset, size});
    if (!decoded) {
        return unexpected(std::move(decoded.error()));
    }
    if (decoded->sequence != record.record.sequence || decoded->encoded_size != record.record.size.value ||
        decoded->opcode != record.opcode) {
        return fail(ErrorCode::invalid_reference, "read generation RecordRef identity mismatch");
    }
    return std::move(*decoded);
}

// One immutable Delta cell. Ownership of keys and pins is lifted to the
// generation arena; Swiss pages copy only this stable pointer. The Writer is
// the sole appender and Readers can only observe fully constructed cells that
// were published through a generation release/acquire edge.
struct alignas(64) DeltaRecord final {
    union KeyStorage {
        std::array<char, 16> inline_key{};
        const char* external_key;
    };
    RecordRef record;
    KeyStorage key;
    const void* pin{};
    std::uint32_t key_size{};
    Opcode opcode{Opcode::put};
    bool durable{};
};
static_assert(sizeof(DeltaRecord) == 64, "a Delta record must occupy exactly one cache line");
static_assert(std::is_trivially_copyable_v<DeltaRecord>);

[[nodiscard]] auto delta_record_key(const DeltaRecord& record) noexcept -> std::string_view {
    return record.key_size <= record.key.inline_key.size()
               ? std::string_view{record.key.inline_key.data(), record.key_size}
               : std::string_view{record.key.external_key, record.key_size};
}

[[nodiscard]] auto delta_record_view(const DeltaRecord& record, const std::uint64_t hash) noexcept
    -> ReadRecordView {
    return {.hash = hash,
            .key = delta_record_key(record),
            .record = record.record,
            .segment = !record.durable && record.pin != nullptr ? static_cast<const SegmentPtr*>(record.pin)
                                                                : nullptr,
            .durable = record.durable && record.pin != nullptr
                           ? static_cast<const DurableReadPin*>(record.pin)
                           : nullptr,
            .opcode = record.opcode};
}

struct DeltaKeyBlock final {
    std::unique_ptr<char[]> bytes;
    std::size_t capacity{};
    std::size_t used{};
};

struct DeltaRecordBlock final {
    std::unique_ptr<DeltaRecord[]> records;
    std::size_t used{};
};

class DeltaArena final {
  public:
    explicit DeltaArena(const std::size_t maximum_records) : maximum_records_(maximum_records) {
        record_blocks_.reserve((maximum_records + kDeltaArenaBlockRecords - 1U) / kDeltaArenaBlockRecords);
    }

    [[nodiscard]] auto store(const ReadRecordView& source) -> const DeltaRecord* {
        if (record_count_ == maximum_records_ ||
            source.key.size() > std::numeric_limits<std::uint32_t>::max()) {
            throw std::bad_alloc{};
        }
        ensure_record_capacity();
        auto* external_key = source.key.size() > DeltaRecord::KeyStorage{}.inline_key.size()
                                 ? reserve_key(source.key.size())
                                 : nullptr;
        const void* pin = nullptr;
        bool durable{};
        if (source.opcode == Opcode::put && source.durable != nullptr) {
            durable = true;
            pin = retain_durable_pin(*source.durable);
        } else if (source.opcode == Opcode::put && source.segment != nullptr) {
            pin = retain_segment_pin(SegmentPtr{*source.segment}, source.record);
        }

        auto& block = record_blocks_.back();
        auto* destination = &block.records[block.used++];
        destination->record = source.record;
        destination->pin = pin;
        destination->key_size = static_cast<std::uint32_t>(source.key.size());
        destination->opcode = source.opcode;
        destination->durable = durable;
        if (external_key == nullptr) {
            std::ranges::copy(source.key, destination->key.inline_key.begin());
        } else {
            std::ranges::copy(source.key, external_key);
            destination->key.external_key = external_key;
            key_blocks_.back().used += source.key.size();
            key_bytes_ += source.key.size();
        }
        ++record_count_;
        return destination;
    }

    [[nodiscard]] auto record_count() const noexcept -> std::size_t {
        return record_count_;
    }

    [[nodiscard]] auto available_records() const noexcept -> std::size_t {
        return maximum_records_ - record_count_;
    }

    [[nodiscard]] auto allocated_record_bytes() const noexcept -> std::size_t {
        return record_blocks_.size() * kDeltaArenaBlockRecords * sizeof(DeltaRecord);
    }

    [[nodiscard]] auto key_bytes() const noexcept -> std::size_t {
        return key_bytes_;
    }

    [[nodiscard]] auto allocated_key_bytes() const noexcept -> std::size_t {
        return allocated_key_bytes_;
    }

  private:
    void ensure_record_capacity() {
        if (!record_blocks_.empty() && record_blocks_.back().used != kDeltaArenaBlockRecords) {
            return;
        }
        record_blocks_.push_back({
            .records = std::make_unique_for_overwrite<DeltaRecord[]>(kDeltaArenaBlockRecords),
        });
    }

    [[nodiscard]] auto reserve_key(const std::size_t size) -> char* {
        constexpr std::size_t kKeyBlockBytes = 4U * 1024U;
        if (key_blocks_.empty() || size > key_blocks_.back().capacity - key_blocks_.back().used) {
            const auto capacity = std::max(kKeyBlockBytes, size);
            key_blocks_.push_back({.bytes = std::make_unique<char[]>(capacity), .capacity = capacity});
            allocated_key_bytes_ += capacity;
        }
        return key_blocks_.back().bytes.get() + key_blocks_.back().used;
    }

    // Pins live in a deque so element addresses stay stable across push_back
    // (DeltaRecord::pin stores a raw SegmentPtr*). By-value intake lets callers
    // move a SegmentPtr in; steady-state same-segment hits avoid any insert.
    [[nodiscard]] auto retain_segment_pin(SegmentPtr source, const RecordRef& record) -> const SegmentPtr* {
        if (!segment_pins_.empty() && same_segment(segment_pins_.back(), record)) {
            return &segment_pins_.back();
        }
        const auto found = std::find_if(segment_pins_.begin(), segment_pins_.end(),
                                        [&](const auto& pin) { return same_segment(pin, record); });
        if (found != segment_pins_.end()) {
            return &*found;
        }
        segment_pins_.push_back(std::move(source));
        return &segment_pins_.back();
    }

    [[nodiscard]] auto retain_durable_pin(const DurableReadPin& source) -> const DurableReadPin* {
        if (!durable_pins_.empty() && durable_pins_.back()->same_generation(source)) {
            return durable_pins_.back().get();
        }
        const auto found = std::find_if(durable_pins_.begin(), durable_pins_.end(),
                                        [&](const auto& pin) { return pin->same_generation(source); });
        if (found != durable_pins_.end()) {
            return found->get();
        }
        auto pin = std::make_unique<DurableReadPin>(source);
        const auto* result = pin.get();
        durable_pins_.push_back(std::move(pin));
        return result;
    }

    const std::size_t maximum_records_;
    std::size_t record_count_{};
    std::size_t key_bytes_{};
    std::size_t allocated_key_bytes_{};
    std::vector<DeltaRecordBlock> record_blocks_;
    std::vector<DeltaKeyBlock> key_blocks_;
    std::deque<SegmentPtr> segment_pins_;
    std::vector<std::unique_ptr<DurableReadPin>> durable_pins_;
};

struct DeltaPage final {
    DeltaPage() {
        control.fill(kSwissEmpty);
    }

    std::array<std::uint8_t, kDeltaPageSlots> control{};
    std::array<std::uint64_t, kDeltaPageSlots> hashes{};
    std::array<const DeltaRecord*, kDeltaPageSlots> records{};
};

struct DeltaDirectoryBlock final {
    std::array<std::shared_ptr<const DeltaPage>, kDeltaDirectoryBlockPages> pages{};
};

struct DeltaDirectoryChunk final {
    std::array<std::shared_ptr<const DeltaDirectoryBlock>, kDeltaDirectoryChunkBlocks> blocks{};
};

struct DeltaBuilderScratch final {
    DeltaBuilderScratch() {
        mutable_chunks.reserve(kMaximumPublicationBatch);
        mutable_blocks.reserve(kMaximumPublicationBatch);
        mutable_pages.reserve(kMaximumPublicationBatch);
    }

    void reset() noexcept {
        mutable_chunks.clear();
        mutable_blocks.clear();
        mutable_pages.clear();
    }

    std::vector<std::pair<std::size_t, std::shared_ptr<DeltaDirectoryChunk>>> mutable_chunks;
    std::vector<std::pair<std::size_t, std::shared_ptr<DeltaDirectoryBlock>>> mutable_blocks;
    std::vector<std::pair<std::size_t, std::shared_ptr<DeltaPage>>> mutable_pages;
};

[[nodiscard]] auto copy_flat_delta_spine(const std::vector<std::shared_ptr<const DeltaPage>>& source)
    -> std::vector<std::shared_ptr<const DeltaPage>> {
    GS_PHASE_PUT(delta_flat_spine_copy);
    return source;
}

[[nodiscard]] auto
copy_directory_delta_spine(const std::vector<std::shared_ptr<const DeltaDirectoryChunk>>& source)
    -> std::vector<std::shared_ptr<const DeltaDirectoryChunk>> {
    GS_PHASE_PUT(delta_directory_spine_copy);
    return source;
}

[[nodiscard]] auto current_delta_builder_scratch() -> DeltaBuilderScratch& {
    thread_local DeltaBuilderScratch scratch;
    return scratch;
}

[[nodiscard]] auto post_delta_builder_scratch() -> DeltaBuilderScratch& {
    thread_local DeltaBuilderScratch scratch;
    return scratch;
}

struct CompactReadRecord;

struct CompactReadRecord final {
    union KeyStorage {
        std::array<char, 16> inline_key{};
        const char* external_key;
    };
    RecordRef record{};
    std::uint64_t hash{};
    KeyStorage key{};
    std::uint32_t key_size{};
    std::uint32_t pin_and_flags{};

    static constexpr std::uint32_t kDurableFlag = 1U << 31U;
    static constexpr std::uint32_t kEraseFlag = 1U << 30U;
    static constexpr std::uint32_t kPinMask = kEraseFlag - 1U;
    static constexpr std::uint32_t kNoPin = kPinMask;

    [[nodiscard]] auto pin_index() const noexcept -> std::uint32_t {
        return pin_and_flags & kPinMask;
    }
    [[nodiscard]] auto durable() const noexcept -> bool {
        return (pin_and_flags & kDurableFlag) != 0;
    }
    [[nodiscard]] auto opcode() const noexcept -> Opcode {
        return (pin_and_flags & kEraseFlag) != 0 ? Opcode::erase : Opcode::put;
    }
    void set_metadata(const std::uint32_t pin_index, const bool is_durable,
                      const Opcode record_opcode) noexcept {
        pin_and_flags =
            pin_index | (is_durable ? kDurableFlag : 0U) | (record_opcode == Opcode::erase ? kEraseFlag : 0U);
    }
};
static_assert(sizeof(CompactReadRecord) == 64,
              "the immutable Reader record and full hash must occupy one cache line");

struct KeyBlock final {
    std::unique_ptr<char[]> bytes;
    std::size_t capacity{};
    std::size_t used{};
};

} // namespace

auto immutable_base_spare_mapping_bytes() noexcept -> std::size_t {
    return immutable_base_spare_mapping_payload_bytes.load(std::memory_order_relaxed);
}

class ImmutableReadIndex final {
  public:
    ImmutableReadIndex() {
        initialize(0, true);
    }

    explicit ImmutableReadIndex(MutableReadIndex source) {
        initialize(source.size(), true);
        for (const auto& record : source) {
            append(*record);
        }
    }

    explicit ImmutableReadIndex(const std::span<const DurableRuntimeCatalog::PublishedReadRecord> source) {
        initialize(source.size(), true);
        for (const auto& record : source) {
            append({.hash = record.key_hash(),
                    .key = record.key(),
                    .record = record.reference(),
                    .durable = &record.pin(),
                    .opcode = Opcode::put});
        }
    }

#if defined(__unix__) || defined(__APPLE__)
    explicit ImmutableReadIndex(std::shared_ptr<LargeImmutableMappingPool> mapping_pool)
        : records_(RecordAllocator{std::move(mapping_pool)}) {
        initialize(0, true);
    }

    [[nodiscard]] auto mapping_pool() const -> std::shared_ptr<LargeImmutableMappingPool> {
        return records_.get_allocator().mapping_pool();
    }
#endif

    [[nodiscard]] auto find(const HashedKey& key) const noexcept -> const CompactReadRecord* {
        const auto wanted = fingerprint(key.hash);
        auto group_start = probe_start(key.hash);
        for (std::size_t probed = 0; probed < capacity_; probed += kSwissGroupSize) {
            const auto* group = &control_[group_start];
            const auto control_word = detail::load_control_group64(group);
            const auto matches = detail::equal_byte_mask(group, wanted);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto control = detail::control_byte_at(control_word, offset);
                if (control == kSwissEmpty) {
                    return nullptr;
                }
                if ((matches & (1ULL << offset)) == 0) {
                    continue;
                }
                const auto slot = group_start + offset;
                const auto record_index = record_indices_[slot];
                if (record_index >= records_.size()) {
                    return nullptr;
                }
                const auto* record = &records_[record_index];
                if (record->hash == key.hash && key_at(*record) == key.key) {
                    return record;
                }
            }
            group_start = next_group(group_start);
        }
        return nullptr;
    }

    [[nodiscard]] auto get(const HashedKey& key, const std::uint64_t now_ns) const -> Result<OwnedValue> {
        const auto* record = find(key);
        if (record == nullptr || record->opcode() == Opcode::erase) {
            return fail(ErrorCode::not_found, "key not found");
        }
        const auto pin_index = record->pin_index();
        if (record->durable() || pin_index == CompactReadRecord::kNoPin || pin_index >= segments_.size()) {
            return fail(record->durable() ? ErrorCode::invalid_argument : ErrorCode::invalid_reference,
                        record->durable() ? "durable read generation requires asynchronous preparation"
                                          : "read generation has no exact Segment generation pin");
        }
        const auto& segment = segments_[pin_index];
        if (!same_segment(segment, record->record)) {
            return fail(ErrorCode::invalid_reference, "read generation has no exact Segment generation pin");
        }
        const auto offset = static_cast<std::size_t>(record->record.offset.value);
        const auto size = static_cast<std::size_t>(record->record.size.value);
        if (offset > segment->capacity() || size > segment->capacity() - offset) {
            return fail(ErrorCode::invalid_reference, "read generation RecordRef exceeds pinned Segment");
        }
        auto decoded = decode_record({segment->base() + offset, size});
        if (!decoded) {
            return unexpected(std::move(decoded.error()));
        }
        if (decoded->sequence != record->record.sequence ||
            decoded->encoded_size != record->record.size.value || decoded->opcode != record->opcode() ||
            decoded->key_string() != key.key) {
            return fail(ErrorCode::invalid_reference, "read generation RecordRef identity mismatch");
        }
        if (decoded->expired(now_ns)) {
            return fail(ErrorCode::not_found, "key not found");
        }
        return OwnedValue::from_bytes(decoded->value, decoded->sequence.value, decoded->expire_at_ns);
    }

    [[nodiscard]] auto prepare_durable(const HashedKey& key) const
        -> Result<DurableRuntimeCatalog::PublishedReadView> {
        const auto* record = find(key);
        if (record == nullptr || record->opcode() == Opcode::erase) {
            return fail(ErrorCode::not_found, "key not found");
        }
        const auto pin_index = record->pin_index();
        if (!record->durable() || pin_index == CompactReadRecord::kNoPin ||
            pin_index >= durable_pins_.size() || !durable_pins_[pin_index].matches(record->record)) {
            return fail(ErrorCode::invalid_reference,
                        "durable read generation has no exact file-generation pin");
        }
        return DurableRuntimeCatalog::PublishedReadView::borrow(key_at(*record), key.hash, record->record,
                                                                durable_pins_[pin_index]);
    }

    [[nodiscard]] auto records() const -> MutableReadIndex {
        MutableReadIndex result;
        result.reserve(size_);
        for (std::size_t slot = 0; slot < capacity_; ++slot) {
            if (control_[slot] != kSwissEmpty) {
                const auto record_index = record_indices_[slot];
                if (record_index < records_.size()) {
                    result.push_back(materialize(view_at(records_[record_index])));
                }
            }
        }
        std::sort(result.begin(), result.end(), record_less);
        return result;
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
    }

    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }

    [[nodiscard]] auto record_at(const std::size_t slot) const noexcept -> std::optional<ReadRecordView> {
        if (slot >= capacity_) {
            return std::nullopt;
        }
        if (control_[slot] == kSwissEmpty || record_indices_[slot] >= records_.size()) {
            return std::nullopt;
        }
        return view_at(records_[record_indices_[slot]]);
    }

    [[nodiscard]] auto memory_stats() const noexcept -> ReadGenerationMemoryStats {
        ReadGenerationMemoryStats stats{
            .base_entries = size_,
            .base_capacity = capacity_,
            .base_record_storage_bytes = saturating_multiply(records_.capacity(), sizeof(CompactReadRecord)),
            .base_record_mapped_storage_bytes =
                ReleasingLargeAllocator<CompactReadRecord>::mapped_storage_bytes(records_.capacity()),
            .base_lookup_storage_bytes =
                saturating_multiply(capacity_, sizeof(std::uint8_t) + sizeof(std::uint32_t)),
            .base_key_bytes = key_bytes_,
            .base_pin_storage_bytes =
                saturating_add(saturating_multiply(segments_.capacity(), sizeof(SegmentPtr)),
                               saturating_multiply(durable_pins_.capacity(), sizeof(DurableReadPin))),
        };
        stats.base_key_storage_bytes = saturating_add(
            saturating_multiply(key_blocks_.capacity(), sizeof(KeyBlock)), allocated_key_storage_bytes_);
        auto total = sizeof(ImmutableReadIndex);
        total = saturating_add(total, stats.base_record_storage_bytes);
        total = saturating_add(total, stats.base_lookup_storage_bytes);
        total = saturating_add(total, stats.base_key_storage_bytes);
        total = saturating_add(total, stats.base_pin_storage_bytes);
        stats.base_allocated_lower_bound_bytes = total;
        return stats;
    }

  private:
    using RecordAllocator = ReleasingLargeAllocator<CompactReadRecord>;

    [[nodiscard]] static constexpr auto maximum_occupancy(const std::size_t capacity) noexcept
        -> std::size_t {
        return capacity - capacity / 4U;
    }

    [[nodiscard]] auto probe_start(const std::uint64_t hash) const noexcept -> std::size_t {
        return ((hash >> 7U) & ((capacity_ / kSwissGroupSize) - 1U)) * kSwissGroupSize;
    }

    [[nodiscard]] auto next_group(const std::size_t group_start) const noexcept -> std::size_t {
        const auto next = group_start + kSwissGroupSize;
        return next == capacity_ ? 0 : next;
    }

    void initialize(const std::size_t maximum_records, const bool initialize_control) {
        if (maximum_records > std::numeric_limits<std::uint32_t>::max()) {
            throw std::bad_alloc{};
        }
        capacity_ = kSwissGroupSize;
        while (maximum_occupancy(capacity_) < maximum_records) {
            if (capacity_ > std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::bad_alloc{};
            }
            capacity_ *= 2U;
        }
        control_ = std::make_unique_for_overwrite<std::uint8_t[]>(capacity_);
        record_indices_ = std::make_unique_for_overwrite<std::uint32_t[]>(capacity_);
        if (initialize_control) {
            std::fill_n(control_.get(), capacity_, kSwissEmpty);
        }
        records_.reserve(maximum_records);
    }

    void initialize_control_slots(const std::size_t first, const std::size_t count) noexcept {
        std::fill_n(control_.get() + first, count, kSwissEmpty);
    }

    [[nodiscard]] auto key_at(const CompactReadRecord& record) const noexcept -> std::string_view {
        if (record.key_size == 0) {
            return {};
        }
        if (record.key_size <= record.key.inline_key.size()) {
            return {record.key.inline_key.data(), record.key_size};
        }
        return {record.key.external_key, record.key_size};
    }

    [[nodiscard]] auto view_at(const CompactReadRecord& record) const noexcept -> ReadRecordView {
        const auto pin_index = record.pin_index();
        return {.hash = record.hash,
                .key = key_at(record),
                .record = record.record,
                .segment = !record.durable() && pin_index != CompactReadRecord::kNoPin ? &segments_[pin_index]
                                                                                       : nullptr,
                .durable = record.durable() && pin_index != CompactReadRecord::kNoPin
                               ? &durable_pins_[pin_index]
                               : nullptr,
                .opcode = record.opcode()};
    }

    void append(const ReadRecord& source) {
        append(view_of(source));
    }

    void append(const ReadRecordView& source) {
        if (records_.size() == records_.capacity()) {
            throw std::bad_alloc{};
        }
        if (source.key.size() > std::numeric_limits<std::uint32_t>::max()) {
            throw std::bad_alloc{};
        }
        auto pin_index = CompactReadRecord::kNoPin;
        bool durable{};
        if (source.opcode == Opcode::put && source.durable != nullptr) {
            durable = true;
            const auto found = std::find_if(durable_pins_.begin(), durable_pins_.end(), [&](const auto& pin) {
                return pin.same_generation(*source.durable);
            });
            if (found == durable_pins_.end()) {
                if (durable_pins_.size() == CompactReadRecord::kNoPin) {
                    throw std::bad_alloc{};
                }
                pin_index = static_cast<std::uint32_t>(durable_pins_.size());
                durable_pins_.push_back(*source.durable);
            } else {
                pin_index = static_cast<std::uint32_t>(std::distance(durable_pins_.begin(), found));
            }
        } else if (source.opcode == Opcode::put && source.segment != nullptr) {
            const auto found = std::find(segments_.begin(), segments_.end(), *source.segment);
            if (found == segments_.end()) {
                if (segments_.size() == CompactReadRecord::kNoPin) {
                    throw std::bad_alloc{};
                }
                pin_index = static_cast<std::uint32_t>(segments_.size());
                segments_.push_back(*source.segment);
            } else {
                pin_index = static_cast<std::uint32_t>(std::distance(segments_.begin(), found));
            }
        }
        CompactReadRecord compact{.record = source.record,
                                  .hash = source.hash,
                                  .key_size = static_cast<std::uint32_t>(source.key.size())};
        compact.set_metadata(pin_index, durable, source.opcode);
        if (source.key.size() <= compact.key.inline_key.size()) {
            std::ranges::copy(source.key, compact.key.inline_key.begin());
        } else {
            compact.key.external_key = append_key(source.key);
        }
        records_.push_back(std::move(compact));
        place(source.hash, records_.size() - 1U);
    }

    [[nodiscard]] auto append_key(const std::string_view key) -> const char* {
        if (key.empty()) {
            return nullptr;
        }
        constexpr std::size_t kKeyBlockBytes = 64U * 1024U;
        if (key_blocks_.empty() || key.size() > key_blocks_.back().capacity - key_blocks_.back().used) {
            const auto capacity = std::max(kKeyBlockBytes, key.size());
            key_blocks_.push_back({.bytes = std::make_unique<char[]>(capacity), .capacity = capacity});
            allocated_key_storage_bytes_ = saturating_add(allocated_key_storage_bytes_, capacity);
        }
        auto& block = key_blocks_.back();
        auto* destination = block.bytes.get() + block.used;
        std::ranges::copy(key, destination);
        block.used += key.size();
        key_bytes_ += key.size();
        return destination;
    }

    void place(const std::uint64_t hash, const std::size_t record_index) {
        if (record_index > std::numeric_limits<std::uint32_t>::max()) {
            throw std::bad_alloc{};
        }
        auto group_start = probe_start(hash);
        for (;;) {
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto slot = group_start + offset;
                if (control_[slot] == kSwissEmpty) {
                    control_[slot] = fingerprint(hash);
                    record_indices_[slot] = static_cast<std::uint32_t>(record_index);
                    ++size_;
                    return;
                }
            }
            group_start = next_group(group_start);
        }
    }

    std::size_t capacity_{};
    std::size_t size_{};
    std::size_t key_bytes_{};
    std::size_t allocated_key_storage_bytes_{};
    std::vector<CompactReadRecord, RecordAllocator> records_;
    std::vector<KeyBlock> key_blocks_;
    std::vector<SegmentPtr> segments_;
    std::vector<DurableReadPin> durable_pins_;
    std::unique_ptr<std::uint8_t[]> control_;
    std::unique_ptr<std::uint32_t[]> record_indices_;

    friend class IncrementalBaseBuilder;
};

class IncrementalBaseBuilder final {
  public:
    explicit IncrementalBaseBuilder(const std::size_t maximum_records
#if defined(__unix__) || defined(__APPLE__)
                                    ,
                                    std::shared_ptr<LargeImmutableMappingPool> mapping_pool
#endif
                                    )
#if defined(__unix__) || defined(__APPLE__)
        : index_(std::make_unique<ImmutableReadIndex>(std::move(mapping_pool))) {
#else
        : index_(std::make_unique<ImmutableReadIndex>()) {
#endif
        index_->initialize(maximum_records, false);
    }

    [[nodiscard]] auto initialize_next(const std::size_t maximum_slots) noexcept -> std::size_t {
        const auto count = std::min(maximum_slots, index_->capacity_ - initialized_slots_);
        index_->initialize_control_slots(initialized_slots_, count);
        initialized_slots_ += count;
        return count;
    }

    [[nodiscard]] auto initialized() const noexcept -> bool {
        return initialized_slots_ == index_->capacity_;
    }

    [[nodiscard]] auto remaining_initialization_slots() const noexcept -> std::size_t {
        return index_->capacity_ - initialized_slots_;
    }

    [[nodiscard]] auto contains(const HashedKey& key) const noexcept -> bool {
        return index_->find(key) != nullptr;
    }

    void insert(const ReadRecord& record) {
        index_->append(record);
    }

    void insert(const ReadRecordView& record) {
        index_->append(record);
    }

    [[nodiscard]] auto freeze() && -> std::shared_ptr<const ImmutableReadIndex> {
        return std::shared_ptr<const ImmutableReadIndex>{index_.release()};
    }

  private:
    std::unique_ptr<ImmutableReadIndex> index_;
    std::size_t initialized_slots_{};
};

class DeltaState final {
  public:
    DeltaState(const std::size_t capacity, const std::size_t maximum_entries, const std::size_t size,
               const std::size_t allocated_page_count, const std::size_t allocated_block_count,
               const std::size_t allocated_chunk_count,
               std::vector<std::shared_ptr<const DeltaPage>> flat_pages,
               std::vector<std::shared_ptr<const DeltaDirectoryChunk>> directory_chunks,
               std::shared_ptr<DeltaArena> primary_arena, std::shared_ptr<DeltaArena> secondary_arena = {})
        : capacity_(capacity), maximum_entries_(maximum_entries), size_(size),
          allocated_page_count_(allocated_page_count), allocated_block_count_(allocated_block_count),
          allocated_chunk_count_(allocated_chunk_count), flat_pages_(std::move(flat_pages)),
          directory_chunks_(std::move(directory_chunks)), primary_arena_(std::move(primary_arena)),
          secondary_arena_(std::move(secondary_arena)) {
        if (!primary_arena_) {
            throw std::invalid_argument{"Delta state has no primary record arena"};
        }
    }

    [[nodiscard]] auto find_handle(const HashedKey& key) const noexcept -> const DeltaRecord* {
        return find(key);
    }

    [[nodiscard]] auto find(const HashedKey& key) const noexcept -> const DeltaRecord* {
        const auto wanted = fingerprint(key.hash);
        auto group_start = probe_start(key.hash);
        for (std::size_t probed = 0; probed < capacity_; probed += kSwissGroupSize) {
            const auto page_index = group_start / kDeltaPageSlots;
            const auto page_offset = group_start % kDeltaPageSlots;
            const auto* page = page_at(page_index);
            if (page == nullptr) {
                return nullptr;
            }
            const auto* group = &page->control[page_offset];
            const auto control_word = detail::load_control_group64(group);
            const auto matches = detail::equal_byte_mask(group, wanted);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto control = detail::control_byte_at(control_word, offset);
                if (control == kSwissEmpty) {
                    return nullptr;
                }
                if ((matches & (1ULL << offset)) == 0) {
                    continue;
                }
                const auto slot = page_offset + offset;
                const auto* record = page->records[slot];
                if (page->hashes[slot] == key.hash && delta_record_key(*record) == key.key) {
                    return record;
                }
            }
            group_start = next_group(group_start);
        }
        return nullptr;
    }

    template <typename Callback> void for_each(Callback&& callback) const {
        const auto page_count = capacity_ / kDeltaPageSlots;
        for (std::size_t page_index = 0; page_index < page_count; ++page_index) {
            const auto* page = page_at(page_index);
            if (page == nullptr) {
                continue;
            }
            for (std::size_t slot = 0; slot < kDeltaPageSlots; ++slot) {
                if (page->control[slot] != kSwissEmpty) {
                    callback(delta_record_view(*page->records[slot], page->hashes[slot]));
                }
            }
        }
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
    }

    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }

    [[nodiscard]] auto maximum_entries() const noexcept -> std::size_t {
        return maximum_entries_;
    }

    [[nodiscard]] auto record_versions() const noexcept -> std::size_t {
        return allocation_arena()->record_count();
    }

    [[nodiscard]] auto available_record_versions() const noexcept -> std::size_t {
        return allocation_arena()->available_records();
    }

    [[nodiscard]] auto arena_record_bytes() const noexcept -> std::size_t {
        auto result = primary_arena_->allocated_record_bytes();
        if (secondary_arena_) {
            result += secondary_arena_->allocated_record_bytes();
        }
        return result;
    }

    [[nodiscard]] auto arena_key_bytes() const noexcept -> std::size_t {
        auto result = primary_arena_->key_bytes();
        if (secondary_arena_) {
            result += secondary_arena_->key_bytes();
        }
        return result;
    }

    [[nodiscard]] auto arena_key_storage_bytes() const noexcept -> std::size_t {
        auto result = primary_arena_->allocated_key_bytes();
        if (secondary_arena_) {
            result += secondary_arena_->allocated_key_bytes();
        }
        return result;
    }

    [[nodiscard]] auto record_at(const std::size_t slot) const noexcept -> std::optional<ReadRecordView> {
        if (slot >= capacity_) {
            return std::nullopt;
        }
        const auto* page = page_at(slot / kDeltaPageSlots);
        const auto offset = slot % kDeltaPageSlots;
        return page != nullptr && page->control[offset] != kSwissEmpty
                   ? std::optional<ReadRecordView>{delta_record_view(*page->records[offset],
                                                                     page->hashes[offset])}
                   : std::nullopt;
    }

    void append_memory_stats(ReadGenerationMemoryStats& stats) const noexcept {
        stats.delta_entries = size_;
        stats.delta_capacity = capacity_;
        stats.delta_record_versions = record_versions();
        stats.delta_arena_record_bytes = arena_record_bytes();
        stats.delta_arena_key_bytes = arena_key_bytes();
        stats.delta_arena_key_storage_bytes = arena_key_storage_bytes();

        auto lookup = saturating_add(
            saturating_multiply(flat_pages_.capacity(), sizeof(std::shared_ptr<const DeltaPage>)),
            saturating_multiply(directory_chunks_.capacity(),
                                sizeof(std::shared_ptr<const DeltaDirectoryChunk>)));
        lookup = saturating_add(lookup, saturating_multiply(allocated_page_count_, sizeof(DeltaPage)));
        lookup =
            saturating_add(lookup, saturating_multiply(allocated_block_count_, sizeof(DeltaDirectoryBlock)));
        lookup =
            saturating_add(lookup, saturating_multiply(allocated_chunk_count_, sizeof(DeltaDirectoryChunk)));
        stats.delta_lookup_storage_bytes = lookup;
        stats.delta_allocated_lower_bound_bytes = saturating_add(
            lookup, saturating_add(stats.delta_arena_record_bytes, stats.delta_arena_key_storage_bytes));
    }

  private:
    [[nodiscard]] auto allocation_arena() const noexcept -> const std::shared_ptr<DeltaArena>& {
        return secondary_arena_ ? secondary_arena_ : primary_arena_;
    }

    [[nodiscard]] auto page_at(const std::size_t page_index) const noexcept -> const DeltaPage* {
        if (directory_chunks_.empty()) {
            return flat_pages_[page_index].get();
        }
        const auto block_index = page_index / kDeltaDirectoryBlockPages;
        const auto& chunk = directory_chunks_[block_index / kDeltaDirectoryChunkBlocks];
        if (!chunk) {
            return nullptr;
        }
        const auto& block = chunk->blocks[block_index % kDeltaDirectoryChunkBlocks];
        return block ? block->pages[page_index % kDeltaDirectoryBlockPages].get() : nullptr;
    }

    [[nodiscard]] auto probe_start(const std::uint64_t hash) const noexcept -> std::size_t {
        return ((hash >> 7U) & ((capacity_ / kSwissGroupSize) - 1U)) * kSwissGroupSize;
    }

    [[nodiscard]] auto next_group(const std::size_t group_start) const noexcept -> std::size_t {
        const auto next = group_start + kSwissGroupSize;
        return next == capacity_ ? 0 : next;
    }

    std::size_t capacity_{};
    std::size_t maximum_entries_{};
    std::size_t size_{};
    // Exact topology census maintained by the Writer while cloning the COW
    // directory. memory_stats() must remain O(1): it runs after every publish.
    std::size_t allocated_page_count_{};
    std::size_t allocated_block_count_{};
    std::size_t allocated_chunk_count_{};
    std::vector<std::shared_ptr<const DeltaPage>> flat_pages_;
    std::vector<std::shared_ptr<const DeltaDirectoryChunk>> directory_chunks_;
    std::shared_ptr<DeltaArena> primary_arena_;
    std::shared_ptr<DeltaArena> secondary_arena_;

    friend class DeltaBuilder;
};

[[nodiscard]] auto make_empty_delta(const std::size_t maximum_entries) -> DeltaState {
    if (maximum_entries > std::numeric_limits<std::size_t>::max() - kMaximumPublicationBatch) {
        throw std::bad_alloc{};
    }
    const auto required = maximum_entries + kMaximumPublicationBatch;
    auto capacity = kDeltaPageSlots;
    while (capacity - capacity / 4U < required) {
        if (capacity > std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::bad_alloc{};
        }
        capacity *= 2U;
    }
    const auto page_count = capacity / kDeltaPageSlots;
    auto arena = std::make_shared<DeltaArena>(maximum_entries);
    if (page_count <= kFlatDeltaMaximumPages) {
        return DeltaState{capacity,
                          maximum_entries,
                          0,
                          0,
                          0,
                          0,
                          std::vector<std::shared_ptr<const DeltaPage>>(page_count),
                          std::vector<std::shared_ptr<const DeltaDirectoryChunk>>{},
                          std::move(arena)};
    }
    const auto directory_count = (page_count + kDeltaDirectoryBlockPages - 1U) / kDeltaDirectoryBlockPages;
    const auto chunk_count = (directory_count + kDeltaDirectoryChunkBlocks - 1U) / kDeltaDirectoryChunkBlocks;
    return DeltaState{capacity,
                      maximum_entries,
                      0,
                      0,
                      0,
                      0,
                      std::vector<std::shared_ptr<const DeltaPage>>{},
                      std::vector<std::shared_ptr<const DeltaDirectoryChunk>>(chunk_count),
                      std::move(arena)};
}

class DeltaBuilder final {
  public:
    explicit DeltaBuilder(const DeltaState& previous, DeltaBuilderScratch& scratch,
                          std::shared_ptr<DeltaArena> allocation_arena = {})
        : previous_(&previous), flat_pages_(copy_flat_delta_spine(previous.flat_pages_)),
          directory_chunks_(copy_directory_delta_spine(previous.directory_chunks_)),
          primary_arena_(previous.primary_arena_), secondary_arena_(previous.secondary_arena_),
          scratch_(&scratch), size_(previous.size_), allocated_page_count_(previous.allocated_page_count_),
          allocated_block_count_(previous.allocated_block_count_),
          allocated_chunk_count_(previous.allocated_chunk_count_) {
        if (allocation_arena) {
            if (allocation_arena != primary_arena_ && allocation_arena != secondary_arena_) {
                if (secondary_arena_) {
                    throw std::invalid_argument{"Delta state cannot own more than cut and post arenas"};
                }
                secondary_arena_ = std::move(allocation_arena);
            }
        }
        allocation_arena_ = secondary_arena_ ? secondary_arena_ : primary_arena_;
        scratch_->reset();
    }

    struct PreparedSlot final {
        DeltaPage* page{};
        std::size_t slot{};
        std::uint64_t hash{};
        std::uint8_t control{};
        bool inserted{};
    };

    [[nodiscard]] auto prepare(const std::uint64_t hash, const std::string_view key) -> PreparedSlot {
        const auto wanted = fingerprint(hash);
        auto group_start = probe_start(hash);
        for (std::size_t probed = 0; probed < previous_->capacity_; probed += kSwissGroupSize) {
            const auto page_index = group_start / kDeltaPageSlots;
            const auto page_offset = group_start % kDeltaPageSlots;
            const auto* page = page_at(page_index);
            if (page == nullptr) {
                if (size_ == previous_->maximum_entries_) {
                    throw std::bad_alloc{};
                }
                return {.page = &mutable_page(page_index),
                        .slot = page_offset,
                        .hash = hash,
                        .control = wanted,
                        .inserted = true};
            }
            const auto* group = &page->control[page_offset];
            const auto control_word = detail::load_control_group64(group);
            const auto matches = detail::equal_byte_mask(group, wanted);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto control = detail::control_byte_at(control_word, offset);
                const auto slot = page_offset + offset;
                if (control == kSwissEmpty) {
                    if (size_ == previous_->maximum_entries_) {
                        throw std::bad_alloc{};
                    }
                    return {.page = &mutable_page(page_index),
                            .slot = slot,
                            .hash = hash,
                            .control = wanted,
                            .inserted = true};
                }
                if ((matches & (1ULL << offset)) != 0 && page->hashes[slot] == hash &&
                    delta_record_key(*page->records[slot]) == key) {
                    return {.page = &mutable_page(page_index), .slot = slot, .hash = hash, .control = wanted};
                }
            }
            group_start = next_group(group_start);
        }
        throw std::bad_alloc{};
    }

    [[nodiscard]] auto store(const ReadRecordView& record) -> const DeltaRecord* {
        GS_PHASE_PUT(delta_record_store);
        return allocation_arena_->store(record);
    }

    void commit(const PreparedSlot prepared, const DeltaRecord* record) noexcept {
        prepared.page->control[prepared.slot] = prepared.control;
        prepared.page->hashes[prepared.slot] = prepared.hash;
        prepared.page->records[prepared.slot] = record;
        if (prepared.inserted) {
            ++size_;
        }
    }

    [[nodiscard]] auto freeze() && -> DeltaState {
        return DeltaState{previous_->capacity_,       previous_->maximum_entries_,  size_,
                          allocated_page_count_,      allocated_block_count_,       allocated_chunk_count_,
                          std::move(flat_pages_),     std::move(directory_chunks_), std::move(primary_arena_),
                          std::move(secondary_arena_)};
    }

    [[nodiscard]] auto allocation_arena() const noexcept -> const std::shared_ptr<DeltaArena>& {
        return allocation_arena_;
    }

  private:
    [[nodiscard]] auto page_at(const std::size_t page_index) const noexcept -> const DeltaPage* {
        if (directory_chunks_.empty()) {
            return flat_pages_[page_index].get();
        }
        const auto block_index = page_index / kDeltaDirectoryBlockPages;
        const auto& chunk = directory_chunks_[block_index / kDeltaDirectoryChunkBlocks];
        if (!chunk) {
            return nullptr;
        }
        const auto& block = chunk->blocks[block_index % kDeltaDirectoryChunkBlocks];
        return block ? block->pages[page_index % kDeltaDirectoryBlockPages].get() : nullptr;
    }

    [[nodiscard]] auto probe_start(const std::uint64_t hash) const noexcept -> std::size_t {
        return ((hash >> 7U) & ((previous_->capacity_ / kSwissGroupSize) - 1U)) * kSwissGroupSize;
    }

    [[nodiscard]] auto next_group(const std::size_t group_start) const noexcept -> std::size_t {
        const auto next = group_start + kSwissGroupSize;
        return next == previous_->capacity_ ? 0 : next;
    }

    [[nodiscard]] auto mutable_page(const std::size_t page_index) -> DeltaPage& {
        if (last_mutable_page_index_ == page_index && last_mutable_page_ != nullptr) {
            return *last_mutable_page_;
        }
        auto& mutable_pages = scratch_->mutable_pages;
        const auto found_page = std::find_if(mutable_pages.begin(), mutable_pages.end(),
                                             [&](const auto& entry) { return entry.first == page_index; });
        if (found_page != mutable_pages.end()) {
            last_mutable_page_index_ = page_index;
            last_mutable_page_ = found_page->second.get();
            return *found_page->second;
        }
        const auto* existing = page_at(page_index);
        std::shared_ptr<DeltaPage> page;
        {
            GS_PHASE_PUT(delta_page_clone);
            page = existing ? std::make_shared<DeltaPage>(*existing) : std::make_shared<DeltaPage>();
        }
        if (existing == nullptr) {
            ++allocated_page_count_;
        }
        mutable_pages.emplace_back(page_index, page);
        if (directory_chunks_.empty()) {
            flat_pages_[page_index] = page;
        } else {
            const auto block_index = page_index / kDeltaDirectoryBlockPages;
            const auto chunk_index = block_index / kDeltaDirectoryChunkBlocks;
            const auto block_offset = block_index % kDeltaDirectoryChunkBlocks;
            auto& mutable_chunks = scratch_->mutable_chunks;
            auto found_chunk = std::find_if(mutable_chunks.begin(), mutable_chunks.end(),
                                            [&](const auto& entry) { return entry.first == chunk_index; });
            if (found_chunk == mutable_chunks.end()) {
                std::shared_ptr<DeltaDirectoryChunk> chunk;
                const bool chunk_exists = directory_chunks_[chunk_index] != nullptr;
                {
                    GS_PHASE_PUT(delta_chunk_clone);
                    chunk = chunk_exists
                                ? std::make_shared<DeltaDirectoryChunk>(*directory_chunks_[chunk_index])
                                : std::make_shared<DeltaDirectoryChunk>();
                }
                if (!chunk_exists) {
                    ++allocated_chunk_count_;
                }
                mutable_chunks.emplace_back(chunk_index, chunk);
                directory_chunks_[chunk_index] = chunk;
                found_chunk = std::prev(mutable_chunks.end());
            }
            auto& mutable_blocks = scratch_->mutable_blocks;
            auto found_block = std::find_if(mutable_blocks.begin(), mutable_blocks.end(),
                                            [&](const auto& entry) { return entry.first == block_index; });
            if (found_block == mutable_blocks.end()) {
                std::shared_ptr<DeltaDirectoryBlock> block;
                const bool block_exists = found_chunk->second->blocks[block_offset] != nullptr;
                {
                    GS_PHASE_PUT(delta_block_clone);
                    block = block_exists ? std::make_shared<DeltaDirectoryBlock>(
                                               *found_chunk->second->blocks[block_offset])
                                         : std::make_shared<DeltaDirectoryBlock>();
                }
                if (!block_exists) {
                    ++allocated_block_count_;
                }
                mutable_blocks.emplace_back(block_index, block);
                found_chunk->second->blocks[block_offset] = block;
                found_block = std::prev(mutable_blocks.end());
            }
            found_block->second->pages[page_index % kDeltaDirectoryBlockPages] = page;
        }
        last_mutable_page_index_ = page_index;
        last_mutable_page_ = page.get();
        return *page;
    }

    const DeltaState* previous_{};
    std::vector<std::shared_ptr<const DeltaPage>> flat_pages_;
    std::vector<std::shared_ptr<const DeltaDirectoryChunk>> directory_chunks_;
    std::shared_ptr<DeltaArena> primary_arena_;
    std::shared_ptr<DeltaArena> secondary_arena_;
    std::shared_ptr<DeltaArena> allocation_arena_;
    // Writer-thread-local reusable capacity; never shared with Reader and
    // cleared before each publication. The separate current/post scratch
    // instances cover the only two simultaneously live builders.
    DeltaBuilderScratch* scratch_{};
    DeltaPage* last_mutable_page_{};
    std::size_t last_mutable_page_index_{std::numeric_limits<std::size_t>::max()};
    std::size_t size_{};
    std::size_t allocated_page_count_{};
    std::size_t allocated_block_count_{};
    std::size_t allocated_chunk_count_{};
};

struct PairReadMerge::State final {
    enum class Phase : std::uint8_t { initialize, base, delta, ready };

    State(std::shared_ptr<const PairReadGeneration> merge_cut,
          std::unique_ptr<IncrementalBaseBuilder> next_base, DeltaState post, const std::size_t maximum_post)
        : cut(std::move(merge_cut)), current(cut), builder(std::move(next_base)), post_delta(std::move(post)),
          maximum_post_entries(maximum_post) {}

    std::shared_ptr<const PairReadGeneration> cut;
    std::shared_ptr<const PairReadGeneration> current;
    std::unique_ptr<IncrementalBaseBuilder> builder;
    DeltaState post_delta;
    std::size_t maximum_post_entries{};
    std::size_t base_cursor{};
    std::size_t delta_cursor{};
    Phase phase{Phase::initialize};
};

PairReadMerge::PairReadMerge(std::unique_ptr<State> state) noexcept : state_(std::move(state)) {}
PairReadMerge::~PairReadMerge() = default;
PairReadMerge::PairReadMerge(PairReadMerge&&) noexcept = default;
auto PairReadMerge::operator=(PairReadMerge&&) noexcept -> PairReadMerge& = default;

PairReadGeneration::PairReadGeneration(const WorkerRoutingState routing,
                                       std::shared_ptr<const ImmutableReadIndex> base,
                                       const DeltaState* delta, const std::uint64_t epoch,
                                       const std::uint64_t visible_through) noexcept
    : routing_(routing), base_(std::move(base)), delta_(delta), epoch_(epoch),
      visible_through_(visible_through) {}

struct PairReadGenerationEnableShared final : PairReadGeneration {
    PairReadGenerationEnableShared(WorkerRoutingState routing, std::shared_ptr<const ImmutableReadIndex> base,
                                   DeltaState delta, const std::uint64_t epoch,
                                   const std::uint64_t visible_through) noexcept
        : PairReadGeneration(routing, std::move(base), nullptr, epoch, visible_through),
          delta_storage_(std::move(delta)) {
        bind_delta(&delta_storage_);
    }

    DeltaState delta_storage_;
};

[[nodiscard]] auto make_shared_generation(WorkerRoutingState routing,
                                          std::shared_ptr<const ImmutableReadIndex> base, DeltaState delta,
                                          const std::uint64_t epoch, const std::uint64_t visible_through)
    -> std::shared_ptr<const PairReadGeneration> {
    // Co-allocate generation shell + embedded DeltaState in one control block.
    GS_PHASE_PUT(generation_shell_allocate);
    return std::make_shared<PairReadGenerationEnableShared>(routing, std::move(base), std::move(delta), epoch,
                                                            visible_through);
}

[[nodiscard]] auto
make_shared_generation_in_shell(WorkerRoutingState routing, std::shared_ptr<const ImmutableReadIndex> base,
                                DeltaState delta, const std::uint64_t epoch,
                                const std::uint64_t visible_through,
                                std::shared_ptr<experimental::PairReadGenerationShellStorage> storage)
    -> std::shared_ptr<const PairReadGeneration> {
    if (!storage) {
        throw std::bad_alloc{};
    }
    GS_PHASE_PUT(generation_shell_allocate);
    using Allocator = experimental::PairReadGenerationShellAllocator<PairReadGenerationEnableShared>;
    return std::allocate_shared<PairReadGenerationEnableShared>(
        Allocator{std::move(storage)}, routing, std::move(base), std::move(delta), epoch, visible_through);
}

[[nodiscard]] auto
make_shared_generation_in_borrowed_shell(WorkerRoutingState routing,
                                         std::shared_ptr<const ImmutableReadIndex> base, DeltaState delta,
                                         const std::uint64_t epoch, const std::uint64_t visible_through,
                                         experimental::PairReadGenerationInlineShellStorage& storage)
    -> std::shared_ptr<const PairReadGeneration> {
    GS_PHASE_PUT(generation_shell_allocate);
    using Allocator = experimental::PairReadGenerationBorrowedShellAllocator<PairReadGenerationEnableShared>;
    return std::allocate_shared<PairReadGenerationEnableShared>(Allocator{storage}, routing, std::move(base),
                                                                std::move(delta), epoch, visible_through);
}

auto PairReadGeneration::empty(const WorkerRoutingState routing)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    return make_shared_generation(routing, std::make_shared<const ImmutableReadIndex>(),
                                  make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries), 0,
                                  0);
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "read generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "read generation construction failed");
}

auto PairReadGeneration::from_durable_snapshot(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
    -> Result<std::shared_ptr<const PairReadGeneration>> {
    return PairReadGeneration::build_durable_snapshot(routing, records, 0, 0);
}

auto PairReadGeneration::replace_durable_snapshot(
    std::shared_ptr<const PairReadGeneration> previous,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
    -> Result<std::shared_ptr<const PairReadGeneration>> {
    if (!previous) {
        return fail(ErrorCode::invalid_argument, "durable snapshot replacement has no previous generation");
    }
    if (previous->epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    return PairReadGeneration::build_durable_snapshot(previous->routing_, records, previous->epoch_ + 1U,
                                                      previous->visible_through_);
}

auto PairReadGeneration::build_durable_snapshot(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records, const std::uint64_t epoch,
    const std::uint64_t visible_floor) -> Result<std::shared_ptr<const PairReadGeneration>> try {
    auto visible_through = visible_floor;
    for (const auto& record : records) {
        visible_through = std::max(visible_through, record.reference().sequence.value);
    }
    return make_shared_generation(routing, std::make_shared<const ImmutableReadIndex>(records),
                                  make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries),
                                  epoch, visible_through);
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "durable read generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "durable read generation construction failed");
}

auto PairReadGeneration::publish(std::shared_ptr<const PairReadGeneration> previous,
                                 const std::span<const ReadMutation> mutations,
                                 const std::size_t merge_delta_entries)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    if (!previous || merge_delta_entries == 0 || mutations.size() > kMaximumPublicationBatch) {
        return fail(ErrorCode::invalid_argument, "invalid read generation publication");
    }
    if (mutations.empty()) {
        return previous;
    }
    if (previous->epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }

    DeltaBuilder builder{*previous->delta_, current_delta_builder_scratch()};
    auto visible_through = previous->visible_through_;
    for (const auto& mutation : mutations) {
        const bool durable_put = mutation.opcode == Opcode::put && mutation.durable.has_value();
        const bool volatile_pinned = same_segment(mutation.segment, mutation.record);
        if (mutation.opcode == Opcode::put && !durable_put && !volatile_pinned) {
            return fail(ErrorCode::invalid_reference,
                        "publication rejected a RecordRef without its exact Segment pin");
        }
        if (durable_put && (mutation.durable->key_hash() != mutation.key.hash ||
                            mutation.durable->key() != mutation.key.key ||
                            mutation.durable->reference() != mutation.record)) {
            return fail(ErrorCode::invalid_reference,
                        "durable publication record disagrees with its exact generation pin");
        }
        const auto prepared = builder.prepare(mutation.key.hash, mutation.key.key);
        const auto* stored = builder.store(view_of(mutation));
        builder.commit(prepared, stored);
        visible_through = std::max(visible_through, mutation.record.sequence.value);
    }

    auto next_delta = std::move(builder).freeze();
    auto next_base = previous->base_;
    if (next_delta.size() >= merge_delta_entries) {
        auto merged = next_base->records();
        next_delta.for_each([&](const ReadRecordView record) { apply_record(merged, materialize(record)); });
        next_base = std::make_shared<const ImmutableReadIndex>(std::move(merged));
        next_delta = make_empty_delta(std::max(merge_delta_entries, previous->delta_->maximum_entries()));
    }
    return make_shared_generation(previous->routing_, std::move(next_base), std::move(next_delta),
                                  previous->epoch_ + 1U, visible_through);
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "read generation publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "read generation publication failed");
}

auto PairReadGeneration::publish_incremental(std::shared_ptr<const PairReadGeneration> previous,
                                             const std::span<const ReadMutation> mutations,
                                             PairReadMerge* merge)
    -> Result<std::shared_ptr<const PairReadGeneration>> {
    if (!previous) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read publication");
    }
    const auto* previous_view = previous.get();
    return publish_incremental_construct(*previous_view, std::move(previous), mutations, merge, {}, nullptr,
                                         nullptr, nullptr);
}

auto PairReadGeneration::publish_incremental_in_shell(
    std::shared_ptr<const PairReadGeneration> previous, const std::span<const ReadMutation> mutations,
    PairReadMerge* merge, std::shared_ptr<experimental::PairReadGenerationShellStorage> storage)
    -> Result<std::shared_ptr<const PairReadGeneration>> {
    if (!previous) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read publication");
    }
    const auto* previous_view = previous.get();
    return publish_incremental_construct(*previous_view, std::move(previous), mutations, merge,
                                         std::move(storage), nullptr, nullptr, nullptr);
}

auto PairReadGeneration::publish_incremental_in_borrowed_shell(
    std::shared_ptr<const PairReadGeneration> previous, const std::span<const ReadMutation> mutations,
    experimental::PairReadGenerationInlineShellStorage& storage)
    -> Result<std::shared_ptr<const PairReadGeneration>> {
    if (!previous) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read publication");
    }
    const auto* previous_view = previous.get();
    return publish_incremental_construct(*previous_view, std::move(previous), mutations, nullptr, {},
                                         &storage, nullptr, nullptr);
}

auto PairReadGeneration::publish_incremental_direct(const PairReadGeneration& previous,
                                                    const std::span<const ReadMutation> mutations,
                                                    GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> {
    const PairReadGeneration* direct_result{};
    auto built = publish_incremental_construct(previous, {}, mutations, nullptr, {}, nullptr, &storage,
                                               &direct_result);
    if (!built) {
        return unexpected(std::move(built.error()));
    }
    if (direct_result == nullptr) {
        return fail(ErrorCode::internal_error, "direct generation construction returned no object");
    }
    return direct_result;
}

auto PairReadGeneration::empty_direct(const WorkerRoutingState routing, GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> try {
    static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
    static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
    auto base = std::make_shared<const ImmutableReadIndex>();
    auto delta = make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries);
    auto* location =
        storage.claim(sizeof(PairReadGenerationEnableShared), alignof(PairReadGenerationEnableShared));
    try {
        return std::construct_at(static_cast<PairReadGenerationEnableShared*>(location), routing,
                                 std::move(base), std::move(delta), 0, 0);
    } catch (...) {
        storage.release(location);
        throw;
    }
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "direct empty generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "direct empty generation construction failed");
}

auto PairReadGeneration::from_durable_snapshot_direct(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records,
    GenerationDirectStorage& storage) -> Result<const PairReadGeneration*> {
    return build_durable_snapshot_direct(routing, records, 0, 0, storage);
}

auto PairReadGeneration::replace_durable_snapshot_direct(
    const PairReadGeneration& previous,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records,
    GenerationDirectStorage& storage) -> Result<const PairReadGeneration*> {
    if (previous.epoch_ >= GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    return build_durable_snapshot_direct(previous.routing_, records, previous.epoch_ + 1U,
                                         previous.visible_through_, storage);
}

auto PairReadGeneration::build_durable_snapshot_direct(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records, const std::uint64_t epoch,
    const std::uint64_t visible_floor, GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> try {
    if (epoch > GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
    static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
    auto visible_through = visible_floor;
    for (const auto& record : records) {
        visible_through = std::max(visible_through, record.reference().sequence.value);
    }
    auto base = std::make_shared<const ImmutableReadIndex>(records);
    auto delta = make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries);
    auto* location =
        storage.claim(sizeof(PairReadGenerationEnableShared), alignof(PairReadGenerationEnableShared));
    try {
        return std::construct_at(static_cast<PairReadGenerationEnableShared*>(location), routing,
                                 std::move(base), std::move(delta), epoch, visible_through);
    } catch (...) {
        storage.release(location);
        throw;
    }
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "direct durable read generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "direct durable read generation construction failed");
}

auto PairReadGeneration::finish_incremental_merge_direct(const PairReadGeneration& current,
                                                         PairReadMerge& merge,
                                                         GenerationDirectStorage& storage)
    -> Result<const PairReadGeneration*> try {
    if (!merge.state_ || merge.state_->current.get() != &current ||
        merge.state_->phase != PairReadMerge::State::Phase::ready || !merge.state_->builder) {
        return fail(ErrorCode::invalid_argument, "incremental read merge is not ready");
    }
    if (current.epoch_ >= GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
    static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
    auto next_base = std::move(*merge.state_->builder).freeze();
    merge.state_->builder.reset();
    auto* location =
        storage.claim(sizeof(PairReadGenerationEnableShared), alignof(PairReadGenerationEnableShared));
    try {
        return std::construct_at(static_cast<PairReadGenerationEnableShared*>(location), current.routing_,
                                 std::move(next_base), std::move(merge.state_->post_delta),
                                 current.epoch_ + 1U, current.visible_through_);
    } catch (...) {
        storage.release(location);
        throw;
    }
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "direct incremental merge publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "direct incremental merge publication failed");
}

void PairReadGeneration::destroy_direct(const PairReadGeneration* generation,
                                        GenerationDirectStorage& storage) noexcept {
    if (generation == nullptr) {
        return;
    }
    auto* concrete = const_cast<PairReadGenerationEnableShared*>(
        static_cast<const PairReadGenerationEnableShared*>(generation));
    std::destroy_at(concrete);
    storage.release(concrete);
}

auto PairReadGeneration::publish_incremental_construct(
    const PairReadGeneration& previous, std::shared_ptr<const PairReadGeneration> previous_owner,
    const std::span<const ReadMutation> mutations, PairReadMerge* merge,
    std::shared_ptr<experimental::PairReadGenerationShellStorage> owned_storage,
    experimental::PairReadGenerationInlineShellStorage* borrowed_storage,
    GenerationDirectStorage* direct_storage, const PairReadGeneration** direct_result)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    const auto construction_modes = static_cast<unsigned>(owned_storage != nullptr) +
                                    static_cast<unsigned>(borrowed_storage != nullptr) +
                                    static_cast<unsigned>(direct_storage != nullptr);
    if (construction_modes > 1U || (direct_storage != nullptr) != (direct_result != nullptr)) {
        return fail(ErrorCode::invalid_argument, "read publication has invalid construction ownership");
    }
    if (direct_result != nullptr) {
        *direct_result = nullptr;
    }
    if (mutations.size() > kMaximumPublicationBatch ||
        (merge != nullptr &&
         (!previous_owner || !merge->state_ || merge->state_->current.get() != &previous))) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read publication");
    }
    if (mutations.empty()) {
        if (!previous_owner) {
            return fail(ErrorCode::invalid_argument, "direct publication requires a non-empty mutation");
        }
        return previous_owner;
    }
    if (previous.epoch_ >= GenerationPublicationToken::kMaximumEpoch) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    if (!can_publish_incremental(previous, merge, mutations.size())) {
        return fail(ErrorCode::resource_exhausted, "incremental read delta capacity exhausted");
    }

    std::optional<DeltaBuilder> post_builder;
    if (merge != nullptr) {
        post_builder.emplace(merge->state_->post_delta, post_delta_builder_scratch());
    }
    DeltaBuilder current_builder{*previous.delta_, current_delta_builder_scratch(),
                                 post_builder ? post_builder->allocation_arena()
                                              : std::shared_ptr<DeltaArena>{}};
    auto visible_through = previous.visible_through_;
    for (const auto& mutation : mutations) {
        const bool durable_put = mutation.opcode == Opcode::put && mutation.durable.has_value();
        const bool volatile_pinned = same_segment(mutation.segment, mutation.record);
        if (mutation.opcode == Opcode::put && !durable_put && !volatile_pinned) {
            return fail(ErrorCode::invalid_reference,
                        "publication rejected a RecordRef without its exact Segment pin");
        }
        if (durable_put && (mutation.durable->key_hash() != mutation.key.hash ||
                            mutation.durable->key() != mutation.key.key ||
                            mutation.durable->reference() != mutation.record)) {
            return fail(ErrorCode::invalid_reference,
                        "durable publication record disagrees with its exact generation pin");
        }
        const auto current_slot = current_builder.prepare(mutation.key.hash, mutation.key.key);
        std::optional<DeltaBuilder::PreparedSlot> post_slot;
        if (post_builder) {
            post_slot.emplace(post_builder->prepare(mutation.key.hash, mutation.key.key));
        }
        const auto* stored = current_builder.store(view_of(mutation));
        current_builder.commit(current_slot, stored);
        if (post_slot) {
            post_builder->commit(*post_slot, stored);
        }
        visible_through = std::max(visible_through, mutation.record.sequence.value);
    }

    auto next_delta = std::move(current_builder).freeze();
    std::optional<DeltaState> next_post;
    if (post_builder) {
        next_post.emplace(std::move(*post_builder).freeze());
    }
    std::shared_ptr<const PairReadGeneration> next;
    if (direct_storage != nullptr) {
        static_assert(sizeof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kBytes);
        static_assert(alignof(PairReadGenerationEnableShared) <= GenerationDirectStorage::kAlignment);
        auto* location = direct_storage->claim(sizeof(PairReadGenerationEnableShared),
                                               alignof(PairReadGenerationEnableShared));
        try {
            auto* constructed = std::construct_at(static_cast<PairReadGenerationEnableShared*>(location),
                                                  previous.routing_, previous.base_, std::move(next_delta),
                                                  previous.epoch_ + 1U, visible_through);
            *direct_result = constructed;
            // Merge retains a non-owning view; the slot pool owns destruction.
            next = std::shared_ptr<const PairReadGeneration>(constructed, [](const PairReadGeneration*) {});
        } catch (...) {
            direct_storage->release(location);
            throw;
        }
    } else if (borrowed_storage != nullptr) {
        next = make_shared_generation_in_borrowed_shell(previous.routing_, previous.base_,
                                                        std::move(next_delta), previous.epoch_ + 1U,
                                                        visible_through, *borrowed_storage);
    } else if (owned_storage) {
        next =
            make_shared_generation_in_shell(previous.routing_, previous.base_, std::move(next_delta),
                                            previous.epoch_ + 1U, visible_through, std::move(owned_storage));
    } else {
        next = make_shared_generation(previous.routing_, previous.base_, std::move(next_delta),
                                      previous.epoch_ + 1U, visible_through);
    }
    if (merge != nullptr) {
        merge->state_->post_delta = std::move(*next_post);
        merge->state_->current = next;
    }
    return next;
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "incremental read publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "incremental read publication failed");
}

auto PairReadGeneration::start_incremental_merge(std::shared_ptr<const PairReadGeneration> cut,
                                                 const std::size_t maximum_post_entries)
    -> Result<std::unique_ptr<PairReadMerge>> try {
    if (!cut || maximum_post_entries == 0 || cut->delta_->size() == 0 ||
        cut->delta_->size() > cut->delta_->maximum_entries()) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read merge boundary");
    }
    if (cut->base_->size() > std::numeric_limits<std::size_t>::max() - cut->delta_->size()) {
        return fail(ErrorCode::arithmetic_overflow, "incremental read merge size overflows size_t");
    }
    const auto bounded_post_entries =
        std::min(maximum_post_entries, cut->delta_->maximum_entries() - cut->delta_->size());
    auto builder = std::make_unique<IncrementalBaseBuilder>(cut->base_->size() + cut->delta_->size()
#if defined(__unix__) || defined(__APPLE__)
                                                                ,
                                                            cut->base_->mapping_pool()
#endif
    );
    auto post_delta = make_empty_delta(cut->delta_->maximum_entries());
    auto state = std::make_unique<PairReadMerge::State>(cut, std::move(builder), std::move(post_delta),
                                                        bounded_post_entries);
    return std::unique_ptr<PairReadMerge>{new PairReadMerge{std::move(state)}};
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "incremental read merge allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "incremental read merge construction failed");
}

auto PairReadGeneration::advance_incremental_merge(PairReadMerge& merge, const std::size_t maximum_slots)
    -> Result<std::size_t> try {
    if (!merge.state_ || maximum_slots == 0) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read merge quantum");
    }
    auto& state = *merge.state_;
    std::size_t processed{};
    while (state.phase != PairReadMerge::State::Phase::ready) {
        if (state.phase == PairReadMerge::State::Phase::initialize) {
            if (state.builder->initialized()) {
                state.phase = PairReadMerge::State::Phase::base;
                continue;
            }
            if (processed == maximum_slots) {
                break;
            }
            processed += state.builder->initialize_next(maximum_slots - processed);
            continue;
        }
        if (state.phase == PairReadMerge::State::Phase::base) {
            if (state.base_cursor == state.cut->base_->capacity()) {
                state.phase = PairReadMerge::State::Phase::delta;
                continue;
            }
            if (processed == maximum_slots) {
                break;
            }
            auto record = state.cut->base_->record_at(state.base_cursor++);
            ++processed;
            if (!record) {
                continue;
            }
            const HashedKey key{.key = record->key, .hash = record->hash};
            const auto* override = state.cut->delta_->find_handle(key);
            if (override == nullptr) {
                state.builder->insert(*record);
            } else if (override->opcode == Opcode::put) {
                state.builder->insert(delta_record_view(*override, key.hash));
            }
            continue;
        }

        if (state.delta_cursor == state.cut->delta_->capacity()) {
            state.phase = PairReadMerge::State::Phase::ready;
            continue;
        }
        if (processed == maximum_slots) {
            break;
        }
        auto record = state.cut->delta_->record_at(state.delta_cursor++);
        ++processed;
        if (!record || record->opcode == Opcode::erase) {
            continue;
        }
        const HashedKey key{.key = record->key, .hash = record->hash};
        if (!state.builder->contains(key)) {
            state.builder->insert(*record);
        }
    }
    return processed;
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "incremental read merge quantum allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "incremental read merge quantum failed");
}

auto PairReadGeneration::finish_incremental_merge(std::shared_ptr<const PairReadGeneration> current,
                                                  PairReadMerge& merge)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    if (!current || !merge.state_ || merge.state_->current.get() != current.get() ||
        merge.state_->phase != PairReadMerge::State::Phase::ready || !merge.state_->builder) {
        return fail(ErrorCode::invalid_argument, "incremental read merge is not ready");
    }
    if (current->epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    auto next_base = std::move(*merge.state_->builder).freeze();
    merge.state_->builder.reset();
    return make_shared_generation(current->routing_, std::move(next_base),
                                  std::move(merge.state_->post_delta), current->epoch_ + 1U,
                                  current->visible_through_);
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "incremental read merge publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "incremental read merge publication failed");
}

auto PairReadGeneration::merge_ready(const PairReadMerge& merge) noexcept -> bool {
    return merge.state_ && merge.state_->phase == PairReadMerge::State::Phase::ready;
}

auto PairReadGeneration::merge_post_entries(const PairReadMerge& merge) noexcept -> std::size_t {
    return merge.state_ ? merge.state_->post_delta.size() : 0U;
}

auto PairReadGeneration::merge_remaining_slots(const PairReadMerge& merge) noexcept -> std::size_t {
    if (!merge.state_ || !merge.state_->builder) {
        return 0U;
    }
    const auto& state = *merge.state_;
    auto remaining = state.builder->remaining_initialization_slots();
    if (state.phase == PairReadMerge::State::Phase::initialize ||
        state.phase == PairReadMerge::State::Phase::base) {
        remaining = saturating_add(remaining, state.cut->base_->capacity() - state.base_cursor);
    }
    if (state.phase != PairReadMerge::State::Phase::ready) {
        remaining = saturating_add(remaining, state.cut->delta_->capacity() - state.delta_cursor);
    }
    return remaining;
}

auto PairReadGeneration::merge_post_capacity_remaining(const PairReadMerge& merge) noexcept -> std::size_t {
    if (!merge.state_) {
        return 0U;
    }
    const auto& state = *merge.state_;
    if (state.post_delta.size() > state.maximum_post_entries) {
        return 0U;
    }
    return std::min(state.maximum_post_entries - state.post_delta.size(),
                    state.post_delta.available_record_versions());
}

auto PairReadGeneration::merge_advance_budget(const PairReadMerge& merge,
                                              const std::size_t maximum_new_records,
                                              const std::size_t minimum_slots) noexcept -> std::size_t {
    const auto remaining_slots = merge_remaining_slots(merge);
    if (remaining_slots == 0U) {
        return 0U;
    }
    const auto post_capacity = merge_post_capacity_remaining(merge);
    if (maximum_new_records == 0U) {
        return std::min(remaining_slots, minimum_slots);
    }
    if (maximum_new_records >= post_capacity) {
        return remaining_slots;
    }

    // ceil(remaining_slots * maximum_new_records / post_capacity), decomposed
    // before multiplication. maximum_new_records and the remainder are both
    // bounded by kMaximumIncrementalDeltaEntries, so the tail product cannot
    // overflow while the quotient term is at most remaining_slots.
    const auto quotient = remaining_slots / post_capacity;
    const auto remainder = remaining_slots % post_capacity;
    auto proportional = quotient * maximum_new_records;
    const auto tail_product = remainder * maximum_new_records;
    proportional += tail_product / post_capacity;
    if (tail_product % post_capacity != 0U) {
        ++proportional;
    }
    return std::min(remaining_slots, std::max(minimum_slots, proportional));
}

auto PairReadGeneration::can_publish_incremental(const PairReadGeneration& current,
                                                 const PairReadMerge* merge,
                                                 const std::size_t maximum_new_entries) noexcept -> bool {
    const auto delta_size = current.delta_->size();
    if (delta_size > current.delta_->maximum_entries() ||
        maximum_new_entries > current.delta_->maximum_entries() - delta_size ||
        maximum_new_entries > current.delta_->available_record_versions()) {
        return false;
    }
    if (merge == nullptr) {
        return true;
    }
    if (!merge->state_) {
        return false;
    }
    const auto post_size = merge->state_->post_delta.size();
    return post_size <= merge->state_->maximum_post_entries &&
           maximum_new_entries <= merge->state_->maximum_post_entries - post_size &&
           maximum_new_entries <= merge->state_->post_delta.available_record_versions();
}

auto PairReadGeneration::get(const HashedKey& key, const std::uint64_t now_ns) const -> Result<OwnedValue> {
    GS_PHASE_GET(index_lookup);
    const auto* delta_record = delta_->find(key);
    if (delta_record == nullptr) {
        return base_->get(key, now_ns);
    }
    const auto record = delta_record_view(*delta_record, key.hash);
    if (record.opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key not found");
    }
    if (record.durable != nullptr) {
        return fail(ErrorCode::invalid_argument, "durable read generation requires asynchronous preparation");
    }
    auto decoded = decode_pinned(record);
    if (!decoded) {
        return unexpected(std::move(decoded.error()));
    }
    if (decoded->key_string() != key.key) {
        return fail(ErrorCode::invalid_reference, "read generation Index key mismatch");
    }
    if (decoded->expired(now_ns)) {
        return fail(ErrorCode::not_found, "key not found");
    }
    GS_PHASE_GET(value_copy);
    return OwnedValue::from_bytes(decoded->value, decoded->sequence.value, decoded->expire_at_ns);
}

auto PairReadGeneration::prepare_durable(const HashedKey& key) const
    -> Result<DurableRuntimeCatalog::PublishedReadView> {
    const auto* delta_record = delta_->find(key);
    if (delta_record == nullptr) {
        return base_->prepare_durable(key);
    }
    const auto record = delta_record_view(*delta_record, key.hash);
    if (record.opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key not found");
    }
    if (record.durable == nullptr || record.hash != key.hash || record.key != key.key ||
        !record.durable->matches(record.record)) {
        return fail(ErrorCode::invalid_reference, "durable read generation has no exact file-generation pin");
    }
    return DurableRuntimeCatalog::PublishedReadView::borrow(record.key, record.hash, record.record,
                                                            *record.durable);
}

auto PairReadGeneration::delta_entries() const noexcept -> std::size_t {
    return delta_->size();
}

auto PairReadGeneration::delta_record_versions() const noexcept -> std::size_t {
    return delta_->record_versions();
}

auto PairReadGeneration::delta_arena_record_bytes() const noexcept -> std::size_t {
    return delta_->arena_record_bytes();
}

auto PairReadGeneration::delta_arena_key_bytes() const noexcept -> std::size_t {
    return delta_->arena_key_bytes();
}

auto PairReadGeneration::delta_arena_key_storage_bytes() const noexcept -> std::size_t {
    return delta_->arena_key_storage_bytes();
}

auto PairReadGeneration::base_entries() const noexcept -> std::size_t {
    return base_->size();
}

auto PairReadGeneration::memory_stats() const noexcept -> ReadGenerationMemoryStats {
    auto stats = base_->memory_stats();
    delta_->append_memory_stats(stats);
    stats.generation_shell_bytes = sizeof(PairReadGenerationEnableShared);
    stats.current_allocated_lower_bound_bytes =
        saturating_add(stats.generation_shell_bytes, saturating_add(stats.base_allocated_lower_bound_bytes,
                                                                    stats.delta_allocated_lower_bound_bytes));
    return stats;
}

} // namespace glyphastore::store::paired

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental(
    std::shared_ptr<const store::paired::PairReadGeneration> previous,
    const std::span<const store::paired::ReadMutation> mutations,
    std::shared_ptr<PairReadGenerationShellStorage> storage, store::paired::PairReadMerge* merge)
    -> Result<std::shared_ptr<const store::paired::PairReadGeneration>> {
    return store::paired::PairReadGeneration::publish_incremental_in_shell(std::move(previous), mutations,
                                                                           merge, std::move(storage));
}

auto glyphastore::experimental::PairReadGenerationShellAccess::empty_direct(
    const WorkerRoutingState routing, PairReadGenerationDirectStorage& storage)
    -> Result<const store::paired::PairReadGeneration*> {
    return store::paired::PairReadGeneration::empty_direct(routing, storage);
}

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental_borrowed(
    std::shared_ptr<const store::paired::PairReadGeneration> previous,
    const std::span<const store::paired::ReadMutation> mutations,
    PairReadGenerationInlineShellStorage& storage)
    -> Result<std::shared_ptr<const store::paired::PairReadGeneration>> {
    return store::paired::PairReadGeneration::publish_incremental_in_borrowed_shell(std::move(previous),
                                                                                    mutations, storage);
}

auto glyphastore::experimental::PairReadGenerationShellAccess::publish_incremental_direct(
    const store::paired::PairReadGeneration& previous,
    const std::span<const store::paired::ReadMutation> mutations, PairReadGenerationDirectStorage& storage)
    -> Result<const store::paired::PairReadGeneration*> {
    return store::paired::PairReadGeneration::publish_incremental_direct(previous, mutations, storage);
}

void glyphastore::experimental::PairReadGenerationShellAccess::destroy_direct(
    const store::paired::PairReadGeneration* generation, PairReadGenerationDirectStorage& storage) noexcept {
    store::paired::PairReadGeneration::destroy_direct(generation, storage);
}
