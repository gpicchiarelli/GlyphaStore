#include "glyphastore/server/pair_read_generation.hpp"

#include "glyphastore/index/swiss_control_group.hpp"

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace glyphastore::server {
namespace {

inline constexpr std::size_t kDeltaPageSlots = 16;
inline constexpr std::size_t kDeltaDirectoryBlockPages = 16;
inline constexpr std::size_t kFlatDeltaMaximumPages = 32;
inline constexpr std::size_t kMaximumPublicationBatch = 32;
static_assert(kDeltaPageSlots % kSwissGroupSize == 0);

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

struct DeltaPage final {
    DeltaPage() {
        control.fill(kSwissEmpty);
    }

    std::array<std::uint8_t, kDeltaPageSlots> control{};
    std::array<std::uint64_t, kDeltaPageSlots> hashes{};
    std::array<ReadRecordHandle, kDeltaPageSlots> records{};
};

struct DeltaDirectoryBlock final {
    std::array<std::shared_ptr<const DeltaPage>, kDeltaDirectoryBlockPages> pages{};
};

struct CompactReadRecord;

struct CompactReadRecord final {
    union KeyStorage {
        std::array<char, 16> inline_key{};
        const char* external_key;
    };
    RecordRef record;
    KeyStorage key;
    std::uint32_t key_size{};
    std::uint32_t pin_index{};
    Opcode opcode{Opcode::put};
    bool durable{};
};
static_assert(sizeof(CompactReadRecord) <= 64,
              "the immutable Reader record must remain one cache line or smaller");

struct KeyBlock final {
    std::unique_ptr<char[]> bytes;
    std::size_t capacity{};
    std::size_t used{};
};

} // namespace

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
                const auto* record = table_[slot];
                if (hashes_[slot] == key.hash && key_at(*record) == key.key) {
                    return record;
                }
            }
            group_start = next_group(group_start);
        }
        return nullptr;
    }

    [[nodiscard]] auto get(const HashedKey& key, const std::uint64_t now_ns) const -> Result<OwnedValue> {
        const auto* record = find(key);
        if (record == nullptr || record->opcode == Opcode::erase) {
            return fail(ErrorCode::not_found, "key not found");
        }
        if (record->durable || record->pin_index == kNoPin || record->pin_index >= segments_.size()) {
            return fail(record->durable ? ErrorCode::invalid_argument : ErrorCode::invalid_reference,
                        record->durable ? "durable read generation requires asynchronous preparation"
                                        : "read generation has no exact Segment generation pin");
        }
        const auto& segment = segments_[record->pin_index];
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
            decoded->encoded_size != record->record.size.value || decoded->opcode != record->opcode ||
            decoded->key_string() != key.key) {
            return fail(ErrorCode::invalid_reference, "read generation RecordRef identity mismatch");
        }
        if (decoded->expired(now_ns)) {
            return fail(ErrorCode::not_found, "key not found");
        }
        OwnedValue value;
        value.bytes.assign(decoded->value.begin(), decoded->value.end());
        value.sequence = decoded->sequence.value;
        value.expire_at_ns = decoded->expire_at_ns;
        return value;
    }

    [[nodiscard]] auto prepare_durable(const HashedKey& key) const
        -> Result<DurableRuntimeCatalog::PublishedReadRecord> {
        const auto* record = find(key);
        if (record == nullptr || record->opcode == Opcode::erase) {
            return fail(ErrorCode::not_found, "key not found");
        }
        if (!record->durable || record->pin_index == kNoPin || record->pin_index >= durable_pins_.size() ||
            !durable_pins_[record->pin_index].matches(record->record)) {
            return fail(ErrorCode::invalid_reference,
                        "durable read generation has no exact file-generation pin");
        }
        return DurableRuntimeCatalog::PublishedReadRecord::bind(
            std::string{key_at(*record)}, key.hash, record->record, durable_pins_[record->pin_index]);
    }

    [[nodiscard]] auto records() const -> MutableReadIndex {
        MutableReadIndex result;
        result.reserve(size_);
        for (std::size_t slot = 0; slot < capacity_; ++slot) {
            if (control_[slot] != kSwissEmpty) {
                result.push_back(materialize(view_at(*table_[slot], hashes_[slot])));
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
        return control_[slot] != kSwissEmpty
                   ? std::optional<ReadRecordView>{view_at(*table_[slot], hashes_[slot])}
                   : std::nullopt;
    }

  private:
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
        hashes_ = std::make_unique_for_overwrite<std::uint64_t[]>(capacity_);
        table_ = std::make_unique_for_overwrite<const CompactReadRecord*[]>(capacity_);
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

    [[nodiscard]] auto view_at(const CompactReadRecord& record, const std::uint64_t hash) const noexcept
        -> ReadRecordView {
        return {.hash = hash,
                .key = key_at(record),
                .record = record.record,
                .segment =
                    !record.durable && record.pin_index != kNoPin ? &segments_[record.pin_index] : nullptr,
                .durable =
                    record.durable && record.pin_index != kNoPin ? &durable_pins_[record.pin_index] : nullptr,
                .opcode = record.opcode};
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
        auto pin_index = kNoPin;
        bool durable{};
        if (source.opcode == Opcode::put && source.durable != nullptr) {
            durable = true;
            const auto found = std::find_if(durable_pins_.begin(), durable_pins_.end(), [&](const auto& pin) {
                return pin.same_generation(*source.durable);
            });
            if (found == durable_pins_.end()) {
                if (durable_pins_.size() == kNoPin) {
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
                if (segments_.size() == kNoPin) {
                    throw std::bad_alloc{};
                }
                pin_index = static_cast<std::uint32_t>(segments_.size());
                segments_.push_back(*source.segment);
            } else {
                pin_index = static_cast<std::uint32_t>(std::distance(segments_.begin(), found));
            }
        }
        CompactReadRecord compact{.record = source.record,
                                  .key_size = static_cast<std::uint32_t>(source.key.size()),
                                  .pin_index = pin_index,
                                  .opcode = source.opcode,
                                  .durable = durable};
        if (source.key.size() <= compact.key.inline_key.size()) {
            std::ranges::copy(source.key, compact.key.inline_key.begin());
        } else {
            compact.key.external_key = append_key(source.key);
        }
        records_.push_back(std::move(compact));
        place(source.hash, &records_.back());
    }

    [[nodiscard]] auto append_key(const std::string_view key) -> const char* {
        if (key.empty()) {
            return nullptr;
        }
        constexpr std::size_t kKeyBlockBytes = 64U * 1024U;
        if (key_blocks_.empty() || key.size() > key_blocks_.back().capacity - key_blocks_.back().used) {
            const auto capacity = std::max(kKeyBlockBytes, key.size());
            key_blocks_.push_back({.bytes = std::make_unique<char[]>(capacity), .capacity = capacity});
        }
        auto& block = key_blocks_.back();
        auto* destination = block.bytes.get() + block.used;
        std::ranges::copy(key, destination);
        block.used += key.size();
        return destination;
    }

    void place(const std::uint64_t hash, const CompactReadRecord* record) {
        auto group_start = probe_start(hash);
        for (;;) {
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto slot = group_start + offset;
                if (control_[slot] == kSwissEmpty) {
                    control_[slot] = fingerprint(hash);
                    hashes_[slot] = hash;
                    table_[slot] = record;
                    ++size_;
                    return;
                }
            }
            group_start = next_group(group_start);
        }
    }

    std::size_t capacity_{};
    std::size_t size_{};
    static constexpr auto kNoPin = std::numeric_limits<std::uint32_t>::max();
    std::vector<CompactReadRecord> records_;
    std::vector<KeyBlock> key_blocks_;
    std::vector<SegmentPtr> segments_;
    std::vector<DurableReadPin> durable_pins_;
    std::unique_ptr<std::uint8_t[]> control_;
    std::unique_ptr<std::uint64_t[]> hashes_;
    std::unique_ptr<const CompactReadRecord*[]> table_;

    friend class IncrementalBaseBuilder;
};

class IncrementalBaseBuilder final {
  public:
    explicit IncrementalBaseBuilder(const std::size_t maximum_records)
        : index_(std::make_unique<ImmutableReadIndex>()) {
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
               std::vector<std::shared_ptr<const DeltaPage>> flat_pages,
               std::vector<std::shared_ptr<const DeltaDirectoryBlock>> directory)
        : capacity_(capacity), maximum_entries_(maximum_entries), size_(size),
          flat_pages_(std::move(flat_pages)), directory_(std::move(directory)) {}

    [[nodiscard]] auto find_handle(const HashedKey& key) const noexcept -> const ReadRecord* {
        return find(key);
    }

    [[nodiscard]] auto find(const HashedKey& key) const noexcept -> const ReadRecord* {
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
                if (page->hashes[slot] == key.hash && record_key(*page->records[slot]) == key.key) {
                    return page->records[slot].get();
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
                    callback(*page->records[slot]);
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

    [[nodiscard]] auto record_at(const std::size_t slot) const noexcept -> const ReadRecord* {
        if (slot >= capacity_) {
            return nullptr;
        }
        const auto* page = page_at(slot / kDeltaPageSlots);
        const auto offset = slot % kDeltaPageSlots;
        return page != nullptr && page->control[offset] != kSwissEmpty ? page->records[offset].get()
                                                                       : nullptr;
    }

  private:
    [[nodiscard]] auto page_at(const std::size_t page_index) const noexcept -> const DeltaPage* {
        if (directory_.empty()) {
            return flat_pages_[page_index].get();
        }
        const auto& block = directory_[page_index / kDeltaDirectoryBlockPages];
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
    std::vector<std::shared_ptr<const DeltaPage>> flat_pages_;
    std::vector<std::shared_ptr<const DeltaDirectoryBlock>> directory_;

    friend class DeltaBuilder;
};

[[nodiscard]] auto make_empty_delta(const std::size_t maximum_entries) -> std::shared_ptr<const DeltaState> {
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
    if (page_count <= kFlatDeltaMaximumPages) {
        return std::make_shared<const DeltaState>(capacity, maximum_entries, 0,
                                                  std::vector<std::shared_ptr<const DeltaPage>>(page_count),
                                                  std::vector<std::shared_ptr<const DeltaDirectoryBlock>>{});
    }
    const auto directory_count = (page_count + kDeltaDirectoryBlockPages - 1U) / kDeltaDirectoryBlockPages;
    return std::make_shared<const DeltaState>(
        capacity, maximum_entries, 0, std::vector<std::shared_ptr<const DeltaPage>>{},
        std::vector<std::shared_ptr<const DeltaDirectoryBlock>>(directory_count));
}

class DeltaBuilder final {
  public:
    explicit DeltaBuilder(std::shared_ptr<const DeltaState> previous)
        : previous_(std::move(previous)), flat_pages_(previous_->flat_pages_),
          directory_(previous_->directory_), size_(previous_->size_) {
        mutable_blocks_.reserve(kMaximumPublicationBatch);
        mutable_pages_.reserve(kMaximumPublicationBatch);
    }

    void insert_or_assign(ReadRecord record) {
        const auto wanted = fingerprint(record.hash);
        auto group_start = probe_start(record.hash);
        for (std::size_t probed = 0; probed < previous_->capacity_; probed += kSwissGroupSize) {
            const auto page_index = group_start / kDeltaPageSlots;
            const auto page_offset = group_start % kDeltaPageSlots;
            const auto* page = page_at(page_index);
            if (page == nullptr) {
                place(page_index, page_offset, wanted, std::move(record), true);
                return;
            }
            const auto* group = &page->control[page_offset];
            const auto control_word = detail::load_control_group64(group);
            const auto matches = detail::equal_byte_mask(group, wanted);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto control = detail::control_byte_at(control_word, offset);
                const auto slot = page_offset + offset;
                if (control == kSwissEmpty) {
                    place(page_index, slot, wanted, std::move(record), true);
                    return;
                }
                if ((matches & (1ULL << offset)) != 0 && page->hashes[slot] == record.hash &&
                    record_key(*page->records[slot]) == record_key(record)) {
                    place(page_index, slot, wanted, std::move(record), false);
                    return;
                }
            }
            group_start = next_group(group_start);
        }
        throw std::bad_alloc{};
    }

    [[nodiscard]] auto freeze() && -> std::shared_ptr<const DeltaState> {
        return std::make_shared<const DeltaState>(previous_->capacity_, previous_->maximum_entries_, size_,
                                                  std::move(flat_pages_), std::move(directory_));
    }

  private:
    [[nodiscard]] auto page_at(const std::size_t page_index) const noexcept -> const DeltaPage* {
        if (directory_.empty()) {
            return flat_pages_[page_index].get();
        }
        const auto& block = directory_[page_index / kDeltaDirectoryBlockPages];
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
        const auto found_page = std::find_if(mutable_pages_.begin(), mutable_pages_.end(),
                                             [&](const auto& entry) { return entry.first == page_index; });
        if (found_page != mutable_pages_.end()) {
            return *found_page->second;
        }
        const auto* existing = page_at(page_index);
        auto page = existing ? std::make_shared<DeltaPage>(*existing) : std::make_shared<DeltaPage>();
        mutable_pages_.emplace_back(page_index, page);
        if (directory_.empty()) {
            flat_pages_[page_index] = page;
        } else {
            const auto block_index = page_index / kDeltaDirectoryBlockPages;
            auto found_block = std::find_if(mutable_blocks_.begin(), mutable_blocks_.end(),
                                            [&](const auto& entry) { return entry.first == block_index; });
            if (found_block == mutable_blocks_.end()) {
                auto block = directory_[block_index]
                                 ? std::make_shared<DeltaDirectoryBlock>(*directory_[block_index])
                                 : std::make_shared<DeltaDirectoryBlock>();
                mutable_blocks_.emplace_back(block_index, block);
                directory_[block_index] = block;
                found_block = std::prev(mutable_blocks_.end());
            }
            found_block->second->pages[page_index % kDeltaDirectoryBlockPages] = page;
        }
        return *page;
    }

    void place(const std::size_t page_index, const std::size_t slot, const std::uint8_t wanted,
               ReadRecord record, const bool inserted) {
        if (inserted && size_ >= previous_->maximum_entries_) {
            throw std::bad_alloc{};
        }
        auto& page = mutable_page(page_index);
        auto stored = std::make_shared<const ReadRecord>(std::move(record));
        page.control[slot] = wanted;
        page.hashes[slot] = stored->hash;
        page.records[slot] = std::move(stored);
        if (inserted) {
            ++size_;
        }
    }

    std::shared_ptr<const DeltaState> previous_;
    std::vector<std::shared_ptr<const DeltaPage>> flat_pages_;
    std::vector<std::shared_ptr<const DeltaDirectoryBlock>> directory_;
    std::vector<std::pair<std::size_t, std::shared_ptr<DeltaDirectoryBlock>>> mutable_blocks_;
    std::vector<std::pair<std::size_t, std::shared_ptr<DeltaPage>>> mutable_pages_;
    std::size_t size_{};
};

struct PairReadMerge::State final {
    enum class Phase : std::uint8_t { initialize, base, delta, ready };

    State(std::shared_ptr<const PairReadGeneration> merge_cut,
          std::unique_ptr<IncrementalBaseBuilder> next_base, std::shared_ptr<const DeltaState> post,
          const std::size_t maximum_post) noexcept
        : cut(std::move(merge_cut)), current(cut), builder(std::move(next_base)), post_delta(std::move(post)),
          maximum_post_entries(maximum_post) {}

    std::shared_ptr<const PairReadGeneration> cut;
    std::shared_ptr<const PairReadGeneration> current;
    std::unique_ptr<IncrementalBaseBuilder> builder;
    std::shared_ptr<const DeltaState> post_delta;
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
                                       std::shared_ptr<const DeltaState> delta, const std::uint64_t epoch,
                                       const std::uint64_t visible_through) noexcept
    : routing_(routing), base_(std::move(base)), delta_(std::move(delta)), epoch_(epoch),
      visible_through_(visible_through) {}

auto PairReadGeneration::empty(const WorkerRoutingState routing)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    return std::shared_ptr<const PairReadGeneration>{
        new PairReadGeneration{routing, std::make_shared<const ImmutableReadIndex>(),
                               make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries), 0, 0}};
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
    return std::shared_ptr<const PairReadGeneration>{new PairReadGeneration{
        routing, std::make_shared<const ImmutableReadIndex>(records),
        make_empty_delta(PairReadGeneration::kMaximumIncrementalDeltaEntries), epoch, visible_through}};
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

    DeltaBuilder builder{previous->delta_};
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
        builder.insert_or_assign(
            ReadRecord{.hash = mutation.key.hash,
                       .key = durable_put ? std::string{} : std::string{mutation.key.key},
                       .record = mutation.record,
                       .segment = mutation.segment,
                       .durable = mutation.durable,
                       .opcode = mutation.opcode});
        visible_through = std::max(visible_through, mutation.record.sequence.value);
    }

    auto next_delta = std::move(builder).freeze();
    auto next_base = previous->base_;
    if (next_delta->size() >= merge_delta_entries) {
        auto merged = next_base->records();
        next_delta->for_each(
            [&](const ReadRecord& record) { apply_record(merged, materialize(view_of(record))); });
        next_base = std::make_shared<const ImmutableReadIndex>(std::move(merged));
        next_delta = make_empty_delta(std::max(merge_delta_entries, previous->delta_->maximum_entries()));
    }
    return std::shared_ptr<const PairReadGeneration>{
        new PairReadGeneration{previous->routing_, std::move(next_base), std::move(next_delta),
                               previous->epoch_ + 1U, visible_through}};
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "read generation publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "read generation publication failed");
}

auto PairReadGeneration::publish_incremental(std::shared_ptr<const PairReadGeneration> previous,
                                             const std::span<const ReadMutation> mutations,
                                             PairReadMerge* merge)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    if (!previous || mutations.size() > kMaximumPublicationBatch ||
        (merge != nullptr && (!merge->state_ || merge->state_->current.get() != previous.get()))) {
        return fail(ErrorCode::invalid_argument, "invalid incremental read publication");
    }
    if (mutations.empty()) {
        return previous;
    }
    if (previous->epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return fail(ErrorCode::arithmetic_overflow, "read generation epoch exhausted");
    }
    if (!can_publish_incremental(*previous, merge, mutations.size())) {
        return fail(ErrorCode::resource_exhausted, "incremental read delta capacity exhausted");
    }

    DeltaBuilder current_builder{previous->delta_};
    std::optional<DeltaBuilder> post_builder;
    if (merge != nullptr) {
        post_builder.emplace(merge->state_->post_delta);
    }
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
        auto record = ReadRecord{.hash = mutation.key.hash,
                                 .key = durable_put ? std::string{} : std::string{mutation.key.key},
                                 .record = mutation.record,
                                 .segment = mutation.segment,
                                 .durable = mutation.durable,
                                 .opcode = mutation.opcode};
        if (post_builder) {
            current_builder.insert_or_assign(record);
            post_builder->insert_or_assign(std::move(record));
        } else {
            current_builder.insert_or_assign(std::move(record));
        }
        visible_through = std::max(visible_through, mutation.record.sequence.value);
    }

    auto next_delta = std::move(current_builder).freeze();
    std::shared_ptr<const DeltaState> next_post;
    if (post_builder) {
        next_post = std::move(*post_builder).freeze();
    }
    auto next = std::shared_ptr<const PairReadGeneration>{new PairReadGeneration{
        previous->routing_, previous->base_, std::move(next_delta), previous->epoch_ + 1U, visible_through}};
    if (merge != nullptr) {
        merge->state_->post_delta = std::move(next_post);
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
    auto builder = std::make_unique<IncrementalBaseBuilder>(cut->base_->size() + cut->delta_->size());
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
    while (processed < maximum_slots && state.phase != PairReadMerge::State::Phase::ready) {
        if (state.phase == PairReadMerge::State::Phase::initialize) {
            processed += state.builder->initialize_next(maximum_slots - processed);
            if (state.builder->initialized()) {
                state.phase = PairReadMerge::State::Phase::base;
            }
            continue;
        }
        if (state.phase == PairReadMerge::State::Phase::base) {
            if (state.base_cursor == state.cut->base_->capacity()) {
                state.phase = PairReadMerge::State::Phase::delta;
                continue;
            }
            auto record = state.cut->base_->record_at(state.base_cursor++);
            ++processed;
            if (!record) {
                continue;
            }
            const HashedKey key{.key = record->key, .hash = record->hash};
            auto override = state.cut->delta_->find_handle(key);
            if (!override) {
                state.builder->insert(*record);
            } else if (override->opcode == Opcode::put) {
                state.builder->insert(view_of(*override));
            }
            continue;
        }

        if (state.delta_cursor == state.cut->delta_->capacity()) {
            state.phase = PairReadMerge::State::Phase::ready;
            continue;
        }
        auto record = state.cut->delta_->record_at(state.delta_cursor++);
        ++processed;
        if (!record || record->opcode == Opcode::erase) {
            continue;
        }
        const HashedKey key{.key = record_key(*record), .hash = record->hash};
        if (!state.builder->contains(key)) {
            state.builder->insert(view_of(*record));
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
    return std::shared_ptr<const PairReadGeneration>{
        new PairReadGeneration{current->routing_, std::move(next_base), merge.state_->post_delta,
                               current->epoch_ + 1U, current->visible_through_}};
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "incremental read merge publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "incremental read merge publication failed");
}

auto PairReadGeneration::merge_ready(const PairReadMerge& merge) noexcept -> bool {
    return merge.state_ && merge.state_->phase == PairReadMerge::State::Phase::ready;
}

auto PairReadGeneration::merge_post_entries(const PairReadMerge& merge) noexcept -> std::size_t {
    return merge.state_ ? merge.state_->post_delta->size() : 0U;
}

auto PairReadGeneration::can_publish_incremental(const PairReadGeneration& current,
                                                 const PairReadMerge* merge,
                                                 const std::size_t maximum_new_entries) noexcept -> bool {
    const auto delta_size = current.delta_->size();
    if (delta_size > current.delta_->maximum_entries() ||
        maximum_new_entries > current.delta_->maximum_entries() - delta_size) {
        return false;
    }
    if (merge == nullptr) {
        return true;
    }
    if (!merge->state_) {
        return false;
    }
    const auto post_size = merge->state_->post_delta->size();
    return post_size <= merge->state_->maximum_post_entries &&
           maximum_new_entries <= merge->state_->maximum_post_entries - post_size;
}

auto PairReadGeneration::get(const HashedKey& key, const std::uint64_t now_ns) const -> Result<OwnedValue> {
    const auto* delta_record = delta_->find(key);
    if (delta_record == nullptr) {
        return base_->get(key, now_ns);
    }
    const auto record = view_of(*delta_record);
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
    OwnedValue value;
    value.bytes.assign(decoded->value.begin(), decoded->value.end());
    value.sequence = decoded->sequence.value;
    value.expire_at_ns = decoded->expire_at_ns;
    return value;
}

auto PairReadGeneration::prepare_durable(const HashedKey& key) const
    -> Result<DurableRuntimeCatalog::PublishedReadRecord> {
    const auto* delta_record = delta_->find(key);
    if (delta_record == nullptr) {
        return base_->prepare_durable(key);
    }
    const auto record = view_of(*delta_record);
    if (record.opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key not found");
    }
    if (record.durable == nullptr || record.hash != key.hash || record.key != key.key ||
        !record.durable->matches(record.record)) {
        return fail(ErrorCode::invalid_reference, "durable read generation has no exact file-generation pin");
    }
    return DurableRuntimeCatalog::PublishedReadRecord::bind(std::string{record.key}, record.hash,
                                                            record.record, *record.durable);
}

auto PairReadGeneration::delta_entries() const noexcept -> std::size_t {
    return delta_->size();
}

auto PairReadGeneration::base_entries() const noexcept -> std::size_t {
    return base_->size();
}

} // namespace glyphastore::server
