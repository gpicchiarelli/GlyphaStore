#include "glyphastore/server/pair_read_generation.hpp"

#include "glyphastore/index/swiss_control_group.hpp"

#include <algorithm>
#include <array>
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

[[nodiscard]] auto record_less(const ReadRecordHandle& left, const ReadRecordHandle& right) noexcept -> bool {
    return left->hash < right->hash || (left->hash == right->hash && left->key < right->key);
}

void apply_record(MutableReadIndex& index, ReadRecordHandle record) {
    const auto position = std::lower_bound(index.begin(), index.end(), record, record_less);
    const bool present =
        position != index.end() && (*position)->hash == record->hash && (*position)->key == record->key;
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

[[nodiscard]] auto decode_pinned(const ReadRecord& record) -> Result<RecordView> {
    if (!same_segment(record.segment, record.record)) {
        return fail(ErrorCode::invalid_reference, "read generation has no exact Segment generation pin");
    }
    const auto offset = static_cast<std::size_t>(record.record.offset.value);
    const auto size = static_cast<std::size_t>(record.record.size.value);
    if (offset > record.segment->capacity() || size > record.segment->capacity() - offset) {
        return fail(ErrorCode::invalid_reference, "read generation RecordRef exceeds pinned Segment");
    }
    auto decoded = decode_record({record.segment->base() + offset, size});
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

} // namespace

class ImmutableReadIndex final {
  public:
    ImmutableReadIndex()
        : control_(kSwissGroupSize, kSwissEmpty), hashes_(kSwissGroupSize), records_(kSwissGroupSize),
          capacity_(kSwissGroupSize) {}

    explicit ImmutableReadIndex(MutableReadIndex source) {
        capacity_ = kSwissGroupSize;
        while (maximum_occupancy(capacity_) < source.size()) {
            if (capacity_ > std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::bad_alloc{};
            }
            capacity_ *= 2U;
        }
        control_.assign(capacity_, kSwissEmpty);
        hashes_.assign(capacity_, 0);
        records_.resize(capacity_);
        size_ = source.size();
        for (auto& record : source) {
            place(std::move(record));
        }
    }

    [[nodiscard]] auto find(const HashedKey& key) const noexcept -> const ReadRecord* {
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
                if (hashes_[slot] == key.hash && records_[slot]->key == key.key) {
                    return records_[slot].get();
                }
            }
            group_start = next_group(group_start);
        }
        return nullptr;
    }

    [[nodiscard]] auto records() const -> MutableReadIndex {
        MutableReadIndex result;
        result.reserve(size_);
        for (std::size_t slot = 0; slot < capacity_; ++slot) {
            if (control_[slot] != kSwissEmpty) {
                result.push_back(records_[slot]);
            }
        }
        std::sort(result.begin(), result.end(), record_less);
        return result;
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
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

    void place(ReadRecordHandle record) noexcept {
        auto group_start = probe_start(record->hash);
        for (;;) {
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto slot = group_start + offset;
                if (control_[slot] == kSwissEmpty) {
                    control_[slot] = fingerprint(record->hash);
                    hashes_[slot] = record->hash;
                    records_[slot] = std::move(record);
                    return;
                }
            }
            group_start = next_group(group_start);
        }
    }

    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::vector<ReadRecordHandle> records_;
    std::size_t capacity_{};
    std::size_t size_{};
};

class DeltaState final {
  public:
    DeltaState(const std::size_t capacity, const std::size_t size,
               std::vector<std::shared_ptr<const DeltaPage>> flat_pages,
               std::vector<std::shared_ptr<const DeltaDirectoryBlock>> directory)
        : capacity_(capacity), size_(size), flat_pages_(std::move(flat_pages)),
          directory_(std::move(directory)) {}

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
                if (page->hashes[slot] == key.hash && page->records[slot]->key == key.key) {
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
                    callback(page->records[slot]);
                }
            }
        }
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
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
        return std::make_shared<const DeltaState>(capacity, 0,
                                                  std::vector<std::shared_ptr<const DeltaPage>>(page_count),
                                                  std::vector<std::shared_ptr<const DeltaDirectoryBlock>>{});
    }
    const auto directory_count = (page_count + kDeltaDirectoryBlockPages - 1U) / kDeltaDirectoryBlockPages;
    return std::make_shared<const DeltaState>(
        capacity, 0, std::vector<std::shared_ptr<const DeltaPage>>{},
        std::vector<std::shared_ptr<const DeltaDirectoryBlock>>(directory_count));
}

class DeltaBuilder final {
  public:
    explicit DeltaBuilder(std::shared_ptr<const DeltaState> previous)
        : previous_(std::move(previous)), flat_pages_(previous_->flat_pages_),
          directory_(previous_->directory_), mutable_blocks_(directory_.size()),
          mutable_pages_(previous_->capacity_ / kDeltaPageSlots), size_(previous_->size_) {}

    void insert_or_assign(ReadRecordHandle record) {
        const auto wanted = fingerprint(record->hash);
        auto group_start = probe_start(record->hash);
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
                if ((matches & (1ULL << offset)) != 0 && page->hashes[slot] == record->hash &&
                    page->records[slot]->key == record->key) {
                    place(page_index, slot, wanted, std::move(record), false);
                    return;
                }
            }
            group_start = next_group(group_start);
        }
        throw std::bad_alloc{};
    }

    [[nodiscard]] auto freeze() && -> std::shared_ptr<const DeltaState> {
        return std::make_shared<const DeltaState>(previous_->capacity_, size_, std::move(flat_pages_),
                                                  std::move(directory_));
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
        if (!mutable_pages_[page_index]) {
            const auto* existing = page_at(page_index);
            mutable_pages_[page_index] =
                existing ? std::make_shared<DeltaPage>(*existing) : std::make_shared<DeltaPage>();
            if (directory_.empty()) {
                flat_pages_[page_index] = mutable_pages_[page_index];
            } else {
                const auto block_index = page_index / kDeltaDirectoryBlockPages;
                if (!mutable_blocks_[block_index]) {
                    mutable_blocks_[block_index] =
                        directory_[block_index]
                            ? std::make_shared<DeltaDirectoryBlock>(*directory_[block_index])
                            : std::make_shared<DeltaDirectoryBlock>();
                    directory_[block_index] = mutable_blocks_[block_index];
                }
                mutable_blocks_[block_index]->pages[page_index % kDeltaDirectoryBlockPages] =
                    mutable_pages_[page_index];
            }
        }
        return *mutable_pages_[page_index];
    }

    void place(const std::size_t page_index, const std::size_t slot, const std::uint8_t wanted,
               ReadRecordHandle record, const bool inserted) {
        auto& page = mutable_page(page_index);
        page.control[slot] = wanted;
        page.hashes[slot] = record->hash;
        page.records[slot] = std::move(record);
        if (inserted) {
            ++size_;
        }
    }

    std::shared_ptr<const DeltaState> previous_;
    std::vector<std::shared_ptr<const DeltaPage>> flat_pages_;
    std::vector<std::shared_ptr<const DeltaDirectoryBlock>> directory_;
    std::vector<std::shared_ptr<DeltaDirectoryBlock>> mutable_blocks_;
    std::vector<std::shared_ptr<DeltaPage>> mutable_pages_;
    std::size_t size_{};
};

PairReadGeneration::PairReadGeneration(const WorkerRoutingState routing,
                                       std::shared_ptr<const ImmutableReadIndex> base,
                                       std::shared_ptr<const DeltaState> delta, const std::uint64_t epoch,
                                       const std::uint64_t visible_through) noexcept
    : routing_(routing), base_(std::move(base)), delta_(std::move(delta)), epoch_(epoch),
      visible_through_(visible_through) {}

auto PairReadGeneration::empty(const WorkerRoutingState routing)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    return std::shared_ptr<const PairReadGeneration>{new PairReadGeneration{
        routing, std::make_shared<const ImmutableReadIndex>(), make_empty_delta(8'192), 0, 0}};
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "read generation allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "read generation construction failed");
}

auto PairReadGeneration::from_durable_snapshot(
    const WorkerRoutingState routing,
    const std::span<const DurableRuntimeCatalog::PublishedReadRecord> records)
    -> Result<std::shared_ptr<const PairReadGeneration>> try {
    MutableReadIndex base_records;
    base_records.reserve(records.size());
    std::uint64_t visible_through{};
    for (const auto& record : records) {
        const auto hash = record.key_hash();
        visible_through = std::max(visible_through, record.reference().sequence.value);
        base_records.push_back(std::make_shared<const ReadRecord>(ReadRecord{
            .hash = hash,
            .key = std::string{record.key()},
            .record = record.reference(),
            .durable = record,
            .opcode = Opcode::put,
        }));
    }
    return std::shared_ptr<const PairReadGeneration>{
        new PairReadGeneration{routing, std::make_shared<const ImmutableReadIndex>(std::move(base_records)),
                               make_empty_delta(8'192), 0, visible_through}};
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
            std::make_shared<const ReadRecord>(ReadRecord{.hash = mutation.key.hash,
                                                          .key = std::string{mutation.key.key},
                                                          .record = mutation.record,
                                                          .segment = mutation.segment,
                                                          .durable = mutation.durable,
                                                          .opcode = mutation.opcode}));
        visible_through = std::max(visible_through, mutation.record.sequence.value);
    }

    auto next_delta = std::move(builder).freeze();
    auto next_base = previous->base_;
    if (next_delta->size() >= merge_delta_entries) {
        auto merged = next_base->records();
        next_delta->for_each([&](ReadRecordHandle record) { apply_record(merged, std::move(record)); });
        next_base = std::make_shared<const ImmutableReadIndex>(std::move(merged));
        next_delta = make_empty_delta(merge_delta_entries);
    }
    return std::shared_ptr<const PairReadGeneration>{
        new PairReadGeneration{previous->routing_, std::move(next_base), std::move(next_delta),
                               previous->epoch_ + 1U, visible_through}};
} catch (const std::bad_alloc&) {
    return fail(ErrorCode::resource_exhausted, "read generation publication allocation failed");
} catch (...) {
    return fail(ErrorCode::internal_error, "read generation publication failed");
}

auto PairReadGeneration::get(const HashedKey& key, const std::uint64_t now_ns) const -> Result<OwnedValue> {
    const auto* record = delta_->find(key);
    if (record == nullptr) {
        record = base_->find(key);
    }
    if (record == nullptr || record->opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key not found");
    }
    if (record->durable) {
        return fail(ErrorCode::invalid_argument, "durable read generation requires asynchronous preparation");
    }
    auto decoded = decode_pinned(*record);
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
    const auto* record = delta_->find(key);
    if (record == nullptr) {
        record = base_->find(key);
    }
    if (record == nullptr || record->opcode == Opcode::erase) {
        return fail(ErrorCode::not_found, "key not found");
    }
    if (!record->durable || record->durable->key_hash() != key.hash || record->durable->key() != key.key ||
        record->durable->reference() != record->record) {
        return fail(ErrorCode::invalid_reference, "durable read generation has no exact file-generation pin");
    }
    return *record->durable;
}

auto PairReadGeneration::delta_entries() const noexcept -> std::size_t {
    return delta_->size();
}

auto PairReadGeneration::base_entries() const noexcept -> std::size_t {
    return base_->size();
}

} // namespace glyphastore::server
