#include "experimental/paired_shard.hpp"

#include "experimental/spsc_ring.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/index/swiss_control_group.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace glyphastore::experimental {
namespace {

using Clock = std::chrono::steady_clock;
using MutableReadIndex = std::vector<struct ReadRecord>;

void update_high_watermark(std::atomic_size_t& maximum, const std::size_t candidate) noexcept {
    auto current = maximum.load(std::memory_order_relaxed);
    while (current < candidate &&
           !maximum.compare_exchange_weak(current, candidate, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
    }
}

struct ReadRecord final {
    std::uint64_t hash{};
    std::string key;
    std::vector<std::byte> value;
    std::uint64_t sequence{};
    std::uint64_t expire_at_ns{};
    bool tombstone{};
};

[[nodiscard]] auto record_less(const ReadRecord& left, const ReadRecord& right) noexcept -> bool {
    return left.hash < right.hash || (left.hash == right.hash && left.key < right.key);
}

void apply_record(MutableReadIndex& index, ReadRecord record) {
    const auto position = std::lower_bound(index.begin(), index.end(), record, record_less);
    if (position != index.end() && position->hash == record.hash && position->key == record.key) {
        *position = std::move(record);
        return;
    }
    index.insert(position, std::move(record));
}

// Read-only flat index. Construction and all allocation happen on the Writer;
// lookup is allocation-free and probes fixed eight-byte control groups.
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

    [[nodiscard]] auto find(const std::string_view key, const std::uint64_t hash) const noexcept
        -> const ReadRecord* {
        const auto fingerprint = h2(hash);
        auto group_start = probe_start(hash);
        for (std::size_t probed = 0; probed < capacity_; probed += kSwissGroupSize) {
            const auto* const group = &control_[group_start];
            const auto control_word = detail::load_control_group64(group);
            const auto matches = detail::equal_byte_mask(group, fingerprint);
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto control = detail::control_byte_at(control_word, offset);
                if (control == kSwissEmpty) {
                    return nullptr;
                }
                if ((matches & (1ULL << offset)) == 0) {
                    continue;
                }
                const auto slot = group_start + offset;
                if (hashes_[slot] == hash && records_[slot].key == key) {
                    return &records_[slot];
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

    [[nodiscard]] auto storage_bytes() const noexcept -> std::uint64_t {
        auto bytes = static_cast<std::uint64_t>(control_.capacity() * sizeof(std::uint8_t) +
                                                hashes_.capacity() * sizeof(std::uint64_t) +
                                                records_.capacity() * sizeof(ReadRecord));
        for (std::size_t slot = 0; slot < capacity_; ++slot) {
            if (control_[slot] != kSwissEmpty) {
                bytes += records_[slot].key.capacity();
                bytes += records_[slot].value.capacity();
            }
        }
        return bytes;
    }

  private:
    [[nodiscard]] static constexpr auto maximum_occupancy(const std::size_t capacity) noexcept
        -> std::size_t {
        return capacity - capacity / 4U;
    }

    [[nodiscard]] static auto h2(const std::uint64_t hash) noexcept -> std::uint8_t {
        const auto fingerprint = static_cast<std::uint8_t>(hash & 0x7FU);
        return fingerprint == 0 ? static_cast<std::uint8_t>(1) : fingerprint;
    }

    [[nodiscard]] auto probe_start(const std::uint64_t hash) const noexcept -> std::size_t {
        return ((hash >> 7U) & ((capacity_ / kSwissGroupSize) - 1U)) * kSwissGroupSize;
    }

    [[nodiscard]] auto next_group(const std::size_t group_start) const noexcept -> std::size_t {
        const auto next = group_start + kSwissGroupSize;
        return next == capacity_ ? 0 : next;
    }

    void place(ReadRecord record) noexcept {
        auto group_start = probe_start(record.hash);
        for (;;) {
            for (std::size_t offset = 0; offset < kSwissGroupSize; ++offset) {
                const auto slot = group_start + offset;
                if (control_[slot] == kSwissEmpty) {
                    control_[slot] = h2(record.hash);
                    hashes_[slot] = record.hash;
                    records_[slot] = std::move(record);
                    return;
                }
            }
            group_start = next_group(group_start);
        }
    }

    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::vector<ReadRecord> records_;
    std::size_t capacity_{};
    std::size_t size_{};
};

struct ReadGeneration final {
    std::shared_ptr<const ImmutableReadIndex> base;
    ImmutableReadIndex delta;
    std::uint64_t visible_through{};
    std::uint64_t epoch{};
};

struct PublicationDescriptor final {
    const ReadGeneration* generation{};
    std::uint64_t epoch{};
    std::uint64_t visible_through{};
};

enum class GenerationSlotState : std::uint8_t { free, published, retired };

struct GenerationSlot final {
    std::optional<ReadGeneration> generation;
    PublicationDescriptor descriptor;
    Clock::time_point retired_at{};
    std::uint64_t retire_after_turn{};
    GenerationSlotState state{GenerationSlotState::free};
};

struct MutationSlot final {
    std::array<char, VolatileShardPairPrototype::kMaximumKeyBytes> key{};
    std::uint64_t request_id{};
    std::uint64_t expire_at_ns{};
    std::size_t key_size{};
    std::size_t value_size{};
    PrototypeMutationKind kind{};
};

struct InternalCompletion final {
    PrototypeCompletion completion;
    std::uint32_t slot{};
};

static_assert(std::is_nothrow_move_assignable_v<InternalCompletion>);

} // namespace

struct VolatileShardPairPrototype::Impl final {
    static constexpr std::size_t kGenerationPoolCapacity = kQueueCapacity + 2U;

    explicit Impl(const std::size_t maximum_value, const std::size_t merge_entries)
        : maximum_value_bytes(maximum_value), merge_delta_entries(merge_entries),
          value_arena(kQueueCapacity * maximum_value), base(std::make_shared<const ImmutableReadIndex>()) {
        for (std::size_t index = 0; index < kQueueCapacity; ++index) {
            free_slots[index] = static_cast<std::uint32_t>(kQueueCapacity - 1U - index);
        }
        free_count = kQueueCapacity;

        auto& initial = generations[0];
        initial.generation.emplace(
            ReadGeneration{.base = base, .delta = ImmutableReadIndex{}, .visible_through = 0, .epoch = 0});
        initial.descriptor = {.generation = &*initial.generation, .epoch = 0, .visible_through = 0};
        initial.state = GenerationSlotState::published;
        current_generation_slot = 0;
        publication.store(&initial.descriptor, std::memory_order_release);
        local_publication = &initial.descriptor;
        generation_live.store(1, std::memory_order_relaxed);
    }

    void start() {
        writer = std::jthread([this] { writer_loop(); });
    }

    [[nodiscard]] auto value_span(const std::uint32_t slot) noexcept -> std::span<std::byte> {
        return {value_arena.data() + static_cast<std::size_t>(slot) * maximum_value_bytes,
                maximum_value_bytes};
    }

    [[nodiscard]] auto submit(const PrototypeMutationKind kind, const std::uint64_t request_id,
                              const std::string_view key, const std::span<const std::byte> value,
                              const std::uint64_t expire_at_ns) noexcept -> PrototypeSubmitStatus {
        if (!accepting.load(std::memory_order_acquire)) {
            return PrototypeSubmitStatus::stopped;
        }
        if (key.size() > kMaximumKeyBytes) {
            return PrototypeSubmitStatus::key_too_large;
        }
        if (value.size() > maximum_value_bytes) {
            return PrototypeSubmitStatus::value_too_large;
        }
        if (free_count == 0) {
            queue_full.fetch_add(1U, std::memory_order_relaxed);
            return PrototypeSubmitStatus::queue_full;
        }
        const auto slot = free_slots[--free_count];
        auto& mutation = slots[slot];
        mutation = {.request_id = request_id,
                    .expire_at_ns = expire_at_ns,
                    .key_size = key.size(),
                    .value_size = value.size(),
                    .kind = kind};
        if (!key.empty()) {
            std::memcpy(mutation.key.data(), key.data(), key.size());
        }
        if (!value.empty()) {
            std::copy(value.begin(), value.end(), value_span(slot).begin());
        }
        if (!mutations.try_push(slot)) {
            free_slots[free_count++] = slot;
            queue_full.fetch_add(1U, std::memory_order_relaxed);
            return PrototypeSubmitStatus::queue_full;
        }
        mutation_pushes.fetch_add(1U, std::memory_order_relaxed);
        update_high_watermark(mutation_high_watermark, mutations.size());
        return PrototypeSubmitStatus::submitted;
    }

    void push_completion(const std::uint32_t slot, const PrototypeCompletion& completion) noexcept {
        // Admission reserves one completion credit with every slot. Failure is
        // therefore an invariant violation, not ordinary backpressure.
        while (!completions.try_push(InternalCompletion{.completion = completion, .slot = slot})) {
            accepting.store(false, std::memory_order_release);
            invariant_failed.store(true, std::memory_order_release);
            std::this_thread::yield();
        }
        completion_pushes.fetch_add(1U, std::memory_order_relaxed);
        update_high_watermark(completion_high_watermark, completions.size());
    }

    [[nodiscard]] auto make_record(const std::uint32_t slot, const std::uint64_t sequence) -> ReadRecord {
        const auto& mutation = slots[slot];
        ReadRecord record{.hash = hash_key(std::string_view{mutation.key.data(), mutation.key_size}),
                          .key = std::string{mutation.key.data(), mutation.key_size},
                          .sequence = sequence,
                          .expire_at_ns = mutation.expire_at_ns,
                          .tombstone = mutation.kind == PrototypeMutationKind::erase};
        if (!record.tombstone) {
            const auto source = value_span(slot).first(mutation.value_size);
            record.value.assign(source.begin(), source.end());
            ingress_value_bytes_copied.fetch_add(source.size(), std::memory_order_relaxed);
        }
        return record;
    }

    void reclaim_generations() noexcept {
        const auto turn = reader_turns.load(std::memory_order_seq_cst);
        const auto now = Clock::now();
        for (auto& slot : generations) {
            if (slot.state != GenerationSlotState::retired || turn < slot.retire_after_turn) {
                continue;
            }
            const auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(now - slot.retired_at);
            generation_retire_delay_ns.fetch_add(static_cast<std::uint64_t>(delay.count()),
                                                 std::memory_order_relaxed);
            slot.generation.reset();
            slot.descriptor = {};
            slot.state = GenerationSlotState::free;
            generation_retire_count.fetch_add(1U, std::memory_order_relaxed);
            generation_live.fetch_sub(1U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] auto acquire_generation_slot() noexcept -> std::size_t {
        for (;;) {
            reclaim_generations();
            for (std::size_t index = 0; index < generations.size(); ++index) {
                if (generations[index].state == GenerationSlotState::free) {
                    return index;
                }
            }
            publication_backpressure.fetch_add(1U, std::memory_order_relaxed);
            std::this_thread::yield();
        }
    }

    void publish(const std::size_t next_slot, std::shared_ptr<const ImmutableReadIndex> next_base,
                 ImmutableReadIndex next_delta, const std::uint64_t next_visible,
                 const std::uint64_t next_epoch) {
        auto& next = generations[next_slot];
        next.generation.emplace(ReadGeneration{.base = std::move(next_base),
                                               .delta = std::move(next_delta),
                                               .visible_through = next_visible,
                                               .epoch = next_epoch});
        next.descriptor = {
            .generation = &*next.generation, .epoch = next_epoch, .visible_through = next_visible};
        next.state = GenerationSlotState::published;
        generation_live.fetch_add(1U, std::memory_order_relaxed);
        update_high_watermark(generation_high_watermark, generation_live.load(std::memory_order_relaxed));

        const auto previous_slot = current_generation_slot;
        current_generation_slot = next_slot;
        publication.store(&next.descriptor, std::memory_order_release);

        // Two quiescent boundaries cover a Reader concurrent with publication:
        // one to finish an old-generation turn, one to prove it advanced.
        auto& previous = generations[previous_slot];
        previous.retire_after_turn = reader_turns.load(std::memory_order_seq_cst) + 2U;
        previous.retired_at = Clock::now();
        previous.state = GenerationSlotState::retired;
    }

    void process_batch(const std::span<const std::uint32_t> batch) noexcept {
        const auto started = Clock::now();
        last_batch_size.store(batch.size(), std::memory_order_relaxed);
        update_high_watermark(maximum_batch_size, batch.size());
        try {
            auto candidate_delta = mutable_delta;
            auto next_visible = visible_through;
            for (const auto slot : batch) {
                apply_record(candidate_delta, make_record(slot, ++next_visible));
            }

            auto next_base = base;
            MutableReadIndex frozen_records;
            bool merged = false;
            if (candidate_delta.size() >= merge_delta_entries) {
                auto merged_records = base->records();
                for (const auto& record : candidate_delta) {
                    apply_record(merged_records, record);
                }
                std::erase_if(merged_records, [](const ReadRecord& record) { return record.tombstone; });
                next_base = std::make_shared<const ImmutableReadIndex>(std::move(merged_records));
                merged = true;
            } else {
                frozen_records = candidate_delta;
            }
            ImmutableReadIndex next_delta{std::move(frozen_records)};
            const auto next_epoch = writer_epoch.load(std::memory_order_relaxed) + 1U;
            const auto generation_slot = acquire_generation_slot();
            const auto publication_storage = next_base->storage_bytes() + next_delta.storage_bytes();
            publish(generation_slot, next_base, std::move(next_delta), next_visible, next_epoch);

            base = std::move(next_base);
            mutable_delta = merged ? MutableReadIndex{} : std::move(candidate_delta);
            visible_through = next_visible;
            writer_epoch.store(next_epoch, std::memory_order_relaxed);
            publications.fetch_add(1U, std::memory_order_relaxed);
            publication_records.fetch_add(batch.size(), std::memory_order_relaxed);
            publication_bytes.store(publication_storage, std::memory_order_relaxed);
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started);
            publication_latency_ns.fetch_add(static_cast<std::uint64_t>(elapsed.count()),
                                             std::memory_order_relaxed);

            for (const auto slot : batch) {
                push_completion(slot, PrototypeCompletion{.request_id = slots[slot].request_id,
                                                          .visible_through = next_visible,
                                                          .epoch = next_epoch});
            }
        } catch (const std::bad_alloc&) {
            for (const auto slot : batch) {
                push_completion(slot,
                                PrototypeCompletion{.request_id = slots[slot].request_id,
                                                    .error = ErrorCode::resource_exhausted,
                                                    .visible_through = visible_through,
                                                    .epoch = writer_epoch.load(std::memory_order_relaxed)});
            }
        } catch (...) {
            accepting.store(false, std::memory_order_release);
            for (const auto slot : batch) {
                push_completion(slot,
                                PrototypeCompletion{.request_id = slots[slot].request_id,
                                                    .error = ErrorCode::internal_error,
                                                    .visible_through = visible_through,
                                                    .epoch = writer_epoch.load(std::memory_order_relaxed)});
            }
        }
    }

    void writer_loop() noexcept {
        std::array<std::uint32_t, 32> batch{};
        while (accepting.load(std::memory_order_acquire) || !mutations.empty()) {
            std::size_t count = 0;
            while (count < batch.size() && mutations.try_pop(batch[count])) {
                ++count;
                mutation_pops.fetch_add(1U, std::memory_order_relaxed);
            }
            if (count == 0) {
                reclaim_generations();
                std::this_thread::yield();
                continue;
            }
            process_batch(std::span{batch}.first(count));
        }
        reclaim_generations();
        writer_done.store(true, std::memory_order_release);
    }

    const std::size_t maximum_value_bytes;
    const std::size_t merge_delta_entries;
    std::array<MutationSlot, kQueueCapacity> slots{};
    std::vector<std::byte> value_arena;
    std::array<std::uint32_t, kQueueCapacity> free_slots{};
    std::size_t free_count{};
    SpscRing<std::uint32_t, kQueueCapacity> mutations;
    SpscRing<InternalCompletion, kQueueCapacity> completions;
    std::array<GenerationSlot, kGenerationPoolCapacity> generations{};
    std::atomic<const PublicationDescriptor*> publication{};
    const PublicationDescriptor* local_publication{};
    std::size_t current_generation_slot{}; // Writer-only
    std::shared_ptr<const ImmutableReadIndex> base;
    MutableReadIndex mutable_delta;
    std::uint64_t visible_through{};
    std::jthread writer;
    std::atomic_bool accepting{true};
    std::atomic_bool writer_done{};
    std::atomic_bool invariant_failed{};
    std::atomic_uint64_t reader_turns{};
    std::atomic_uint64_t mutation_pushes{};
    std::atomic_uint64_t mutation_pops{};
    std::atomic_uint64_t completion_pushes{};
    std::atomic_uint64_t completion_pops{};
    std::atomic_uint64_t queue_full{};
    std::atomic_size_t mutation_high_watermark{};
    std::atomic_size_t completion_high_watermark{};
    std::atomic_size_t last_batch_size{};
    std::atomic_size_t maximum_batch_size{};
    std::uint64_t reader_gets{}; // Reader-owner thread only
    std::atomic_uint64_t publications{};
    std::atomic_uint64_t publication_records{};
    std::atomic_uint64_t publication_latency_ns{};
    std::atomic_uint64_t publication_bytes{};
    std::atomic_uint64_t ingress_value_bytes_copied{};
    std::atomic_uint64_t publication_backpressure{};
    std::atomic_uint64_t generation_retire_count{};
    std::atomic_uint64_t generation_retire_delay_ns{};
    std::atomic_size_t generation_live{};
    std::atomic_size_t generation_high_watermark{1};
    std::atomic_uint64_t writer_epoch{};
};

auto VolatileShardPairPrototype::create(const std::size_t maximum_value_bytes,
                                        const std::size_t merge_delta_entries)
    -> Result<std::unique_ptr<VolatileShardPairPrototype>> {
    if (maximum_value_bytes == 0 || merge_delta_entries == 0) {
        return fail(ErrorCode::invalid_argument, "paired prototype limits must be non-zero");
    }
    if (maximum_value_bytes > std::numeric_limits<std::size_t>::max() / kQueueCapacity) {
        return fail(ErrorCode::arithmetic_overflow, "paired prototype value arena overflows");
    }
    try {
        auto prototype = std::unique_ptr<VolatileShardPairPrototype>{
            new VolatileShardPairPrototype{std::make_unique<Impl>(maximum_value_bytes, merge_delta_entries)}};
        prototype->impl_->start();
        return prototype;
    } catch (const std::bad_alloc&) {
        return fail(ErrorCode::resource_exhausted, "paired prototype allocation failed");
    } catch (...) {
        return fail(ErrorCode::internal_error, "paired prototype startup failed");
    }
}

VolatileShardPairPrototype::VolatileShardPairPrototype(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

VolatileShardPairPrototype::~VolatileShardPairPrototype() {
    stop_and_drain();
}

auto VolatileShardPairPrototype::try_submit_put(const std::uint64_t request_id, const std::string_view key,
                                                const std::span<const std::byte> value,
                                                const std::uint64_t expire_at_ns) noexcept
    -> PrototypeSubmitStatus {
    return impl_->submit(PrototypeMutationKind::put, request_id, key, value, expire_at_ns);
}

auto VolatileShardPairPrototype::try_submit_erase(const std::uint64_t request_id,
                                                  const std::string_view key) noexcept
    -> PrototypeSubmitStatus {
    return impl_->submit(PrototypeMutationKind::erase, request_id, key, {}, 0);
}

auto VolatileShardPairPrototype::try_pop_completion() noexcept -> std::optional<PrototypeCompletion> {
    InternalCompletion completion;
    if (!impl_->completions.try_pop(completion)) {
        return std::nullopt;
    }
    impl_->free_slots[impl_->free_count++] = completion.slot;
    impl_->completion_pops.fetch_add(1U, std::memory_order_relaxed);
    return completion.completion;
}

void VolatileShardPairPrototype::adopt_publication() noexcept {
    // A turn boundary invalidates spans returned by the preceding turn. The
    // Writer waits for two such boundaries before reclaiming retired storage.
    impl_->reader_turns.fetch_add(1U, std::memory_order_seq_cst);
    impl_->local_publication = impl_->publication.load(std::memory_order_acquire);
}

auto VolatileShardPairPrototype::get(const std::string_view key, const std::uint64_t now_ns) noexcept
    -> std::optional<PrototypeRead> {
    ++impl_->reader_gets;
    const auto& generation = *impl_->local_publication->generation;
    const auto hash = hash_key(key);
    const auto* record = generation.delta.find(key, hash);
    if (record == nullptr) {
        record = generation.base->find(key, hash);
    }
    if (record == nullptr || record->tombstone ||
        (record->expire_at_ns != 0 && now_ns != 0 && record->expire_at_ns <= now_ns)) {
        return std::nullopt;
    }
    return PrototypeRead{.value = std::span<const std::byte>{record->value}, .sequence = record->sequence};
}

void VolatileShardPairPrototype::stop_and_drain() noexcept {
    if (!impl_) {
        return;
    }
    impl_->accepting.store(false, std::memory_order_release);
    // Complete a real adoption between two quiescent boundaries. This keeps
    // local_publication valid even when the Reader had not adopted the most
    // recent pre-shutdown generation. The pool is sized for every mutation
    // that admission could already have accepted.
    impl_->reader_turns.fetch_add(1U, std::memory_order_seq_cst);
    impl_->local_publication = impl_->publication.load(std::memory_order_acquire);
    impl_->reader_turns.fetch_add(1U, std::memory_order_seq_cst);
    if (impl_->writer.joinable()) {
        impl_->writer.join();
    }
    // The Writer may have published queued mutations during the join. Current
    // storage is never reclaimed, so this final acquire is lifetime-safe.
    impl_->local_publication = impl_->publication.load(std::memory_order_acquire);
}

auto VolatileShardPairPrototype::stats() const noexcept -> PrototypePairStats {
    return {.mutation_pushes = impl_->mutation_pushes.load(std::memory_order_relaxed),
            .mutation_pops = impl_->mutation_pops.load(std::memory_order_relaxed),
            .completion_pushes = impl_->completion_pushes.load(std::memory_order_relaxed),
            .completion_pops = impl_->completion_pops.load(std::memory_order_relaxed),
            .queue_full = impl_->queue_full.load(std::memory_order_relaxed),
            .mutation_queue_depth = impl_->mutations.size(),
            .mutation_queue_high_watermark = impl_->mutation_high_watermark.load(std::memory_order_relaxed),
            .completion_queue_depth = impl_->completions.size(),
            .completion_queue_high_watermark =
                impl_->completion_high_watermark.load(std::memory_order_relaxed),
            .last_writer_batch_size = impl_->last_batch_size.load(std::memory_order_relaxed),
            .maximum_writer_batch_size = impl_->maximum_batch_size.load(std::memory_order_relaxed),
            .reader_gets = impl_->reader_gets,
            .publications = impl_->publications.load(std::memory_order_relaxed),
            .publication_records = impl_->publication_records.load(std::memory_order_relaxed),
            .publication_latency_ns = impl_->publication_latency_ns.load(std::memory_order_relaxed),
            .publication_bytes = impl_->publication_bytes.load(std::memory_order_relaxed),
            .ingress_value_bytes_copied = impl_->ingress_value_bytes_copied.load(std::memory_order_relaxed),
            .publication_backpressure = impl_->publication_backpressure.load(std::memory_order_relaxed),
            .generation_live = impl_->generation_live.load(std::memory_order_relaxed),
            .generation_high_watermark = impl_->generation_high_watermark.load(std::memory_order_relaxed),
            .generation_retire_count = impl_->generation_retire_count.load(std::memory_order_relaxed),
            .generation_retire_delay_ns = impl_->generation_retire_delay_ns.load(std::memory_order_relaxed),
            .reader_turns = impl_->reader_turns.load(std::memory_order_relaxed),
            .reader_epoch = impl_->local_publication->epoch,
            .writer_epoch = impl_->writer_epoch.load(std::memory_order_relaxed),
            .visible_through = impl_->local_publication->visible_through};
}

} // namespace glyphastore::experimental
