#include "glyphastore/index/index.hpp"

#include <algorithm>
#include <unordered_map>
#include <utility>

namespace glyphastore {
namespace {

class StandardIndexBackend final : public IndexBackend {
  public:
    auto find(std::string_view key) const -> std::optional<RecordRef> override {
        const auto it = entries_.find(std::string{key});
        if (it == entries_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    auto insert_or_assign(std::string_view key, RecordRef ref) -> IndexMutationResult override {
        auto [it, inserted] = entries_.try_emplace(std::string{key}, ref);
        if (inserted) {
            return {.inserted = true, .previous = std::nullopt};
        }
        const auto previous = it->second;
        it->second = ref;
        return {.inserted = false, .previous = previous};
    }

    auto erase(std::string_view key) -> IndexMutationResult override {
        const auto it = entries_.find(std::string{key});
        if (it == entries_.end()) {
            return {};
        }
        const auto previous = it->second;
        entries_.erase(it);
        return {.inserted = false, .previous = previous};
    }

    void reserve(std::size_t count) override {
        entries_.reserve(count);
    }

    auto entries() const -> std::vector<IndexEntry> override {
        std::vector<IndexEntry> result;
        result.reserve(entries_.size());
        for (const auto& [key, ref] : entries_) {
            result.push_back({key, ref});
        }
        return result;
    }

    auto stats() const noexcept -> IndexStats override {
        return {entries_.size(), entries_.bucket_count(), entries_.load_factor()};
    }

    auto clone_empty() const -> std::unique_ptr<IndexBackend> override {
        return std::make_unique<StandardIndexBackend>();
    }

  private:
    // Bootstrap backend only. The architecture targets a benchmark-selected flat,
    // open-addressing table inspired by SwissTable/F14.
    std::unordered_map<std::string, RecordRef> entries_;
};

struct LatestRecord {
    RecordRef ref;
    bool deleted{};
    bool expired{};
};

} // namespace

Index::Index() : backend_(std::make_unique<StandardIndexBackend>()) {}
Index::Index(std::unique_ptr<IndexBackend> backend) : backend_(std::move(backend)) {}
Index::~Index() = default;
Index::Index(Index&&) noexcept = default;
auto Index::operator=(Index&&) noexcept -> Index& = default;

auto Index::find(std::string_view key) const -> std::optional<RecordRef> {
    return backend_->find(key);
}
auto Index::insert_or_assign(std::string_view key, RecordRef ref) -> IndexMutationResult {
    return backend_->insert_or_assign(key, ref);
}
auto Index::erase(std::string_view key) -> IndexMutationResult {
    return backend_->erase(key);
}
void Index::reserve(std::size_t count) {
    backend_->reserve(count);
}
auto Index::entries() const -> std::vector<IndexEntry> {
    return backend_->entries();
}
auto Index::stats() const noexcept -> IndexStats {
    return backend_->stats();
}
auto Index::make_empty() const -> Index {
    return Index{backend_->clone_empty()};
}

auto rebuild_index_from_segments(std::span<const SegmentPtr> segments, std::uint64_t now_ns)
    -> Result<RebuildResult> {
    std::unordered_map<std::string, LatestRecord> latest;
    RebuildStats stats{};

    for (const auto& segment : segments) {
        auto refs = segment->scan();
        if (!refs) {
            return std::unexpected(refs.error());
        }
        for (const auto& ref : *refs) {
            ++stats.records_scanned;
            auto record = segment->read(ref);
            if (!record) {
                return std::unexpected(record.error());
            }
            const std::string key{record->key_string()};
            const auto current = latest.find(key);
            if (current != latest.end() && current->second.ref.sequence.value == record->sequence.value) {
                return fail(ErrorCode::sequence_conflict,
                            "duplicate sequence for the same key during rebuild");
            }
            if (current == latest.end() || current->second.ref.sequence.value < record->sequence.value) {
                latest.insert_or_assign(key, LatestRecord{
                                                 .ref = ref,
                                                 .deleted = record->opcode == Opcode::erase,
                                                 .expired = record->expired(now_ns),
                                             });
            }
        }
    }

    Index index;
    index.reserve(latest.size());
    for (const auto& [key, record] : latest) {
        if (record.deleted) {
            ++stats.tombstones;
        } else if (record.expired) {
            ++stats.expired;
        } else {
            index.insert_or_assign(key, record.ref);
            ++stats.records_visible;
        }
    }
    return RebuildResult{.index = std::move(index), .stats = stats};
}

} // namespace glyphastore
