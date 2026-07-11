#include "glyphastore/index/index.hpp"

#include "glyphastore/segment/record.hpp"

#include <unordered_map>
#include <utility>

namespace glyphastore {
namespace {

struct LatestRecord {
    RecordRef ref;
    bool deleted{};
    bool expired{};
};

} // namespace

Index::Index() = default;
Index::Index(SwissTableIndex table) : table_(std::move(table)) {}
Index::~Index() = default;
Index::Index(Index&&) noexcept = default;
auto Index::operator=(Index&&) noexcept -> Index& = default;

auto Index::find(const std::string_view key) const -> std::optional<RecordRef> {
    return table_.find(key);
}

auto Index::insert_or_assign(const std::string_view key, RecordRef ref) -> IndexMutationResult {
    return table_.insert_or_assign(key, ref);
}

auto Index::erase(const std::string_view key) -> IndexMutationResult {
    return table_.erase(key);
}

void Index::reserve(const std::size_t count) {
    table_.reserve(count);
}

auto Index::entries() const -> std::vector<IndexEntry> {
    return table_.entries();
}

auto Index::stats() const noexcept -> IndexStats {
    return table_.stats();
}

auto Index::make_empty() const -> Index {
    return Index{table_.clone_empty()};
}

auto rebuild_index_from_segments(const std::span<const SegmentPtr> segments, const std::uint64_t now_ns)
    -> Result<RebuildResult> {
    std::unordered_map<std::string, LatestRecord> latest;
    RebuildStats stats{};

    for (const auto& segment : segments) {
        auto refs = segment->scan();
        if (!refs) {
            return unexpected(refs.error());
        }
        for (const auto& ref : *refs) {
            ++stats.records_scanned;
            auto record = segment->read(ref);
            if (!record) {
                return unexpected(record.error());
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
