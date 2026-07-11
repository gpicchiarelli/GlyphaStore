#include "glyphastore/index/index.hpp"
#include "glyphastore/segment/segment.hpp"

#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>

int main() {
    constexpr std::size_t operations = 200'000;
    glyphastore::Index index;
    index.reserve(operations);
    const auto started = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < operations; ++i) {
        const auto key = std::to_string(i);
        index.insert_or_assign(
            key, glyphastore::RecordRef{glyphastore::SegmentId{1}, glyphastore::RecordOffset{0},
                                        glyphastore::RecordSize{64}, glyphastore::SequenceNumber{i},
                                        glyphastore::GenerationId{1}});
    }
    std::size_t hits{};
    for (std::size_t i = 0; i < operations; ++i) {
        hits += index.find(std::to_string(i)).has_value() ? 1U : 0U;
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    const auto total = static_cast<double>(operations * 2);
    std::cout << "bootstrap_index operations=" << static_cast<std::size_t>(total) << " hits=" << hits
              << " seconds=" << elapsed << " ops_per_second=" << total / elapsed
              << " ns_per_op=" << elapsed * 1.0e9 / total << '\n';
}
