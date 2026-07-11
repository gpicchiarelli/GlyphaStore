#include "glyphastore/segment/segment.hpp"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > 4096) {
        return 0;
    }
    glyphastore::Segment segment{glyphastore::SegmentId{1}};
    const auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte*>(data), size};
    static_cast<void>(segment.append({
        .sequence = glyphastore::SequenceNumber{1},
        .key = bytes.first(size / 2),
        .value = bytes.subspan(size / 2),
    }));
    static_cast<void>(segment.scan());
    return 0;
}
