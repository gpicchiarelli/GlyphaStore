#include "glyphastore/segment/record.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    std::size_t offset{};
    while (offset < size) {
        auto decoded =
            glyphastore::decode_record({reinterpret_cast<const std::byte*>(data + offset), size - offset});
        if (!decoded || decoded->encoded_size == 0) {
            break;
        }
        offset += decoded->encoded_size;
    }
    return 0;
}
