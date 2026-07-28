#include "glyphastore/segment/record.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    static_cast<void>(glyphastore::decode_record({reinterpret_cast<const std::byte*>(data), size}));
    return 0;
}
