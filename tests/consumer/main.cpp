#include "glyphastore/store/store.hpp"

#if __has_include("glyphastore/index/index.hpp")
#error "Index headers must not be present in the installed API"
#endif
#if __has_include("glyphastore/segment/segment.hpp")
#error "Segment headers must not be present in the installed API"
#endif
#if __has_include("glyphastore/server/server.hpp")
#error "Server implementation headers must not be present in the installed API"
#endif
#if __has_include("glyphastore/worker/worker.hpp")
#error "Worker headers must not be present in the installed API"
#endif

#include <cstddef>
#include <span>
#include <string_view>
#include <vector>

namespace {

auto bytes(const std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

} // namespace

int main() {
    auto store = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    if (!store || !(**store).put("consumer", bytes("value"))) {
        return 1;
    }
    const auto value = (**store).get("consumer");
    return value && value->bytes == std::vector<std::byte>{bytes("value").begin(), bytes("value").end()} ? 0
                                                                                                         : 1;
}
