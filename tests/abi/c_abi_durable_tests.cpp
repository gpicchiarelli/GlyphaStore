#include "glyphastore/abi/glyphastore.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

auto view(const void* data, const std::size_t size) -> gs_bytes_view {
    return {static_cast<const std::uint8_t*>(data), size};
}

auto require(const bool condition, const char* expression, const int line) -> bool {
    if (!condition) {
        std::cerr << "requirement failed at line " << line << ": " << expression << '\n';
    }
    return condition;
}

#define REQUIRE(expression)                                                                          \
    do {                                                                                             \
        if (!require((expression), #expression, __LINE__)) {                                         \
            return 1;                                                                                \
        }                                                                                            \
    } while (false)

struct DirectoryGuard final {
    std::filesystem::path path;
    ~DirectoryGuard() {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
};

} // namespace

int main() {
    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    DirectoryGuard directory{
        std::filesystem::temp_directory_path() / ("glyphastore-c-abi-" + std::to_string(nonce))};
    const auto native_path = directory.path.string();

    gs_store_options options{};
    REQUIRE(gs_store_options_init(&options).code == GS_OK);
    options.storage_mode = GS_STORAGE_DURABLE_SYNC;
    options.durable_open_mode = GS_CREATE_NEW;
    options.data_directory = view(native_path.data(), native_path.size());

    gs_store* store = nullptr;
    REQUIRE(gs_store_open(&options, &store).code == GS_OK);
    const std::array<std::uint8_t, 3> key{0x61, 0x00, 0x62};
    const std::array<std::uint8_t, 5> value{9, 8, 7, 6, 5};
    const auto put = gs_store_put(store, view(key.data(), key.size()), view(value.data(), value.size()), 0);
    REQUIRE(put.status.code == GS_OK);
    REQUIRE(put.outcome == GS_MUTATION_COMMITTED);
    REQUIRE(gs_store_close(store).code == GS_OK);

    options.durable_open_mode = GS_OPEN_EXISTING;
    REQUIRE(gs_store_open(&options, &store).code == GS_OK);
    std::array<std::uint8_t, 5> output{};
    std::size_t required = 0;
    REQUIRE(gs_store_get(store, view(key.data(), key.size()), output.data(), output.size(), &required).code ==
            GS_OK);
    REQUIRE(required == value.size());
    REQUIRE(output == value);

    std::atomic<bool> failed{};
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (std::uint32_t thread_id = 0; thread_id < 4; ++thread_id) {
        threads.emplace_back([&, thread_id] {
            for (std::uint32_t iteration = 0; iteration < 50; ++iteration) {
                const std::array<std::uint8_t, 8> payload{
                    static_cast<std::uint8_t>(thread_id), static_cast<std::uint8_t>(iteration),
                    2, 3, 4, 5, 6, 7};
                const auto result =
                    gs_store_put(store, view(key.data(), key.size()), view(payload.data(), payload.size()), 0);
                if (result.status.code != GS_OK || result.outcome != GS_MUTATION_COMMITTED) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    REQUIRE(!failed.load(std::memory_order_relaxed));
    REQUIRE(gs_store_close(store).code == GS_OK);

    options.struct_size = static_cast<std::uint32_t>(sizeof(options) + 8U);
    REQUIRE(gs_store_open(&options, &store).code == GS_INCOMPATIBLE_ABI);
    REQUIRE(store == nullptr);
    return 0;
}
