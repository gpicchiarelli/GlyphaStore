#include "glyphastore/server/bounded_mpsc_queue.hpp"
#include "test.hpp"

#include <cstddef>
#include <thread>
#include <vector>

GLYPHA_TEST("bounded MPSC queue reports capacity without losing values") {
    glyphastore::server::BoundedMpscQueue<std::size_t> queue{2};
    GLYPHA_REQUIRE(queue.capacity() == 2);
    GLYPHA_REQUIRE(queue.try_push(std::size_t{10}));
    GLYPHA_REQUIRE(queue.try_push(std::size_t{20}));
    GLYPHA_REQUIRE(!queue.try_push(std::size_t{30}));
    const auto first = queue.try_pop();
    const auto second = queue.try_pop();
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(*first == 10);
    GLYPHA_REQUIRE(*second == 20);
    GLYPHA_REQUIRE(!queue.try_pop().has_value());
}

GLYPHA_TEST("bounded MPSC queue serializes concurrent producers") {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t values_per_producer = 1000;
    constexpr std::size_t value_count = producer_count * values_per_producer;
    glyphastore::server::BoundedMpscQueue<std::size_t> queue{64};
    std::vector<std::jthread> producers;
    producers.reserve(producer_count);
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::size_t offset = 0; offset < values_per_producer; ++offset) {
                const auto value = producer * values_per_producer + offset;
                while (!queue.try_push(std::size_t{value})) {
                    std::this_thread::yield();
                }
            }
        });
    }

    std::vector<bool> observed(value_count);
    std::size_t received{};
    while (received < value_count) {
        auto value = queue.try_pop();
        if (!value) {
            std::this_thread::yield();
            continue;
        }
        GLYPHA_REQUIRE(*value < observed.size());
        GLYPHA_REQUIRE(!observed[*value]);
        observed[*value] = true;
        ++received;
    }
    GLYPHA_REQUIRE(received == value_count);
}
