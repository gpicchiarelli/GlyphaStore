#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "server_reactor_test_support.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace glyphastore::test::server_reactor_support;

namespace {

[[nodiscard]] auto stats_counter(const std::string_view report, const std::string_view name)
    -> std::optional<std::uint64_t> {
    const auto marker = report.find(name);
    if (marker == std::string_view::npos || marker + name.size() >= report.size() ||
        report[marker + name.size()] != '=') {
        return std::nullopt;
    }
    const auto newline = report.find('\n', marker);
    if (newline == std::string_view::npos) {
        return std::nullopt;
    }
    const auto first = report.data() + marker + name.size() + 1U;
    const auto last = report.data() + newline;
    std::uint64_t value{};
    const auto parsed = std::from_chars(first, last, value);
    if (parsed.ec != std::errc{} || parsed.ptr != last) {
        return std::nullopt;
    }
    return value;
}

} // namespace

GLYPHA_TEST("durable cold GET pipeline remains contiguous to preserve read overlap") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    constexpr std::size_t kValueBytes = 64U * 1024U;
    std::vector<std::byte> first_value(kValueBytes, std::byte{0x31});
    std::vector<std::byte> second_value(kValueBytes, std::byte{0x72});
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("scatter-pipeline-a", first_value).has_value());
        GLYPHA_REQUIRE((*seed)->put("scatter-pipeline-b", second_value).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .maximum_output_bytes = 256U * 1024U},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto first_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 62,
        .key = bytes("scatter-pipeline-a"),
    });
    const auto second_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 63,
        .key = bytes("scatter-pipeline-b"),
    });
    GLYPHA_REQUIRE(first_get.has_value());
    GLYPHA_REQUIRE(second_get.has_value());
    std::vector<std::byte> pipeline;
    pipeline.reserve(first_get->size() + second_get->size());
    pipeline.insert(pipeline.end(), first_get->begin(), first_get->end());
    pipeline.insert(pipeline.end(), second_get->begin(), second_get->end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    const auto first_frame = receive_response(socket);
    const auto second_frame = receive_response(socket);
    const auto first = glyphastore::server::decode_response(first_frame, 256U * 1024U);
    const auto second = glyphastore::server::decode_response(second_frame, 256U * 1024U);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(first->frame.request_id == 62);
    GLYPHA_REQUIRE(second->frame.request_id == 63);
    GLYPHA_REQUIRE(std::ranges::equal(first->frame.value, first_value));
    GLYPHA_REQUIRE(std::ranges::equal(second->frame.value, second_value));

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->find("output_scatter_responses=0\n") != std::string::npos);
    GLYPHA_REQUIRE(report->find("output_scatter_completions=0\n") != std::string::npos);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("deep mutation pipeline keeps input compaction linear") {
    constexpr std::size_t kRequests = 128;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .maximum_input_bytes = 64U * 1024U});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    std::vector<std::byte> pipeline;
    pipeline.reserve(16U * 1024U);
    for (std::size_t index = 0; index < kRequests; ++index) {
        const auto key = std::string{"sliding-pipeline-"} + std::to_string(index);
        const auto value = std::string{"value-"} + std::to_string(index);
        const auto request = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 1'000U + index,
            .key = bytes(key),
            .value = bytes(value),
        });
        GLYPHA_REQUIRE(request.has_value());
        pipeline.insert(pipeline.end(), request->begin(), request->end());
    }
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    for (std::size_t index = 0; index < kRequests; ++index) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(response->frame.request_id == 1'000U + index);
    }

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    const auto compactions = stats_counter(*report, "input_buffer_compactions");
    const auto bytes_moved = stats_counter(*report, "input_buffer_bytes_moved");
    const auto output_compactions = stats_counter(*report, "output_buffer_compactions");
    const auto output_bytes_moved = stats_counter(*report, "output_buffer_bytes_moved");
    GLYPHA_REQUIRE(compactions.has_value());
    GLYPHA_REQUIRE(bytes_moved.has_value());
    GLYPHA_REQUIRE(output_compactions.has_value());
    GLYPHA_REQUIRE(output_bytes_moved.has_value());
    const auto typed_stats = server.reactor_buffer_stats();
    GLYPHA_REQUIRE(typed_stats.input_compactions == *compactions);
    GLYPHA_REQUIRE(typed_stats.input_bytes_moved == *bytes_moved);
    GLYPHA_REQUIRE(typed_stats.output_compactions == *output_compactions);
    GLYPHA_REQUIRE(typed_stats.output_bytes_moved == *output_bytes_moved);
    // Capacity growth can trigger a bounded append-time compaction when TCP splits
    // the pipeline. It must never return to one suffix memmove per completion.
    GLYPHA_REQUIRE(*compactions < kRequests / 4U);
    GLYPHA_REQUIRE(*bytes_moved <= pipeline.size() * 2U);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("late cold-read completion cannot target a reused connection slot") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("stale-read", bytes("old-value")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .disk_read_thread_count = 1, .disk_read_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto old_socket = connect_to(server.port());
    GLYPHA_REQUIRE(old_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(old_socket, 0, 1));
    blocker.arm();
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 50,
        .key = bytes("stale-read"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(old_socket, *get));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    linger reset_on_close{.l_onoff = 1, .l_linger = 0};
    GLYPHA_REQUIRE(::setsockopt(old_socket, SOL_SOCKET, SO_LINGER, &reset_on_close, sizeof(reset_on_close)) ==
                   0);
    static_cast<void>(::close(old_socket));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (server.active_connections_per_executor()[0] != 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const auto old_connection_closed = server.active_connections_per_executor()[0] == 0;
    if (!old_connection_closed) {
        blocker.release();
    }
    GLYPHA_REQUIRE(old_connection_closed);

    // maximum_connections=1 forces the next connection to reuse the same slot
    // with a new generation while the old pinned read is still in flight.
    const auto reused_socket = connect_to(server.port());
    if (reused_socket < 0) {
        blocker.release();
    }
    GLYPHA_REQUIRE(reused_socket >= 0);
    const auto reused_initialized = initialize_and_bind(reused_socket, 0, 1);
    if (!reused_initialized) {
        blocker.release();
    }
    GLYPHA_REQUIRE(reused_initialized);
    blocker.release();
    GLYPHA_REQUIRE(blocker.wait_until_finished());

    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 51,
        .value = bytes("new-generation"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(reused_socket, *ping));
    const auto frame = receive_response(reused_socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 51);
    GLYPHA_REQUIRE(text(response->frame.value) == "new-generation");

    static_cast<void>(::close(reused_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server shutdown drains an in-flight pinned cold read before Store close") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("shutdown-read", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 60,
        .key = bytes("shutdown-read"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    std::atomic_bool join_finished{};
    bool join_succeeded{};
    server.request_stop();
    std::thread joiner{[&] {
        join_succeeded = server.join().has_value();
        join_finished.store(true, std::memory_order_release);
    }};
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (!join_finished.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < drain_deadline) {
        std::this_thread::yield();
    }
    const auto waited_for_read = !join_finished.load(std::memory_order_acquire);
    blocker.release();
    joiner.join();
    GLYPHA_REQUIRE(waited_for_read);
    GLYPHA_REQUIRE(join_succeeded);
    static_cast<void>(::close(socket));
}

GLYPHA_TEST("TCP reactor handles partial and pipelined protocol frames") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 16});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 10,
        .value = bytes("first"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data(), 3}));
    GLYPHA_REQUIRE(send_all(socket, std::span<const std::byte>{ping->data() + 3, ping->size() - 3}));

    const auto first_frame = receive_response(socket);
    GLYPHA_REQUIRE(!first_frame.empty());
    const auto first = glyphastore::server::decode_response(first_frame);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(first->frame.request_id == 10);
    GLYPHA_REQUIRE(text(first->frame.value) == "first");

    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 11,
    });
    const auto second_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 12,
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(second_ping.has_value());
    std::vector<std::byte> pipelined;
    pipelined.insert(pipelined.end(), init->begin(), init->end());
    pipelined.insert(pipelined.end(), second_ping->begin(), second_ping->end());
    GLYPHA_REQUIRE(send_all(socket, pipelined));

    const auto init_frame = receive_response(socket);
    const auto second_ping_frame = receive_response(socket);
    const auto init_response = glyphastore::server::decode_response(init_frame);
    const auto ping_response = glyphastore::server::decode_response(second_ping_frame);
    GLYPHA_REQUIRE(init_response.has_value());
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(init_response->frame.request_id == 11);
    GLYPHA_REQUIRE(text(init_response->frame.value) == "GlyphaStore/2");
    GLYPHA_REQUIRE(init_response->frame.worker_count == 1);
    GLYPHA_REQUIRE(ping_response->frame.request_id == 12);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "second");
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 14,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(socket, *bind));
    const auto bind_frame = receive_response(socket);
    const auto bind_response = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bind_response.has_value());
    GLYPHA_REQUIRE(bind_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(bind_response->frame.owner_worker == 0);

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 20,
        .key = bytes("network-key"),
        .value = bytes("network-value"),
    });
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 21,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(get.has_value());
    std::vector<std::byte> store_pipeline;
    store_pipeline.insert(store_pipeline.end(), put->begin(), put->end());
    store_pipeline.insert(store_pipeline.end(), get->begin(), get->end());
    GLYPHA_REQUIRE(send_all(socket, store_pipeline));
    const auto put_frame = receive_response(socket);
    const auto get_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "network-value");

    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 22,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 23,
        .key = bytes("network-key"),
    });
    GLYPHA_REQUIRE(missing_get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *missing_get));
    const auto missing_frame = receive_response(socket);
    const auto missing_response = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(missing_response.has_value());
    GLYPHA_REQUIRE(missing_response->frame.status == glyphastore::server::ResponseStatus::not_found);

    std::vector<std::byte> large_payload(1536U * 1024U, std::byte{0x5A});
    const auto large_ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 13,
        .value = large_payload,
    });
    GLYPHA_REQUIRE(large_ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *large_ping));
    const auto large_response_frame = receive_response(socket);
    const auto large_response = glyphastore::server::decode_response(large_response_frame);
    GLYPHA_REQUIRE(large_response.has_value());
    GLYPHA_REQUIRE(large_response->frame.request_id == 13);
    GLYPHA_REQUIRE(large_response->frame.value.size() == large_payload.size());
    GLYPHA_REQUIRE(std::ranges::equal(large_response->frame.value, large_payload));

    static_cast<void>(::close(socket));

    const auto half_closed_socket = connect_to(server.port());
    GLYPHA_REQUIRE(half_closed_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(half_closed_socket, 0, 1));
    const auto final_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 30,
        .key = bytes("half-close-key"),
        .value = bytes("half-close-value"),
    });
    GLYPHA_REQUIRE(final_put.has_value());
    GLYPHA_REQUIRE(send_all(half_closed_socket, *final_put));
    GLYPHA_REQUIRE(::shutdown(half_closed_socket, SHUT_WR) == 0);
    const auto final_frame = receive_response(half_closed_socket);
    const auto final_response = glyphastore::server::decode_response(final_frame);
    GLYPHA_REQUIRE(final_response.has_value());
    GLYPHA_REQUIRE(final_response->frame.request_id == 30);
    GLYPHA_REQUIRE(final_response->frame.status == glyphastore::server::ResponseStatus::ok);
    static_cast<void>(::close(half_closed_socket));

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("half-close drains pipelined mutations before teardown") {
    // SHUT_WR means done-sending: frames already received must still run and ACK.
    // write_ready used to close on peer_read_closed before draining residual input.
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put1 = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 40,
        .key = bytes("half-close-a"),
        .value = bytes("one"),
    });
    const auto put2 = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 41,
        .key = bytes("half-close-b"),
        .value = bytes("two"),
    });
    GLYPHA_REQUIRE(put1.has_value());
    GLYPHA_REQUIRE(put2.has_value());
    std::vector<std::byte> pipeline(put1->begin(), put1->end());
    pipeline.insert(pipeline.end(), put2->begin(), put2->end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));
    GLYPHA_REQUIRE(::shutdown(socket, SHUT_WR) == 0);

    const auto frame1 = receive_response(socket);
    const auto frame2 = receive_response(socket);
    const auto ack1 = glyphastore::server::decode_response(frame1);
    const auto ack2 = glyphastore::server::decode_response(frame2);
    GLYPHA_REQUIRE(ack1.has_value());
    GLYPHA_REQUIRE(ack2.has_value());
    GLYPHA_REQUIRE(ack1->frame.request_id == 40);
    GLYPHA_REQUIRE(ack2->frame.request_id == 41);
    GLYPHA_REQUIRE(ack1->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(ack2->frame.status == glyphastore::server::ResponseStatus::ok);

    static_cast<void>(::close(socket));

    const auto probe = connect_to(server.port());
    GLYPHA_REQUIRE(probe >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe, 0, 1));
    for (const auto& [id, key, want] :
         {std::tuple{50ULL, "half-close-a", "one"}, std::tuple{51ULL, "half-close-b", "two"}}) {
        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = id,
            .key = bytes(key),
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(probe, *get));
        const auto get_frame = receive_response(probe);
        const auto got = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(got.has_value());
        GLYPHA_REQUIRE(got->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(got->frame.value) == want);
    }
    static_cast<void>(::close(probe));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("bound Reactor redirects wrong owners without forwarding") {
    GLYPHA_REQUIRE(glyphastore::route_worker("bounded-key-0", 2) == 1);
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 2, .reuse_port = false});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto wrong_socket = connect_to(server.port());
    GLYPHA_REQUIRE(wrong_socket >= 0);
    const auto premature_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 98,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(premature_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *premature_bind));
    const auto premature_bind_frame = receive_response(wrong_socket);
    const auto premature = glyphastore::server::decode_response(premature_bind_frame);
    GLYPHA_REQUIRE(premature.has_value());
    GLYPHA_REQUIRE(premature->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(premature->frame.owner_worker == glyphastore::server::kNoWorker);

    const auto unbound_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 99,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(unbound_get.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *unbound_get));
    const auto unbound_frame = receive_response(wrong_socket);
    const auto unbound = glyphastore::server::decode_response(unbound_frame);
    GLYPHA_REQUIRE(unbound.has_value());
    GLYPHA_REQUIRE(unbound->frame.status == glyphastore::server::ResponseStatus::not_bound);
    GLYPHA_REQUIRE(unbound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(initialize_and_bind(wrong_socket, 0, 2));
    const auto repeated_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 100,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(repeated_bind.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *repeated_bind));
    const auto repeated_bind_frame = receive_response(wrong_socket);
    const auto repeated = glyphastore::server::decode_response(repeated_bind_frame);
    GLYPHA_REQUIRE(repeated.has_value());
    GLYPHA_REQUIRE(repeated->frame.status == glyphastore::server::ResponseStatus::invalid_request);
    GLYPHA_REQUIRE(repeated->frame.owner_worker == 0);

    const auto misplaced_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 101,
        .key = bytes("bounded-key-0"),
        .value = bytes("stored"),
    });
    GLYPHA_REQUIRE(misplaced_put.has_value());
    GLYPHA_REQUIRE(send_all(wrong_socket, *misplaced_put));
    const auto redirect_frame = receive_response(wrong_socket);
    const auto redirect = glyphastore::server::decode_response(redirect_frame);
    GLYPHA_REQUIRE(redirect.has_value());
    GLYPHA_REQUIRE(redirect->frame.status == glyphastore::server::ResponseStatus::wrong_owner);
    GLYPHA_REQUIRE(redirect->frame.owner_worker == 1);
    static_cast<void>(::close(wrong_socket));

    const auto owner_socket = connect_to(server.port());
    GLYPHA_REQUIRE(owner_socket >= 0);
    const auto owner_init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 101,
    });
    const auto owner_bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 102,
        .target_worker = 1,
    });
    const auto missing_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 103,
        .key = bytes("bounded-key-0"),
    });
    GLYPHA_REQUIRE(owner_init.has_value());
    GLYPHA_REQUIRE(owner_bind.has_value());
    GLYPHA_REQUIRE(missing_get.has_value());
    std::vector<std::byte> bind_pipeline;
    bind_pipeline.insert(bind_pipeline.end(), owner_init->begin(), owner_init->end());
    bind_pipeline.insert(bind_pipeline.end(), owner_bind->begin(), owner_bind->end());
    bind_pipeline.insert(bind_pipeline.end(), missing_get->begin(), missing_get->end());
    GLYPHA_REQUIRE(send_all(owner_socket, bind_pipeline));
    const auto owner_init_frame = receive_response(owner_socket);
    const auto owner_bind_frame = receive_response(owner_socket);
    const auto missing_frame = receive_response(owner_socket);
    const auto owner_initialized = glyphastore::server::decode_response(owner_init_frame);
    const auto owner_bound = glyphastore::server::decode_response(owner_bind_frame);
    const auto missing = glyphastore::server::decode_response(missing_frame);
    GLYPHA_REQUIRE(owner_initialized.has_value());
    GLYPHA_REQUIRE(owner_initialized->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound.has_value());
    GLYPHA_REQUIRE(owner_bound->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(owner_bound->frame.owner_worker == 1);
    GLYPHA_REQUIRE(missing.has_value());
    GLYPHA_REQUIRE(missing->frame.status == glyphastore::server::ResponseStatus::not_found);
    static_cast<void>(::close(owner_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("multi-Reactor executors distribute connections and share one Store") {
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 16, .worker_count = 2, .executor_affinity = true});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.executor_count() == 2);
    GLYPHA_REQUIRE(server.start().has_value());

    for (std::uint64_t request = 0; request < 16; ++request) {
        const auto key = std::string{"reuse-key-"} + std::to_string(request);
        const auto value = std::string{"reuse-value-"} + std::to_string(request);
        const auto owner = static_cast<std::uint32_t>(glyphastore::route_worker(key, 2));
        const auto writer = connect_to(server.port());
        GLYPHA_REQUIRE(writer >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(writer, owner, 2));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = request * 2U,
            .key = bytes(key),
            .value = bytes(value),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(writer, *put));
        const auto put_frame = receive_response(writer);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(writer));

        const auto reader = connect_to(server.port());
        GLYPHA_REQUIRE(reader >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(reader, owner, 2));
        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = request * 2U + 1U,
            .key = bytes(key),
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(reader, *get));
        const auto get_frame = receive_response(reader);
        const auto get_response = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(get_response.has_value());
        GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(get_response->frame.value) == value);
        static_cast<void>(::close(reader));
    }

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    const auto adopted = server.adopted_connections_per_executor();
    GLYPHA_REQUIRE(adopted.size() == 2);
    GLYPHA_REQUIRE(adopted[0] > 0);
    GLYPHA_REQUIRE(adopted[1] > 0);
    const auto affinity = server.executor_affinity_results();
    GLYPHA_REQUIRE(affinity.size() == 2);
    GLYPHA_REQUIRE(affinity[0].mode != glyphastore::server::ExecutorAffinityMode::disabled);
    GLYPHA_REQUIRE(affinity[1].mode != glyphastore::server::ExecutorAffinityMode::disabled);
}

GLYPHA_TEST("server connection rate limit returns overloaded") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 8,
        .worker_count = 1,
        .abuse =
            {
                .connection_max_requests_per_sec = 2,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    // INIT + BIND consume the two-request budget; the next PING must be overloaded.
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 99,
        .value = bytes("x"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *ping));
    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    const auto stats = probe_lifecycle(socket, glyphastore::server::RequestOpcode::stats, 100);
    GLYPHA_REQUIRE(stats.has_value());
    GLYPHA_REQUIRE(stats->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(report.find("abuse_connection_rate_rejected=") != std::string_view::npos);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("connection rate limit OVERLOADED survives trailing decode failure") {
    // INIT+BIND exhaust the budget; PING queues OVERLOADED then a bad-version frame
    // used to make read_ready close before flush — silent EOF, no rate-limit signal.
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 8,
        .worker_count = 1,
        .abuse =
            {
                .connection_max_requests_per_sec = 2,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 99,
        .value = bytes("x"),
    });
    GLYPHA_REQUIRE(ping.has_value());
    std::vector<std::byte> pipeline(ping->begin(), ping->end());
    std::array<std::byte, glyphastore::server::kRequestHeaderBytes> bad{};
    bad[0] = std::byte{static_cast<unsigned char>(glyphastore::server::kRequestHeaderBytes)};
    bad[4] = std::byte{0xff}; // unsupported version
    pipeline.insert(pipeline.end(), bad.begin(), bad.end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 99);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server idle timeout closes quiet connections") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
        .abuse =
            {
                .idle_timeout_ms = 50,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    bool closed = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        char byte{};
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(closed);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server request timeout closes in-flight cold read and cancels") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("timeout-read", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 1,
         .disk_read_thread_count = 1,
         .abuse = {.request_timeout_ms = 50}},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 71,
        .key = bytes("timeout-read"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    bool closed = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        char byte{};
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(closed);

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    const auto marker = report->find("abuse_request_timeout_closed=");
    GLYPHA_REQUIRE(marker != std::string::npos);
    const auto value_start = marker + std::strlen("abuse_request_timeout_closed=");
    GLYPHA_REQUIRE(value_start < report->size());
    GLYPHA_REQUIRE((*report)[value_start] != '0');

    blocker.release();
    GLYPHA_REQUIRE(blocker.wait_until_finished());

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server request timeout closes partial request frames") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
        .abuse =
            {
                .request_timeout_ms = 50,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    // Incomplete frame starts the partial-assembly budget; peer must be closed.
    constexpr std::array<std::byte, 8> partial{
        std::byte{0x28}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        std::byte{0x02}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
    };
    GLYPHA_REQUIRE(send_all(socket, partial));

    bool closed = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < deadline) {
        char byte{};
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(closed);

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->find("abuse_request_timeout_closed=") != std::string::npos);
    const auto marker = report->find("abuse_request_timeout_closed=");
    const auto value_start = marker + std::strlen("abuse_request_timeout_closed=");
    GLYPHA_REQUIRE(value_start < report->size());
    GLYPHA_REQUIRE((*report)[value_start] != '0');

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("partial request timeout drains prior decided response before close") {
    // A small server send buffer makes the large PING retain decided user-space
    // bytes when a trailing partial frame times out. Keep the client receive window
    // normal so completion does not depend on platform TCP retransmit intervals.
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
        .accepted_socket_send_buffer_bytes = 4U * 1024U,
        .abuse =
            {
                .request_timeout_ms = 80,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    constexpr std::size_t kPayload = 64U * 1024U;
    std::vector<std::byte> payload(kPayload, std::byte{0x5a});
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 55,
        .value = payload,
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *ping));

    // Incomplete follow-up starts the partial-assembly budget while PING output is
    // still bounded by the accepted socket's deliberately small send buffer.
    constexpr std::array<std::byte, 8> partial{
        std::byte{0x28}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        std::byte{0x02}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
    };
    GLYPHA_REQUIRE(send_all(socket, partial));
    std::this_thread::sleep_for(std::chrono::milliseconds{150});

    std::vector<std::byte> received;
    received.reserve(kPayload + 64);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (std::chrono::steady_clock::now() < deadline) {
        std::array<std::byte, 16U * 1024U> chunk{};
        const auto n = ::recv(socket, chunk.data(), chunk.size(), 0);
        if (n > 0) {
            received.insert(received.end(), chunk.begin(), chunk.begin() + n);
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(received.size() >= glyphastore::server::kResponseHeaderBytes);
    const auto decoded = glyphastore::server::decode_response(received);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->complete);
    GLYPHA_REQUIRE(decoded->frame.request_id == 55);
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(decoded->frame.value.size() == kPayload);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("half-close with pending large response drains before poller teardown") {
    // Large PING + small server SO_SNDBUF leaves decided bytes queued; SHUT_WR must
    // still deliver them. Hangup is handled before raw poller error so co-reported
    // error|hangup cannot hard-close and discard the remainder after EAGAIN.
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
        .accepted_socket_send_buffer_bytes = 4U * 1024U,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    constexpr std::size_t kPayload = 64U * 1024U;
    std::vector<std::byte> payload(kPayload, std::byte{0x5c});
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 59,
        .value = payload,
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *ping));

    bool saw_response = false;
    const auto peek_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < peek_deadline) {
        char byte{};
        const auto peeked = ::recv(socket, &byte, 1, MSG_PEEK);
        if (peeked > 0) {
            saw_response = true;
            break;
        }
        if (peeked == 0) {
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(saw_response);
    GLYPHA_REQUIRE(::shutdown(socket, SHUT_WR) == 0);

    std::vector<std::byte> received;
    received.reserve(kPayload + 64);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (std::chrono::steady_clock::now() < deadline) {
        std::array<std::byte, 16U * 1024U> chunk{};
        const auto n = ::recv(socket, chunk.data(), chunk.size(), 0);
        if (n > 0) {
            received.insert(received.end(), chunk.begin(), chunk.begin() + n);
            if (received.size() >= glyphastore::server::kResponseHeaderBytes) {
                const auto decoded = glyphastore::server::decode_response(received);
                if (decoded && decoded->complete) {
                    break;
                }
            }
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(received.size() >= glyphastore::server::kResponseHeaderBytes);
    const auto decoded = glyphastore::server::decode_response(received);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->complete);
    GLYPHA_REQUIRE(decoded->frame.request_id == 59);
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(decoded->frame.value.size() == kPayload);
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.healthy());

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("input buffer allocation failure drains decided response without daemon fail-stop") {
    // Large PING fills the peer receive window so decided bytes remain queued.
    // A follow-up that fails input-buffer growth must drain that ACK, not escalate
    // to executor fail-stop or hard-close that discards it.
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 1,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    int rcvbuf = 4 * 1024;
    GLYPHA_REQUIRE(::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) == 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    constexpr std::size_t kPayload = 64U * 1024U;
    std::vector<std::byte> payload(kPayload, std::byte{0x5b});
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 56,
        .value = payload,
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all(socket, *ping));
    // Wait until decided PING bytes are visible — response is queued server-side and
    // may still be blocked on the small client receive window.
    bool saw_response = false;
    const auto peek_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < peek_deadline) {
        char byte{};
        const auto peeked = ::recv(socket, &byte, 1, MSG_PEEK);
        if (peeked > 0) {
            saw_response = true;
            break;
        }
        if (peeked == 0) {
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(saw_response);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::input_buffer);
    const auto follow_up = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 57,
        .value = bytes("follow"),
    });
    GLYPHA_REQUIRE(follow_up.has_value());
    GLYPHA_REQUIRE(send_all(socket, *follow_up));

    std::vector<std::byte> received;
    received.reserve(kPayload + 64);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < deadline) {
        std::array<std::byte, 16U * 1024U> chunk{};
        const auto n = ::recv(socket, chunk.data(), chunk.size(), 0);
        if (n > 0) {
            received.insert(received.end(), chunk.begin(), chunk.begin() + n);
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(received.size() >= glyphastore::server::kResponseHeaderBytes);
    const auto decoded = glyphastore::server::decode_response(received);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->complete);
    GLYPHA_REQUIRE(decoded->frame.request_id == 56);
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(decoded->frame.value.size() == kPayload);
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.healthy());

    const auto probe = connect_to(server.port());
    GLYPHA_REQUIRE(probe >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe, 0, 1));
    const auto health = probe_lifecycle(probe, glyphastore::server::RequestOpcode::health, 58);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);

    static_cast<void>(::close(socket));
    static_cast<void>(::close(probe));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
#endif

GLYPHA_TEST("server accept rate limit drops excess handshakes") {
    auto opened = glyphastore::server::Server::create({
        .port = 0,
        .maximum_connections = 32,
        .worker_count = 1,
        .abuse =
            {
                .max_accepts_per_sec = 1,
            },
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first = connect_to(server.port());
    GLYPHA_REQUIRE(first >= 0);
    // Give the reactor time to accept the first connection against the budget.
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    const auto second = connect_to(server.port());
    GLYPHA_REQUIRE(second >= 0);

    // First peer can still speak; the second accept is dropped so INIT should fail.
    GLYPHA_REQUIRE(initialize_and_bind(first, 0, 1));
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    static_cast<void>(send_all(second, *init));
    const auto frame = receive_response(second);
    GLYPHA_REQUIRE(frame.empty());

    const auto stats = probe_lifecycle(first, glyphastore::server::RequestOpcode::stats, 7);
    GLYPHA_REQUIRE(stats.has_value());
    const auto report = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(report.find("abuse_accepts_rejected=") != std::string_view::npos);

    static_cast<void>(::close(first));
    static_cast<void>(::close(second));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
