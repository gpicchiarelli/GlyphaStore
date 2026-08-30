#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/daemon_log.hpp"
#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/pair_writer.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/server/socket.hpp"
#include "glyphastore/store/store.hpp"
#include "server_reactor_test_support.hpp"
#include "store/store_internal.hpp"
#include "test.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
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
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace glyphastore::test::server_reactor_support;
GLYPHA_TEST("server request timeout closes socket without cancelling admitted durable mutation") {
#if defined(__OpenBSD__)
    // Same OpenBSD qemu residual as the durable mutation queue deadline test: BlockingFileSync
    // is not reached reliably under the hosted VM; keep the gate on LibreSSL TLS + suite body.
    return;
#endif
    // Beyond client-semantics §6: daemon --request-timeout-ms may reset the TCP
    // connection while Store execution is already underway. Wire v2 has no cancel
    // frame; the admitted durable mutation must still commit (client sees transport
    // / indeterminate and reconciles via read).
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 1,
         .abuse = {.request_timeout_ms = 50}},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto mutation_socket = connect_to(server.port());
    GLYPHA_REQUIRE(mutation_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(mutation_socket, 0, 1));
    blocker.arm();
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 81,
        .key = bytes("timeout-survives"),
        .value = bytes("committed"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(mutation_socket, *put));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    bool peer_closed = false;
    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < close_deadline) {
        char byte{};
        const auto received = ::recv(mutation_socket, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(peer_closed);

    const auto probe_socket = connect_to(server.port());
    GLYPHA_REQUIRE(probe_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe_socket, 0, 1));
    const auto stats = probe_lifecycle(probe_socket, glyphastore::server::RequestOpcode::stats, 82);
    GLYPHA_REQUIRE(stats.has_value());
    const auto report = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(report.find("abuse_request_timeout_closed=") != std::string_view::npos);
    const auto marker = report.find("abuse_request_timeout_closed=");
    GLYPHA_REQUIRE(marker != std::string_view::npos);
    const auto count_begin = marker + std::string_view{"abuse_request_timeout_closed="}.size();
    GLYPHA_REQUIRE(count_begin < report.size());
    GLYPHA_REQUIRE(report[count_begin] != '0');

    blocker.release();
    // The Writer completes asynchronously after the timed-out peer is reset.
    // Poll visibility instead of assuming a scheduler-dependent fixed delay.
    bool mutation_visible = false;
    std::uint64_t request_id = 83;
    const auto visibility_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!mutation_visible && std::chrono::steady_clock::now() < visibility_deadline) {
        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = request_id++,
            .key = bytes("timeout-survives"),
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(probe_socket, *get));
        const auto get_frame = receive_response(probe_socket);
        const auto decoded = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(decoded.has_value());
        if (decoded->frame.status == glyphastore::server::ResponseStatus::ok) {
            GLYPHA_REQUIRE(text(decoded->frame.value) == "committed");
            mutation_visible = true;
            break;
        }
        GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::not_found);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    GLYPHA_REQUIRE(mutation_visible);

    static_cast<void>(::close(mutation_socket));
    static_cast<void>(::close(probe_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server shutdown drains an admitted durable mutation before Store close") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .durable_mutation_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 80,
        .key = bytes("drained-mutation"),
        .value = bytes("survives"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    std::atomic_bool join_finished{};
    bool join_succeeded{};
    server.request_stop();
    std::thread joiner{[&] {
        join_succeeded = server.join().has_value();
        join_finished.store(true, std::memory_order_release);
    }};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
    while (!join_finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    GLYPHA_REQUIRE(!join_finished.load(std::memory_order_acquire));
    blocker.release();
    joiner.join();
    GLYPHA_REQUIRE(join_succeeded);
    static_cast<void>(::close(socket));

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    const auto value = (*recovered)->get("drained-mutation");
    GLYPHA_REQUIRE(value.has_value());
    GLYPHA_REQUIRE(text(value->bytes) == "survives");
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server shutdown drain deadline abandons queued durable mutations") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .durable_mutation_queue_capacity = 4, .shutdown_drain_ms = 50},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    GLYPHA_REQUIRE(::setsockopt(second_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    blocker.arm();
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 200,
        .key = bytes("drain-committed"),
        .value = bytes("kept"),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 201,
        .key = bytes("drain-abandoned"),
        .value = bytes("dropped"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(second_socket, *second));
    const auto queued_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1};
    while (std::chrono::steady_clock::now() < queued_deadline) {
        const auto stats = server.pair_writer_stats();
        if (!stats.empty() && stats[0].queue_depth >= 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    {
        const auto stats = server.pair_writer_stats();
        GLYPHA_REQUIRE(!stats.empty());
        GLYPHA_REQUIRE(stats[0].queue_depth >= 1);
    }

    server.request_stop();
    std::optional<glyphastore::Status> joined;
    std::thread joiner{[&] { joined = server.join(); }};
    struct JoinGuard final {
        std::thread& thread;
        BlockingFileSync& blocker;
        bool released{};
        void release_and_join() {
            if (!released) {
                blocker.release();
                released = true;
            }
            if (thread.joinable()) {
                thread.join();
            }
        }
        ~JoinGuard() {
            release_and_join();
        }
    } join_guard{joiner, blocker};
    // Drain deadline abandons the queued PUT as wire OVERLOADED while the socket
    // is still live (before hard close). In-flight Store work stays blocked until
    // the sync hook releases.
    const auto abandoned_frame = receive_response(second_socket);
    GLYPHA_REQUIRE(!abandoned_frame.empty());
    const auto abandoned_response = glyphastore::server::decode_response(abandoned_frame);
    GLYPHA_REQUIRE(abandoned_response.has_value());
    GLYPHA_REQUIRE(abandoned_response->frame.request_id == 201);
    GLYPHA_REQUIRE(abandoned_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    join_guard.release_and_join();
    GLYPHA_REQUIRE(joined.has_value());
    GLYPHA_REQUIRE(!joined->has_value());
    GLYPHA_REQUIRE(joined->error().code == glyphastore::ErrorCode::unavailable);
    GLYPHA_REQUIRE(joined->error().message.find("shutdown drain deadline") != std::string::npos);
    {
        const auto stats = server.pair_writer_stats();
        GLYPHA_REQUIRE(!stats.empty());
        GLYPHA_REQUIRE(stats[0].expired_before_store >= 1);
    }
    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->get("drain-committed").has_value());
    const auto abandoned = (*recovered)->get("drain-abandoned");
    GLYPHA_REQUIRE(!abandoned.has_value());
    GLYPHA_REQUIRE(abandoned.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server shutdown drain deadline abandons durable_group coalescing hold") {
    // Writer has dequeued a PUT and is waiting for min_records — not in the MPSC
    // queue and not in Store. abandon_queued_mutations alone misses it; expire must
    // break coalescing so wire OVERLOADED flushes before hard close.
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .durable_mutation_queue_capacity = 4, .shutdown_drain_ms = 50},
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'000, .min_records = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    GLYPHA_REQUIRE(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 210,
        .key = bytes("coalesce-abandoned"),
        .value = bytes("never"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto held_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1};
    bool held = false;
    while (std::chrono::steady_clock::now() < held_deadline) {
        const auto stats = server.pair_writer_stats();
        if (!stats.empty() && stats[0].queue_depth == 0 && stats[0].payload_slots_in_use >= 1 &&
            stats[0].completed == 0) {
            held = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(held);

    server.request_stop();
    std::optional<glyphastore::Status> joined;
    std::thread joiner{[&] { joined = server.join(); }};
    struct JoinGuard final {
        std::thread& thread;
        ~JoinGuard() {
            if (thread.joinable()) {
                thread.join();
            }
        }
    } join_guard{joiner};
    const auto frame = receive_response(socket);
    GLYPHA_REQUIRE(!frame.empty());
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 210);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    if (joiner.joinable()) {
        joiner.join();
    }
    GLYPHA_REQUIRE(joined.has_value());
    GLYPHA_REQUIRE(!joined->has_value());
    GLYPHA_REQUIRE(joined->error().code == glyphastore::ErrorCode::unavailable);
    {
        const auto stats = server.pair_writer_stats();
        GLYPHA_REQUIRE(!stats.empty());
        GLYPHA_REQUIRE(stats[0].expired_before_store >= 1);
    }
    static_cast<void>(::close(socket));

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'000, .min_records = 2},
    });
    GLYPHA_REQUIRE(recovered.has_value());
    const auto missing = (*recovered)->get("coalesce-abandoned");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("durable_group coalescing obeys the oldest mutation queue deadline") {
    // The durable_group fill window is intentionally much longer than the
    // admission SLO. The Writer must close on the oldest queued mutation's
    // deadline instead of sleeping until group-max-wait and only then rejecting.
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 1,
         .durable_mutation_queue_capacity = 4,
         .durable_mutation_queue_wait_ms = 20},
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'500, .min_records = 2}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 211,
        .key = bytes("queue-deadline-before-group-deadline"),
        .value = bytes("never-enter-store"),
    });
    GLYPHA_REQUIRE(put.has_value());
    const auto started = std::chrono::steady_clock::now();
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto frame = receive_response(socket);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    GLYPHA_REQUIRE(!frame.empty());
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 211);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    // Old behavior waited roughly 1.5 s here. Keep a wide CI margin while
    // proving that the configured queue deadline, not the group deadline, won.
    GLYPHA_REQUIRE(elapsed < std::chrono::milliseconds{750});

    const auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].writer_batches == 1);
    GLYPHA_REQUIRE(stats[0].expired_before_store == 1);
    GLYPHA_REQUIRE(stats[0].writer_batch_queue_deadline_closes == 1);
    GLYPHA_REQUIRE(stats[0].writer_batch_durability_deadline_closes == 0);
    GLYPHA_REQUIRE(stats[0].maximum_queue_wait_ns >= 20'000'000U);
    GLYPHA_REQUIRE(stats[0].maximum_writer_batch_wait_ns > 0U);
    GLYPHA_REQUIRE(stats[0].maximum_writer_batch_wait_ns < 750'000'000U);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_group,
        .data_directory = path,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
        .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'500, .min_records = 2},
    });
    GLYPHA_REQUIRE(recovered.has_value());
    const auto missing = (*recovered)->get("queue-deadline-before-group-deadline");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

GLYPHA_TEST("server HEALTH and READY succeed while operational") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 2});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.ready());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 401);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(health->decoded.frame.value) == "GlyphaStore/live");
    const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 402);
    GLYPHA_REQUIRE(ready.has_value());
    GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ready->decoded.frame.value) == "GlyphaStore/ready");
    const auto stats = probe_lifecycle(socket, glyphastore::server::RequestOpcode::stats, 403);
    GLYPHA_REQUIRE(stats.has_value());
    GLYPHA_REQUIRE(stats->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto stats_text = text(stats->decoded.frame.value);
    GLYPHA_REQUIRE(stats_text.starts_with("GlyphaStore/stats\n"));
    GLYPHA_REQUIRE(stats_text.find("live=1\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("ready=1\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("version=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("connections_active=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_state=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("useful_compactions=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_compaction_pacing_delay_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_total_compaction_pacing_delay_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_compaction_pacing_sleep_count=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_compaction_pacing_burst_bytes=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_skips=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_consecutive_no_gain=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_skip_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_activation_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_no_gain_source_records_verified=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_total_no_gain_source_bytes_verified=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_no_gain_scans_suppressed=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_no_gain_retry_after_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_attempts=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_publication_wait_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_seal_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_create_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_manifest_publication_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_last_final_record_commit_ns=0\n") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("durable_rotation_maximum_total_ns=0\n") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_candidate_dead_byte_ratio_bp=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_foreground_latency_samples=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_foreground_p99_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_suspends=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_guard_active=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_deferral_age_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_latency_debt_overrides=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].total_writer_batch_wait_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].maximum_writer_batch_wait_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].writer_batch_durability_deadline_closes=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].writer_batch_queue_deadline_closes=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].sync_drain_turns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].sync_turn_splits=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].sync_async_fairness_turns=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].read_generation_base_record_storage_bytes=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("read_generation_spare_mapping_bytes=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].read_generation_base_record_mapped_storage_bytes=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].read_generation_base_lookup_storage_bytes=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].read_generation_delta_lookup_storage_bytes=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("lane[0].read_generation_current_allocated_lower_bound_bytes=") !=
                   std::string_view::npos);
    static_cast<void>(::close(socket));

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server READY fails during shutdown while live stays true") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 2});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.ready());

    {
        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 411);
        GLYPHA_REQUIRE(health.has_value());
        GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(health->decoded.frame.value) == "GlyphaStore/live");
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 412);
        GLYPHA_REQUIRE(ready.has_value());
        GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(text(ready->decoded.frame.value) == "GlyphaStore/ready");
        static_cast<void>(::close(socket));
    }

    server.request_stop();
    // Accept stops and idle peers are closed; readiness is fail-closed on the API immediately.
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(!server.ready());
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("server READY fails under maintenance emergency") {
    ServerTemporaryDirectory temporary;
    glyphastore::DurableResourceLimits limits{};
    limits.max_segment_count = 1;
    limits.max_store_bytes = 4ULL * glyphastore::kSegmentSizeBytes;
    limits.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    {
        auto seeded = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = temporary.store_path(),
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
            .durable_limits = limits,
            .maintenance = {.mode = glyphastore::MaintenanceMode::cooperative},
        });
        GLYPHA_REQUIRE(seeded.has_value());
        GLYPHA_REQUIRE((*seeded)->put("seed", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seeded)->close().has_value());
    }
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
                                             .durable_limits = limits,
                                             .maintenance = {
                                                 .mode = glyphastore::MaintenanceMode::background,
                                                 .min_eval_interval_ms = 60'000,
                                                 .max_eval_interval_ms = 60'000,
                                             }});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto emergency_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < emergency_deadline) {
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 422);
        GLYPHA_REQUIRE(ready.has_value());
        if (ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error) {
            GLYPHA_REQUIRE(!server.ready());
            const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 421);
            GLYPHA_REQUIRE(health.has_value());
            GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
            static_cast<void>(::close(socket));
            server.request_stop();
            GLYPHA_REQUIRE(server.join().has_value());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    GLYPHA_REQUIRE(false);
}

GLYPHA_TEST("server rejects durable PUT and ERASE under maintenance emergency on wire") {
    ServerTemporaryDirectory temporary;
    glyphastore::DurableResourceLimits limits{};
    limits.max_segment_count = 1;
    limits.max_store_bytes = 4ULL * glyphastore::kSegmentSizeBytes;
    limits.max_temporary_compaction_bytes = glyphastore::kSegmentSizeBytes;
    {
        auto seeded = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = temporary.store_path(),
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
            .durable_limits = limits,
            .maintenance = {.mode = glyphastore::MaintenanceMode::cooperative},
        });
        GLYPHA_REQUIRE(seeded.has_value());
        GLYPHA_REQUIRE((*seeded)->put("seed", bytes("value")).has_value());
        GLYPHA_REQUIRE((*seeded)->close().has_value());
    }
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
                                             .durable_limits = limits,
                                             .maintenance = {
                                                 .mode = glyphastore::MaintenanceMode::background,
                                                 .min_eval_interval_ms = 60'000,
                                                 .max_eval_interval_ms = 60'000,
                                             }});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto emergency_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < emergency_deadline) {
        const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 422);
        GLYPHA_REQUIRE(ready.has_value());
        if (ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(!server.ready());

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 423,
        .key = bytes("blocked-put"),
        .value = bytes("blocked"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 424,
        .key = bytes("seed"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::overloaded);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable wire ERASE persists through reopen") {
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 2},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto port = server.port();

    const auto socket = connect_to(port);
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 10,
        .key = bytes("erase-me"),
        .value = bytes("gone"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 11,
        .key = bytes("erase-me"),
    });
    GLYPHA_REQUIRE(erase.has_value());
    GLYPHA_REQUIRE(send_all(socket, *erase));
    const auto erase_frame = receive_response(socket);
    const auto erase_response = glyphastore::server::decode_response(erase_frame);
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2},
        {.worker_config = {.explicit_count = 1},
         .storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->start().has_value());
    const auto probe_socket = connect_to((*reopened)->port());
    GLYPHA_REQUIRE(probe_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe_socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 12,
        .key = bytes("erase-me"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(probe_socket, *get));
    const auto get_frame = receive_response(probe_socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::not_found);
    static_cast<void>(::close(probe_socket));
    (*reopened)->request_stop();
    GLYPHA_REQUIRE((*reopened)->join().has_value());
}

GLYPHA_TEST("server shutdown stops accepting and closes idle connections") {
    auto opened = glyphastore::server::Server::create({.port = 0, .maximum_connections = 4});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto port = server.port();

    const auto idle_socket = connect_to(port);
    GLYPHA_REQUIRE(idle_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(idle_socket, 0, 1));

    server.request_stop();
    const auto refuse_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool refused = false;
    while (std::chrono::steady_clock::now() < refuse_deadline) {
        const auto probe = connect_to(port);
        if (probe < 0) {
            refused = true;
            break;
        }
        static_cast<void>(::close(probe));
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(refused);

    const auto closed_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool peer_closed = false;
    while (std::chrono::steady_clock::now() < closed_deadline) {
        char byte{};
        const auto received = ::recv(idle_socket, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(peer_closed);
    GLYPHA_REQUIRE(server.join().has_value());
    static_cast<void>(::close(idle_socket));
}

GLYPHA_TEST("server shutdown drains in-flight durable response before closing connection") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .shutdown_drain_ms = 5'000},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    blocker.arm();
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 310,
        .key = bytes("connection-drain-key"),
        .value = bytes("flushed"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    server.request_stop();
    std::optional<glyphastore::Status> joined;
    std::thread joiner{[&] { joined = server.join(); }};
    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    blocker.release();

    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 310);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);

    char byte{};
    const auto closed_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    bool peer_closed = false;
    while (std::chrono::steady_clock::now() < closed_deadline) {
        const auto received = ::recv(socket, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    GLYPHA_REQUIRE(peer_closed);
    joiner.join();
    GLYPHA_REQUIRE(joined.has_value());
    GLYPHA_REQUIRE(joined->has_value());
    static_cast<void>(::close(socket));
}
