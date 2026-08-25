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
    // Allow the Writer to finish after the peer was already reset.
    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 83,
        .key = bytes("timeout-survives"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(probe_socket, *get));
    const auto get_frame = receive_response(probe_socket);
    const auto decoded = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(decoded->frame.value) == "committed");

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
    GLYPHA_REQUIRE(stats_text.find("maintenance_skips=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_consecutive_no_gain=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_skip_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_activation_reason=") != std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_last_no_gain_source_records_verified=") !=
                   std::string_view::npos);
    GLYPHA_REQUIRE(stats_text.find("maintenance_total_no_gain_source_bytes_verified=") !=
                   std::string_view::npos);
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

GLYPHA_TEST("paired Writer closes strict durable groups without concurrent shard mutators") {
    ServerTemporaryDirectory temporary;
    GroupBatchObserver observer;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 2, .durable_mutation_queue_capacity = 4},
        {.storage_mode = glyphastore::StorageMode::durable_group,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .durable_group = {.max_records = 2, .max_bytes = 65'536, .max_wait_ms = 1'000, .min_records = 2},
         .filesystem_hooks = {.context = &observer,
                              .before = &GroupBatchObserver::before,
                              .file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
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
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 90,
        .key = bytes("group-first"),
        .value = bytes("first"),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 91,
        .key = bytes("group-second"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    blocker.arm();
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(send_all(second_socket, *second));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    const auto in_flight_batch_stats = server.durable_batch_stats();
    GLYPHA_REQUIRE(in_flight_batch_stats.size() == 1);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].pending_records == 2);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].flush_attempts == 1);
    GLYPHA_REQUIRE(in_flight_batch_stats[0].committed_batches == 0);
    blocker.release();

    const auto first_frame = receive_response(first_socket);
    const auto second_frame = receive_response(second_socket);
    const auto first_response = glyphastore::server::decode_response(first_frame);
    const auto second_response = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first_response.has_value());
    GLYPHA_REQUIRE(second_response.has_value());
    GLYPHA_REQUIRE(first_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(second_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(observer.maximum_writes_before_sync() == 2);
    GLYPHA_REQUIRE(observer.sync_count() == 1);
    const auto batch_stats = server.durable_batch_stats();
    GLYPHA_REQUIRE(batch_stats.size() == 1);
    GLYPHA_REQUIRE(batch_stats[0].enabled);
    GLYPHA_REQUIRE(batch_stats[0].pending_records == 0);
    GLYPHA_REQUIRE(batch_stats[0].pending_bytes == 0);
    GLYPHA_REQUIRE(batch_stats[0].current_record_target == 2);
    GLYPHA_REQUIRE(batch_stats[0].flush_attempts == 1);
    GLYPHA_REQUIRE(batch_stats[0].committed_batches == 1);
    GLYPHA_REQUIRE(batch_stats[0].failed_batches == 0);
    GLYPHA_REQUIRE(batch_stats[0].committed_records == 2);
    GLYPHA_REQUIRE(batch_stats[0].committed_bytes > 0);
    GLYPHA_REQUIRE(batch_stats[0].maximum_batch_records == 2);
    GLYPHA_REQUIRE(batch_stats[0].maximum_batch_bytes == batch_stats[0].committed_bytes);
    GLYPHA_REQUIRE(batch_stats[0].total_commit_duration_ns > 0);
    GLYPHA_REQUIRE(batch_stats[0].maximum_commit_duration_ns == batch_stats[0].total_commit_duration_ns);
    GLYPHA_REQUIRE(batch_stats[0].deadline_closes == 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("paired Writer preserves same-key order across strict durable group boundaries") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .durable_mutation_queue_capacity = 4},
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
    const auto put = glyphastore::server::encode_request({.opcode = glyphastore::server::RequestOpcode::put,
                                                          .request_id = 92,
                                                          .key = bytes("same-key-group"),
                                                          .value = bytes("value")});
    const auto erase = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::erase,
        .request_id = 93,
        .key = bytes("same-key-group"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(erase.has_value());
    std::vector<std::byte> pipeline;
    pipeline.reserve(put->size() + erase->size());
    pipeline.insert(pipeline.end(), put->begin(), put->end());
    pipeline.insert(pipeline.end(), erase->begin(), erase->end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    const auto put_response = glyphastore::server::decode_response(receive_response(socket));
    const auto erase_response = glyphastore::server::decode_response(receive_response(socket));
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(erase_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(erase_response->frame.status == glyphastore::server::ResponseStatus::ok);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
    auto recovered =
        glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                  .storage_mode = glyphastore::StorageMode::durable_sync,
                                  .data_directory = path,
                                  .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(recovered.has_value());
    const auto missing = (*recovered)->get("same-key-group");
    GLYPHA_REQUIRE(!missing.has_value());
    GLYPHA_REQUIRE(missing.error().code == glyphastore::ErrorCode::not_found);
}

GLYPHA_TEST("blocked durable cold GET leaves its Reactor responsive and applies bounded admission") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("cold-a", bytes("value-a")).has_value());
        GLYPHA_REQUIRE((*seed)->put("cold-b", bytes("value-b")).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    BlockingColdRead blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .disk_read_thread_count = 1, .disk_read_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
         .filesystem_hooks = {
             .file_io = {.context = &blocker, .read_some_at = &BlockingColdRead::read_some_at}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto blocked_socket = connect_to(server.port());
    GLYPHA_REQUIRE(blocked_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(blocked_socket, 0, 1));
    blocker.arm();
    const auto cold_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 40,
        .key = bytes("cold-a"),
    });
    const auto ordered_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 43,
        .key = bytes("ordered-after-cold"),
        .value = bytes("ordered-value"),
    });
    GLYPHA_REQUIRE(cold_a.has_value());
    GLYPHA_REQUIRE(ordered_put.has_value());
    std::vector<std::byte> blocked_pipeline;
    blocked_pipeline.insert(blocked_pipeline.end(), cold_a->begin(), cold_a->end());
    blocked_pipeline.insert(blocked_pipeline.end(), ordered_put->begin(), ordered_put->end());
    GLYPHA_REQUIRE(send_all(blocked_socket, blocked_pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    // The only disk-read admission is occupied, but the owner-affine Reactor
    // must still accept, initialize, mutate, and respond on another socket.
    const auto responsive_socket = connect_to(server.port());
    GLYPHA_REQUIRE(responsive_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(responsive_socket, 0, 1));
    const auto ordered_not_yet_visible = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 44,
        .key = bytes("ordered-after-cold"),
    });
    GLYPHA_REQUIRE(ordered_not_yet_visible.has_value());
    GLYPHA_REQUIRE(send_all(responsive_socket, *ordered_not_yet_visible));
    const auto not_yet_visible_frame = receive_response(responsive_socket);
    const auto not_yet_visible = glyphastore::server::decode_response(not_yet_visible_frame);
    GLYPHA_REQUIRE(not_yet_visible.has_value());
    GLYPHA_REQUIRE(not_yet_visible->frame.status == glyphastore::server::ResponseStatus::not_found);
    const auto saturated_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 41,
        .key = bytes("cold-b"),
    });
    const auto same_worker_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 42,
        .key = bytes("reactor-remains-live"),
        .value = bytes("stored"),
    });
    GLYPHA_REQUIRE(saturated_get.has_value());
    GLYPHA_REQUIRE(same_worker_put.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), saturated_get->begin(), saturated_get->end());
    pipeline.insert(pipeline.end(), same_worker_put->begin(), same_worker_put->end());
    GLYPHA_REQUIRE(send_all(responsive_socket, pipeline));

    const auto overload_frame = receive_response(responsive_socket);
    const auto put_frame = receive_response(responsive_socket);
    const auto overload = glyphastore::server::decode_response(overload_frame);
    const auto put = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(overload.has_value());
    GLYPHA_REQUIRE(overload->frame.request_id == 41);
    GLYPHA_REQUIRE(overload->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(put->frame.request_id == 42);
    GLYPHA_REQUIRE(put->frame.status == glyphastore::server::ResponseStatus::ok);

    blocker.release();
    const auto cold_frame = receive_response(blocked_socket);
    const auto cold = glyphastore::server::decode_response(cold_frame);
    GLYPHA_REQUIRE(cold.has_value());
    GLYPHA_REQUIRE(cold->frame.request_id == 40);
    GLYPHA_REQUIRE(cold->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(cold->frame.value) == "value-a");
    const auto ordered_put_frame = receive_response(blocked_socket);
    const auto ordered_put_response = glyphastore::server::decode_response(ordered_put_frame);
    GLYPHA_REQUIRE(ordered_put_response.has_value());
    GLYPHA_REQUIRE(ordered_put_response->frame.request_id == 43);
    GLYPHA_REQUIRE(ordered_put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    // Both ACKs must follow immutable durable-generation publication. These
    // GETs exercise the captured active-file pins directly; neither key was in
    // the recovery bootstrap generation.
    const auto visible_after_ack = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 45,
        .key = bytes("reactor-remains-live"),
    });
    const auto ordered_after_ack = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 46,
        .key = bytes("ordered-after-cold"),
    });
    GLYPHA_REQUIRE(visible_after_ack.has_value());
    GLYPHA_REQUIRE(ordered_after_ack.has_value());
    GLYPHA_REQUIRE(send_all(responsive_socket, *visible_after_ack));
    const auto visible_frame = receive_response(responsive_socket);
    const auto visible = glyphastore::server::decode_response(visible_frame);
    GLYPHA_REQUIRE(visible.has_value());
    GLYPHA_REQUIRE(visible->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(visible->frame.value) == "stored");
    GLYPHA_REQUIRE(send_all(blocked_socket, *ordered_after_ack));
    const auto ordered_frame = receive_response(blocked_socket);
    const auto ordered = glyphastore::server::decode_response(ordered_frame);
    GLYPHA_REQUIRE(ordered.has_value());
    GLYPHA_REQUIRE(ordered->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ordered->frame.value) == "ordered-value");

    static_cast<void>(::close(blocked_socket));
    static_cast<void>(::close(responsive_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable cold GET keeps one bounded scatter lease across partial socket writes") {
#if defined(__OpenBSD__)
    // Partial-write scatter timing is not stable under OpenBSD qemu (SO_RCVBUF / kqueue
    // scheduling); Linux ASan remains the authority for this lease invariant.
    return;
#endif
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    constexpr std::size_t kValueBytes = 768U * 1024U;
    std::vector<std::byte> expected(kValueBytes);
    for (std::size_t index = 0; index < expected.size(); ++index) {
        expected[index] = static_cast<std::byte>(index & 0xFFU);
    }
    {
        auto seed = glyphastore::Store::open({.worker_config = {.explicit_count = 1},
                                              .storage_mode = glyphastore::StorageMode::durable_sync,
                                              .data_directory = path,
                                              .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(seed.has_value());
        GLYPHA_REQUIRE((*seed)->put("scatter-large", expected).has_value());
        GLYPHA_REQUIRE((*seed)->close().has_value());
    }

    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 1,
         .maximum_output_bytes = 1024U * 1024U,
         .accepted_socket_send_buffer_bytes = 4U * 1024U,
         .disk_read_queue_capacity = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const int receive_buffer = 4U * 1024U;
    GLYPHA_REQUIRE(::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &receive_buffer, sizeof(receive_buffer)) == 0);

    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 61,
        .key = bytes("scatter-large"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));

    bool observed_partial{};
    const auto partial_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!observed_partial && std::chrono::steady_clock::now() < partial_deadline) {
        const auto report = server.stats_report();
        GLYPHA_REQUIRE(report.has_value());
        observed_partial = report->find("output_scatter_partial_writes=0\n") == std::string::npos;
        if (!observed_partial) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    GLYPHA_REQUIRE(observed_partial);

    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame, 1024U * 1024U);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 61);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(std::ranges::equal(response->frame.value, expected));

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->find("output_scatter_responses=1\n") != std::string::npos);
    GLYPHA_REQUIRE(report->find("output_scatter_bytes=786432\n") != std::string::npos);
    GLYPHA_REQUIRE(report->find("output_scatter_completions=1\n") != std::string::npos);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("BIND_WORKER handoff saturation returns overloaded then closes") {
    // Prefill Worker 1 handoff ring (capacity-1 → ring of 2), pump only Reactor 0,
    // cross-Reactor BIND must observe OVERLOADED then EOF — not silent success ACK.
    auto store = open_paired_store_for_writer(2, 8);
    GLYPHA_REQUIRE(store.has_value());
    glyphastore::server::ConnectionHandoffMesh mesh{2, 1};
    auto disk_reads = glyphastore::server::DiskReadExecutor::create(**store, 2, 8);
    GLYPHA_REQUIRE(disk_reads.has_value());
    auto pair_writers = glyphastore::server::PairWriterPool::create(**store, 2, 8, kTestMutationArenaBytes,
                                                                    std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(pair_writers.has_value());
    GLYPHA_REQUIRE((*pair_writers)->start().has_value());

    glyphastore::server::ReactorConfig config{
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 2,
        .connection_handoff_capacity = 1,
        .disk_read_thread_count = 2,
    };
    auto listener = glyphastore::server::TcpListener::bind(config.bind_address, 0);
    GLYPHA_REQUIRE(listener.has_value());
    auto reactor0 = glyphastore::server::Reactor::create(config, 0, std::move(*listener), {}, {}, **store,
                                                         mesh, **disk_reads, **pair_writers);
    GLYPHA_REQUIRE(reactor0.has_value());
    auto reactor1 = glyphastore::server::Reactor::create(config, 1, {}, {}, {}, **store, mesh, **disk_reads,
                                                         **pair_writers);
    GLYPHA_REQUIRE(reactor1.has_value());

    for (int i = 0; i < 2; ++i) {
        int fds[2]{-1, -1};
        GLYPHA_REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        GLYPHA_REQUIRE(glyphastore::server::set_nonblocking(fds[0]).has_value());
        glyphastore::server::ConnectionHandoff filler{
            .socket = glyphastore::server::SocketHandle{fds[0]},
            .bound_worker = 1,
            .initialized = true,
        };
        GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(filler)));
        static_cast<void>(::close(fds[1]));
    }

    std::atomic<bool> stop{false};
    std::thread pump{[&] {
        while (!stop.load(std::memory_order_acquire)) {
            static_cast<void>((*reactor0)->run_once(5));
        }
    }};

    const auto client = connect_to((*reactor0)->port());
    GLYPHA_REQUIRE(client >= 0);
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(send_all(client, *init));
    const auto init_frame = receive_response(client);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    GLYPHA_REQUIRE(initialized.has_value());
    GLYPHA_REQUIRE(initialized->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(client, *bind));
    const auto bind_frame = receive_response(client);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(bound->frame.request_id == 2);

    bool peer_closed = false;
    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < close_deadline) {
        char byte{};
        const auto received = ::recv(client, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(peer_closed);

    stop.store(true, std::memory_order_release);
    pump.join();
    static_cast<void>(::close(client));
    GLYPHA_REQUIRE((*pair_writers)->stop_and_drain().has_value());
}

GLYPHA_TEST("shutdown force-close rejects pending BIND handoff with OVERLOADED") {
    // Destination idle_for_shutdown used to ignore the mesh: BIND OK sat in an MPSC
    // cell, then teardown destroyed the socket with neither OK nor OVERLOADED.
    auto store = open_paired_store_for_writer(2, 8);
    GLYPHA_REQUIRE(store.has_value());
    glyphastore::server::ConnectionHandoffMesh mesh{2, 8};
    auto disk_reads = glyphastore::server::DiskReadExecutor::create(**store, 2, 8);
    GLYPHA_REQUIRE(disk_reads.has_value());
    auto pair_writers = glyphastore::server::PairWriterPool::create(**store, 2, 8, kTestMutationArenaBytes,
                                                                    std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(pair_writers.has_value());
    GLYPHA_REQUIRE((*pair_writers)->start().has_value());

    glyphastore::server::ReactorConfig config{
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 2,
        .connection_handoff_capacity = 8,
        .disk_read_thread_count = 2,
    };
    auto reactor1 = glyphastore::server::Reactor::create(config, 1, {}, {}, {}, **store, mesh, **disk_reads,
                                                         **pair_writers);
    GLYPHA_REQUIRE(reactor1.has_value());

    int fds[2]{-1, -1};
    GLYPHA_REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    GLYPHA_REQUIRE(glyphastore::server::set_nonblocking(fds[0]).has_value());
    const auto bind_ok = glyphastore::server::encode_response({
        .status = glyphastore::server::ResponseStatus::ok,
        .request_id = 2,
        .owner_worker = 1,
        .worker_count = 2,
        .routing_epoch = 1,
    });
    GLYPHA_REQUIRE(bind_ok.has_value());
    glyphastore::server::ConnectionHandoff pending{
        .socket = glyphastore::server::SocketHandle{fds[0]},
        .output = *bind_ok,
        .bound_worker = 1,
        .initialized = true,
    };
    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(pending)));
    GLYPHA_REQUIRE(mesh.has_pending(1));
    GLYPHA_REQUIRE((*reactor1)->active_connections() == 0);
    GLYPHA_REQUIRE(!(*reactor1)->idle_for_shutdown());

    // Force-close path (shutdown drain timeout): must surface OVERLOADED, not bare EOF.
    (*reactor1)->stop_accepting();
    (*reactor1)->close_all_connections();
    GLYPHA_REQUIRE(!mesh.has_pending(1));
    GLYPHA_REQUIRE((*reactor1)->idle_for_shutdown());

    const auto bind_frame = receive_response(fds[1]);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.request_id == 2);
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::overloaded);

    static_cast<void>(::close(fds[1]));
    GLYPHA_REQUIRE((*pair_writers)->stop_and_drain().has_value());
}

GLYPHA_TEST("BIND_WORKER destination slot exhaustion returns overloaded then closes") {
    auto store = open_paired_store_for_writer(2, 8);
    GLYPHA_REQUIRE(store.has_value());
    glyphastore::server::ConnectionHandoffMesh mesh{2, 8};
    auto disk_reads = glyphastore::server::DiskReadExecutor::create(**store, 2, 8);
    GLYPHA_REQUIRE(disk_reads.has_value());
    auto pair_writers = glyphastore::server::PairWriterPool::create(**store, 2, 8, kTestMutationArenaBytes,
                                                                    std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(pair_writers.has_value());
    GLYPHA_REQUIRE((*pair_writers)->start().has_value());

    glyphastore::server::ReactorConfig config0{
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 2,
        .connection_handoff_capacity = 8,
        .disk_read_thread_count = 2,
    };
    glyphastore::server::ReactorConfig config1 = config0;
    config1.maximum_connections = 1;

    auto listener = glyphastore::server::TcpListener::bind(config0.bind_address, 0);
    GLYPHA_REQUIRE(listener.has_value());
    auto reactor0 = glyphastore::server::Reactor::create(config0, 0, std::move(*listener), {}, {}, **store,
                                                         mesh, **disk_reads, **pair_writers);
    GLYPHA_REQUIRE(reactor0.has_value());
    auto reactor1 = glyphastore::server::Reactor::create(config1, 1, {}, {}, {}, **store, mesh, **disk_reads,
                                                         **pair_writers);
    GLYPHA_REQUIRE(reactor1.has_value());

    int filler_fds[2]{-1, -1};
    GLYPHA_REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, filler_fds) == 0);
    GLYPHA_REQUIRE(glyphastore::server::set_nonblocking(filler_fds[0]).has_value());
    glyphastore::server::ConnectionHandoff filler{
        .socket = glyphastore::server::SocketHandle{filler_fds[0]},
        .bound_worker = 1,
        .initialized = true,
    };
    GLYPHA_REQUIRE(mesh.try_handoff(1, std::move(filler)));
    const auto adopt_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while ((*reactor1)->active_connections() == 0 && std::chrono::steady_clock::now() < adopt_deadline) {
        static_cast<void>((*reactor1)->run_once(5));
    }
    GLYPHA_REQUIRE((*reactor1)->active_connections() == 1);

    std::atomic<bool> stop{false};
    std::thread pump{[&] {
        while (!stop.load(std::memory_order_acquire)) {
            static_cast<void>((*reactor0)->run_once(5));
            static_cast<void>((*reactor1)->run_once(5));
        }
    }};

    const auto client = connect_to((*reactor0)->port());
    GLYPHA_REQUIRE(client >= 0);
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(send_all(client, *init));
    const auto init_frame = receive_response(client);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    GLYPHA_REQUIRE(initialized.has_value());
    GLYPHA_REQUIRE(initialized->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(client, *bind));
    const auto bind_frame = receive_response(client);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(bound->frame.request_id == 2);

    bool peer_closed = false;
    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < close_deadline) {
        char byte{};
        const auto received = ::recv(client, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(peer_closed);

    stop.store(true, std::memory_order_release);
    pump.join();
    static_cast<void>(::close(client));
    static_cast<void>(::close(filler_fds[1]));
    GLYPHA_REQUIRE((*pair_writers)->stop_and_drain().has_value());
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("BIND_WORKER poller remove failure returns overloaded without dual registration") {
    // Unregister must succeed before socket ownership moves. A failed remove used to
    // hand off while the source poller still watched the fd (stale-token spin).
    auto store = open_paired_store_for_writer(2, 8);
    GLYPHA_REQUIRE(store.has_value());
    glyphastore::server::ConnectionHandoffMesh mesh{2, 8};
    auto disk_reads = glyphastore::server::DiskReadExecutor::create(**store, 2, 8);
    GLYPHA_REQUIRE(disk_reads.has_value());
    auto pair_writers = glyphastore::server::PairWriterPool::create(**store, 2, 8, kTestMutationArenaBytes,
                                                                    std::chrono::milliseconds{0});
    GLYPHA_REQUIRE(pair_writers.has_value());
    GLYPHA_REQUIRE((*pair_writers)->start().has_value());

    glyphastore::server::ReactorConfig config{
        .port = 0,
        .maximum_connections = 4,
        .worker_count = 2,
        .connection_handoff_capacity = 8,
        .disk_read_thread_count = 2,
    };
    auto listener = glyphastore::server::TcpListener::bind(config.bind_address, 0);
    GLYPHA_REQUIRE(listener.has_value());
    auto reactor0 = glyphastore::server::Reactor::create(config, 0, std::move(*listener), {}, {}, **store,
                                                         mesh, **disk_reads, **pair_writers);
    GLYPHA_REQUIRE(reactor0.has_value());
    auto reactor1 = glyphastore::server::Reactor::create(config, 1, {}, {}, {}, **store, mesh, **disk_reads,
                                                         **pair_writers);
    GLYPHA_REQUIRE(reactor1.has_value());

    std::atomic<bool> stop{false};
    std::thread pump{[&] {
        while (!stop.load(std::memory_order_acquire)) {
            static_cast<void>((*reactor0)->run_once(5));
            static_cast<void>((*reactor1)->run_once(5));
        }
    }};

    const auto client = connect_to((*reactor0)->port());
    GLYPHA_REQUIRE(client >= 0);
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(send_all(client, *init));
    const auto init_frame = receive_response(client);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    GLYPHA_REQUIRE(initialized.has_value());
    GLYPHA_REQUIRE(initialized->frame.status == glyphastore::server::ResponseStatus::ok);

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::poller_remove);
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = 1,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(client, *bind));
    const auto bind_frame = receive_response(client);
    glyphastore::fault::reset();
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(bound->frame.request_id == 2);
    GLYPHA_REQUIRE((*reactor1)->active_connections() == 0);

    bool peer_closed = false;
    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < close_deadline) {
        char byte{};
        const auto received = ::recv(client, &byte, 1, 0);
        if (received == 0) {
            peer_closed = true;
            break;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }
        break;
    }
    GLYPHA_REQUIRE(peer_closed);

    stop.store(true, std::memory_order_release);
    pump.join();
    static_cast<void>(::close(client));
    GLYPHA_REQUIRE((*pair_writers)->stop_and_drain().has_value());
}
#endif

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("sticky post-commit Writer failure is INTERNAL_ERROR on the wire") {
    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 77,
        .key = bytes("sticky-wire"),
        .value = bytes("first"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.request_id == 77);
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok ||
                   put_response->frame.status == glyphastore::server::ResponseStatus::internal_error);
    GLYPHA_REQUIRE(put_response->frame.status != glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(!server.healthy());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(!server.ready());
    GLYPHA_REQUIRE(!server.store_operational());
    GLYPHA_REQUIRE(!server.pair_writers_healthy());
    GLYPHA_REQUIRE(glyphastore::server::classify_ready_loss(server) ==
                   glyphastore::server::ReadyLossReason::store_not_operational);

    const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 79);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 80);
    GLYPHA_REQUIRE(ready.has_value());
    GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("pre-Store sibling after sticky capture is wire OVERLOADED not INTERNAL_ERROR") {
    // Two connections, same key: first commits then Site::capture sticky; second must
    // not enter Store and must not look like reconcile-first INTERNAL_ERROR.
    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 8, .worker_count = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first = connect_to(server.port());
    const auto second = connect_to(server.port());
    GLYPHA_REQUIRE(first >= 0);
    GLYPHA_REQUIRE(second >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second, 0, 1));

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::capture);
    const auto put1 = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 101,
        .key = bytes("sibling-fc-key"),
        .value = bytes("first"),
    });
    const auto put2 = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 102,
        .key = bytes("sibling-fc-key"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(put1.has_value());
    GLYPHA_REQUIRE(put2.has_value());
    GLYPHA_REQUIRE(send_all(first, *put1));
    GLYPHA_REQUIRE(send_all(second, *put2));

    const auto frame1 = receive_response(first);
    const auto frame2 = receive_response(second);
    glyphastore::fault::reset();
    const auto response1 = glyphastore::server::decode_response(frame1);
    const auto response2 = glyphastore::server::decode_response(frame2);
    GLYPHA_REQUIRE(response1.has_value());
    GLYPHA_REQUIRE(response2.has_value());
    GLYPHA_REQUIRE(response1->frame.request_id == 101);
    GLYPHA_REQUIRE(response2->frame.request_id == 102);
    // First: ACK-after-drain success (or sticky INTERNAL_ERROR if drain fails).
    GLYPHA_REQUIRE(response1->frame.status == glyphastore::server::ResponseStatus::ok ||
                   response1->frame.status == glyphastore::server::ResponseStatus::internal_error);
    GLYPHA_REQUIRE(response1->frame.status != glyphastore::server::ResponseStatus::overloaded);
    // Second: never Store-entered after sticky → OVERLOADED, not INTERNAL_ERROR.
    GLYPHA_REQUIRE(response2->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(!server.healthy());
    GLYPHA_REQUIRE(server.live());

    static_cast<void>(::close(first));
    static_cast<void>(::close(second));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("response queue allocation failure closes connection without daemon fail-stop") {
    // Committed PUT must not escalate an ACK-buffer bad_alloc into executor failed_.
    // Close without inventing OVERLOADED (mutation may already be durable).
    ServerTemporaryDirectory temporary;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::response_queue);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 81,
        .key = bytes("ack-alloc-key"),
        .value = bytes("ack-alloc-value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    glyphastore::fault::reset();
    GLYPHA_REQUIRE(put_frame.empty());
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.healthy());
    GLYPHA_REQUIRE(server.ready());
    static_cast<void>(::close(socket));

    const auto probe = connect_to(server.port());
    GLYPHA_REQUIRE(probe >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe, 0, 1));
    const auto health = probe_lifecycle(probe, glyphastore::server::RequestOpcode::health, 82);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 83,
        .key = bytes("ack-alloc-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(probe, *get));
    const auto get_frame = receive_response(probe);
    const auto got = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(got->frame.request_id == 83);
    GLYPHA_REQUIRE(got->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(std::ranges::equal(got->frame.value, bytes("ack-alloc-value")));

    static_cast<void>(::close(probe));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("INIT identity allocation failure returns OVERLOADED without daemon fail-stop") {
    // Identity encode bad_alloc must not mark initialized or stop the executor.
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    glyphastore::fault::reset();
    glyphastore::fault::fail_once(glyphastore::fault::Site::init_identity);
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 91,
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(send_all(socket, *init));
    const auto init_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto initialized = glyphastore::server::decode_response(init_frame);
    GLYPHA_REQUIRE(initialized.has_value());
    GLYPHA_REQUIRE(initialized->frame.request_id == 91);
    GLYPHA_REQUIRE(initialized->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.healthy());

    // Same connection must not be treated as initialized (BIND without INIT succeeds
    // only after a real INIT OK).
    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 92,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(bind.has_value());
    GLYPHA_REQUIRE(send_all(socket, *bind));
    const auto bind_frame = receive_response(socket);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::invalid_request);

    const auto probe = connect_to(server.port());
    GLYPHA_REQUIRE(probe >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(probe, 0, 1));

    static_cast<void>(::close(socket));
    static_cast<void>(::close(probe));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
#endif

GLYPHA_TEST("BIND_WORKER OK is flushed before pipelined decode failure closes") {
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto init = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::init,
        .request_id = 1,
    });
    GLYPHA_REQUIRE(init.has_value());
    GLYPHA_REQUIRE(send_all(socket, *init));
    const auto init_frame = receive_response(socket);
    const auto initialized = glyphastore::server::decode_response(init_frame);
    GLYPHA_REQUIRE(initialized.has_value());
    GLYPHA_REQUIRE(initialized->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto bind = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::bind_worker,
        .request_id = 2,
        .target_worker = 0,
    });
    GLYPHA_REQUIRE(bind.has_value());
    std::vector<std::byte> pipeline(bind->begin(), bind->end());
    std::array<std::byte, glyphastore::server::kRequestHeaderBytes> bad{};
    bad[0] = std::byte{static_cast<unsigned char>(glyphastore::server::kRequestHeaderBytes)};
    bad[4] = std::byte{0xff};
    pipeline.insert(pipeline.end(), bad.begin(), bad.end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    const auto bind_frame = receive_response(socket);
    const auto bound = glyphastore::server::decode_response(bind_frame);
    GLYPHA_REQUIRE(bound.has_value());
    GLYPHA_REQUIRE(bound->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(bound->frame.request_id == 2);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable segment_full surfaces as wire OVERLOADED not INTERNAL_ERROR") {
    // Append rejected before commit must not look like sticky INTERNAL_ERROR / reconcile.
    ServerTemporaryDirectory temporary;
    struct SegmentFullOnPut final {
        std::size_t write_records{};
        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<SegmentFullOnPut*>(opaque);
            if (operation != glyphastore::FilesystemOperation::write_record) {
                return {};
            }
            ++state.write_records;
            return glyphastore::fail(glyphastore::ErrorCode::segment_full, "injected segment full");
        }
    } failure;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &failure, .before = &SegmentFullOnPut::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 90,
        .key = bytes("segment-full-key"),
        .value = bytes("value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 90);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(failure.write_records >= 1);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable not-committed io_error surfaces as wire OVERLOADED not INTERNAL_ERROR") {
    // Pre-write append reject is authoritative not_committed; Writer must not leave
    // ErrorCode::io_error (Reactor → INTERNAL_ERROR → client indeterminate/reconcile).
    ServerTemporaryDirectory temporary;
    struct IoErrorOnPut final {
        std::size_t write_records{};
        static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
            -> glyphastore::Status {
            auto& state = *static_cast<IoErrorOnPut*>(opaque);
            if (operation != glyphastore::FilesystemOperation::write_record) {
                return {};
            }
            ++state.write_records;
            return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected append reject");
        }
    } failure;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.context = &failure, .before = &IoErrorOnPut::before}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 91,
        .key = bytes("io-error-not-committed-key"),
        .value = bytes("value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto frame = receive_response(socket);
    const auto response = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(response.has_value());
    GLYPHA_REQUIRE(response->frame.request_id == 91);
    GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(failure.write_records >= 1);

    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 92,
        .key = bytes("io-error-not-committed-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    const auto get_frame = receive_response(socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.request_id == 92);
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::not_found);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable not-committed INTERNAL_ERROR-bucket codes surface as wire OVERLOADED") {
    // corrupted_data / internal_error on authoritative not_committed must not stay in the
    // Reactor INTERNAL_ERROR bucket (client indeterminate). Mirrors rotation/open and
    // pre-boundary catch carriers that already stamp not_committed with those codes.
    const std::array codes{glyphastore::ErrorCode::corrupted_data, glyphastore::ErrorCode::internal_error};
    for (std::size_t index = 0; index < codes.size(); ++index) {
        ServerTemporaryDirectory temporary;
        struct InjectedReject final {
            glyphastore::ErrorCode code{};
            std::size_t write_records{};
            static auto before(void* opaque, const glyphastore::FilesystemOperation operation)
                -> glyphastore::Status {
                auto& state = *static_cast<InjectedReject*>(opaque);
                if (operation != glyphastore::FilesystemOperation::write_record) {
                    return {};
                }
                ++state.write_records;
                return glyphastore::fail(state.code, "injected not_committed reject");
            }
        } failure{.code = codes[index]};
        auto opened = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 4, .worker_count = 1, .disk_read_thread_count = 1},
            {.storage_mode = glyphastore::StorageMode::durable_sync,
             .data_directory = temporary.store_path(),
             .durable_open_mode = glyphastore::DurableOpenMode::create_new,
             .filesystem_hooks = {.context = &failure, .before = &InjectedReject::before}});
        GLYPHA_REQUIRE(opened.has_value());
        auto& server = **opened;
        GLYPHA_REQUIRE(server.start().has_value());

        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
        const auto key_text = std::string{"bucket-not-committed-"} + std::to_string(index);
        const auto key = bytes(key_text);
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 200U + static_cast<std::uint64_t>(index),
            .key = key,
            .value = bytes("value"),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == 200U + static_cast<std::uint64_t>(index));
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::overloaded);
        GLYPHA_REQUIRE(failure.write_records >= 1);

        const auto get = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::get,
            .request_id = 210U + static_cast<std::uint64_t>(index),
            .key = key,
        });
        GLYPHA_REQUIRE(get.has_value());
        GLYPHA_REQUIRE(send_all(socket, *get));
        const auto get_frame = receive_response(socket);
        const auto get_response = glyphastore::server::decode_response(get_frame);
        GLYPHA_REQUIRE(get_response.has_value());
        GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::not_found);

        static_cast<void>(::close(socket));
        server.request_stop();
        GLYPHA_REQUIRE(server.join().has_value());
    }
}
