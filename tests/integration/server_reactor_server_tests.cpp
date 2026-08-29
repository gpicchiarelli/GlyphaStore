#include "glyphastore/core/fault_injection.hpp"
#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/server/daemon_log.hpp"
#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "server/reactor_detail.hpp"
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
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

using namespace glyphastore::test::server_reactor_support;

GLYPHA_TEST("server rejects unsupported worker counts and undersized protocol buffers") {
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .worker_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_input_bytes = glyphastore::server::kRequestHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .maximum_output_bytes = glyphastore::server::kResponseHeaderBytes - 1U})
                        .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0,
                         .accepted_socket_send_buffer_bytes =
                             static_cast<std::size_t>(std::numeric_limits<int>::max()) + 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .disk_read_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_capacity = 0}).has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .durable_mutation_queue_bytes = 0}).has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create(
                        {.port = 0, .disk_read_thread_count = glyphastore::kMaximumWorkerCount + 1U})
                        .has_value());
    GLYPHA_REQUIRE(
        !glyphastore::server::Server::create({.port = 0, .worker_count = 2, .disk_read_thread_count = 1})
             .has_value());
    GLYPHA_REQUIRE(!glyphastore::server::Server::create({.port = 0, .worker_count = 2},
                                                        {.worker_config = {.explicit_count = 1}})
                        .has_value());
}

GLYPHA_TEST("server StoreConfig persists acknowledged wire writes across restart") {
    ServerTemporaryDirectory temporary;
    const auto path = temporary.store_path();
    {
        auto opened = glyphastore::server::Server::create(
            {.port = 0, .maximum_connections = 4},
            {.storage_mode = glyphastore::StorageMode::durable_sync,
             .data_directory = path,
             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
        GLYPHA_REQUIRE(opened.has_value());
        auto& server = **opened;
        GLYPHA_REQUIRE(server.start().has_value());

        const auto socket = connect_to(server.port());
        GLYPHA_REQUIRE(socket >= 0);
        GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = 3,
            .key = bytes("durable-wire-key"),
            .value = bytes("durable-wire-value"),
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto put_frame = receive_response(socket);
        const auto put_response = glyphastore::server::decode_response(put_frame);
        GLYPHA_REQUIRE(put_response.has_value());
        GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);
        static_cast<void>(::close(socket));
        server.request_stop();
        GLYPHA_REQUIRE(server.join().has_value());
    }

    auto reopened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = path,
         .durable_open_mode = glyphastore::DurableOpenMode::open_existing});
    GLYPHA_REQUIRE(reopened.has_value());
    auto& server = **reopened;
    GLYPHA_REQUIRE(server.start().has_value());
    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const auto get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 4,
        .key = bytes("durable-wire-key"),
    });
    GLYPHA_REQUIRE(get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *get));
    const auto get_frame = receive_response(socket);
    const auto get_response = glyphastore::server::decode_response(get_frame);
    GLYPHA_REQUIRE(get_response.has_value());
    GLYPHA_REQUIRE(get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(get_response->frame.value) == "durable-wire-value");
    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP before INIT returns NOT_BOUND and creates no destination") {
    // BACKUP is Bound-state only; unbound frames must not run the fenced path.
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    const auto backup_dir = temporary.store_path().parent_path() / "unbound-backup";
    const auto backup_path = backup_dir.string();
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 89,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.request_id == 89);
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::not_bound);
    GLYPHA_REQUIRE(!std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP copies a live durable Server catalog into an empty destination") {
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 90,
        .key = bytes("backup-live-key"),
        .value = bytes("backup-live-value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto backup_dir = temporary.store_path().parent_path() / "online-backup";
    const auto backup_path = backup_dir.string();
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 91,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(backup_response->frame.value);
    GLYPHA_REQUIRE(report.find("status=ok") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("admission_fence_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("catalog_copy_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("destination_verify_ns=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("segment_copy_workers=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("source_crc_scanned=") != std::string_view::npos);
    GLYPHA_REQUIRE(report.find("destination_crc_scanned=") != std::string_view::npos);

    // Offline tool still fails while the Server holds the lock.
    const auto contested = glyphastore::backup_durable_store(
        temporary.store_path(), temporary.store_path().parent_path() / "offline-contested");
    GLYPHA_REQUIRE(!contested.has_value());

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_dir = temporary.store_path().parent_path() / "restored";
    const auto restored = glyphastore::restore_durable_store(backup_dir, restored_dir);
    GLYPHA_REQUIRE(restored.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    const auto got = (*reopened)->get("backup-live-key");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(text(got->bytes) == "backup-live-value");
}

GLYPHA_TEST("wire BACKUP refuses before fence when OK report cannot fit output budget") {
    // Oversized OK report used to map to OVERLOADED after a successful fenced copy —
    // false known-not-committed polarity while the destination already held the backup.
    ServerTemporaryDirectory temporary;
    const auto backup_dir = temporary.store_path().parent_path() / "fit-refuse-backup";
    const auto backup_path = backup_dir.string();
    const auto estimated =
        glyphastore::server::reactor_detail::backup_ok_report_max_bytes(backup_path.size());
    GLYPHA_REQUIRE(estimated > 64);
    const auto max_output = glyphastore::server::kResponseHeaderBytes + 64;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .maximum_output_bytes = max_output},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 92,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.request_id == 92);
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(!std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("wire BACKUP keeps OK after report formatting fails post-commit") {
#if !defined(GLYPHASTORE_FAULT_INJECTION)
    return;
#else
    // Site::backup_report throws after backup_to succeeds. Probe must still return
    // success (minimal status=ok) — not INTERNAL_ERROR with destination already filled.
    ServerTemporaryDirectory temporary;
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = temporary.store_path(),
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 93,
        .key = bytes("backup-report-key"),
        .value = bytes("backup-report-value"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok);

    const auto backup_dir = temporary.store_path().parent_path() / "report-fault-backup";
    const auto backup_path = backup_dir.string();
    glyphastore::fault::fail_once(glyphastore::fault::Site::backup_report);
    const auto backup = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::backup,
        .request_id = 94,
        .key = bytes(backup_path),
    });
    GLYPHA_REQUIRE(backup.has_value());
    GLYPHA_REQUIRE(send_all(socket, *backup));
    const auto backup_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto backup_response = glyphastore::server::decode_response(backup_frame);
    GLYPHA_REQUIRE(backup_response.has_value());
    GLYPHA_REQUIRE(backup_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto report = text(backup_response->frame.value);
    GLYPHA_REQUIRE(report.find("status=ok") != std::string_view::npos);
    GLYPHA_REQUIRE(std::filesystem::exists(backup_dir));

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_dir = temporary.store_path().parent_path() / "report-fault-restored";
    const auto restored = glyphastore::restore_durable_store(backup_dir, restored_dir);
    GLYPHA_REQUIRE(restored.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    const auto got = (*reopened)->get("backup-report-key");
    GLYPHA_REQUIRE(got.has_value());
    GLYPHA_REQUIRE(text(got->bytes) == "backup-report-value");
#endif
}

GLYPHA_TEST("blocked durable mutation leaves its Reactor responsive with bounded FIFO admission") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 4, .durable_mutation_queue_capacity = 2},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());

    const auto first_socket = connect_to(server.port());
    const auto second_socket = connect_to(server.port());
    const auto responsive_socket = connect_to(server.port());
    GLYPHA_REQUIRE(first_socket >= 0);
    GLYPHA_REQUIRE(second_socket >= 0);
    GLYPHA_REQUIRE(responsive_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(first_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(second_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(responsive_socket, 0, 1));

    blocker.arm();
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 70,
        .key = bytes("async-first"),
        .value = bytes("first"),
    });
    const auto ordered_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 74,
        .key = bytes("async-first"),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(ordered_get.has_value());
    std::vector<std::byte> first_pipeline;
    first_pipeline.insert(first_pipeline.end(), first->begin(), first->end());
    first_pipeline.insert(first_pipeline.end(), ordered_get->begin(), ordered_get->end());
    GLYPHA_REQUIRE(send_all(first_socket, first_pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());

    // A second mutation must be admitted without waiting for the lane's slow
    // I/O, proving that its queue mutex is not an equivalent storage lock.
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 71,
        .key = bytes("async-second"),
        .value = bytes("second"),
    });
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(send_all(second_socket, *second));

    // send_all only proves kernel admission. Wait until the second mutation has
    // consumed the remaining bounded lane slot before asserting that the next
    // connection is rejected; slow OpenBSD runners can otherwise schedule the
    // responsive socket first.
    bool lane_full = false;
    const auto admission_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (std::chrono::steady_clock::now() < admission_deadline) {
        const auto stats = server.pair_writer_stats();
        if (stats.size() == 1 && stats[0].payload_slots_in_use == 2) {
            lane_full = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(lane_full);

    // The per-Worker admission budget is now exhausted. Rejection and the
    // following non-storage request are both handled while fsync is suspended.
    const auto rejected = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 72,
        .key = bytes("async-rejected"),
        .value = bytes("rejected"),
    });
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 73,
        .value = bytes("reactor-live"),
    });
    GLYPHA_REQUIRE(rejected.has_value());
    GLYPHA_REQUIRE(ping.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), rejected->begin(), rejected->end());
    pipeline.insert(pipeline.end(), ping->begin(), ping->end());
    GLYPHA_REQUIRE(send_all(responsive_socket, pipeline));
    const auto rejected_frame = receive_response(responsive_socket);
    const auto ping_frame = receive_response(responsive_socket);
    const auto rejected_response = glyphastore::server::decode_response(rejected_frame);
    const auto ping_response = glyphastore::server::decode_response(ping_frame);
    GLYPHA_REQUIRE(rejected_response.has_value());
    GLYPHA_REQUIRE(rejected_response->frame.request_id == 72);
    GLYPHA_REQUIRE(rejected_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    GLYPHA_REQUIRE(ping_response.has_value());
    GLYPHA_REQUIRE(ping_response->frame.request_id == 73);
    GLYPHA_REQUIRE(text(ping_response->frame.value) == "reactor-live");

    blocker.release();
    const auto first_frame = receive_response(first_socket);
    const auto ordered_get_frame = receive_response(first_socket);
    const auto second_frame = receive_response(second_socket);
    const auto first_response = glyphastore::server::decode_response(first_frame);
    const auto ordered_get_response = glyphastore::server::decode_response(ordered_get_frame);
    const auto second_response = glyphastore::server::decode_response(second_frame);
    GLYPHA_REQUIRE(first_response.has_value());
    GLYPHA_REQUIRE(ordered_get_response.has_value());
    GLYPHA_REQUIRE(second_response.has_value());
    GLYPHA_REQUIRE(first_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(ordered_get_response->frame.request_id == 74);
    GLYPHA_REQUIRE(ordered_get_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(text(ordered_get_response->frame.value) == "first");
    GLYPHA_REQUIRE(second_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto mutation_stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(mutation_stats.size() == 1);
    GLYPHA_REQUIRE(mutation_stats[0].queue_depth == 0);
    GLYPHA_REQUIRE(mutation_stats[0].queued_bytes == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queue_depth >= 1);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_queued_bytes > 0);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slot_capacity == 2);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slots_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_slots_in_use == 2);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_capacity_bytes == 16U * 1024U * 1024U);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_storage_bytes >
                   mutation_stats[0].payload_arena_capacity_bytes);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_bytes_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_arena_bytes_in_use >= 34);
    GLYPHA_REQUIRE(mutation_stats[0].payload_admission_bytes_in_use == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_payload_admission_bytes_in_use >= 290);
    GLYPHA_REQUIRE(mutation_stats[0].payload_slot_full_total == 1);
    GLYPHA_REQUIRE(mutation_stats[0].payload_arena_full_total == 0);
    GLYPHA_REQUIRE(mutation_stats[0].payload_too_large_total == 0);
    GLYPHA_REQUIRE(mutation_stats[0].admitted == 2);
    GLYPHA_REQUIRE(mutation_stats[0].rejected == 1);
    GLYPHA_REQUIRE(mutation_stats[0].expired_before_store == 0);
    GLYPHA_REQUIRE(mutation_stats[0].completed == 2);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retries == 0);
    GLYPHA_REQUIRE(mutation_stats[0].conflict_retry_commits == 0);
    GLYPHA_REQUIRE(mutation_stats[0].maximum_service_ns > 0);
    GLYPHA_REQUIRE(mutation_stats[0].read_generation_memory.current_allocated_lower_bound_bytes > 0);
    GLYPHA_REQUIRE(mutation_stats[0].read_generation_memory.delta_entries == mutation_stats[0].delta_entries);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    static_cast<void>(::close(responsive_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("mutation completion resumes a bounded pipeline without reordering decided responses") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1},
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

    const auto put_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 501,
        .key = bytes("resume-a"),
        .value = bytes("one"),
    });
    const auto get_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 502,
        .key = bytes("resume-a"),
    });
    const auto put_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 503,
        .key = bytes("resume-b"),
        .value = bytes("two"),
    });
    const auto get_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 504,
        .key = bytes("resume-b"),
    });
    GLYPHA_REQUIRE(put_a.has_value());
    GLYPHA_REQUIRE(get_a.has_value());
    GLYPHA_REQUIRE(put_b.has_value());
    GLYPHA_REQUIRE(get_b.has_value());

    std::vector<std::byte> pipeline;
    for (const auto* frame : {&*put_a, &*get_a, &*put_b, &*get_b}) {
        pipeline.insert(pipeline.end(), frame->begin(), frame->end());
    }
    // The second completion encounters this only after ACK/GET responses have
    // been decided. They must drain in order before the connection closes.
    std::array<std::byte, glyphastore::server::kRequestHeaderBytes> malformed{};
    malformed[0] = std::byte{static_cast<unsigned char>(glyphastore::server::kRequestHeaderBytes)};
    malformed[4] = std::byte{0xff};
    pipeline.insert(pipeline.end(), malformed.begin(), malformed.end());

    blocker.arm();
    GLYPHA_REQUIRE(send_all(socket, pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();

    for (const auto& [request_id, expected_value] :
         {std::pair{501ULL, std::string_view{}}, std::pair{502ULL, std::string_view{"one"}},
          std::pair{503ULL, std::string_view{}}, std::pair{504ULL, std::string_view{"two"}}}) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == request_id);
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        if (!expected_value.empty()) {
            GLYPHA_REQUIRE(text(response->frame.value) == expected_value);
        }
    }

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("volatile large GET pipeline uses bounded scatter leases in response order") {
    constexpr std::size_t kValueBytes = 64U * 1024U;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .maximum_output_bytes = 256U * 1024U});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));
    const std::vector<std::byte> first_value(kValueBytes, std::byte{0x31});
    const std::vector<std::byte> second_value(kValueBytes, std::byte{0x72});
    for (const auto& [request_id, key, value] :
         {std::tuple{601ULL, std::string_view{"scatter-hot-a"}, &first_value},
          std::tuple{602ULL, std::string_view{"scatter-hot-b"}, &second_value}}) {
        const auto put = glyphastore::server::encode_request({
            .opcode = glyphastore::server::RequestOpcode::put,
            .request_id = request_id,
            .key = bytes(key),
            .value = *value,
        });
        GLYPHA_REQUIRE(put.has_value());
        GLYPHA_REQUIRE(send_all(socket, *put));
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
    }

    const auto first_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 603,
        .key = bytes("scatter-hot-a"),
    });
    const auto second_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 604,
        .key = bytes("scatter-hot-b"),
    });
    GLYPHA_REQUIRE(first_get.has_value());
    GLYPHA_REQUIRE(second_get.has_value());
    std::vector<std::byte> pipeline;
    pipeline.insert(pipeline.end(), first_get->begin(), first_get->end());
    pipeline.insert(pipeline.end(), second_get->begin(), second_get->end());
    GLYPHA_REQUIRE(send_all(socket, pipeline));

    for (const auto& [request_id, expected] :
         {std::pair{603ULL, &first_value}, std::pair{604ULL, &second_value}}) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame, 256U * 1024U);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == request_id);
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        GLYPHA_REQUIRE(std::ranges::equal(response->frame.value, *expected));
    }

    // The hot pipelined threshold is deliberately above the established 4 KiB
    // cold-read threshold. Keep this boundary contiguous on every platform row.
    const std::vector<std::byte> boundary_value(4U * 1024U, std::byte{0x55});
    const auto boundary_put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 605,
        .key = bytes("scatter-hot-boundary"),
        .value = boundary_value,
    });
    GLYPHA_REQUIRE(boundary_put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *boundary_put));
    const auto boundary_put_response = glyphastore::server::decode_response(receive_response(socket));
    GLYPHA_REQUIRE(boundary_put_response.has_value());
    GLYPHA_REQUIRE(boundary_put_response->frame.request_id == 605);
    GLYPHA_REQUIRE(boundary_put_response->frame.status == glyphastore::server::ResponseStatus::ok);
    const auto boundary_get = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::get,
        .request_id = 606,
        .key = bytes("scatter-hot-boundary"),
    });
    GLYPHA_REQUIRE(boundary_get.has_value());
    GLYPHA_REQUIRE(send_all(socket, *boundary_get));
    const auto boundary_frame = receive_response(socket);
    const auto boundary_response = glyphastore::server::decode_response(boundary_frame, 256U * 1024U);
    GLYPHA_REQUIRE(boundary_response.has_value());
    GLYPHA_REQUIRE(boundary_response->frame.request_id == 606);
    GLYPHA_REQUIRE(std::ranges::equal(boundary_response->frame.value, boundary_value));

    const auto report = server.stats_report();
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->find("output_scatter_responses=2\n") != std::string::npos);
    GLYPHA_REQUIRE(report->find("output_scatter_bytes=131072\n") != std::string::npos);
    GLYPHA_REQUIRE(report->find("output_scatter_completions=2\n") != std::string::npos);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("pending output stops mutation completion pipeline resume until socket drain") {
#if defined(__OpenBSD__)
    // This backpressure timing test depends on a small SO_RCVBUF retaining the
    // large response in user space. OpenBSD under hosted qemu can drain it while
    // the VM is descheduled; ordered pipeline responses remain covered above.
    return;
#endif
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0, .maximum_connections = 1, .accepted_socket_send_buffer_bytes = 4U * 1024U},
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
    int receive_buffer_bytes = 4 * 1024;
    GLYPHA_REQUIRE(::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &receive_buffer_bytes,
                                sizeof(receive_buffer_bytes)) == 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    const auto put_a = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 511,
        .key = bytes("slow-a"),
        .value = bytes("one"),
    });
    // Use a protocol-maximum response. A 512 KiB response can be absorbed in
    // loopback autotuning buffers even after SO_RCVBUF/SO_SNDBUF are reduced,
    // which makes the intended EAGAIN boundary timing-dependent on Linux and
    // macOS. The maximum legal frame creates the same bounded condition without
    // test-only production hooks or relaxed assertions.
    constexpr std::size_t kPingBytes =
        glyphastore::server::kMaxFrameBytes - glyphastore::server::kRequestHeaderBytes;
    const std::vector<std::byte> ping_value(kPingBytes, std::byte{0x61});
    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 512,
        .value = ping_value,
    });
    const auto put_b = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 513,
        .key = bytes("slow-b"),
        .value = bytes("two"),
    });
    const auto put_c = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 514,
        .key = bytes("slow-c"),
        .value = bytes("three"),
    });
    GLYPHA_REQUIRE(put_a.has_value());
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(put_b.has_value());
    GLYPHA_REQUIRE(put_c.has_value());

    std::vector<std::byte> pipeline;
    for (const auto* frame : {&*put_a, &*ping, &*put_b, &*put_c}) {
        pipeline.insert(pipeline.end(), frame->begin(), frame->end());
    }

    blocker.arm();
    GLYPHA_REQUIRE(send_all(socket, pipeline));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    blocker.release();

    // Completion A resumes the buffered PING and admits B. Its large response
    // cannot drain into the deliberately small TCP windows. Completion B must
    // therefore leave C buffered instead of advancing the Writer lane.
    bool two_completed = false;
    const auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < completion_deadline) {
        const auto stats = server.pair_writer_stats();
        if (stats.size() == 1 && stats[0].completed == 2) {
            GLYPHA_REQUIRE(stats[0].admitted == 2);
            two_completed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    GLYPHA_REQUIRE(two_completed);
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 2);

    for (const auto request_id : {511ULL, 512ULL, 513ULL, 514ULL}) {
        const auto frame = receive_response(socket);
        const auto response = glyphastore::server::decode_response(frame);
        GLYPHA_REQUIRE(response.has_value());
        GLYPHA_REQUIRE(response->frame.request_id == request_id);
        GLYPHA_REQUIRE(response->frame.status == glyphastore::server::ResponseStatus::ok);
        if (request_id == 512) {
            GLYPHA_REQUIRE(response->frame.value.size() == kPingBytes);
        }
    }

    stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 3);
    GLYPHA_REQUIRE(stats[0].completed == 3);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("mutation payload arena applies byte backpressure independently of queue slots") {
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 4,
         .durable_mutation_queue_bytes = 300},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
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

    const std::string value(64, 'v');
    const auto first = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 77,
        .key = bytes("arena-first"),
        .value = bytes(value),
    });
    const auto second = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 78,
        .key = bytes("arena-second"),
        .value = bytes(value),
    });
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    blocker.arm();
    GLYPHA_REQUIRE(send_all(first_socket, *first));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(second_socket, *second));

    const auto rejected_frame = receive_response(second_socket);
    const auto rejected = glyphastore::server::decode_response(rejected_frame);
    GLYPHA_REQUIRE(rejected.has_value());
    GLYPHA_REQUIRE(rejected->frame.request_id == 78);
    GLYPHA_REQUIRE(rejected->frame.status == glyphastore::server::ResponseStatus::overloaded);
    auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].payload_slot_capacity == 4);
    GLYPHA_REQUIRE(stats[0].payload_slots_in_use == 1);
    GLYPHA_REQUIRE(stats[0].payload_arena_capacity_bytes == 300);
    GLYPHA_REQUIRE(stats[0].payload_arena_bytes_in_use == 75);
    GLYPHA_REQUIRE(stats[0].payload_admission_bytes_in_use == 203);
    GLYPHA_REQUIRE(stats[0].payload_slot_full_total == 0);
    GLYPHA_REQUIRE(stats[0].payload_arena_full_total == 1);

    blocker.release();
    const auto committed_frame = receive_response(first_socket);
    const auto committed = glyphastore::server::decode_response(committed_frame);
    GLYPHA_REQUIRE(committed.has_value());
    GLYPHA_REQUIRE(committed->frame.request_id == 77);
    GLYPHA_REQUIRE(committed->frame.status == glyphastore::server::ResponseStatus::ok);
    stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats[0].payload_slots_in_use == 0);
    GLYPHA_REQUIRE(stats[0].payload_arena_bytes_in_use == 0);
    GLYPHA_REQUIRE(stats[0].payload_admission_bytes_in_use == 0);

    static_cast<void>(::close(first_socket));
    static_cast<void>(::close(second_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}

GLYPHA_TEST("durable mutation queue deadline rejects only before Store execution") {
#if defined(__OpenBSD__)
    // Hosted OpenBSD qemu VMs do not reliably reach the hooked durable sync barrier for
    // this multi-connection deadline race; Linux/FreeBSD/macOS remain the authority.
    return;
#endif
    ServerTemporaryDirectory temporary;
    BlockingFileSync blocker;
    auto opened = glyphastore::server::Server::create(
        {.port = 0,
         .maximum_connections = 2,
         .durable_mutation_queue_capacity = 2,
         .durable_mutation_queue_wait_ms = 10},
        {.storage_mode = glyphastore::StorageMode::durable_sync,
         .data_directory = temporary.store_path(),
         .durable_open_mode = glyphastore::DurableOpenMode::create_new,
         .filesystem_hooks = {.file_io = {.context = &blocker, .sync_file = &BlockingFileSync::sync_file}}});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    SyncReleaseGuard release_on_exit{blocker};
    GLYPHA_REQUIRE(server.start().has_value());
    const auto blocked_socket = connect_to(server.port());
    const auto expiring_socket = connect_to(server.port());
    GLYPHA_REQUIRE(blocked_socket >= 0);
    GLYPHA_REQUIRE(expiring_socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(blocked_socket, 0, 1));
    GLYPHA_REQUIRE(initialize_and_bind(expiring_socket, 0, 1));

    blocker.arm();
    const auto blocked = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 75,
        .key = bytes("deadline-blocker"),
        .value = bytes("committed"),
    });
    const auto expiring = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 76,
        .key = bytes("deadline-expired"),
        .value = bytes("must-not-commit"),
    });
    GLYPHA_REQUIRE(blocked.has_value());
    GLYPHA_REQUIRE(expiring.has_value());
    GLYPHA_REQUIRE(send_all(blocked_socket, *blocked));
    GLYPHA_REQUIRE(blocker.wait_until_blocked());
    GLYPHA_REQUIRE(send_all(expiring_socket, *expiring));
    const auto expiry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{30};
    while (std::chrono::steady_clock::now() < expiry_deadline) {
        std::this_thread::yield();
    }
    blocker.release();

    const auto blocked_frame = receive_response(blocked_socket);
    const auto expired_frame = receive_response(expiring_socket);
    const auto blocked_response = glyphastore::server::decode_response(blocked_frame);
    const auto expired_response = glyphastore::server::decode_response(expired_frame);
    GLYPHA_REQUIRE(blocked_response.has_value());
    GLYPHA_REQUIRE(expired_response.has_value());
    GLYPHA_REQUIRE(blocked_response->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(expired_response->frame.status == glyphastore::server::ResponseStatus::overloaded);
    const auto stats = server.pair_writer_stats();
    GLYPHA_REQUIRE(stats.size() == 1);
    GLYPHA_REQUIRE(stats[0].admitted == 2);
    GLYPHA_REQUIRE(stats[0].expired_before_store == 1);
    GLYPHA_REQUIRE(stats[0].completed == 2);
    GLYPHA_REQUIRE(stats[0].maximum_queue_wait_ns >= 10'000'000U);

    static_cast<void>(::close(blocked_socket));
    static_cast<void>(::close(expiring_socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    auto recovered = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = temporary.store_path(),
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(recovered.has_value());
    GLYPHA_REQUIRE((*recovered)->get("deadline-blocker").has_value());
    const auto absent = (*recovered)->get("deadline-expired");
    GLYPHA_REQUIRE(!absent.has_value());
    GLYPHA_REQUIRE(absent.error().code == glyphastore::ErrorCode::not_found);
    GLYPHA_REQUIRE((*recovered)->close().has_value());
}

#if defined(GLYPHASTORE_FAULT_INJECTION)
GLYPHA_TEST("volatile pair sticky fails READY with pair_fail_closed reason") {
    // Store catalog stays operational on volatile sticky; ready() already fails on
    // pair_writers_->healthy(), but classify_ready_loss must not report none.
    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4, .worker_count = 1});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());
    GLYPHA_REQUIRE(server.ready());
    GLYPHA_REQUIRE(server.store_operational());
    GLYPHA_REQUIRE(server.pair_writers_healthy());
    GLYPHA_REQUIRE(glyphastore::server::classify_ready_loss(server) ==
                   glyphastore::server::ReadyLossReason::none);

    const auto socket = connect_to(server.port());
    GLYPHA_REQUIRE(socket >= 0);
    GLYPHA_REQUIRE(initialize_and_bind(socket, 0, 1));

    glyphastore::fault::reset();
    glyphastore::fault::configure(1, 0, 0);
    glyphastore::fault::fail_once(glyphastore::fault::Site::publish);
    const auto put = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::put,
        .request_id = 91,
        .key = bytes("volatile-sticky"),
        .value = bytes("x"),
    });
    GLYPHA_REQUIRE(put.has_value());
    GLYPHA_REQUIRE(send_all(socket, *put));
    const auto put_frame = receive_response(socket);
    glyphastore::fault::reset();
    const auto put_response = glyphastore::server::decode_response(put_frame);
    GLYPHA_REQUIRE(put_response.has_value());
    GLYPHA_REQUIRE(put_response->frame.request_id == 91);
    GLYPHA_REQUIRE(put_response->frame.status == glyphastore::server::ResponseStatus::ok ||
                   put_response->frame.status == glyphastore::server::ResponseStatus::internal_error);
    GLYPHA_REQUIRE(put_response->frame.status != glyphastore::server::ResponseStatus::overloaded);

    GLYPHA_REQUIRE(server.live());
    GLYPHA_REQUIRE(server.store_operational());
    GLYPHA_REQUIRE(!server.pair_writers_healthy());
    GLYPHA_REQUIRE(!server.ready());
    GLYPHA_REQUIRE(glyphastore::server::classify_ready_loss(server) ==
                   glyphastore::server::ReadyLossReason::pair_fail_closed);

    const auto health = probe_lifecycle(socket, glyphastore::server::RequestOpcode::health, 92);
    GLYPHA_REQUIRE(health.has_value());
    GLYPHA_REQUIRE(health->decoded.frame.status == glyphastore::server::ResponseStatus::ok);
    const auto ready = probe_lifecycle(socket, glyphastore::server::RequestOpcode::ready, 93);
    GLYPHA_REQUIRE(ready.has_value());
    GLYPHA_REQUIRE(ready->decoded.frame.status == glyphastore::server::ResponseStatus::internal_error);

    static_cast<void>(::close(socket));
    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());
}
#endif
