#include "glyphastore/client/client.hpp"
#include "glyphastore/persistence/filesystem.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/persistence/store_verify.hpp"
#include "glyphastore/segment/crc32c.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

class BackupTemporaryDirectory final {
  public:
    BackupTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-backup-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~BackupTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto bytes(std::string_view value) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

auto value_string(const glyphastore::OwnedValue& value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.bytes.data()), value.bytes.size()};
}

inline constexpr std::size_t kManifestChecksumOffset = 80;

void put_u16(std::span<std::byte> out, std::size_t offset, std::uint16_t value) {
    out[offset] = static_cast<std::byte>(value & 0xFFU);
    out[offset + 1] = static_cast<std::byte>((value >> 8U) & 0xFFU);
}

void put_u32(std::span<std::byte> out, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        out[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
    }
}

void refresh_manifest_checksum(std::span<std::byte> bytes) {
    put_u32(bytes, kManifestChecksumOffset, 0);
    put_u32(bytes, kManifestChecksumOffset, glyphastore::crc32c(bytes));
}

auto read_file_bytes(const std::filesystem::path& path) -> std::vector<std::byte> {
    std::ifstream input{path, std::ios::binary};
    GLYPHA_REQUIRE(static_cast<bool>(input));
    input.seekg(0, std::ios::end);
    const auto size = static_cast<std::size_t>(input.tellg());
    input.seekg(0, std::ios::beg);
    std::vector<std::byte> bytes(size);
    if (size > 0) {
        input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
        GLYPHA_REQUIRE(static_cast<bool>(input));
    }
    return bytes;
}

void write_file_bytes(const std::filesystem::path& path, std::span<const std::byte> bytes) {
    std::ofstream output{path, std::ios::binary | std::ios::trunc};
    GLYPHA_REQUIRE(static_cast<bool>(output));
    if (!bytes.empty()) {
        output.write(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<std::streamsize>(bytes.size()));
        GLYPHA_REQUIRE(static_cast<bool>(output));
    }
}

auto seed_single_worker_store(const std::filesystem::path& data_dir) -> void {
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = data_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    GLYPHA_REQUIRE(opened.has_value());
    GLYPHA_REQUIRE((*opened)->put("keep", bytes("alive")).has_value());
    GLYPHA_REQUIRE((*opened)->close().has_value());
}

} // namespace

GLYPHA_TEST("backup_durable_store copies a verified Store and restores committed keys") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto restored = root.path() / "restored";

    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("alpha", bytes("one")).has_value());
        GLYPHA_REQUIRE((*opened)->put("beta", bytes("two")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    const auto backed = glyphastore::backup_durable_store(source, backup);
    GLYPHA_REQUIRE(backed.has_value());
    GLYPHA_REQUIRE(backed->files_copied >= 2);
    GLYPHA_REQUIRE(backed->admission_fence_ns == 0);
    GLYPHA_REQUIRE(backed->catalog_copy_ns > 0);
    GLYPHA_REQUIRE(backed->destination_verify_ns > 0);
    GLYPHA_REQUIRE(backed->source_crc_scanned);
    GLYPHA_REQUIRE(backed->destination_crc_scanned);
    GLYPHA_REQUIRE(backed->source_verification.scanned_records > 0);
    GLYPHA_REQUIRE(backed->destination_verification.scanned_records > 0);
    GLYPHA_REQUIRE(backed->source_verification.segments.size() ==
                   backed->destination_verification.segments.size());

    const auto restored_copy = glyphastore::restore_durable_store(backup, restored);
    GLYPHA_REQUIRE(restored_copy.has_value());

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("alpha")) == "one");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("beta")) == "two");
}

GLYPHA_TEST("backup_durable_store fails closed when the source is locked") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("live", bytes("v")).has_value());

        const auto contested = glyphastore::backup_durable_store(source, backup);
        GLYPHA_REQUIRE(!contested.has_value());
        GLYPHA_REQUIRE(contested.error().code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
}

GLYPHA_TEST("backup_durable_store refuses a non-empty destination") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto destination = root.path() / "destination";
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("k", bytes("v")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    {
        auto occupied = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = destination,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(occupied.has_value());
        GLYPHA_REQUIRE((*occupied)->put("other", bytes("x")).has_value());
        GLYPHA_REQUIRE((*occupied)->close().has_value());
    }

    const auto refused = glyphastore::backup_durable_store(source, destination);
    GLYPHA_REQUIRE(!refused.has_value());
    GLYPHA_REQUIRE(refused.error().code == glyphastore::ErrorCode::sequence_conflict ||
                   refused.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("Store::backup_to copies while the Store remains open under writer fence") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto restored = root.path() / "restored";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = source,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("alpha", bytes("one")).has_value());
    GLYPHA_REQUIRE(store.put("beta", bytes("two")).has_value());

    // Offline path still fails closed against the live lock.
    const auto contested = glyphastore::backup_durable_store(source, root.path() / "offline");
    GLYPHA_REQUIRE(!contested.has_value());
    GLYPHA_REQUIRE(contested.error().code == glyphastore::ErrorCode::io_error);

    std::error_code ec;
    std::filesystem::create_directories(backup, ec);
    GLYPHA_REQUIRE(!ec);

    // Single-threaded online backup first (Store stays open; flock retained).
    {
        const auto dest = backup / "solo";
        const auto backed = store.backup_to(dest);
        GLYPHA_REQUIRE(backed.has_value());
        GLYPHA_REQUIRE(backed->files_copied >= 2);
        GLYPHA_REQUIRE(backed->admission_fence_ns > 0);
        GLYPHA_REQUIRE(backed->catalog_copy_ns > 0);
        GLYPHA_REQUIRE(backed->destination_verify_ns > 0);
        // Online fence uses structural source check only; CRC scan is on the destination.
        GLYPHA_REQUIRE(!backed->source_crc_scanned);
        GLYPHA_REQUIRE(backed->destination_crc_scanned);
        GLYPHA_REQUIRE(backed->destination_verification.segments.size() >= 1);
    }
    GLYPHA_REQUIRE(store.put("gamma", bytes("three")).has_value());

    std::atomic_bool stop{false};
    std::atomic_uint64_t writes{0};
    std::thread writer{[&] {
        std::uint64_t i = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const auto key = "w" + std::to_string(i++);
            if (store.put(key, bytes("v")).has_value()) {
                writes.fetch_add(1, std::memory_order_relaxed);
                writes.notify_one();
            } else {
                // Admission fence during backup: brief pause then retry.
                std::this_thread::yield();
            }
        }
    }};

    writes.wait(0, std::memory_order_relaxed);

    for (int round = 0; round < 3; ++round) {
        const auto dest = backup / ("round-" + std::to_string(round));
        const auto backed = store.backup_to(dest);
        GLYPHA_REQUIRE(backed.has_value());
        GLYPHA_REQUIRE(backed->files_copied >= 2);
    }

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    GLYPHA_REQUIRE(writes.load(std::memory_order_relaxed) > 0);

    const auto final_backup = store.backup_to(backup / "final");
    GLYPHA_REQUIRE(final_backup.has_value());
    GLYPHA_REQUIRE(store.close().has_value());

    const auto restored_copy = glyphastore::restore_durable_store(backup / "final", restored);
    GLYPHA_REQUIRE(restored_copy.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("alpha")) == "one");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("beta")) == "two");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("gamma")) == "three");
}

GLYPHA_TEST("Server::backup_to copies a live durable daemon catalog") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto restored = root.path() / "restored";

    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = source,
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto& client = *connected;
    GLYPHA_REQUIRE(client.put("daemon-key", "daemon-value").committed());

    std::error_code ec;
    std::filesystem::create_directories(backup, ec);
    GLYPHA_REQUIRE(!ec);
    const auto dest = backup / "live";
    const auto backed = server.backup_to(dest);
    GLYPHA_REQUIRE(backed.has_value());
    GLYPHA_REQUIRE(backed->files_copied >= 2);

    const auto contested = glyphastore::backup_durable_store(source, backup / "offline-contested");
    GLYPHA_REQUIRE(!contested.has_value());

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_copy = glyphastore::restore_durable_store(dest, restored);
    GLYPHA_REQUIRE(restored_copy.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("daemon-key")) == "daemon-value");
}

GLYPHA_TEST("Client::backup copies a live durable daemon catalog") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto dest = root.path() / "client-backup";
    const auto restored = root.path() / "restored";

    auto opened =
        glyphastore::server::Server::create({.port = 0, .maximum_connections = 4},
                                            {.worker_config = {.explicit_count = 1},
                                             .storage_mode = glyphastore::StorageMode::durable_sync,
                                             .data_directory = source,
                                             .durable_open_mode = glyphastore::DurableOpenMode::create_new});
    GLYPHA_REQUIRE(opened.has_value());
    auto& server = **opened;
    GLYPHA_REQUIRE(server.start().has_value());

    auto connected = glyphastore::client::Client::connect({.port = server.port()});
    GLYPHA_REQUIRE(connected.has_value());
    auto& client = *connected;
    GLYPHA_REQUIRE(client.put("client-backup-key", "client-backup-value").committed());

    auto backed = client.backup(dest.string());
    GLYPHA_REQUIRE(backed.has_value());
    const auto report = std::string_view{reinterpret_cast<const char*>(backed->data()), backed->size()};
    GLYPHA_REQUIRE(report.find("status=ok") != std::string_view::npos);

    server.request_stop();
    GLYPHA_REQUIRE(server.join().has_value());

    const auto restored_copy = glyphastore::restore_durable_store(dest, restored);
    GLYPHA_REQUIRE(restored_copy.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("client-backup-key")) == "client-backup-value");
}

GLYPHA_TEST("backup_durable_store copies multi-Worker catalogs with parallel Segment workers") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto restored = root.path() / "restored";

    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 4},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        for (int i = 0; i < 64; ++i) {
            const auto key = "k" + std::to_string(i);
            GLYPHA_REQUIRE((*opened)->put(key, bytes("v")).has_value());
        }
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    const auto backed = glyphastore::backup_durable_store(source, backup);
    GLYPHA_REQUIRE(backed.has_value());
    GLYPHA_REQUIRE(backed->source_verification.segments.size() >= 2);
    GLYPHA_REQUIRE(backed->segment_copy_workers >= 2);
    GLYPHA_REQUIRE(backed->segment_copy_workers <= backed->source_verification.segments.size());
    GLYPHA_REQUIRE(backed->files_copied == backed->source_verification.segments.size() + 1);
    GLYPHA_REQUIRE(backed->destination_verification.segments.size() ==
                   backed->source_verification.segments.size());

    const auto restored_copy = glyphastore::restore_durable_store(backup, restored);
    GLYPHA_REQUIRE(restored_copy.has_value());
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 4},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("k0")) == "v");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("k63")) == "v");
}

GLYPHA_TEST("incomplete backup destination fails verify and leaves source healthy (HAZ-021)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto complete = root.path() / "complete";
    const auto incomplete = root.path() / "incomplete";
    const auto restored = root.path() / "restored";

    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("keep", bytes("alive")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    const auto backed = glyphastore::backup_durable_store(source, complete);
    GLYPHA_REQUIRE(backed.has_value());

    std::error_code ec;
    std::filesystem::create_directories(incomplete, ec);
    GLYPHA_REQUIRE(!ec);
    // Simulate crash mid-copy: Segment file present, Manifest missing.
    for (const auto& entry : std::filesystem::directory_iterator{complete}) {
        if (entry.path().filename() == glyphastore::kManifestFilename) {
            continue;
        }
        std::filesystem::copy_file(entry.path(), incomplete / entry.path().filename(), ec);
        GLYPHA_REQUIRE(!ec);
    }
    GLYPHA_REQUIRE(!std::filesystem::exists(incomplete / glyphastore::kManifestFilename));

    const auto verified = glyphastore::verify_durable_store_path(incomplete);
    GLYPHA_REQUIRE(!verified.has_value());

    const auto restore_refused = glyphastore::restore_durable_store(incomplete, restored);
    GLYPHA_REQUIRE(!restore_refused.has_value());

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = source,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("keep")) == "alive");
}

GLYPHA_TEST("failed online backup leaves the live Store usable (HAZ-021)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto bad_destination = root.path() / "not-a-directory";

    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = source,
        .durable_open_mode = glyphastore::DurableOpenMode::create_new,
    });
    GLYPHA_REQUIRE(opened.has_value());
    auto& store = **opened;
    GLYPHA_REQUIRE(store.put("before", bytes("1")).has_value());

    {
        std::ofstream blocker{bad_destination};
        blocker << "occupied";
        GLYPHA_REQUIRE(static_cast<bool>(blocker));
    }

    const auto failed = store.backup_to(bad_destination);
    GLYPHA_REQUIRE(!failed.has_value());

    GLYPHA_REQUIRE(store.put("after", bytes("2")).has_value());
    GLYPHA_REQUIRE(value_string(*store.get("before")) == "1");
    GLYPHA_REQUIRE(value_string(*store.get("after")) == "2");
    GLYPHA_REQUIRE(store.close().has_value());
}

GLYPHA_TEST("future Manifest version fails closed on verify restore and open (HAZ-022)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto future = root.path() / "future";
    const auto restored = root.path() / "restored";

    seed_single_worker_store(source);
    const auto backed = glyphastore::backup_durable_store(source, backup);
    GLYPHA_REQUIRE(backed.has_value());

    std::error_code ec;
    std::filesystem::create_directories(future, ec);
    GLYPHA_REQUIRE(!ec);
    for (const auto& entry : std::filesystem::directory_iterator{backup}) {
        std::filesystem::copy_file(entry.path(), future / entry.path().filename(), ec);
        GLYPHA_REQUIRE(!ec);
    }

    const auto manifest_path = future / glyphastore::kManifestFilename;
    auto manifest_bytes = read_file_bytes(manifest_path);
    GLYPHA_REQUIRE(manifest_bytes.size() >= glyphastore::kManifestHeaderBytes);
    // Correctly checksummed future Manifest format version (STORE-FUTURE-REQUIRED).
    put_u16(manifest_bytes, 4, 2);
    refresh_manifest_checksum(manifest_bytes);
    write_file_bytes(manifest_path, manifest_bytes);

    const auto verified = glyphastore::verify_durable_store_path(future);
    GLYPHA_REQUIRE(!verified.has_value());
    GLYPHA_REQUIRE(verified.error().code == glyphastore::ErrorCode::invalid_record ||
                   verified.error().code == glyphastore::ErrorCode::corrupted_data);

    const auto restore_refused = glyphastore::restore_durable_store(future, restored);
    GLYPHA_REQUIRE(!restore_refused.has_value());

    auto opened_future = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = future,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(!opened_future.has_value());

    auto reopened_source = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = source,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened_source.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened_source)->get("keep")) == "alive");
}

GLYPHA_TEST("future pinned Record version in Manifest fails closed (HAZ-022)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto future = root.path() / "future-record";
    const auto restored = root.path() / "restored";

    seed_single_worker_store(source);
    GLYPHA_REQUIRE(glyphastore::backup_durable_store(source, backup).has_value());

    std::error_code ec;
    std::filesystem::create_directories(future, ec);
    GLYPHA_REQUIRE(!ec);
    for (const auto& entry : std::filesystem::directory_iterator{backup}) {
        std::filesystem::copy_file(entry.path(), future / entry.path().filename(), ec);
        GLYPHA_REQUIRE(!ec);
    }

    const auto manifest_path = future / glyphastore::kManifestFilename;
    auto manifest_bytes = read_file_bytes(manifest_path);
    // Manifest pins Record format at offset 62; bump while keeping checksum valid.
    put_u16(manifest_bytes, 62, 2);
    refresh_manifest_checksum(manifest_bytes);
    write_file_bytes(manifest_path, manifest_bytes);

    GLYPHA_REQUIRE(!glyphastore::verify_durable_store_path(future).has_value());
    GLYPHA_REQUIRE(!glyphastore::restore_durable_store(future, restored).has_value());
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = future,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(!opened.has_value());
}

GLYPHA_TEST("truncated Manifest backup refuses restore and open (HAZ-022)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto truncated = root.path() / "truncated";
    const auto restored = root.path() / "restored";

    seed_single_worker_store(source);
    GLYPHA_REQUIRE(glyphastore::backup_durable_store(source, backup).has_value());

    std::error_code ec;
    std::filesystem::create_directories(truncated, ec);
    GLYPHA_REQUIRE(!ec);
    for (const auto& entry : std::filesystem::directory_iterator{backup}) {
        std::filesystem::copy_file(entry.path(), truncated / entry.path().filename(), ec);
        GLYPHA_REQUIRE(!ec);
    }

    const auto manifest_path = truncated / glyphastore::kManifestFilename;
    auto manifest_bytes = read_file_bytes(manifest_path);
    GLYPHA_REQUIRE(manifest_bytes.size() > glyphastore::kManifestHeaderBytes / 2);
    manifest_bytes.resize(glyphastore::kManifestHeaderBytes / 2);
    write_file_bytes(manifest_path, manifest_bytes);

    GLYPHA_REQUIRE(!glyphastore::verify_durable_store_path(truncated).has_value());
    GLYPHA_REQUIRE(!glyphastore::restore_durable_store(truncated, restored).has_value());
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = truncated,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(!opened.has_value());
}

GLYPHA_TEST("restored Store refuses mismatched Worker count (HAZ-022)") {
    BackupTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto backup = root.path() / "backup";
    const auto restored = root.path() / "restored";

    seed_single_worker_store(source);
    GLYPHA_REQUIRE(glyphastore::backup_durable_store(source, backup).has_value());
    GLYPHA_REQUIRE(glyphastore::restore_durable_store(backup, restored).has_value());

    auto wrong_count = glyphastore::Store::open({
        .worker_config = {.explicit_count = 2},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(!wrong_count.has_value());
    GLYPHA_REQUIRE(wrong_count.error().code == glyphastore::ErrorCode::invalid_argument);

    auto matching = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = restored,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(matching.has_value());
    GLYPHA_REQUIRE(value_string(*(*matching)->get("keep")) == "alive");
}
