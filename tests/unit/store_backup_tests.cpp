#include "glyphastore/client/client.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "glyphastore/persistence/store_backup.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <atomic>
#include <filesystem>
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
