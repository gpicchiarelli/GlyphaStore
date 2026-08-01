#include "glyphastore/core/key_hash.hpp"
#include "glyphastore/persistence/filesystem_hooks.hpp"
#include "glyphastore/persistence/store_migrate.hpp"
#include "glyphastore/persistence/store_verify.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class MigrateTemporaryDirectory final {
  public:
    MigrateTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-migrate-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~MigrateTemporaryDirectory() {
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

struct FailAfterDestinationCommits {
    std::size_t commits_seen{};
    std::size_t fail_after{};

    static auto before(void* context, const glyphastore::FilesystemOperation operation)
        -> glyphastore::Status {
        auto& state = *static_cast<FailAfterDestinationCommits*>(context);
        if (operation == glyphastore::FilesystemOperation::sync_commit_slot &&
            state.commits_seen >= state.fail_after) {
            return glyphastore::fail(glyphastore::ErrorCode::io_error, "injected migrate interrupt");
        }
        return {};
    }

    static void after(void* context, const glyphastore::FilesystemOperation operation) {
        auto& state = *static_cast<FailAfterDestinationCommits*>(context);
        if (operation == glyphastore::FilesystemOperation::sync_commit_slot) {
            ++state.commits_seen;
        }
    }

    [[nodiscard]] auto hooks() -> glyphastore::FilesystemHooks {
        return {.context = this, .before = &before, .after = &after};
    }
};

[[nodiscard]] auto source_store_id_hex(const std::filesystem::path& source) -> std::string {
    const auto verified = glyphastore::verify_durable_store_path(source);
    GLYPHA_REQUIRE(verified.has_value());
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(verified->manifest.store_id.size() * 2);
    for (const auto byte : verified->manifest.store_id) {
        const auto value = static_cast<unsigned>(byte);
        out.push_back(kDigits[(value >> 4) & 0xf]);
        out.push_back(kDigits[value & 0xf]);
    }
    return out;
}

} // namespace

GLYPHA_TEST("migrate_durable_store reshards live keys from 1 to 4 Workers") {
    MigrateTemporaryDirectory root;
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
        GLYPHA_REQUIRE((*opened)->put("alpha", bytes("one")).has_value());
        GLYPHA_REQUIRE((*opened)->put("beta", bytes("two")).has_value());
        GLYPHA_REQUIRE((*opened)->put("gamma", bytes("three"), 9'000'000'000'000'000'000ULL).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    const auto migrated = glyphastore::migrate_durable_store(source, destination, 4);
    GLYPHA_REQUIRE(migrated.has_value());
    GLYPHA_REQUIRE(migrated->source_worker_count == 1);
    GLYPHA_REQUIRE(migrated->target_worker_count == 4);
    GLYPHA_REQUIRE(migrated->keys_copied == 3);
    GLYPHA_REQUIRE(!migrated->resumed);
    GLYPHA_REQUIRE(!std::filesystem::exists(migrated->checkpoint));

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 4},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->worker_count() == 4);
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("alpha")) == "one");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("beta")) == "two");
    const auto gamma = (*reopened)->get("gamma");
    GLYPHA_REQUIRE(gamma.has_value());
    GLYPHA_REQUIRE(value_string(*gamma) == "three");
    GLYPHA_REQUIRE(gamma->expire_at_ns == 9'000'000'000'000'000'000ULL);
    GLYPHA_REQUIRE(glyphastore::route_worker("alpha", 4) ==
                   glyphastore::route_worker(glyphastore::hash_key("alpha"), 4));
    GLYPHA_REQUIRE((*reopened)->close().has_value());

    auto wrong_count = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(!wrong_count.has_value());
    GLYPHA_REQUIRE(wrong_count.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("migrate_durable_store resumes from a sibling checkpoint") {
    MigrateTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto destination = root.path() / "destination";

    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("a", bytes("1")).has_value());
        GLYPHA_REQUIRE((*opened)->put("b", bytes("2")).has_value());
        GLYPHA_REQUIRE((*opened)->put("c", bytes("3")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    const auto first = glyphastore::migrate_durable_store(source, destination, 3);
    GLYPHA_REQUIRE(first.has_value());

    // Simulate an interrupted run: recreate partial destination + checkpoint after key "a".
    std::error_code ignored;
    std::filesystem::remove_all(destination, ignored);
    {
        auto partial = glyphastore::Store::open({
            .worker_config = {.explicit_count = 3},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = destination,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(partial.has_value());
        GLYPHA_REQUIRE((*partial)->put("a", bytes("1")).has_value());
        GLYPHA_REQUIRE((*partial)->close().has_value());
    }
    const auto verified = glyphastore::verify_durable_store_path(source);
    GLYPHA_REQUIRE(verified.has_value());
    const auto checkpoint = glyphastore::migrate_checkpoint_path(destination);
    {
        std::ofstream out{checkpoint};
        GLYPHA_REQUIRE(static_cast<bool>(out));
        out << "GlyphaStore/migrate-state/1\n"
            << "source_store_id=";
        static constexpr char kDigits[] = "0123456789abcdef";
        for (const auto byte : verified->manifest.store_id) {
            const auto value = static_cast<unsigned>(byte);
            out << kDigits[(value >> 4) & 0xf] << kDigits[value & 0xf];
        }
        out << "\nsource_worker_count=2\ntarget_worker_count=3\nkeys_copied=1\n"
            << "last_key_hex=61\nphase=copying\n";
    }

    const auto resumed = glyphastore::migrate_durable_store(source, destination, 3);
    GLYPHA_REQUIRE(resumed.has_value());
    GLYPHA_REQUIRE(resumed->resumed);
    GLYPHA_REQUIRE(resumed->keys_skipped >= 1);

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 3},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("a")) == "1");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("b")) == "2");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("c")) == "3");
    GLYPHA_REQUIRE(!std::filesystem::exists(checkpoint));
}

GLYPHA_TEST("migrate_durable_store refuses a locked source and occupied destination") {
    MigrateTemporaryDirectory root;
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

        const auto contested = glyphastore::migrate_durable_store(source, destination, 2);
        GLYPHA_REQUIRE(!contested.has_value());
        GLYPHA_REQUIRE(contested.error().code == glyphastore::ErrorCode::io_error);
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    {
        auto occupied = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = destination,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(occupied.has_value());
        GLYPHA_REQUIRE((*occupied)->put("x", bytes("y")).has_value());
        GLYPHA_REQUIRE((*occupied)->close().has_value());
    }
    const auto refused = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(!refused.has_value());
    GLYPHA_REQUIRE(refused.error().code == glyphastore::ErrorCode::sequence_conflict ||
                   refused.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("migrate_durable_store downscales from 4 to 1 Workers") {
    MigrateTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto destination = root.path() / "destination";
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 4},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        for (int index = 0; index < 16; ++index) {
            const auto key = "key-" + std::to_string(index);
            GLYPHA_REQUIRE((*opened)->put(key, bytes(key)).has_value());
        }
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    const auto migrated = glyphastore::migrate_durable_store(source, destination, 1);
    GLYPHA_REQUIRE(migrated.has_value());
    GLYPHA_REQUIRE(migrated->keys_copied == 16);
    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE((*reopened)->worker_count() == 1);
    for (int index = 0; index < 16; ++index) {
        const auto key = "key-" + std::to_string(index);
        GLYPHA_REQUIRE(value_string(*(*reopened)->get(key)) == key);
    }
}

GLYPHA_TEST("migrate_durable_store resumes after an injected mid-copy interrupt") {
    MigrateTemporaryDirectory root;
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
        GLYPHA_REQUIRE((*opened)->put("a", bytes("1")).has_value());
        GLYPHA_REQUIRE((*opened)->put("b", bytes("2")).has_value());
        GLYPHA_REQUIRE((*opened)->put("c", bytes("3")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    FailAfterDestinationCommits interrupt{.fail_after = 1};
    const auto interrupted =
        glyphastore::migrate_durable_store(source, destination, 2, true, {}, interrupt.hooks());
    GLYPHA_REQUIRE(!interrupted.has_value());
    GLYPHA_REQUIRE(interrupted.error().code == glyphastore::ErrorCode::io_error);
    GLYPHA_REQUIRE(std::filesystem::exists(glyphastore::migrate_checkpoint_path(destination)));
    // Destination partially populated and checkpoint present.
    GLYPHA_REQUIRE(std::filesystem::exists(destination));

    const auto resumed = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(resumed.has_value());
    GLYPHA_REQUIRE(resumed->resumed);
    GLYPHA_REQUIRE(resumed->keys_skipped >= 1);
    GLYPHA_REQUIRE(!std::filesystem::exists(resumed->checkpoint));

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 2},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("a")) == "1");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("b")) == "2");
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("c")) == "3");
}

GLYPHA_TEST("migrate_durable_store refuses corrupt or mismatched checkpoints") {
    MigrateTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto destination = root.path() / "destination";
    const auto checkpoint = glyphastore::migrate_checkpoint_path(destination);

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

    // Checkpoint without destination.
    {
        std::ofstream out{checkpoint};
        GLYPHA_REQUIRE(static_cast<bool>(out));
        out << "GlyphaStore/migrate-state/1\n"
            << "source_store_id=" << source_store_id_hex(source) << '\n'
            << "source_worker_count=1\ntarget_worker_count=2\nkeys_copied=0\nphase=copying\n";
    }
    const auto missing_dest = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(!missing_dest.has_value());
    GLYPHA_REQUIRE(missing_dest.error().code == glyphastore::ErrorCode::invalid_argument);

    std::error_code ignored;
    std::filesystem::remove(checkpoint, ignored);

    // Occupied destination without checkpoint already covered; add corrupt magic with empty dest.
    {
        std::ofstream out{checkpoint};
        GLYPHA_REQUIRE(static_cast<bool>(out));
        out << "GlyphaStore/migrate-state/999\n"
            << "source_store_id=" << source_store_id_hex(source) << '\n'
            << "source_worker_count=1\ntarget_worker_count=2\nkeys_copied=0\n";
    }
    const auto bad_magic = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(!bad_magic.has_value());
    GLYPHA_REQUIRE(bad_magic.error().code == glyphastore::ErrorCode::invalid_argument);
    std::filesystem::remove(checkpoint, ignored);

    // Partial destination + worker-count mismatch in checkpoint.
    {
        auto partial = glyphastore::Store::open({
            .worker_config = {.explicit_count = 2},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = destination,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(partial.has_value());
        GLYPHA_REQUIRE((*partial)->put("k", bytes("v")).has_value());
        GLYPHA_REQUIRE((*partial)->close().has_value());
    }
    {
        std::ofstream out{checkpoint};
        GLYPHA_REQUIRE(static_cast<bool>(out));
        out << "GlyphaStore/migrate-state/1\n"
            << "source_store_id=" << source_store_id_hex(source) << '\n'
            << "source_worker_count=1\ntarget_worker_count=3\nkeys_copied=1\n"
            << "last_key_hex=6b\nphase=copying\n";
    }
    const auto mismatch = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(!mismatch.has_value());
    GLYPHA_REQUIRE(mismatch.error().code == glyphastore::ErrorCode::invalid_argument);

    // Source identity mismatch.
    {
        std::ofstream out{checkpoint};
        GLYPHA_REQUIRE(static_cast<bool>(out));
        out << "GlyphaStore/migrate-state/1\n"
            << "source_store_id=00000000000000000000000000000000\n"
            << "source_worker_count=1\ntarget_worker_count=2\nkeys_copied=1\n"
            << "last_key_hex=6b\nphase=copying\n";
    }
    const auto wrong_source = glyphastore::migrate_durable_store(source, destination, 2);
    GLYPHA_REQUIRE(!wrong_source.has_value());
    GLYPHA_REQUIRE(wrong_source.error().code == glyphastore::ErrorCode::invalid_argument);
}
