#include "glyphastore/persistence/store_repair.hpp"
#include "glyphastore/store/store.hpp"
#include "test.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class RepairTemporaryDirectory final {
  public:
    RepairTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-repair-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~RepairTemporaryDirectory() {
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

void write_private_file(const std::filesystem::path& path, const std::string_view contents) {
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    GLYPHA_REQUIRE(static_cast<bool>(out));
    out << contents;
    GLYPHA_REQUIRE(static_cast<bool>(out));
    GLYPHA_REQUIRE(::chmod(path.c_str(), S_IRUSR | S_IWUSR) == 0);
}

} // namespace

GLYPHA_TEST("repair_durable_store quarantines unlisted Segment and opens a clean store") {
    RepairTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto workspace = root.path() / "workspace";

    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("keep", bytes("value")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }

    write_private_file(source / "segment-00000000000000ff-0000000a.glypha", "orphan-bytes");
    write_private_file(source / "operator-note.txt", "do-not-adopt");

    const auto repaired = glyphastore::repair_durable_store(source, workspace);
    GLYPHA_REQUIRE(repaired.has_value());
    GLYPHA_REQUIRE(repaired->quarantined.size() == 2);
    GLYPHA_REQUIRE(std::filesystem::exists(repaired->quarantine_directory / "audit.txt"));
    GLYPHA_REQUIRE(std::filesystem::exists(repaired->quarantine_directory /
                                           "segment-00000000000000ff-0000000a.glypha"));
    GLYPHA_REQUIRE(std::filesystem::exists(source / "operator-note.txt"));
    GLYPHA_REQUIRE(
        std::filesystem::exists(source / "segment-00000000000000ff-0000000a.glypha"));

    auto reopened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = glyphastore::StorageMode::durable_sync,
        .data_directory = repaired->repaired_store,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    GLYPHA_REQUIRE(reopened.has_value());
    GLYPHA_REQUIRE(value_string(*(*reopened)->get("keep")) == "value");
    GLYPHA_REQUIRE((*reopened)->close().has_value());
}

GLYPHA_TEST("repair_durable_store refuses unsafe symlink entries") {
    RepairTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto workspace = root.path() / "workspace";
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("keep", bytes("value")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    std::filesystem::create_symlink(glyphastore::kManifestFilename, source / "evil-link");
    const auto repaired = glyphastore::repair_durable_store(source, workspace);
    GLYPHA_REQUIRE(!repaired.has_value());
    GLYPHA_REQUIRE(repaired.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(!std::filesystem::exists(workspace / "store"));
}

GLYPHA_TEST("repair_durable_store refuses a non-empty workspace") {
    RepairTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto workspace = root.path() / "workspace";
    std::filesystem::create_directories(workspace);
    write_private_file(workspace / "stale", "x");
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    const auto repaired = glyphastore::repair_durable_store(source, workspace);
    GLYPHA_REQUIRE(!repaired.has_value());
    GLYPHA_REQUIRE(repaired.error().code == glyphastore::ErrorCode::invalid_argument);
}

GLYPHA_TEST("repair_durable_store refuses a missing catalog Segment") {
    RepairTemporaryDirectory root;
    const auto source = root.path() / "source";
    const auto workspace = root.path() / "workspace";
    std::string catalog_segment;
    {
        auto opened = glyphastore::Store::open({
            .worker_config = {.explicit_count = 1},
            .storage_mode = glyphastore::StorageMode::durable_sync,
            .data_directory = source,
            .durable_open_mode = glyphastore::DurableOpenMode::create_new,
        });
        GLYPHA_REQUIRE(opened.has_value());
        GLYPHA_REQUIRE((*opened)->put("keep", bytes("value")).has_value());
        GLYPHA_REQUIRE((*opened)->close().has_value());
    }
    for (const auto& entry : std::filesystem::directory_iterator{source}) {
        const auto name = entry.path().filename().string();
        if (name.starts_with("segment-") && name.ends_with(".glypha")) {
            catalog_segment = name;
            break;
        }
    }
    GLYPHA_REQUIRE(!catalog_segment.empty());
    GLYPHA_REQUIRE(std::filesystem::remove(source / catalog_segment));
    const auto repaired = glyphastore::repair_durable_store(source, workspace);
    GLYPHA_REQUIRE(!repaired.has_value());
    GLYPHA_REQUIRE(repaired.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(!std::filesystem::exists(workspace / "store"));
}
