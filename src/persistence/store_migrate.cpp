#include "glyphastore/persistence/store_migrate.hpp"

#include "glyphastore/store/store.hpp"
#include "store/store_internal.hpp"

#include <charconv>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace glyphastore {
namespace {

constexpr std::string_view kCheckpointMagic = "GlyphaStore/migrate-state/1";

[[nodiscard]] auto to_hex(const StoreId& store_id) -> std::string {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(store_id.size() * 2, '\0');
    for (std::size_t index = 0; index < store_id.size(); ++index) {
        const auto value = static_cast<unsigned>(store_id[index]);
        out[index * 2] = kDigits[(value >> 4) & 0xf];
        out[index * 2 + 1] = kDigits[value & 0xf];
    }
    return out;
}

[[nodiscard]] auto key_to_hex(const std::string_view key) -> std::string {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(key.size() * 2, '\0');
    for (std::size_t index = 0; index < key.size(); ++index) {
        const auto value = static_cast<unsigned char>(key[index]);
        out[index * 2] = kDigits[(value >> 4) & 0xf];
        out[index * 2 + 1] = kDigits[value & 0xf];
    }
    return out;
}

[[nodiscard]] auto hex_to_key(const std::string_view hex) -> Result<std::string> {
    if ((hex.size() % 2U) != 0U) {
        return fail(ErrorCode::invalid_argument, "migrate checkpoint last_key_hex length is not even");
    }
    std::string key;
    key.resize(hex.size() / 2U);
    const auto nibble = [](const char ch) -> int {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        }
        if (ch >= 'a' && ch <= 'f') {
            return 10 + (ch - 'a');
        }
        if (ch >= 'A' && ch <= 'F') {
            return 10 + (ch - 'A');
        }
        return -1;
    };
    for (std::size_t index = 0; index < key.size(); ++index) {
        const int high = nibble(hex[index * 2]);
        const int low = nibble(hex[index * 2 + 1]);
        if (high < 0 || low < 0) {
            return fail(ErrorCode::invalid_argument, "migrate checkpoint last_key_hex is not hexadecimal");
        }
        key[index] = static_cast<char>((high << 4) | low);
    }
    return key;
}

[[nodiscard]] auto parse_u64_field(const std::string_view text, const std::string_view field)
    -> Result<std::uint64_t> {
    if (text.empty()) {
        return fail(ErrorCode::invalid_argument, "migrate checkpoint numeric field is empty");
    }
    std::uint64_t value{};
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
        return fail(ErrorCode::invalid_argument,
                    std::string{"migrate checkpoint field is not an integer: "} + std::string{field});
    }
    return value;
}

struct MigrateCheckpoint {
    std::string source_store_id_hex;
    std::size_t source_worker_count{};
    std::size_t target_worker_count{};
    std::string last_key;
    std::uint64_t keys_copied{};
    bool has_last_key{};
};

[[nodiscard]] auto parse_checkpoint(const std::filesystem::path& path) -> Result<MigrateCheckpoint> {
    std::ifstream input{path};
    if (!input) {
        return fail(ErrorCode::io_error, "unable to read migrate checkpoint");
    }
    std::string line;
    if (!std::getline(input, line) || line != kCheckpointMagic) {
        return fail(ErrorCode::invalid_argument, "migrate checkpoint magic is unsupported");
    }
    MigrateCheckpoint checkpoint{};
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const auto separator = line.find('=');
        if (separator == std::string::npos) {
            return fail(ErrorCode::invalid_argument, "migrate checkpoint line is malformed");
        }
        const auto key = line.substr(0, separator);
        const auto value = std::string_view{line}.substr(separator + 1);
        if (key == "source_store_id") {
            checkpoint.source_store_id_hex = std::string{value};
        } else if (key == "source_worker_count") {
            auto parsed = parse_u64_field(value, key);
            if (!parsed) {
                return unexpected(parsed.error());
            }
            checkpoint.source_worker_count = static_cast<std::size_t>(*parsed);
        } else if (key == "target_worker_count") {
            auto parsed = parse_u64_field(value, key);
            if (!parsed) {
                return unexpected(parsed.error());
            }
            checkpoint.target_worker_count = static_cast<std::size_t>(*parsed);
        } else if (key == "keys_copied") {
            auto parsed = parse_u64_field(value, key);
            if (!parsed) {
                return unexpected(parsed.error());
            }
            checkpoint.keys_copied = *parsed;
        } else if (key == "last_key_hex") {
            auto decoded = hex_to_key(value);
            if (!decoded) {
                return unexpected(decoded.error());
            }
            checkpoint.last_key = std::move(*decoded);
            checkpoint.has_last_key = true;
        } else if (key == "phase") {
        } else {
            return fail(ErrorCode::invalid_argument, "migrate checkpoint contains an unknown field");
        }
    }
    if (checkpoint.source_store_id_hex.empty() || checkpoint.source_worker_count == 0 ||
        checkpoint.target_worker_count == 0) {
        return fail(ErrorCode::invalid_argument, "migrate checkpoint is incomplete");
    }
    return checkpoint;
}

[[nodiscard]] auto write_checkpoint(const std::filesystem::path& path, const StoreId& source_store_id,
                                    const std::size_t source_worker_count,
                                    const std::size_t target_worker_count, const std::string_view last_key,
                                    const std::uint64_t keys_copied) -> Status {
    const auto temporary = std::filesystem::path{path.string() + ".tmp"};
    {
        std::ofstream output{temporary, std::ios::trunc | std::ios::binary};
        if (!output) {
            return fail(ErrorCode::io_error, "unable to create migrate checkpoint temporary");
        }
        output << kCheckpointMagic << '\n'
               << "source_store_id=" << to_hex(source_store_id) << '\n'
               << "source_worker_count=" << source_worker_count << '\n'
               << "target_worker_count=" << target_worker_count << '\n'
               << "keys_copied=" << keys_copied << '\n'
               << "last_key_hex=" << key_to_hex(last_key) << '\n'
               << "phase=copying\n";
        if (!output) {
            return fail(ErrorCode::io_error, "unable to write migrate checkpoint temporary");
        }
    }
    std::error_code error;
    std::filesystem::rename(temporary, path, error);
    if (error) {
        return fail(ErrorCode::io_error, "unable to publish migrate checkpoint");
    }
    return {};
}

[[nodiscard]] auto directory_exists_nonempty(const std::filesystem::path& path) -> bool {
    std::error_code error;
    if (!std::filesystem::exists(path, error) || error) {
        return false;
    }
    if (!std::filesystem::is_directory(path, error) || error) {
        return true;
    }
    const auto iterator = std::filesystem::directory_iterator{path, error};
    if (error) {
        return true;
    }
    return iterator != std::filesystem::directory_iterator{};
}

} // namespace

auto migrate_durable_store(const std::filesystem::path& source, const std::filesystem::path& destination,
                           const std::size_t target_worker_count, const bool scan_records,
                           const DurableResourceLimits& limits) -> Result<DurableStoreMigrateReport> {
    if (source.empty() || destination.empty()) {
        return fail(ErrorCode::invalid_argument, "migrate source and destination paths are required");
    }
    if (source == destination) {
        return fail(ErrorCode::invalid_argument, "migrate source and destination must differ");
    }
    if (target_worker_count == 0 || target_worker_count > kMaximumWorkerCount) {
        return fail(ErrorCode::invalid_argument, "migrate target Worker count is out of range");
    }

    DurableStoreMigrateReport report{.source = source,
                                     .destination = destination,
                                     .checkpoint = migrate_checkpoint_path(destination),
                                     .target_worker_count = target_worker_count};

    auto source_verification = verify_durable_store_path(source, scan_records, limits);
    if (!source_verification) {
        return unexpected(source_verification.error());
    }
    report.source_verification = std::move(*source_verification);
    report.source_worker_count = report.source_verification.manifest.worker_count;

    const auto source_store_id = report.source_verification.manifest.store_id;
    const auto source_store_id_hex = to_hex(source_store_id);

    std::optional<MigrateCheckpoint> resume;
    std::error_code checkpoint_error;
    const bool checkpoint_exists = std::filesystem::exists(report.checkpoint, checkpoint_error);
    if (checkpoint_error) {
        return fail(ErrorCode::io_error, "unable to inspect migrate checkpoint path");
    }
    if (checkpoint_exists) {
        auto parsed = parse_checkpoint(report.checkpoint);
        if (!parsed) {
            return unexpected(parsed.error());
        }
        if (parsed->source_store_id_hex != source_store_id_hex ||
            parsed->source_worker_count != report.source_worker_count ||
            parsed->target_worker_count != target_worker_count) {
            return fail(ErrorCode::invalid_argument,
                        "migrate checkpoint disagrees with source identity or target Worker count");
        }
        resume = std::move(*parsed);
        report.resumed = true;
        report.keys_copied = resume->keys_copied;
    }

    const bool destination_occupied = directory_exists_nonempty(destination);
    if (destination_occupied && !resume) {
        return fail(ErrorCode::sequence_conflict,
                    "migrate destination exists without a matching checkpoint; refuse to append");
    }
    if (!destination_occupied && resume) {
        return fail(ErrorCode::invalid_argument,
                    "migrate checkpoint exists but destination Store is missing");
    }

    auto source_store = Store::open({
        .worker_config = {.explicit_count = report.source_worker_count},
        .storage_mode = StorageMode::durable_sync,
        .data_directory = source,
        .durable_open_mode = DurableOpenMode::open_existing,
        .durable_limits = limits,
        .maintenance = {.mode = MaintenanceMode::disabled},
    });
    if (!source_store) {
        return unexpected(source_store.error());
    }

    auto keys = detail::StoreAccess::snapshot_live_keys(**source_store);
    if (!keys) {
        (void)(*source_store)->close();
        return unexpected(keys.error());
    }

    const auto destination_mode =
        resume ? DurableOpenMode::open_existing : DurableOpenMode::create_new;
    auto destination_store = Store::open({
        .worker_config = {.explicit_count = target_worker_count},
        .storage_mode = StorageMode::durable_sync,
        .data_directory = destination,
        .durable_open_mode = destination_mode,
        .durable_limits = limits,
        .maintenance = {.mode = MaintenanceMode::disabled},
    });
    if (!destination_store) {
        (void)(*source_store)->close();
        return unexpected(destination_store.error());
    }

    const std::string* resume_last = (resume && resume->has_last_key) ? &resume->last_key : nullptr;

    for (const auto& key : *keys) {
        if (resume_last != nullptr && key < *resume_last) {
            ++report.keys_skipped;
            continue;
        }
        const bool rewriting_checkpoint_key = resume_last != nullptr && key == *resume_last;

        auto value = (*source_store)->get(key);
        if (!value) {
            if (value.error().code == ErrorCode::not_found) {
                continue;
            }
            (void)(*destination_store)->close();
            (void)(*source_store)->close();
            return unexpected(value.error());
        }
        if (auto put = (*destination_store)->put(key, value->view(), value->expire_at_ns); !put) {
            (void)(*destination_store)->close();
            (void)(*source_store)->close();
            return unexpected(put.error());
        }
        if (!rewriting_checkpoint_key) {
            ++report.keys_copied;
        } else {
            ++report.keys_skipped;
        }
        report.bytes_copied += static_cast<std::uint64_t>(value->bytes.size());
        if (auto checkpointed = write_checkpoint(report.checkpoint, source_store_id,
                                                 report.source_worker_count, target_worker_count, key,
                                                 report.keys_copied);
            !checkpointed) {
            (void)(*destination_store)->close();
            (void)(*source_store)->close();
            return unexpected(checkpointed.error());
        }
        resume_last = nullptr;
    }

    if (auto flushed = (*destination_store)->flush(); !flushed) {
        (void)(*destination_store)->close();
        (void)(*source_store)->close();
        return unexpected(flushed.error());
    }
    if (auto closed = (*destination_store)->close(); !closed) {
        (void)(*source_store)->close();
        return unexpected(closed.error());
    }
    if (auto closed = (*source_store)->close(); !closed) {
        return unexpected(closed.error());
    }

    auto destination_verification = verify_durable_store_path(destination, scan_records, limits);
    if (!destination_verification) {
        return unexpected(destination_verification.error());
    }
    report.destination_verification = std::move(*destination_verification);
    if (report.destination_verification.manifest.worker_count != target_worker_count) {
        return fail(ErrorCode::internal_error, "migrated Store Worker count disagrees with target");
    }

    std::error_code remove_error;
    std::filesystem::remove(report.checkpoint, remove_error);
    if (remove_error && remove_error != std::errc::no_such_file_or_directory) {
        return fail(ErrorCode::io_error, "migrate succeeded but checkpoint could not be removed");
    }
    return report;
}

} // namespace glyphastore
