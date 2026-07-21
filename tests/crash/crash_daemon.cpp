#include "glyphastore/client/client.hpp"
#include "glyphastore/store/store.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <netinet/in.h>
#include <optional>
#include <signal.h>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr std::string_view kAckKey{"daemon-ack-key"};
constexpr std::string_view kAckValue{"daemon-ack-value"};

struct Options {
    std::filesystem::path daemon{};
    std::string storage{"durable-sync"};
};

[[nodiscard]] auto parse_options(int argc, char** argv) -> std::optional<Options> {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view arg{argv[index]};
        const auto require_value = [&](const char* flag) -> std::optional<std::string_view> {
            if (arg != flag || index + 1 >= argc) {
                return std::nullopt;
            }
            return std::string_view{argv[++index]};
        };
        if (arg == "--daemon") {
            const auto value = require_value("--daemon");
            if (!value) {
                return std::nullopt;
            }
            options.daemon = *value;
        } else if (arg == "--storage") {
            const auto value = require_value("--storage");
            if (!value) {
                return std::nullopt;
            }
            options.storage = std::string{*value};
        } else if (arg == "--help" || arg == "-h") {
            return std::nullopt;
        } else {
            return std::nullopt;
        }
    }
    if (options.daemon.empty()) {
        return std::nullopt;
    }
    if (options.storage != "durable-sync" && options.storage != "durable-group" &&
        options.storage != "durable-periodic") {
        return std::nullopt;
    }
    return options;
}

void print_usage(const char* program) {
    std::cerr << "usage: " << program
              << " --daemon PATH [--storage durable-sync|durable-group|durable-periodic]\n";
}

[[nodiscard]] auto run_suffix() -> const std::string& {
    static const auto suffix = std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
                               std::to_string(static_cast<unsigned long long>(
                                   std::chrono::steady_clock::now().time_since_epoch().count()));
    return suffix;
}

[[nodiscard]] auto reserve_port() -> std::optional<std::uint16_t> {
    const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
    if (descriptor < 0) {
        return std::nullopt;
    }
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = 0;
    static_cast<void>(::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr));
    if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        static_cast<void>(::close(descriptor));
        return std::nullopt;
    }
    socklen_t size = sizeof(endpoint);
    if (::getsockname(descriptor, reinterpret_cast<sockaddr*>(&endpoint), &size) != 0) {
        static_cast<void>(::close(descriptor));
        return std::nullopt;
    }
    static_cast<void>(::close(descriptor));
    return ntohs(endpoint.sin_port);
}

[[nodiscard]] auto storage_mode(const std::string& storage) -> glyphastore::StorageMode {
    if (storage == "durable-group") {
        return glyphastore::StorageMode::durable_group;
    }
    if (storage == "durable-periodic") {
        return glyphastore::StorageMode::durable_periodic;
    }
    return glyphastore::StorageMode::durable_sync;
}

class DaemonProcess final {
  public:
    DaemonProcess(std::filesystem::path binary, std::filesystem::path data_dir, std::uint16_t port,
                  std::string storage)
        : binary_(std::move(binary)), data_dir_(std::move(data_dir)), port_(port),
          storage_(std::move(storage)) {}

    ~DaemonProcess() {
        stop_kill();
    }

    DaemonProcess(const DaemonProcess&) = delete;
    auto operator=(const DaemonProcess&) -> DaemonProcess& = delete;

    [[nodiscard]] auto start() -> bool {
        if (pid_ > 0) {
            return false;
        }
        const pid_t child = ::fork();
        if (child < 0) {
            std::cerr << "fork failed: " << std::strerror(errno) << '\n';
            return false;
        }
        if (child == 0) {
            const auto port_text = std::to_string(port_);
            const char* argv[] = {binary_.c_str(),
                                  "--bind",
                                  "127.0.0.1",
                                  "--port",
                                  port_text.c_str(),
                                  "--workers",
                                  "1",
                                  "--storage-mode",
                                  storage_.c_str(),
                                  "--data-dir",
                                  data_dir_.c_str(),
                                  "--open-mode",
                                  "open-or-create",
                                  "--quiet",
                                  nullptr};
            ::execv(binary_.c_str(), const_cast<char* const*>(argv));
            std::cerr << "execv failed: " << std::strerror(errno) << '\n';
            std::_Exit(127);
        }
        pid_ = child;
        return true;
    }

    void stop_kill() {
        if (pid_ <= 0) {
            return;
        }
        static_cast<void>(::kill(pid_, SIGKILL));
        int status = 0;
        static_cast<void>(::waitpid(pid_, &status, 0));
        pid_ = -1;
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return port_;
    }

  private:
    std::filesystem::path binary_;
    std::filesystem::path data_dir_;
    std::uint16_t port_{};
    std::string storage_;
    pid_t pid_{-1};
};

[[nodiscard]] auto wait_for_client(const std::uint16_t port, const int timeout_ms = 10'000)
    -> std::optional<glyphastore::client::Client> {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        auto connected = glyphastore::client::Client::connect(
            {.host = "127.0.0.1", .port = port, .connect_timeout_ms = 200, .request_timeout_ms = 2'000});
        if (connected) {
            return std::move(*connected);
        }
        ::usleep(20'000);
    }
    return std::nullopt;
}

[[nodiscard]] auto value_text(const std::span<const std::byte> value) -> std::string_view {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

[[nodiscard]] auto verify_store_recovery(const std::filesystem::path& data_dir,
                                         const std::string& storage) -> bool {
    auto opened = glyphastore::Store::open({
        .worker_config = {.explicit_count = 1},
        .storage_mode = storage_mode(storage),
        .data_directory = data_dir,
        .durable_open_mode = glyphastore::DurableOpenMode::open_existing,
    });
    if (!opened) {
        std::cerr << "Store reopen failed: " << opened.error().message << '\n';
        return false;
    }
    const auto value = (*opened)->get(kAckKey);
    if (!value) {
        std::cerr << "acknowledged key missing after SIGKILL: " << value.error().message << '\n';
        return false;
    }
    if (value_text(value->bytes) != kAckValue) {
        std::cerr << "acknowledged key recovered with unexpected value\n";
        return false;
    }
    if (auto closed = (*opened)->close(); !closed) {
        std::cerr << "Store close failed: " << closed.error().message << '\n';
        return false;
    }
    return true;
}

[[nodiscard]] auto run_post_ack_kill(const Options& options) -> bool {
    const auto root = std::filesystem::temp_directory_path() /
                      ("glyphastore-crash-daemon-" + run_suffix() + "-" + options.storage);
    const auto data_dir = root / "store";
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    std::filesystem::create_directories(data_dir);

    const auto port = reserve_port();
    if (!port) {
        std::cerr << "failed to reserve a loopback port\n";
        return false;
    }

    DaemonProcess daemon{options.daemon, data_dir, *port, options.storage};
    if (!daemon.start()) {
        return false;
    }

    auto client = wait_for_client(daemon.port());
    if (!client) {
        std::cerr << "timed out waiting for glyphastored to accept connections\n";
        return false;
    }

    const auto put = client->put(kAckKey, kAckValue);
    if (!put.committed()) {
        std::cerr << "wire PUT was not acknowledged as committed";
        if (put.error) {
            std::cerr << ": " << put.error->message;
        }
        std::cerr << '\n';
        return false;
    }
    client->close();

    // durable-periodic acknowledges before the flush barrier; wait past the default
    // sync_interval_ms so a completed background flush can make the mutation durable.
    if (options.storage == "durable-periodic") {
        ::usleep(3'000'000);
    }

    // Kill immediately after a committed acknowledgement (or after the periodic flush window);
    // recovery must still see the key for sync/group and for flushed periodic writes.
    daemon.stop_kill();

    if (!verify_store_recovery(data_dir, options.storage)) {
        return false;
    }

    // Prove the same durable directory can serve the key again over the wire.
    DaemonProcess restarted{options.daemon, data_dir, *port, options.storage};
    if (!restarted.start()) {
        return false;
    }
    auto recovered_client = wait_for_client(restarted.port());
    if (!recovered_client) {
        std::cerr << "timed out waiting for restarted glyphastored\n";
        return false;
    }
    const auto get = recovered_client->get(kAckKey);
    if (!get) {
        std::cerr << "wire GET after restart failed: " << get.error().message << '\n';
        return false;
    }
    if (value_text(*get) != kAckValue) {
        std::cerr << "wire GET after restart returned unexpected value\n";
        return false;
    }
    recovered_client->close();
    restarted.stop_kill();

    std::filesystem::remove_all(root, ignored);
    return true;
}

} // namespace

int main(int argc, char** argv) {
    auto options = parse_options(argc, argv);
    if (!options) {
        print_usage(argc > 0 ? argv[0] : "glyphastore_crash_daemon");
        return 2;
    }
    std::cout << "# crash-daemon storage=" << options->storage << " post-ack SIGKILL\n";
    if (!run_post_ack_kill(*options)) {
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}
