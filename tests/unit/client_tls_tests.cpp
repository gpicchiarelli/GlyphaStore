#include "glyphastore/client/client.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/server/tls.hpp"
#include "test.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

class TemporaryDirectory final {
  public:
    TemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-client-tls-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }
    ~TemporaryDirectory() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }
    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto write_self_signed_material(const std::filesystem::path& directory) -> bool {
    const auto key = directory / "server.key";
    const auto cert = directory / "server.crt";
    const auto command = std::string{"openssl req -x509 -newkey rsa:2048 -nodes -keyout '"} + key.string() +
                         "' -out '" + cert.string() + "' -days 1 -subj '/CN=localhost' >/dev/null 2>&1";
    return std::system(command.c_str()) == 0 && std::filesystem::is_regular_file(key) &&
           std::filesystem::is_regular_file(cert);
}

#endif

} // namespace

GLYPHA_TEST("client tls options fail closed when TLS is unavailable or incomplete") {
    glyphastore::client::ClientConfig incomplete{
        .host = "127.0.0.1",
        .tls =
            {
                .enable = true,
                .cert_file = "client.crt",
            },
    };
    const auto opened = glyphastore::client::Client::connect(incomplete);
    GLYPHA_REQUIRE(!opened.has_value());
}

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

GLYPHA_TEST("client connect over TLS can ping") {
    TemporaryDirectory directory;
    if (!write_self_signed_material(directory.path())) {
        return;
    }

    glyphastore::server::ReactorConfig config{
        .port = 0,
        .worker_count = 1,
        .tls =
            {
                .certificate_file = directory.path() / "server.crt",
                .private_key_file = directory.path() / "server.key",
            },
    };
    auto server = glyphastore::server::Server::create(config);
    GLYPHA_REQUIRE(server.has_value());
    GLYPHA_REQUIRE((*server)->start().has_value());
    const auto port = (*server)->port();
    GLYPHA_REQUIRE(port != 0);

    auto client = glyphastore::client::Client::connect({
        .host = "127.0.0.1",
        .port = port,
        .tls =
            {
                .enable = true,
                .ca_file = (directory.path() / "server.crt").string(),
                .server_name = "localhost",
            },
    });
    GLYPHA_REQUIRE(client.has_value());
    const auto payload = std::string_view{"tls-ping"};
    const auto echoed = client->ping({reinterpret_cast<const std::byte*>(payload.data()), payload.size()});
    GLYPHA_REQUIRE(echoed.has_value());
    GLYPHA_REQUIRE(echoed->size() == payload.size());
    GLYPHA_REQUIRE(std::string_view(reinterpret_cast<const char*>(echoed->data()), echoed->size()) ==
                   payload);
    client->close();
    (*server)->request_stop();
}

#else

GLYPHA_TEST("client TLS request reports build without TLS support") {
    glyphastore::client::ClientConfig requested{
        .host = "127.0.0.1",
        .port = 1,
        .tls = {.enable = true},
    };
    const auto opened = glyphastore::client::Client::connect(requested);
    GLYPHA_REQUIRE(!opened.has_value());
    GLYPHA_REQUIRE(opened.error().message.find("without TLS") != std::string::npos);
}

#endif
