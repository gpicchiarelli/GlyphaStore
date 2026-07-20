#include "glyphastore/server/protocol.hpp"
#include "glyphastore/server/server.hpp"
#include "glyphastore/server/tls.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS
#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

class TemporaryDirectory final {
  public:
    TemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-tls-XXXXXX").string();
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
    const auto command = std::string{"openssl req -x509 -newkey rsa:2048 -nodes -keyout '"} +
                         key.string() + "' -out '" + cert.string() +
                         "' -days 1 -subj '/CN=localhost' >/dev/null 2>&1";
    return std::system(command.c_str()) == 0 && std::filesystem::is_regular_file(key) &&
           std::filesystem::is_regular_file(cert);
}

auto send_all_ssl(SSL* ssl, const std::span<const std::byte> data) -> bool {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto written =
            SSL_write(ssl, data.data() + static_cast<std::ptrdiff_t>(sent), static_cast<int>(data.size() - sent));
        if (written <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(written);
    }
    return true;
}

auto receive_exact_ssl(SSL* ssl, const std::span<std::byte> output) -> bool {
    std::size_t received = 0;
    while (received < output.size()) {
        const auto count = SSL_read(ssl, output.data() + static_cast<std::ptrdiff_t>(received),
                                    static_cast<int>(output.size() - received));
        if (count <= 0) {
            return false;
        }
        received += static_cast<std::size_t>(count);
    }
    return true;
}

auto receive_response_ssl(SSL* ssl) -> std::vector<std::byte> {
    std::array<std::byte, glyphastore::server::kResponseHeaderBytes> header{};
    if (!receive_exact_ssl(ssl, header)) {
        return {};
    }
    std::uint32_t frame_size = 0;
    for (std::size_t byte = 0; byte < 4; ++byte) {
        frame_size |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(header[byte])) << (byte * 8U);
    }
    if (frame_size < header.size() || frame_size > glyphastore::server::kMaxFrameBytes) {
        return {};
    }
    std::vector<std::byte> frame(frame_size);
    std::copy(header.begin(), header.end(), frame.begin());
    if (!receive_exact_ssl(ssl, std::span<std::byte>{frame}.subspan(header.size()))) {
        return {};
    }
    return frame;
}

#endif

} // namespace

GLYPHA_TEST("tls config validation fails closed without cert and key") {
    glyphastore::server::TlsConfig incomplete{.certificate_file = "cert.pem"};
    const auto missing_key = glyphastore::server::validate_tls_config(incomplete);
    GLYPHA_REQUIRE(!missing_key.has_value());

    glyphastore::server::TlsConfig ca_only{.client_ca_file = "ca.pem"};
    const auto missing_pair = glyphastore::server::validate_tls_config(ca_only);
    GLYPHA_REQUIRE(!missing_pair.has_value());

    GLYPHA_REQUIRE(glyphastore::server::validate_tls_config({}).has_value());
}

GLYPHA_TEST("tls build reports backend availability") {
    const auto backend = glyphastore::server::tls_backend_name();
    GLYPHA_REQUIRE(!backend.empty());
    if (glyphastore::server::tls_build_enabled()) {
        GLYPHA_REQUIRE(backend != "disabled");
    } else {
        GLYPHA_REQUIRE(backend == "disabled");
        glyphastore::server::TlsConfig requested{
            .certificate_file = "/tmp/missing-cert.pem",
            .private_key_file = "/tmp/missing-key.pem",
        };
        const auto status = glyphastore::server::validate_tls_config(requested);
        GLYPHA_REQUIRE(!status.has_value());
        GLYPHA_REQUIRE(status.error().message.find("without TLS") != std::string::npos);
    }
}

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

GLYPHA_TEST("tls server accepts handshake and serves protocol ping") {
    TemporaryDirectory directory;
    if (!write_self_signed_material(directory.path())) {
        // OpenSSL CLI unavailable in the environment; skip without failing the suite.
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

    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
#if defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x10100000L
    auto* ctx = SSL_CTX_new(TLS_client_method());
#else
    auto* ctx = SSL_CTX_new(SSLv23_client_method());
#endif
    GLYPHA_REQUIRE(ctx != nullptr);
#if defined(TLS1_3_VERSION)
    GLYPHA_REQUIRE(SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) == 1);
#endif
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);

    const auto fd = ::socket(AF_INET, SOCK_STREAM, 0);
    GLYPHA_REQUIRE(fd >= 0);
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    static_cast<void>(::inet_pton(AF_INET, "127.0.0.1", &endpoint.sin_addr));
    GLYPHA_REQUIRE(::connect(fd, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) == 0);

    auto* ssl = SSL_new(ctx);
    GLYPHA_REQUIRE(ssl != nullptr);
    GLYPHA_REQUIRE(SSL_set_fd(ssl, fd) == 1);
    GLYPHA_REQUIRE(SSL_connect(ssl) == 1);

    const auto ping = glyphastore::server::encode_request({
        .opcode = glyphastore::server::RequestOpcode::ping,
        .request_id = 42,
    });
    GLYPHA_REQUIRE(ping.has_value());
    GLYPHA_REQUIRE(send_all_ssl(ssl, *ping));
    const auto frame = receive_response_ssl(ssl);
    const auto decoded = glyphastore::server::decode_response(frame);
    GLYPHA_REQUIRE(decoded.has_value());
    GLYPHA_REQUIRE(decoded->frame.status == glyphastore::server::ResponseStatus::ok);
    GLYPHA_REQUIRE(decoded->frame.request_id == 42);

    SSL_free(ssl);
    static_cast<void>(::close(fd));
    SSL_CTX_free(ctx);
    (*server)->request_stop();
    GLYPHA_REQUIRE((*server)->join().has_value());
}

GLYPHA_TEST("tls server create rejects missing certificate files") {
    glyphastore::server::ReactorConfig config{
        .port = 0,
        .tls =
            {
                .certificate_file = "/tmp/glyphastore-missing-cert.pem",
                .private_key_file = "/tmp/glyphastore-missing-key.pem",
            },
    };
    const auto server = glyphastore::server::Server::create(config);
    GLYPHA_REQUIRE(!server.has_value());
}

#endif
