#include "glyphastore/server/tls.hpp"

#include <chrono>
#include <filesystem>
#include <poll.h>
#include <string>
#include <system_error>
#include <utility>

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#endif

namespace glyphastore::server {
namespace {

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

[[nodiscard]] auto tls_error_string() -> std::string {
    const auto code = ERR_get_error();
    if (code == 0) {
        return "TLS error";
    }
    char buffer[256];
    ERR_error_string_n(code, buffer, sizeof(buffer));
    return std::string{buffer};
}

[[nodiscard]] auto map_ssl_io_error(const SSL* ssl, const int result, const char* operation)
    -> Result<TlsIoResult> {
    const auto error = SSL_get_error(ssl, result);
    switch (error) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
        return TlsIoResult{.kind = TlsIoKind::would_block, .bytes = 0};
    case SSL_ERROR_ZERO_RETURN:
        return TlsIoResult{.kind = TlsIoKind::closed, .bytes = 0};
    case SSL_ERROR_SYSCALL:
        if (result == 0) {
            return TlsIoResult{.kind = TlsIoKind::closed, .bytes = 0};
        }
        return fail(ErrorCode::io_error, std::string{operation} + " syscall failure");
    default:
        return fail(ErrorCode::io_error, std::string{operation} + ": " + tls_error_string());
    }
}

[[nodiscard]] auto wait_for_socket(const int descriptor, const int ssl_error, const int timeout_ms)
    -> Status {
    pollfd interest{.fd = descriptor, .events = 0, .revents = 0};
    if (ssl_error == SSL_ERROR_WANT_READ) {
        interest.events = POLLIN;
    } else if (ssl_error == SSL_ERROR_WANT_WRITE) {
        interest.events = POLLOUT;
    } else {
        return fail(ErrorCode::io_error, "unexpected TLS handshake wait state");
    }
    const auto ready = ::poll(&interest, 1, timeout_ms);
    if (ready < 0) {
        return fail(ErrorCode::io_error, "TLS handshake poll failed");
    }
    if (ready == 0) {
        return fail(ErrorCode::unavailable, "TLS handshake timed out");
    }
    return {};
}

#endif

} // namespace

auto validate_tls_config(const TlsConfig& config) -> Status {
    if (!config.requested()) {
        return {};
    }
    if (!tls_build_enabled()) {
        return fail(ErrorCode::invalid_argument,
                    "TLS was requested but GlyphaStore was built without TLS support "
                    "(GLYPHASTORE_ENABLE_TLS=OFF or no LibreSSL/OpenSSL found)");
    }
    if (config.certificate_file.empty() || config.private_key_file.empty()) {
        return fail(ErrorCode::invalid_argument,
                    "TLS requires both --tls-cert and --tls-key (fail closed)");
    }
    std::error_code ec;
    if (!std::filesystem::is_regular_file(config.certificate_file, ec) || ec) {
        return fail(ErrorCode::invalid_argument,
                    "TLS certificate file is missing or not a regular file: " +
                        config.certificate_file.string());
    }
    if (!std::filesystem::is_regular_file(config.private_key_file, ec) || ec) {
        return fail(ErrorCode::invalid_argument,
                    "TLS private key file is missing or not a regular file: " +
                        config.private_key_file.string());
    }
    if (!config.client_ca_file.empty() &&
        (!std::filesystem::is_regular_file(config.client_ca_file, ec) || ec)) {
        return fail(ErrorCode::invalid_argument,
                    "TLS client CA file is missing or not a regular file: " +
                        config.client_ca_file.string());
    }
    return {};
}

auto validate_client_tls_config(const ClientTlsConfig& config) -> Status {
    if (!config.requested()) {
        return {};
    }
    if (!tls_build_enabled()) {
        return fail(ErrorCode::invalid_argument,
                    "TLS was requested but GlyphaStore was built without TLS support "
                    "(GLYPHASTORE_ENABLE_TLS=OFF or no LibreSSL/OpenSSL found)");
    }
    const auto has_cert = !config.certificate_file.empty();
    const auto has_key = !config.private_key_file.empty();
    if (has_cert != has_key) {
        return fail(ErrorCode::invalid_argument,
                    "TLS client mTLS requires both cert_file and key_file (fail closed)");
    }
    std::error_code ec;
    if (!config.ca_file.empty() && (!std::filesystem::is_regular_file(config.ca_file, ec) || ec)) {
        return fail(ErrorCode::invalid_argument,
                    "TLS CA file is missing or not a regular file: " + config.ca_file.string());
    }
    if (has_cert && (!std::filesystem::is_regular_file(config.certificate_file, ec) || ec)) {
        return fail(ErrorCode::invalid_argument,
                    "TLS client certificate file is missing or not a regular file: " +
                        config.certificate_file.string());
    }
    if (has_key && (!std::filesystem::is_regular_file(config.private_key_file, ec) || ec)) {
        return fail(ErrorCode::invalid_argument,
                    "TLS client private key file is missing or not a regular file: " +
                        config.private_key_file.string());
    }
    return {};
}

auto tls_backend_name() -> std::string {
#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS
#if defined(GLYPHASTORE_TLS_BACKEND_NAME)
    return GLYPHASTORE_TLS_BACKEND_NAME;
#else
    return "OpenSSL";
#endif
#else
    return "disabled";
#endif
}

#if defined(GLYPHASTORE_HAS_TLS) && GLYPHASTORE_HAS_TLS

struct TlsContext::Impl {
    SSL_CTX* ctx{};
};

struct TlsSession::Impl {
    SSL* ssl{};
};

TlsContext::TlsContext() : impl_(std::make_unique<Impl>()) {}

TlsContext::~TlsContext() {
    if (impl_ && impl_->ctx != nullptr) {
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
    }
}

auto TlsContext::create(const TlsConfig& config) -> Result<std::shared_ptr<TlsContext>> {
    if (auto valid = validate_tls_config(config); !valid) {
        return unexpected(valid.error());
    }
#if defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x10100000L
    auto* ctx = SSL_CTX_new(TLS_server_method());
#else
    auto* ctx = SSL_CTX_new(SSLv23_server_method());
#endif
    if (ctx == nullptr) {
        return fail(ErrorCode::internal_error, "SSL_CTX_new failed: " + tls_error_string());
    }
    auto context = std::shared_ptr<TlsContext>(new TlsContext());
    context->impl_->ctx = ctx;
    context->mtls_enabled_ = config.mtls_enabled();
    context->handshake_timeout_ms_ =
        config.handshake_timeout_ms == 0 ? 10'000U : config.handshake_timeout_ms;

#if defined(TLS1_3_VERSION)
    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "cannot require TLS 1.3 (SSL_CTX_set_min_proto_version failed)");
    }
    if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "cannot cap TLS at 1.3 (SSL_CTX_set_max_proto_version failed)");
    }
#else
    return fail(ErrorCode::invalid_argument, "TLS library build does not advertise TLS 1.3");
#endif

#if defined(SSL_OP_NO_TLSv1_2)
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 |
                                 SSL_OP_NO_TLSv1_2);
#else
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
#endif
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    const auto cert = config.certificate_file.string();
    const auto key = config.private_key_file.string();
    if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "cannot load TLS certificate chain: " + tls_error_string());
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1) {
        return fail(ErrorCode::invalid_argument, "cannot load TLS private key: " + tls_error_string());
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "TLS private key does not match certificate: " + tls_error_string());
    }

    if (config.mtls_enabled()) {
        const auto ca = config.client_ca_file.string();
        if (SSL_CTX_load_verify_locations(ctx, ca.c_str(), nullptr) != 1) {
            return fail(ErrorCode::invalid_argument,
                        "cannot load TLS client CA: " + tls_error_string());
        }
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        SSL_CTX_set_verify_depth(ctx, 4);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    }

    return context;
}

auto TlsContext::create_client(const ClientTlsConfig& config) -> Result<std::shared_ptr<TlsContext>> {
    if (auto valid = validate_client_tls_config(config); !valid) {
        return unexpected(valid.error());
    }
#if defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x10100000L
    auto* ctx = SSL_CTX_new(TLS_client_method());
#else
    auto* ctx = SSL_CTX_new(SSLv23_client_method());
#endif
    if (ctx == nullptr) {
        return fail(ErrorCode::internal_error, "SSL_CTX_new failed: " + tls_error_string());
    }
    auto context = std::shared_ptr<TlsContext>(new TlsContext());
    context->impl_->ctx = ctx;
    context->client_mode_ = true;
    context->insecure_skip_verify_ = config.insecure_skip_verify;
    context->handshake_timeout_ms_ =
        config.handshake_timeout_ms == 0 ? 10'000U : config.handshake_timeout_ms;

#if defined(TLS1_3_VERSION)
    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "cannot require TLS 1.3 (SSL_CTX_set_min_proto_version failed)");
    }
    if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        return fail(ErrorCode::invalid_argument,
                    "cannot cap TLS at 1.3 (SSL_CTX_set_max_proto_version failed)");
    }
#else
    return fail(ErrorCode::invalid_argument, "TLS library build does not advertise TLS 1.3");
#endif

#if defined(SSL_OP_NO_TLSv1_2)
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 |
                                 SSL_OP_NO_TLSv1_2);
#else
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
#endif
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    if (config.insecure_skip_verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
        SSL_CTX_set_verify_depth(ctx, 4);
        if (!config.ca_file.empty()) {
            const auto ca = config.ca_file.string();
            if (SSL_CTX_load_verify_locations(ctx, ca.c_str(), nullptr) != 1) {
                return fail(ErrorCode::invalid_argument,
                            "cannot load TLS CA file: " + tls_error_string());
            }
        } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
            return fail(ErrorCode::invalid_argument,
                        "cannot load system TLS trust store: " + tls_error_string());
        }
    }

    if (!config.certificate_file.empty() && !config.private_key_file.empty()) {
        context->mtls_enabled_ = true;
        const auto cert = config.certificate_file.string();
        const auto key = config.private_key_file.string();
        if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1) {
            return fail(ErrorCode::invalid_argument,
                        "cannot load TLS client certificate chain: " + tls_error_string());
        }
        if (SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1) {
            return fail(ErrorCode::invalid_argument,
                        "cannot load TLS client private key: " + tls_error_string());
        }
        if (SSL_CTX_check_private_key(ctx) != 1) {
            return fail(ErrorCode::invalid_argument,
                        "TLS client private key does not match certificate: " + tls_error_string());
        }
    }

    return context;
}

auto TlsContext::accept_socket(const int descriptor) const -> Result<std::unique_ptr<TlsSession>> {
    if (client_mode_) {
        return fail(ErrorCode::invalid_argument, "TLS context is client-mode; use connect_socket");
    }
    if (impl_ == nullptr || impl_->ctx == nullptr) {
        return fail(ErrorCode::internal_error, "TLS context is not initialized");
    }
    auto* ssl = SSL_new(impl_->ctx);
    if (ssl == nullptr) {
        return fail(ErrorCode::internal_error, "SSL_new failed: " + tls_error_string());
    }
    if (SSL_set_fd(ssl, descriptor) != 1) {
        SSL_free(ssl);
        return fail(ErrorCode::io_error, "SSL_set_fd failed: " + tls_error_string());
    }
    SSL_set_accept_state(ssl);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{handshake_timeout_ms_};
    while (true) {
        const auto result = SSL_accept(ssl);
        if (result == 1) {
            break;
        }
        const auto error = SSL_get_error(ssl, result);
        if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
            const auto message = "TLS handshake failed: " + tls_error_string();
            SSL_free(ssl);
            return fail(ErrorCode::io_error, message);
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   deadline - std::chrono::steady_clock::now())
                                   .count();
        if (remaining <= 0) {
            SSL_free(ssl);
            return fail(ErrorCode::unavailable, "TLS handshake timed out");
        }
        if (auto waited = wait_for_socket(descriptor, error, static_cast<int>(remaining)); !waited) {
            SSL_free(ssl);
            return unexpected(waited.error());
        }
    }

    if (mtls_enabled_) {
#if defined(LIBRESSL_VERSION_NUMBER) ||                                                        \
    (defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER < 0x30000000L)
        auto* peer = SSL_get_peer_certificate(ssl);
#else
        auto* peer = SSL_get1_peer_certificate(ssl);
#endif
        if (peer == nullptr) {
            SSL_free(ssl);
            return fail(ErrorCode::io_error, "mTLS required but peer certificate is missing");
        }
        X509_free(peer);
        if (SSL_get_verify_result(ssl) != X509_V_OK) {
            SSL_free(ssl);
            return fail(ErrorCode::io_error, "mTLS peer certificate verification failed");
        }
    }

    auto session = std::unique_ptr<TlsSession>(new TlsSession(std::make_unique<TlsSession::Impl>()));
    session->impl_->ssl = ssl;
    return session;
}

auto TlsContext::connect_socket(const int descriptor, const std::string_view server_name) const
    -> Result<std::unique_ptr<TlsSession>> {
    if (!client_mode_) {
        return fail(ErrorCode::invalid_argument, "TLS context is server-mode; use accept_socket");
    }
    if (impl_ == nullptr || impl_->ctx == nullptr) {
        return fail(ErrorCode::internal_error, "TLS context is not initialized");
    }
    if (server_name.empty()) {
        return fail(ErrorCode::invalid_argument, "TLS server_name is required for client handshake");
    }
    auto* ssl = SSL_new(impl_->ctx);
    if (ssl == nullptr) {
        return fail(ErrorCode::internal_error, "SSL_new failed: " + tls_error_string());
    }
    if (SSL_set_fd(ssl, descriptor) != 1) {
        SSL_free(ssl);
        return fail(ErrorCode::io_error, "SSL_set_fd failed: " + tls_error_string());
    }
    SSL_set_connect_state(ssl);

    const std::string sni{server_name};
    if (SSL_set_tlsext_host_name(ssl, sni.c_str()) != 1) {
        SSL_free(ssl);
        return fail(ErrorCode::io_error, "TLS SNI configuration failed: " + tls_error_string());
    }
    if (!insecure_skip_verify_) {
#if defined(LIBRESSL_VERSION_NUMBER) ||                                                        \
    (defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x10100000L)
        if (SSL_set1_host(ssl, sni.c_str()) != 1) {
            SSL_free(ssl);
            return fail(ErrorCode::io_error, "TLS hostname verification setup failed");
        }
#endif
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{handshake_timeout_ms_};
    while (true) {
        const auto result = SSL_connect(ssl);
        if (result == 1) {
            break;
        }
        const auto error = SSL_get_error(ssl, result);
        if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
            const auto message = "TLS handshake failed: " + tls_error_string();
            SSL_free(ssl);
            return fail(ErrorCode::io_error, message);
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   deadline - std::chrono::steady_clock::now())
                                   .count();
        if (remaining <= 0) {
            SSL_free(ssl);
            return fail(ErrorCode::unavailable, "TLS handshake timed out");
        }
        if (auto waited = wait_for_socket(descriptor, error, static_cast<int>(remaining)); !waited) {
            SSL_free(ssl);
            return unexpected(waited.error());
        }
    }

    if (!insecure_skip_verify_ && SSL_get_verify_result(ssl) != X509_V_OK) {
        SSL_free(ssl);
        return fail(ErrorCode::io_error, "TLS server certificate verification failed");
    }

    auto session = std::unique_ptr<TlsSession>(new TlsSession(std::make_unique<TlsSession::Impl>()));
    session->impl_->ssl = ssl;
    return session;
}

TlsSession::TlsSession(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

TlsSession::~TlsSession() {
    if (impl_ && impl_->ssl != nullptr) {
        SSL_free(impl_->ssl);
        impl_->ssl = nullptr;
    }
}

TlsSession::TlsSession(TlsSession&& other) noexcept : impl_(std::move(other.impl_)) {}

auto TlsSession::operator=(TlsSession&& other) noexcept -> TlsSession& {
    if (this != &other) {
        impl_ = std::move(other.impl_);
    }
    return *this;
}

auto TlsSession::valid() const noexcept -> bool {
    return impl_ != nullptr && impl_->ssl != nullptr;
}

auto TlsSession::read(std::byte* data, const std::size_t size) -> Result<TlsIoResult> {
    if (!valid()) {
        return fail(ErrorCode::invalid_argument, "TLS session is not valid");
    }
    const auto result = SSL_read(impl_->ssl, data, static_cast<int>(size));
    if (result > 0) {
        return TlsIoResult{.kind = TlsIoKind::ok, .bytes = static_cast<std::size_t>(result)};
    }
    return map_ssl_io_error(impl_->ssl, result, "SSL_read");
}

auto TlsSession::write(const std::byte* data, const std::size_t size) -> Result<TlsIoResult> {
    if (!valid()) {
        return fail(ErrorCode::invalid_argument, "TLS session is not valid");
    }
    const auto result = SSL_write(impl_->ssl, data, static_cast<int>(size));
    if (result > 0) {
        return TlsIoResult{.kind = TlsIoKind::ok, .bytes = static_cast<std::size_t>(result)};
    }
    return map_ssl_io_error(impl_->ssl, result, "SSL_write");
}

#else

struct TlsContext::Impl {};
struct TlsSession::Impl {};

TlsContext::TlsContext() = default;
TlsContext::~TlsContext() = default;

auto TlsContext::create(const TlsConfig& config) -> Result<std::shared_ptr<TlsContext>> {
    if (auto valid = validate_tls_config(config); !valid) {
        return unexpected(valid.error());
    }
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}

auto TlsContext::create_client(const ClientTlsConfig& config) -> Result<std::shared_ptr<TlsContext>> {
    if (auto valid = validate_client_tls_config(config); !valid) {
        return unexpected(valid.error());
    }
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}

auto TlsContext::accept_socket(const int) const -> Result<std::unique_ptr<TlsSession>> {
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}

auto TlsContext::connect_socket(const int, const std::string_view) const
    -> Result<std::unique_ptr<TlsSession>> {
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}

TlsSession::TlsSession(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
TlsSession::~TlsSession() = default;
TlsSession::TlsSession(TlsSession&& other) noexcept : impl_(std::move(other.impl_)) {}
auto TlsSession::operator=(TlsSession&& other) noexcept -> TlsSession& {
    if (this != &other) {
        impl_ = std::move(other.impl_);
    }
    return *this;
}
auto TlsSession::valid() const noexcept -> bool {
    return false;
}
auto TlsSession::read(std::byte*, const std::size_t) -> Result<TlsIoResult> {
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}
auto TlsSession::write(const std::byte*, const std::size_t) -> Result<TlsIoResult> {
    return fail(ErrorCode::invalid_argument, "GlyphaStore was built without TLS support");
}

#endif

} // namespace glyphastore::server
