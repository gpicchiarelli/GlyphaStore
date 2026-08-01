#include "glyphastore/server/peercred.hpp"
#include "glyphastore/server/socket.hpp"
#include "test.hpp"

#include <cstring>
#include <filesystem>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

namespace {

class SocketTemporaryDirectory final {
  public:
    SocketTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-peercred-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~SocketTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

} // namespace

GLYPHA_TEST("peercred principal uses unix:uid= prefix") {
    const glyphastore::server::PeerCredentials credentials{.uid = 1000, .gid = 100, .pid = 42};
    GLYPHA_REQUIRE(glyphastore::server::peercred_principal(credentials) == "unix:uid=1000");
    GLYPHA_REQUIRE(glyphastore::server::peercred_principal_prefix() == "unix:uid=");
}

GLYPHA_TEST("peercred supported matches known Unix platforms") {
#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    GLYPHA_REQUIRE(glyphastore::server::peercred_supported());
#else
    GLYPHA_REQUIRE(!glyphastore::server::peercred_supported());
#endif
}

#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
GLYPHA_TEST("unix listener accept yields peer credentials for local connector") {
    SocketTemporaryDirectory temporary;
    const auto socket_path = temporary.path() / "glyphastore.sock";
    auto listener = glyphastore::server::UnixListener::bind(socket_path);
    GLYPHA_REQUIRE(listener.has_value());

    const int client = ::socket(AF_UNIX, SOCK_STREAM, 0);
    GLYPHA_REQUIRE(client >= 0);
    sockaddr_un endpoint{};
    endpoint.sun_family = AF_UNIX;
    const auto path_text = socket_path.string();
    GLYPHA_REQUIRE(path_text.size() < sizeof(endpoint.sun_path));
    std::memcpy(endpoint.sun_path, path_text.c_str(), path_text.size() + 1U);
    GLYPHA_REQUIRE(::connect(client, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) == 0);

    auto accepted = listener->accept();
    GLYPHA_REQUIRE(accepted.has_value());
    GLYPHA_REQUIRE(accepted->has_value());
    auto credentials = glyphastore::server::peer_credentials((**accepted).descriptor());
    GLYPHA_REQUIRE(credentials.has_value());
    GLYPHA_REQUIRE(credentials->uid == static_cast<std::uint32_t>(::geteuid()));
    GLYPHA_REQUIRE(credentials->gid == static_cast<std::uint32_t>(::getegid()));
    const auto principal = glyphastore::server::peercred_principal(*credentials);
    GLYPHA_REQUIRE(principal == std::string{glyphastore::server::peercred_principal_prefix()} +
                                    std::to_string(::geteuid()));
#if defined(__linux__)
    GLYPHA_REQUIRE(credentials->pid == static_cast<std::uint32_t>(::getpid()));
#endif
    static_cast<void>(::close(client));
}
#endif
