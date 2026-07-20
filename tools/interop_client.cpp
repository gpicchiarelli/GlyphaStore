#include "glyphastore/client/client.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

[[nodiscard]] auto hex_nibble(const char ch) -> int {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    return -1;
}

[[nodiscard]] auto parse_hex(const std::string_view text) -> std::vector<std::byte> {
    std::vector<std::byte> out;
    out.reserve(text.size() / 2);
    std::size_t index = 0;
    while (index < text.size()) {
        while (index < text.size() &&
               (text[index] == ' ' || text[index] == '\n' || text[index] == '\t')) {
            ++index;
        }
        if (index >= text.size()) {
            break;
        }
        if (index + 1 >= text.size()) {
            throw std::runtime_error("odd hex length");
        }
        const int high = hex_nibble(text[index]);
        const int low = hex_nibble(text[index + 1]);
        if (high < 0 || low < 0) {
            throw std::runtime_error("invalid hex");
        }
        out.push_back(static_cast<std::byte>((high << 4) | low));
        index += 2;
    }
    return out;
}

[[nodiscard]] auto to_hex(const std::span<const std::byte> bytes) -> std::string {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.resize(bytes.size() * 2);
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        const auto value = static_cast<unsigned>(bytes[index]);
        out[index * 2] = kDigits[(value >> 4) & 0xf];
        out[index * 2 + 1] = kDigits[value & 0xf];
    }
    return out;
}

void usage(const char* program) {
    std::cerr
        << "Usage:\n  " << program
        << " --port PORT [--host HOST] [--tls] [--tls-ca PATH] [--tls-cert PATH] [--tls-key PATH] "
           "[--server-name NAME] [--insecure-skip-verify]\n"
        << "    put --key-hex HEX --value-hex HEX [--expire-at-ns N]\n  " << program
        << " ... get --key-hex HEX\n  " << program << " ... erase --key-hex HEX\n  " << program
        << " ... pipeline-put-get --key-hex HEX --value-hex HEX\n";
}

[[nodiscard]] auto require_flag(const int argc, char** argv, int& index, const char* name)
    -> std::string_view {
    if (index + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + name);
    }
    return argv[++index];
}

} // namespace

int main(int argc, char** argv) try {
    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }

    std::string host = "127.0.0.1";
    std::uint16_t port = 0;
    std::string command;
    std::string key_hex;
    std::string value_hex;
    std::uint64_t expire_at_ns = 0;
    glyphastore::client::TlsOptions tls{};

    for (int index = 1; index < argc; ++index) {
        const std::string_view arg = argv[index];
        if (arg == "--help" || arg == "-h") {
            usage(argv[0]);
            return 0;
        }
        if (arg == "--host") {
            host = std::string{require_flag(argc, argv, index, "--host")};
        } else if (arg == "--port") {
            port = static_cast<std::uint16_t>(
                std::stoul(std::string{require_flag(argc, argv, index, "--port")}));
        } else if (arg == "--key-hex") {
            key_hex = std::string{require_flag(argc, argv, index, "--key-hex")};
        } else if (arg == "--value-hex") {
            value_hex = std::string{require_flag(argc, argv, index, "--value-hex")};
        } else if (arg == "--expire-at-ns") {
            expire_at_ns = std::stoull(std::string{require_flag(argc, argv, index, "--expire-at-ns")});
        } else if (arg == "--tls") {
            tls.enable = true;
        } else if (arg == "--tls-ca") {
            tls.ca_file = std::string{require_flag(argc, argv, index, "--tls-ca")};
        } else if (arg == "--tls-cert") {
            tls.cert_file = std::string{require_flag(argc, argv, index, "--tls-cert")};
        } else if (arg == "--tls-key") {
            tls.key_file = std::string{require_flag(argc, argv, index, "--tls-key")};
        } else if (arg == "--server-name") {
            tls.server_name = std::string{require_flag(argc, argv, index, "--server-name")};
        } else if (arg == "--insecure-skip-verify") {
            tls.insecure_skip_verify = true;
        } else if (arg == "put" || arg == "get" || arg == "erase" || arg == "pipeline-put-get") {
            command = std::string{arg};
        } else {
            std::cerr << "unknown argument: " << arg << '\n';
            usage(argv[0]);
            return 2;
        }
    }

    if (port == 0 || command.empty()) {
        usage(argv[0]);
        return 2;
    }

    auto client = glyphastore::client::Client::connect(
        {.host = host, .port = port, .tls = std::move(tls)});
    if (!client) {
        std::cerr << "connect failed: " << client.error().message << '\n';
        return 1;
    }

    const auto key = parse_hex(key_hex);
    if (command == "put") {
        const auto value = parse_hex(value_hex);
        const auto result = client->put(
            key, value, glyphastore::client::PutOptions{.expire_at_ns = expire_at_ns});
        if (!result.committed()) {
            std::cerr << "put not committed\n";
            return 1;
        }
        return 0;
    }
    if (command == "get") {
        auto got = client->get(key);
        if (!got) {
            std::cerr << "get failed: " << got.error().message << '\n';
            return 1;
        }
        std::cout << to_hex(*got) << '\n';
        return 0;
    }
    if (command == "erase") {
        const auto result = client->erase(key);
        if (!result.committed()) {
            std::cerr << "erase not committed\n";
            return 1;
        }
        return 0;
    }
    if (command == "pipeline-put-get") {
        const auto value = parse_hex(value_hex);
        const glyphastore::client::PipelineRequest requests[] = {
            {.opcode = glyphastore::client::PipelineOpcode::put,
             .key = std::span<const std::byte>{key},
             .value = std::span<const std::byte>{value}},
            {.opcode = glyphastore::client::PipelineOpcode::get,
             .key = std::span<const std::byte>{key}},
        };
        auto responses = client->execute_pipeline(requests);
        if (!responses) {
            std::cerr << "pipeline failed: " << responses.error().message << '\n';
            return 1;
        }
        if (responses->size() != 2 || !(*responses)[0].succeeded() || !(*responses)[1].succeeded()) {
            std::cerr << "pipeline outcomes failed\n";
            return 1;
        }
        if ((*responses)[1].value != value) {
            std::cerr << "pipeline value mismatch\n";
            return 1;
        }
        std::cout << to_hex((*responses)[1].value) << '\n';
        return 0;
    }

    usage(argv[0]);
    return 2;
} catch (const std::exception& exception) {
    std::cerr << "error: " << exception.what() << '\n';
    return 1;
}
