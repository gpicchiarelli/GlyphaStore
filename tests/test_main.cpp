#include "test.hpp"

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <string_view>

int main(int argc, char** argv) {
    // Network/TLS tests may write to a peer that already closed (EPIPE). On
    // OpenBSD/BSD this raises SIGPIPE by default and aborts the whole suite.
    // Ignore process-wide; sockets also set SO_NOSIGPIPE where available.
    std::signal(SIGPIPE, SIG_IGN);

    const char* filter_env = std::getenv("GLYPHASTORE_TEST_FILTER");
    const std::string_view filter =
        argc > 1 ? std::string_view{argv[1]}
                 : (filter_env != nullptr ? std::string_view{filter_env} : std::string_view{});
    int failures = 0;
    int ran = 0;
    for (const auto& test : glyphastore::test::registry()) {
        if (!filter.empty() && test.name.find(filter) == std::string::npos) {
            continue;
        }
        ++ran;
        try {
            test.function();
            std::cout << "[PASS] " << test.name << '\n';
        } catch (const std::exception& exception) {
            ++failures;
            std::cerr << "[FAIL] " << test.name << ": " << exception.what() << '\n';
        }
    }
    std::cout << ran << " tests, " << failures << " failures\n";
    if (!filter.empty() && ran == 0) {
        std::cerr << "no tests matched filter: " << filter << '\n';
        return 1;
    }
    return failures == 0 ? 0 : 1;
}
