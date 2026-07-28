#include "test.hpp"

int main() {
    int failures = 0;
    for (const auto& test : glyphastore::test::registry()) {
        try {
            test.function();
            std::cout << "[PASS] " << test.name << '\n';
        } catch (const std::exception& exception) {
            ++failures;
            std::cerr << "[FAIL] " << test.name << ": " << exception.what() << '\n';
        }
    }
    std::cout << glyphastore::test::registry().size() << " tests, " << failures << " failures\n";
    return failures == 0 ? 0 : 1;
}
