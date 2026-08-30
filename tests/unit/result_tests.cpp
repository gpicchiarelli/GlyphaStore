#include "glyphastore/core/error.hpp"
#include "test.hpp"

#include <optional>

GLYPHA_TEST("failed void Result exposes its error") {
    glyphastore::Status failure =
        glyphastore::fail(glyphastore::ErrorCode::invalid_argument, "expected failure");
    GLYPHA_REQUIRE(failure.error().code == glyphastore::ErrorCode::invalid_argument);
    GLYPHA_REQUIRE(failure.error().message == "expected failure");
}

GLYPHA_TEST("successful void Result rejects error access without undefined behavior") {
    glyphastore::Status success{};
    bool rejected{};
    try {
        static_cast<void>(success.error());
    } catch (const std::bad_optional_access&) {
        rejected = true;
    }
    GLYPHA_REQUIRE(rejected);
}
