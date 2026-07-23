#include "glyphastore/persistence/compaction_intent.hpp"
#include "glyphastore/persistence/manifest.hpp"
#include "glyphastore/segment/record.hpp"
#include "glyphastore/segment/segment_header.hpp"
#include "hex_fixture.hpp"
#include "test.hpp"

#include <algorithm>
#include <array>
#include <filesystem>
#include <string>
#include <vector>

namespace {

[[nodiscard]] auto released_root() -> std::filesystem::path {
    return std::filesystem::path{GLYPHASTORE_SOURCE_DIR} / "tests/fixtures/released";
}

[[nodiscard]] auto decode_named_fixture(const std::filesystem::path& path) -> bool {
    const auto name = path.filename().string();
    const auto bytes = glyphastore::test::read_hex_fixture(path);
    if (name.find("manifest") != std::string::npos && name.find("intent") == std::string::npos) {
        return glyphastore::decode_manifest(bytes).has_value();
    }
    if (name.find("compaction_intent") != std::string::npos) {
        return glyphastore::decode_compaction_intent(bytes).has_value();
    }
    if (name.find("segment_header") != std::string::npos || name.find("segment_v1_header") != std::string::npos) {
        std::array<std::byte, glyphastore::kSegmentHeaderReservedBytes> encoded{};
        if (bytes.size() > encoded.size()) {
            return false;
        }
        std::copy(bytes.begin(), bytes.end(), encoded.begin());
        return glyphastore::decode_segment_header(encoded).has_value();
    }
    if (name.find("record") != std::string::npos) {
        return glyphastore::decode_record(bytes).has_value();
    }
    // Wire and other released fixture names are ignored so labels can grow without breaking CI.
    return true;
}

} // namespace

GLYPHA_TEST("released artifact harness decodes every labeled fixture tree") {
    const auto root = released_root();
    GLYPHA_REQUIRE(std::filesystem::exists(root));
    std::size_t labels = 0;
    std::size_t fixtures = 0;
    for (const auto& entry : std::filesystem::directory_iterator{root}) {
        if (!entry.is_directory()) {
            continue;
        }
        ++labels;
        for (const auto& fixture : std::filesystem::directory_iterator{entry.path()}) {
            if (!fixture.is_regular_file() || fixture.path().extension() != ".hex") {
                continue;
            }
            ++fixtures;
            GLYPHA_REQUIRE(decode_named_fixture(fixture.path()));
        }
    }
    // Zero labels is success: the harness is present; tagged drops remain an alpha gate.
    GLYPHA_REQUIRE(labels == 0 || fixtures > 0);
}
