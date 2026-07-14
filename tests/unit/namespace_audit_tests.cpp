#include "glyphastore/persistence/namespace_audit.hpp"
#include "glyphastore/persistence/segment_file.hpp"
#include "test.hpp"

#include <cstddef>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class NamespaceTemporaryDirectory final {
  public:
    NamespaceTemporaryDirectory() {
        auto pattern = (std::filesystem::temp_directory_path() / "glyphastore-namespace-XXXXXX").string();
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const auto* created = ::mkdtemp(writable.data());
        GLYPHA_REQUIRE(created != nullptr);
        path_ = created;
    }

    ~NamespaceTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] auto path() const -> const std::filesystem::path& {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

auto namespace_store_id() -> glyphastore::StoreId {
    return {std::byte{0x31}, std::byte{0x32}, std::byte{0x33}, std::byte{0x34},
            std::byte{0x35}, std::byte{0x36}, std::byte{0x37}, std::byte{0x38},
            std::byte{0x39}, std::byte{0x3A}, std::byte{0x3B}, std::byte{0x3C},
            std::byte{0x3D}, std::byte{0x3E}, std::byte{0x3F}, std::byte{0x40}};
}

auto namespace_manifest() -> glyphastore::Manifest {
    return {
        .store_id = namespace_store_id(),
        .manifest_generation = 1,
        .worker_count = 1,
        .routing_epoch = 1,
        .next_segment_id = glyphastore::SegmentId{2},
        .next_segment_generation = glyphastore::GenerationId{1},
        .segments = {{.segment_id = glyphastore::SegmentId{1},
                      .generation = glyphastore::GenerationId{1},
                      .owner_worker = glyphastore::WorkerId{0},
                      .role = glyphastore::ManifestSegmentRole::active}},
    };
}

auto segment_name(const glyphastore::Manifest& manifest, glyphastore::SegmentId id,
                  glyphastore::GenerationId generation) -> std::string {
    return glyphastore::segment_filename({.store_id = manifest.store_id,
                                          .segment_id = id,
                                          .generation = generation,
                                          .owner_worker = glyphastore::WorkerId{0}});
}

void create_private_file(const std::filesystem::path& path) {
    const auto descriptor = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    GLYPHA_REQUIRE(descriptor >= 0);
    GLYPHA_REQUIRE(::close(descriptor) == 0);
}

auto prepare_catalog(NamespaceTemporaryDirectory& temporary)
    -> std::pair<glyphastore::DataDirectory, glyphastore::Manifest> {
    auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
    GLYPHA_REQUIRE(directory.has_value());
    auto manifest = namespace_manifest();
    GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
    create_private_file(temporary.path() /
                        segment_name(manifest, glyphastore::SegmentId{1}, glyphastore::GenerationId{1}));
    return {std::move(*directory), std::move(manifest)};
}

} // namespace

GLYPHA_TEST("Segment filename parser accepts only exact lowercase non-zero identities") {
    const auto parsed = glyphastore::parse_segment_filename("segment-0123456789abcdef-0000002a.glypha");
    GLYPHA_REQUIRE(parsed.has_value());
    GLYPHA_REQUIRE(parsed->segment_id.value == 0x0123456789ABCDEFULL);
    GLYPHA_REQUIRE(parsed->generation.value == 0x2AU);
    GLYPHA_REQUIRE(!parsed->temporary);

    const auto temporary =
        glyphastore::parse_segment_filename(".segment-0123456789abcdef-0000002a.glypha.tmp");
    GLYPHA_REQUIRE(temporary.has_value());
    GLYPHA_REQUIRE(temporary->temporary);
    GLYPHA_REQUIRE(
        !glyphastore::parse_segment_filename("segment-0123456789abcdeF-0000002a.glypha").has_value());
    GLYPHA_REQUIRE(
        !glyphastore::parse_segment_filename("segment-0000000000000000-0000002a.glypha").has_value());
    GLYPHA_REQUIRE(
        !glyphastore::parse_segment_filename("segment-0123456789abcdef-00000000.glypha").has_value());
}

GLYPHA_TEST("namespace audit is repeatable and tolerates only canonical crash temporaries") {
    NamespaceTemporaryDirectory temporary;
    auto [directory, manifest] = prepare_catalog(temporary);

    const auto clean = glyphastore::audit_data_directory(directory, manifest);
    GLYPHA_REQUIRE(clean.has_value());
    GLYPHA_REQUIRE(clean->clean());
    GLYPHA_REQUIRE(clean->entries_scanned == 3);
    GLYPHA_REQUIRE(clean->catalog_segments_seen == 1);

    create_private_file(temporary.path() / glyphastore::kManifestTemporaryFilename);
    create_private_file(
        temporary.path() /
        ('.' + segment_name(manifest, glyphastore::SegmentId{1}, glyphastore::GenerationId{1}) + ".tmp"));
    const auto first = glyphastore::audit_data_directory(directory, manifest);
    const auto second = glyphastore::audit_data_directory(directory, manifest);
    GLYPHA_REQUIRE(first.has_value());
    GLYPHA_REQUIRE(second.has_value());
    GLYPHA_REQUIRE(first->entries_scanned == 5);
    GLYPHA_REQUIRE(second->entries_scanned == first->entries_scanned);
    GLYPHA_REQUIRE(first->issues.size() == 2);
    GLYPHA_REQUIRE(first->issues[0].kind == glyphastore::NamespaceIssueKind::stale_manifest_temporary);
    GLYPHA_REQUIRE(first->issues[1].kind == glyphastore::NamespaceIssueKind::stale_segment_temporary);
    GLYPHA_REQUIRE(first->recovery_safe());
    GLYPHA_REQUIRE(glyphastore::validate_namespace_for_recovery(*first).has_value());
}

GLYPHA_TEST("namespace audit deterministically rejects unlisted malformed and unknown entries") {
    NamespaceTemporaryDirectory temporary;
    auto [directory, manifest] = prepare_catalog(temporary);
    create_private_file(temporary.path() /
                        segment_name(manifest, glyphastore::SegmentId{2}, glyphastore::GenerationId{7}));
    create_private_file(temporary.path() / "segment-0000000000000003-0000000A.glypha");
    create_private_file(temporary.path() / "operator-note.txt");

    const auto report = glyphastore::audit_data_directory(directory, manifest);
    GLYPHA_REQUIRE(report.has_value());
    GLYPHA_REQUIRE(report->issues.size() == 3);
    GLYPHA_REQUIRE(report->issues[0].name == "operator-note.txt");
    GLYPHA_REQUIRE(report->issues[0].kind == glyphastore::NamespaceIssueKind::unknown_entry);
    GLYPHA_REQUIRE(report->issues[1].kind == glyphastore::NamespaceIssueKind::unlisted_segment);
    GLYPHA_REQUIRE(report->issues[2].kind == glyphastore::NamespaceIssueKind::malformed_engine_name);
    GLYPHA_REQUIRE(!report->recovery_safe());
    const auto policy = glyphastore::validate_namespace_for_recovery(*report);
    GLYPHA_REQUIRE(!policy.has_value());
    GLYPHA_REQUIRE(policy.error().code == glyphastore::ErrorCode::corrupted_data);
    GLYPHA_REQUIRE(policy.error().message.find("operator-note.txt") != std::string::npos);
}

GLYPHA_TEST("namespace audit reports canonical symlinks and hard links as unsafe") {
    {
        NamespaceTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto manifest = namespace_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        const auto expected = segment_name(manifest, glyphastore::SegmentId{1}, glyphastore::GenerationId{1});
        std::filesystem::create_symlink(glyphastore::kManifestFilename, temporary.path() / expected);
        const auto report = glyphastore::audit_data_directory(*directory, manifest);
        GLYPHA_REQUIRE(report.has_value());
        GLYPHA_REQUIRE(report->issues.size() == 1);
        GLYPHA_REQUIRE(report->issues[0].kind == glyphastore::NamespaceIssueKind::unsafe_entry);
        GLYPHA_REQUIRE(report->catalog_segments_seen == 1);
    }
    {
        NamespaceTemporaryDirectory temporary;
        auto [directory, manifest] = prepare_catalog(temporary);
        const auto expected = segment_name(manifest, glyphastore::SegmentId{1}, glyphastore::GenerationId{1});
        GLYPHA_REQUIRE(::link((temporary.path() / expected).c_str(),
                              (temporary.path() / "extra-hard-link").c_str()) == 0);
        const auto report = glyphastore::audit_data_directory(directory, manifest);
        GLYPHA_REQUIRE(report.has_value());
        GLYPHA_REQUIRE(!report->recovery_safe());
        GLYPHA_REQUIRE(report->issues.size() == 2);
        GLYPHA_REQUIRE(report->issues[0].kind == glyphastore::NamespaceIssueKind::unknown_entry);
        GLYPHA_REQUIRE(report->issues[1].kind == glyphastore::NamespaceIssueKind::unsafe_entry);
    }
}

GLYPHA_TEST("namespace audit reports missing catalog files and bounds hostile enumeration") {
    {
        NamespaceTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto manifest = namespace_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        const auto report = glyphastore::audit_data_directory(*directory, manifest);
        GLYPHA_REQUIRE(report.has_value());
        GLYPHA_REQUIRE(report->issues.size() == 1);
        GLYPHA_REQUIRE(report->issues[0].kind == glyphastore::NamespaceIssueKind::missing_catalog_segment);
    }
    {
        NamespaceTemporaryDirectory temporary;
        auto directory = glyphastore::DataDirectory::open_and_lock(temporary.path());
        GLYPHA_REQUIRE(directory.has_value());
        const auto manifest = namespace_manifest();
        GLYPHA_REQUIRE(directory->publish_manifest(manifest).durable());
        for (std::size_t index = 0; index < glyphastore::kNamespaceAnomalyBudget + 2U; ++index) {
            create_private_file(temporary.path() / ("unknown-" + std::to_string(index)));
        }
        const auto report = glyphastore::audit_data_directory(*directory, manifest);
        GLYPHA_REQUIRE(!report.has_value());
        GLYPHA_REQUIRE(report.error().code == glyphastore::ErrorCode::corrupted_data);
    }
}
