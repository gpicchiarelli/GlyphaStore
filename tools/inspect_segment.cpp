#include "cli/arguments.hpp"
#include "glyphastore/core/types.hpp"
#include "glyphastore/persistence/segment_file.hpp"

#include <array>
#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <string_view>

namespace {

enum OptionId : std::size_t { help, version, json, no_scan };

constexpr std::array kOptionSpecs{
    glyphastore::cli::OptionSpec{
        help, "help", 'h', glyphastore::cli::OptionArity::none, {}, "Show this help message and exit"},
    glyphastore::cli::OptionSpec{version,
                                 "version",
                                 'V',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Show version information and exit"},
    glyphastore::cli::OptionSpec{
        json, "json", '\0', glyphastore::cli::OptionArity::none, {}, "Emit a stable JSON report on stdout"},
    glyphastore::cli::OptionSpec{no_scan,
                                 "no-scan",
                                 '\0',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Validate header and commit metadata only (skip committed Record scan)"},
};

void print_help(const std::string_view program) {
    glyphastore::cli::write_help(std::cout, program,
                                 "Inspect a GlyphaStore durable Segment file (read-only).",
                                 "[OPTIONS] <SEGMENT-FILE>", kOptionSpecs);
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

[[nodiscard]] auto error_code_name(const glyphastore::ErrorCode code) -> std::string_view {
    switch (code) {
    case glyphastore::ErrorCode::invalid_argument:
        return "invalid_argument";
    case glyphastore::ErrorCode::arithmetic_overflow:
        return "arithmetic_overflow";
    case glyphastore::ErrorCode::record_too_large:
        return "record_too_large";
    case glyphastore::ErrorCode::segment_full:
        return "segment_full";
    case glyphastore::ErrorCode::segment_sealed:
        return "segment_sealed";
    case glyphastore::ErrorCode::invalid_record:
        return "invalid_record";
    case glyphastore::ErrorCode::checksum_mismatch:
        return "checksum_mismatch";
    case glyphastore::ErrorCode::invalid_reference:
        return "invalid_reference";
    case glyphastore::ErrorCode::sequence_conflict:
        return "sequence_conflict";
    case glyphastore::ErrorCode::corrupted_data:
        return "corrupted_data";
    case glyphastore::ErrorCode::not_found:
        return "not_found";
    case glyphastore::ErrorCode::resource_exhausted:
        return "resource_exhausted";
    case glyphastore::ErrorCode::storage_exhausted:
        return "storage_exhausted";
    case glyphastore::ErrorCode::file_too_large:
        return "file_too_large";
    case glyphastore::ErrorCode::descriptor_exhausted:
        return "descriptor_exhausted";
    case glyphastore::ErrorCode::read_only_filesystem:
        return "read_only_filesystem";
    case glyphastore::ErrorCode::internal_error:
        return "internal_error";
    case glyphastore::ErrorCode::unavailable:
        return "unavailable";
    case glyphastore::ErrorCode::io_error:
        return "io_error";
    }
    return "unknown";
}

[[nodiscard]] auto json_escape(const std::string_view text) -> std::string {
    std::string out;
    out.reserve(text.size() + 8);
    for (const char ch : text) {
        switch (ch) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(ch) < 0x20U) {
                constexpr char kDigits[] = "0123456789abcdef";
                out += "\\u00";
                out += kDigits[(static_cast<unsigned char>(ch) >> 4) & 0xf];
                out += kDigits[static_cast<unsigned char>(ch) & 0xf];
            } else {
                out += ch;
            }
            break;
        }
    }
    return out;
}

[[nodiscard]] auto state_name(const glyphastore::PersistedSegmentState state) -> std::string_view {
    switch (state) {
    case glyphastore::PersistedSegmentState::active:
        return "active";
    case glyphastore::PersistedSegmentState::sealed:
        return "sealed";
    }
    return "unknown";
}

[[nodiscard]] auto filename_match_text(const std::optional<bool>& value) -> std::string_view {
    if (!value.has_value()) {
        return "n/a";
    }
    return *value ? "true" : "false";
}

void write_text_ok(std::ostream& out, const glyphastore::DurableSegmentInspectReport& report) {
    const auto& commit = report.selected.commit;
    out << "status=ok\n"
        << "path=" << report.path.string() << '\n'
        << "store_id=" << to_hex(std::span{report.identity.store_id}) << '\n'
        << "segment_id=" << report.identity.segment_id.value << '\n'
        << "generation=" << report.identity.generation.value << '\n'
        << "owner_worker=" << report.identity.owner_worker.value << '\n'
        << "selected_slot=" << report.selected.slot_index << '\n'
        << "commit_generation=" << commit.commit_generation << '\n'
        << "committed_end=" << commit.committed_end << '\n'
        << "state=" << state_name(commit.state) << '\n'
        << "record_count=" << commit.record_count << '\n'
        << "first_sequence=" << commit.first_sequence.value << '\n'
        << "last_sequence=" << commit.last_sequence.value << '\n'
        << "scanned_records=" << report.scanned_records << '\n'
        << "filename_matches_identity=" << filename_match_text(report.filename_matches_identity) << '\n';
}

void write_json_ok(std::ostream& out, const glyphastore::DurableSegmentInspectReport& report) {
    const auto& commit = report.selected.commit;
    out << '{'
        << "\"status\":\"ok\","
        << "\"path\":\"" << json_escape(report.path.string()) << "\","
        << "\"store_id\":\"" << to_hex(std::span{report.identity.store_id}) << "\","
        << "\"segment_id\":" << report.identity.segment_id.value << ','
        << "\"generation\":" << report.identity.generation.value << ','
        << "\"owner_worker\":" << report.identity.owner_worker.value << ','
        << "\"selected_slot\":" << report.selected.slot_index << ','
        << "\"commit_generation\":" << commit.commit_generation << ','
        << "\"committed_end\":" << commit.committed_end << ','
        << "\"state\":\"" << state_name(commit.state) << "\","
        << "\"record_count\":" << commit.record_count << ','
        << "\"first_sequence\":" << commit.first_sequence.value << ','
        << "\"last_sequence\":" << commit.last_sequence.value << ','
        << "\"scanned_records\":" << report.scanned_records << ',';
    if (!report.filename_matches_identity.has_value()) {
        out << "\"filename_matches_identity\":null";
    } else {
        out << "\"filename_matches_identity\":" << (*report.filename_matches_identity ? "true" : "false");
    }
    out << "}\n";
}

void write_json_error(std::ostream& out, const std::string_view path, const glyphastore::Error& error) {
    out << '{'
        << "\"status\":\"error\","
        << "\"path\":\"" << json_escape(path) << "\","
        << "\"error\":{"
        << "\"code\":\"" << error_code_name(error.code) << "\","
        << "\"message\":\"" << json_escape(error.message) << "\""
        << "}}\n";
}

} // namespace

int main(const int argc, char** argv) try {
    const auto program =
        glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastore_inspect_segment");
    auto parsed = glyphastore::cli::parse_arguments(argc, argv, kOptionSpecs);
    if (!parsed) {
        std::cerr << program << ": error: " << parsed.error().message << "\nTry '" << program
                  << " --help' for more information.\n";
        return 2;
    }
    if (parsed->has(help)) {
        print_help(program);
        return 0;
    }
    if (parsed->has(version)) {
        std::cout << program << ' ' << GLYPHASTORE_VERSION << '\n';
        return 0;
    }
    if (parsed->positionals.size() != 1) {
        std::cerr << program << ": error: expected exactly one segment file\nTry '" << program
                  << " --help' for more information.\n";
        return 2;
    }

    const auto path = std::string{parsed->positionals.front()};
    const bool emit_json = parsed->has(json);
    const bool scan = !parsed->has(no_scan);
    auto report = glyphastore::inspect_durable_segment(path, scan);
    if (!report) {
        if (emit_json) {
            write_json_error(std::cout, path, report.error());
        } else {
            std::cerr << program << ": error: " << error_code_name(report.error().code) << ": "
                      << report.error().message << '\n';
        }
        return 1;
    }
    if (emit_json) {
        write_json_ok(std::cout, *report);
    } else {
        write_text_ok(std::cout, *report);
    }
    return 0;
} catch (const std::exception& exception) {
    const auto program =
        glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastore_inspect_segment");
    std::cerr << program << ": fatal: " << exception.what() << '\n';
    return 1;
} catch (...) {
    std::cerr << "glyphastore_inspect_segment: fatal: unknown non-standard exception\n";
    return 1;
}
