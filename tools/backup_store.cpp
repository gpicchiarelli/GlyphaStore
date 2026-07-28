#include "cli/arguments.hpp"
#include "glyphastore/persistence/store_backup.hpp"

#include <array>
#include <exception>
#include <iostream>
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
                                 "Skip committed Record scans during source/destination verify"},
};

void print_help(const std::string_view program) {
    glyphastore::cli::write_help(
        std::cout, program,
        "Offline backup (or restore) of a GlyphaStore durable data directory.",
        "[OPTIONS] <SOURCE-DATA-DIR> <DESTINATION-DATA-DIR>", kOptionSpecs);
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

void write_text_ok(std::ostream& out, const glyphastore::DurableStoreBackupReport& report) {
    out << "status=ok\n"
        << "source=" << report.source.string() << '\n'
        << "destination=" << report.destination.string() << '\n'
        << "files_copied=" << report.files_copied << '\n'
        << "bytes_copied=" << report.bytes_copied << '\n'
        << "source_segments=" << report.source_verification.segments.size() << '\n'
        << "destination_segments=" << report.destination_verification.segments.size() << '\n';
}

void write_json_ok(std::ostream& out, const glyphastore::DurableStoreBackupReport& report) {
    out << '{'
        << "\"status\":\"ok\","
        << "\"source\":\"" << json_escape(report.source.string()) << "\","
        << "\"destination\":\"" << json_escape(report.destination.string()) << "\","
        << "\"files_copied\":" << report.files_copied << ','
        << "\"bytes_copied\":" << report.bytes_copied << ','
        << "\"source_segments\":" << report.source_verification.segments.size() << ','
        << "\"destination_segments\":" << report.destination_verification.segments.size() << "}\n";
}

void write_json_error(std::ostream& out, const std::string_view source, const std::string_view destination,
                      const glyphastore::Error& error) {
    out << '{'
        << "\"status\":\"error\","
        << "\"source\":\"" << json_escape(source) << "\","
        << "\"destination\":\"" << json_escape(destination) << "\","
        << "\"error\":{"
        << "\"code\":\"" << error_code_name(error.code) << "\","
        << "\"message\":\"" << json_escape(error.message) << "\""
        << "}}\n";
}

} // namespace

int main(const int argc, char** argv) try {
    const auto program =
        glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastore_backup_store");
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
    if (parsed->positionals.size() != 2) {
        std::cerr << program << ": error: expected source and destination data directories\nTry '"
                  << program << " --help' for more information.\n";
        return 2;
    }

    const auto source = std::string{parsed->positionals[0]};
    const auto destination = std::string{parsed->positionals[1]};
    const bool emit_json = parsed->has(json);
    const bool scan = !parsed->has(no_scan);
    auto report = glyphastore::backup_durable_store(source, destination, scan);
    if (!report) {
        if (emit_json) {
            write_json_error(std::cout, source, destination, report.error());
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
        glyphastore::cli::executable_name(argc > 0 ? argv[0] : "glyphastore_backup_store");
    std::cerr << program << ": fatal: " << exception.what() << '\n';
    return 1;
} catch (...) {
    std::cerr << "glyphastore_backup_store: fatal: unknown non-standard exception\n";
    return 1;
}
