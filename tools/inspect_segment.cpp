#include "cli/arguments.hpp"
#include "glyphastore/core/types.hpp"

#include <array>
#include <exception>
#include <iostream>
#include <string_view>

namespace {

enum OptionId : std::size_t { help, version };

constexpr std::array kOptionSpecs{
    glyphastore::cli::OptionSpec{
        help, "help", 'h', glyphastore::cli::OptionArity::none, {}, "Show this help message and exit"},
    glyphastore::cli::OptionSpec{version,
                                 "version",
                                 'V',
                                 glyphastore::cli::OptionArity::none,
                                 {},
                                 "Show version information and exit"},
};

void print_help(const std::string_view program) {
    glyphastore::cli::write_help(std::cout, program, "Inspect a GlyphaStore segment file.",
                                 "[OPTIONS] <SEGMENT-FILE>", kOptionSpecs);
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
    std::cout << "Segment inspector bootstrap\n"
              << "path=" << parsed->positionals.front() << '\n'
              << "expected_segment_size=" << glyphastore::kSegmentSizeBytes << '\n'
              << "Persistent SegmentHeader decoding is not stable yet.\n";
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
