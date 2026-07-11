#include <iostream>

#include "glyphastore/core/types.hpp"

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: glyphastore_inspect_segment <segment-file>\n";
        return 2;
    }
    std::cout << "Segment inspector bootstrap\n"
              << "path=" << argv[1] << '\n'
              << "expected_segment_size=" << glyphastore::kSegmentSizeBytes << '\n'
              << "Persistent SegmentHeader decoding is not stable yet.\n";
    return 0;
}
