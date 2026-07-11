#include <iostream>

int main(int argc, [[maybe_unused]] char** argv) {
    if (argc < 2) {
        std::cerr << "usage: glyphastore_rebuild_index <segment-file>...\n";
        return 2;
    }
    std::cout << "Index rebuild tool bootstrap: " << (argc - 1)
              << " segment path(s) supplied. File-backed recovery is not stable yet.\n";
    return 0;
}
