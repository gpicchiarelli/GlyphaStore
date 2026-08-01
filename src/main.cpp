#include "glyphastore/core/types.hpp"
#include "glyphastore/worker/topology.hpp"

#include <iostream>

int main() {
    const auto topology = glyphastore::detect_worker_topology();
    const auto workers = glyphastore::WorkerCountPolicy::choose(topology, {});
    std::cout << "GlyphaStore architecture bootstrap\n"
              << "segment_size_bytes=" << glyphastore::kSegmentSizeBytes << '\n'
              << "logical_cpus=" << topology.logical_cpus << '\n'
              << "physical_cores=" << topology.physical_cores << '\n'
              << "selected_workers=" << workers << '\n';
    return 0;
}
