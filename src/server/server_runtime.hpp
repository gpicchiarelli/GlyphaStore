#pragma once

#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/pair_writer.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/store/store.hpp"

#include <memory>
#include <vector>

namespace glyphastore::server {

// Owned runtime graph assembled by ServerBuilder. Server is the lifecycle
// façade over this aggregate, not the composition root.
struct ServerRuntime final {
    std::unique_ptr<Store> store;
    std::unique_ptr<DiskReadExecutor> disk_reads;
    std::unique_ptr<PairWriterPool> pair_writers;
    ConnectionHandoffMesh mesh;
    std::vector<std::unique_ptr<Reactor>> reactors;
};

} // namespace glyphastore::server
