#pragma once

#include "glyphastore/core/error.hpp"
#include "glyphastore/server/connection_handoff.hpp"
#include "glyphastore/server/disk_read_executor.hpp"
#include "glyphastore/server/durable_mutation_executor.hpp"
#include "glyphastore/server/reactor.hpp"
#include "glyphastore/server/tls.hpp"
#include "glyphastore/store/store.hpp"

#include <memory>
#include <vector>

namespace glyphastore::server {

class ReactorFactory final {
  public:
    ReactorFactory() = delete;

    [[nodiscard]] static auto create_all(const ReactorConfig& config, Store& store,
                                         ConnectionHandoffMesh& mesh, DiskReadExecutor& disk_reads,
                                         DurableMutationExecutor* durable_mutations,
                                         ServerLifecycleProbes lifecycle_probes,
                                         std::shared_ptr<TlsContext> tls_context,
                                         std::shared_ptr<AbuseController> abuse = {})
        -> Result<std::vector<std::unique_ptr<Reactor>>>;
};

} // namespace glyphastore::server
