#include "glyphastore/server/connection_handoff.hpp"

#include <utility>

namespace glyphastore::server {

ConnectionHandoffMesh::ConnectionHandoffMesh(const std::size_t executor_count,
                                             const std::size_t queue_capacity) {
    endpoints_.reserve(executor_count);
    for (std::size_t index = 0; index < executor_count; ++index) {
        endpoints_.push_back(std::make_unique<Endpoint>(queue_capacity));
    }
}

void ConnectionHandoffMesh::register_wakeup(const std::size_t executor, Wakeup& wakeup) noexcept {
    endpoints_[executor]->wakeup = &wakeup;
}

auto ConnectionHandoffMesh::try_handoff(const std::size_t target_executor, ConnectionHandoff&& connection)
    -> bool {
    auto& endpoint = *endpoints_[target_executor];
    if (!endpoint.connections.try_push(std::move(connection))) {
        return false;
    }
    if (endpoint.wakeup != nullptr) {
        static_cast<void>(endpoint.wakeup->notify());
    }
    return true;
}

auto ConnectionHandoffMesh::try_pop(const std::size_t executor) -> std::optional<ConnectionHandoff> {
    return endpoints_[executor]->connections.try_pop();
}

} // namespace glyphastore::server
