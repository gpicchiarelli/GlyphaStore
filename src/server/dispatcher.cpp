#include "glyphastore/server/dispatcher.hpp"

#include <utility>

namespace glyphastore::server {

DispatchMesh::DispatchMesh(const std::size_t executor_count, const std::size_t inbox_capacity,
                           const std::size_t completion_capacity) {
    endpoints_.reserve(executor_count);
    for (std::size_t index = 0; index < executor_count; ++index) {
        endpoints_.push_back(std::make_unique<Endpoint>(inbox_capacity, completion_capacity));
    }
}

void DispatchMesh::register_wakeup(const std::size_t executor, Wakeup& wakeup) noexcept {
    endpoints_[executor]->wakeup = &wakeup;
}

auto DispatchMesh::try_submit(const std::size_t target_executor, DispatchTask&& task) -> bool {
    auto& endpoint = *endpoints_[target_executor];
    if (!endpoint.inbox.try_push(std::move(task))) {
        return false;
    }
    if (endpoint.wakeup != nullptr) {
        static_cast<void>(endpoint.wakeup->notify());
    }
    return true;
}

auto DispatchMesh::try_pop_task(const std::size_t executor) -> std::optional<DispatchTask> {
    return endpoints_[executor]->inbox.try_pop();
}

auto DispatchMesh::try_complete(const std::size_t target_executor, DispatchCompletion&& completion) -> bool {
    auto& endpoint = *endpoints_[target_executor];
    if (!endpoint.completions.try_push(std::move(completion))) {
        return false;
    }
    if (endpoint.wakeup != nullptr) {
        static_cast<void>(endpoint.wakeup->notify());
    }
    return true;
}

auto DispatchMesh::try_pop_completion(const std::size_t executor) -> std::optional<DispatchCompletion> {
    return endpoints_[executor]->completions.try_pop();
}

auto DispatchMesh::try_handoff(const std::size_t target_executor, SocketHandle&& socket) -> bool {
    auto& endpoint = *endpoints_[target_executor];
    if (!endpoint.accepted.try_push(std::move(socket))) {
        return false;
    }
    if (endpoint.wakeup != nullptr) {
        static_cast<void>(endpoint.wakeup->notify());
    }
    return true;
}

auto DispatchMesh::try_pop_handoff(const std::size_t executor) -> std::optional<SocketHandle> {
    return endpoints_[executor]->accepted.try_pop();
}

} // namespace glyphastore::server
