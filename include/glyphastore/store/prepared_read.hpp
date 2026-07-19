#pragma once

#include <memory>

namespace glyphastore::detail {

class StoreAccess;

// Opaque, move-only ownership of the exact durable RecordRef and immutable
// Segment generation needed by one cold read. Only StoreAccess can create or
// consume it; server queues merely transport the lifetime token.
class PreparedColdRead final {
  public:
    PreparedColdRead(PreparedColdRead&&) noexcept;
    auto operator=(PreparedColdRead&&) noexcept -> PreparedColdRead&;
    ~PreparedColdRead();
    PreparedColdRead(const PreparedColdRead&) = delete;
    auto operator=(const PreparedColdRead&) -> PreparedColdRead& = delete;

  private:
    struct State;
    explicit PreparedColdRead(std::unique_ptr<State> state) noexcept;
    std::unique_ptr<State> state_;
    friend class StoreAccess;
};

} // namespace glyphastore::detail
