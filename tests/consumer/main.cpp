#include "glyphastore/store/store.hpp"

int main() {
    auto store = glyphastore::Store::open({.worker_config = {.explicit_count = 1}});
    return store.has_value() ? 0 : 1;
}
