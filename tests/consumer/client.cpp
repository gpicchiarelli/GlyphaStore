#include "glyphastore/client/client.hpp"

int main() {
    auto invalid = glyphastore::client::Client::connect({.port = 0});
    if (!invalid && invalid.error().code == glyphastore::ErrorCode::invalid_argument) {
        return 0;
    }
    return 1;
}
