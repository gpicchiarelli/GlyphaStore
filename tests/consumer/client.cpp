#include "glyphastore/client/client.hpp"

int main() {
    auto invalid = glyphastore::client::Client::connect({.port = 0});
    return !invalid && invalid.error().code == glyphastore::ErrorCode::invalid_argument ? 0 : 1;
}
