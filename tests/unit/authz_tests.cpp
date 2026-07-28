#include "glyphastore/server/authz.hpp"
#include "glyphastore/server/protocol.hpp"
#include "test.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

[[nodiscard]] auto key_bytes(std::string_view text) -> std::vector<std::byte> {
    std::vector<std::byte> out(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(text[i]));
    }
    return out;
}

} // namespace

GLYPHA_TEST("authz write implies read and admin implies write") {
    using glyphastore::server::Capability;
    using glyphastore::server::normalize_capabilities;
    using glyphastore::server::has_capability;

    const auto write_only = normalize_capabilities(Capability::write);
    GLYPHA_REQUIRE(has_capability(write_only, Capability::write));
    GLYPHA_REQUIRE(has_capability(write_only, Capability::read));

    const auto admin_only = normalize_capabilities(Capability::admin);
    GLYPHA_REQUIRE(has_capability(admin_only, Capability::admin));
    GLYPHA_REQUIRE(has_capability(admin_only, Capability::write));
    GLYPHA_REQUIRE(has_capability(admin_only, Capability::read));
}

GLYPHA_TEST("authz map parses principals and default-denies unknowns") {
    const auto text = R"(
# reader
reader.example read
writer.example write
admin.example admin
)";
    auto policy = glyphastore::server::AuthzPolicy::parse(text);
    GLYPHA_REQUIRE(policy.has_value());
    GLYPHA_REQUIRE(policy->enabled());
    GLYPHA_REQUIRE(policy->size() == 3);

    using glyphastore::server::Capability;
    using glyphastore::server::has_capability;
    using glyphastore::server::authorize_opcode;
    using glyphastore::server::RequestOpcode;

    const auto reader = policy->capabilities_for("reader.example");
    GLYPHA_REQUIRE(has_capability(reader, Capability::read));
    GLYPHA_REQUIRE(!has_capability(reader, Capability::write));
    GLYPHA_REQUIRE(authorize_opcode(*policy, reader, RequestOpcode::get));
    GLYPHA_REQUIRE(authorize_opcode(*policy, reader, RequestOpcode::ping));
    GLYPHA_REQUIRE(!authorize_opcode(*policy, reader, RequestOpcode::put));
    GLYPHA_REQUIRE(authorize_opcode(*policy, reader, RequestOpcode::init));
    GLYPHA_REQUIRE(authorize_opcode(*policy, reader, RequestOpcode::health));

    const auto writer = policy->capabilities_for("writer.example");
    GLYPHA_REQUIRE(authorize_opcode(*policy, writer, RequestOpcode::put));
    GLYPHA_REQUIRE(authorize_opcode(*policy, writer, RequestOpcode::get));
    GLYPHA_REQUIRE(authorize_opcode(*policy, writer, RequestOpcode::erase));

    const auto unknown = policy->capabilities_for("nobody");
    GLYPHA_REQUIRE(unknown == Capability::none);
    GLYPHA_REQUIRE(!authorize_opcode(*policy, unknown, RequestOpcode::get));
    GLYPHA_REQUIRE(authorize_opcode(*policy, unknown, RequestOpcode::ready));
}

GLYPHA_TEST("authz map rejects unknown capability and duplicates") {
    const auto bad_cap = glyphastore::server::AuthzPolicy::parse("alice mutate");
    GLYPHA_REQUIRE(!bad_cap.has_value());

    const auto duplicate = glyphastore::server::AuthzPolicy::parse("alice read\nalice write\n");
    GLYPHA_REQUIRE(!duplicate.has_value());
}

GLYPHA_TEST("authz disabled policy allows all opcodes") {
    glyphastore::server::AuthzPolicy policy;
    GLYPHA_REQUIRE(!policy.enabled());
    GLYPHA_REQUIRE(glyphastore::server::authorize_opcode(
        policy, glyphastore::server::Capability::none, glyphastore::server::RequestOpcode::put));
}

GLYPHA_TEST("authz map parses optional key prefix and rejects empty prefix") {
    auto policy = glyphastore::server::AuthzPolicy::parse(
        "tenant-a write prefix=a/\ntenant-b read prefix=b/\nshared write\n");
    GLYPHA_REQUIRE(policy.has_value());
    GLYPHA_REQUIRE(policy->size() == 3);
    GLYPHA_REQUIRE(policy->prefix_scoped_count() == 2);
    GLYPHA_REQUIRE(policy->key_prefix_for("tenant-a") == "a/");
    GLYPHA_REQUIRE(policy->key_prefix_for("tenant-b") == "b/");
    GLYPHA_REQUIRE(policy->key_prefix_for("shared").empty());

    const auto empty_prefix = glyphastore::server::AuthzPolicy::parse("alice write prefix=");
    GLYPHA_REQUIRE(!empty_prefix.has_value());

    const auto junk = glyphastore::server::AuthzPolicy::parse("alice write scope=a/");
    GLYPHA_REQUIRE(!junk.has_value());
}

GLYPHA_TEST("authz key prefix denies cross-tenant GET PUT ERASE and allows in-prefix") {
    using glyphastore::server::Capability;
    using glyphastore::server::RequestOpcode;
    using glyphastore::server::authorize_request;

    auto policy = glyphastore::server::AuthzPolicy::parse(
        "tenant-a write prefix=tenant-a/\ntenant-b write prefix=tenant-b/\n");
    GLYPHA_REQUIRE(policy.has_value());

    const auto grant_a = policy->grant_for("tenant-a");
    const auto in_a = key_bytes("tenant-a/orders/1");
    const auto in_b = key_bytes("tenant-b/orders/1");
    const auto bare = key_bytes("other");

    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::get, in_a));
    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::put, in_a));
    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::erase, in_a));

    GLYPHA_REQUIRE(!authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                      RequestOpcode::get, in_b));
    GLYPHA_REQUIRE(!authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                      RequestOpcode::put, in_b));
    GLYPHA_REQUIRE(!authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                      RequestOpcode::erase, bare));

    // Prefix does not gate lifecycle / ping; STATS requires admin for prefix tenants (ADR 0027).
    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::ping, in_b));
    GLYPHA_REQUIRE(!authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                      RequestOpcode::stats, bare));
    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::health, std::span<const std::byte>{}));

    glyphastore::server::AuthzPolicy admin_policy;
    admin_policy.bind("tenant-a", Capability::admin, "tenant-a/");
    const auto admin_grant = admin_policy.grant_for("tenant-a");
    GLYPHA_REQUIRE(authorize_request(admin_policy, admin_grant.capabilities, admin_grant.key_prefix,
                                     RequestOpcode::stats, bare));

    // Exact-prefix boundary: prefix alone is allowed; shorter key denied.
    const auto exact = key_bytes("tenant-a/");
    const auto short_key = key_bytes("tenant-a");
    GLYPHA_REQUIRE(authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                     RequestOpcode::get, exact));
    GLYPHA_REQUIRE(!authorize_request(*policy, grant_a.capabilities, grant_a.key_prefix,
                                      RequestOpcode::get, short_key));
}

GLYPHA_TEST("authz unrestricted principal keeps whole-keyspace access") {
    using glyphastore::server::Capability;
    using glyphastore::server::RequestOpcode;
    using glyphastore::server::authorize_request;

    glyphastore::server::AuthzPolicy policy;
    policy.bind("ops", Capability::write);
    const auto grant = policy.grant_for("ops");
    GLYPHA_REQUIRE(grant.key_prefix.empty());
    const auto foreign = key_bytes("anyone/key");
    GLYPHA_REQUIRE(authorize_request(policy, grant.capabilities, grant.key_prefix, RequestOpcode::get,
                                     foreign));
    GLYPHA_REQUIRE(authorize_request(policy, grant.capabilities, grant.key_prefix, RequestOpcode::stats,
                                     foreign));
}

GLYPHA_TEST("authz prefix-scoped STATS requires admin capability") {
    using glyphastore::server::Capability;
    using glyphastore::server::RequestOpcode;
    using glyphastore::server::authorize_opcode;
    using glyphastore::server::required_capability;

    GLYPHA_REQUIRE(required_capability(RequestOpcode::stats, {}) == Capability::read);
    GLYPHA_REQUIRE(required_capability(RequestOpcode::stats, "t/") == Capability::admin);

    glyphastore::server::AuthzPolicy policy;
    policy.bind("tenant", Capability::write, "tenant/");
    const auto grant = policy.grant_for("tenant");
    GLYPHA_REQUIRE(!authorize_opcode(policy, grant.capabilities, RequestOpcode::stats, grant.key_prefix));
    GLYPHA_REQUIRE(authorize_opcode(policy, Capability::admin, RequestOpcode::stats, "tenant/"));
}
