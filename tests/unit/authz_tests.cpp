#include "glyphastore/server/authz.hpp"
#include "glyphastore/server/protocol.hpp"
#include "test.hpp"

#include <string>

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
