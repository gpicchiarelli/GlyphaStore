#include "glyphastore/store/paired/completion_policy.hpp"
#include "glyphastore/store/paired/mutation_recovery.hpp"
#include "test.hpp"

using glyphastore::store::paired::CommitKnowledge;
using glyphastore::store::paired::CompletionDecision;
using glyphastore::store::paired::DurableDecision;
using glyphastore::store::paired::PublicationDecision;
using glyphastore::store::paired::PublicationState;
using glyphastore::store::paired::SyncDurableExceptionContext;
using glyphastore::store::paired::SyncDurableExceptionStatusKind;
using glyphastore::store::paired::decide_completion;
using glyphastore::store::paired::plan_sync_durable_exception_recovery;
using glyphastore::store::paired::plan_sync_durable_exception_status;
using glyphastore::store::paired::status_from_completion;
using glyphastore::store::paired::wire_error_code_for;

GLYPHA_TEST("completion_policy wire codes match taxonomy") {
    GLYPHA_REQUIRE(wire_error_code_for(CompletionDecision::Kind::known_not_committed) ==
                   glyphastore::ErrorCode::resource_exhausted);
    GLYPHA_REQUIRE(wire_error_code_for(CompletionDecision::Kind::indeterminate) ==
                   glyphastore::ErrorCode::unavailable);
}

GLYPHA_TEST("completion_policy status_from_completion polarities") {
    {
        CompletionDecision d{.kind = CompletionDecision::Kind::success};
        const auto status = status_from_completion(d, "k", "i");
        GLYPHA_REQUIRE(status.has_value());
    }
    {
        CompletionDecision d{.kind = CompletionDecision::Kind::known_not_committed};
        const auto status = status_from_completion(d, "known", "indet");
        GLYPHA_REQUIRE(!status.has_value());
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::resource_exhausted);
        GLYPHA_REQUIRE(status.error().message == "known");
    }
    {
        CompletionDecision d{.kind = CompletionDecision::Kind::indeterminate};
        const auto status = status_from_completion(d, "known", "indet");
        GLYPHA_REQUIRE(!status.has_value());
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::unavailable);
        GLYPHA_REQUIRE(status.error().message == "indet");
    }
    // decide_completion → status_from_completion round-trip for known-not-committed.
    {
        DurableDecision durable{.knowledge = CommitKnowledge::known_not_committed, .mutate_entered = true};
        PublicationDecision publication{};
        const auto decided = decide_completion(durable, publication);
        const auto status = status_from_completion(decided, "overloaded", "sticky");
        GLYPHA_REQUIRE(!status.has_value());
        GLYPHA_REQUIRE(status.error().code == glyphastore::ErrorCode::resource_exhausted);
    }
}

GLYPHA_TEST("mutation_recovery sync durable exception: never entered") {
    const SyncDurableExceptionContext ctx{};
    const auto recovery = plan_sync_durable_exception_recovery(ctx);
    GLYPHA_REQUIRE(!recovery.drain_if_unpublished);
    GLYPHA_REQUIRE(!recovery.fail_closed);
    GLYPHA_REQUIRE(!recovery.mark_exception_lifecycle);
    const auto status = plan_sync_durable_exception_status(ctx);
    GLYPHA_REQUIRE(status.kind == SyncDurableExceptionStatusKind::resource_exhausted_never_entered);
}

GLYPHA_TEST("mutation_recovery sync durable exception: entered unpublished") {
    const SyncDurableExceptionContext ctx{.durable_mutate_entered = true};
    const auto recovery = plan_sync_durable_exception_recovery(ctx);
    GLYPHA_REQUIRE(recovery.drain_if_unpublished);
    GLYPHA_REQUIRE(recovery.fail_closed);
    GLYPHA_REQUIRE(recovery.mark_exception_lifecycle);
    const auto status = plan_sync_durable_exception_status(ctx);
    GLYPHA_REQUIRE(status.kind == SyncDurableExceptionStatusKind::unavailable_store_entered);
}

GLYPHA_TEST("mutation_recovery sync durable exception: published committed keeps success") {
    const SyncDurableExceptionContext ctx{.durable_committed = true,
                                          .durable_mutate_entered = true,
                                          .generation_published = true};
    const auto recovery = plan_sync_durable_exception_recovery(ctx);
    GLYPHA_REQUIRE(!recovery.drain_if_unpublished);
    GLYPHA_REQUIRE(recovery.fail_closed);
    const auto status = plan_sync_durable_exception_status(ctx);
    GLYPHA_REQUIRE(status.kind == SyncDurableExceptionStatusKind::success_after_visibility);
}

GLYPHA_TEST("mutation_recovery sync durable exception: status_resolved sticks") {
    const SyncDurableExceptionContext ctx{.durable_committed = true,
                                          .durable_mutate_entered = true,
                                          .generation_published = true,
                                          .status_resolved = true};
    const auto status = plan_sync_durable_exception_status(ctx);
    GLYPHA_REQUIRE(status.kind == SyncDurableExceptionStatusKind::keep_resolved);
}
