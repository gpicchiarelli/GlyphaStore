#include "glyphastore/store/paired/completion_policy.hpp"
#include "glyphastore/store/paired/lane_state.hpp"
#include "glyphastore/store/paired/mutation_state.hpp"
#include "test.hpp"

#include <array>

using glyphastore::store::paired::CommitKnowledge;
using glyphastore::store::paired::CompletionDecision;
using glyphastore::store::paired::decide_completion;
using glyphastore::store::paired::DurableDecision;
using glyphastore::store::paired::MutationLifecycle;
using glyphastore::store::paired::MutationStage;
using glyphastore::store::paired::PublicationDecision;
using glyphastore::store::paired::PublicationState;
using glyphastore::store::paired::wire_error_code_for;

GLYPHA_TEST("property OVERLOADED wire code iff known_not_committed completion") {
    const std::array knowledge{
        CommitKnowledge::known_not_committed,
        CommitKnowledge::committed,
        CommitKnowledge::indeterminate,
    };
    const std::array publications{
        PublicationState::not_required,
        PublicationState::required,
        PublicationState::staged,
        PublicationState::published,
        PublicationState::failed,
    };
    for (const auto commit : knowledge) {
        for (const auto publication : publications) {
            const DurableDecision durable{.knowledge = commit, .mutate_entered = true};
            const PublicationDecision pub{.state = publication};
            const auto decided = decide_completion(durable, pub);
            if (decided.kind == CompletionDecision::Kind::undecided) {
                continue;
            }
            const auto code = wire_error_code_for(decided.kind);
            if (decided.kind == CompletionDecision::Kind::known_not_committed) {
                GLYPHA_REQUIRE(code == glyphastore::ErrorCode::resource_exhausted);
            } else if (decided.kind == CompletionDecision::Kind::indeterminate) {
                GLYPHA_REQUIRE(code == glyphastore::ErrorCode::unavailable);
            } else if (decided.kind == CompletionDecision::Kind::success) {
                // success must not be routed through wire_error_code_for by callers;
                // the helper returns internal_error as a guard rail.
                GLYPHA_REQUIRE(code == glyphastore::ErrorCode::internal_error);
            }
        }
    }
}

GLYPHA_TEST("property decided completion cannot change outcome") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.expire_pre_store());
    GLYPHA_REQUIRE(life.stage() == MutationStage::completion_decided);
    CompletionDecision again{.kind = CompletionDecision::Kind::success};
    GLYPHA_REQUIRE(!life.decide(again));
    GLYPHA_REQUIRE(life.stage() == MutationStage::completion_decided);
}

GLYPHA_TEST("lane_state aggregates keep cache-line alignment contracts") {
    static_assert(alignof(glyphastore::store::paired::AsyncLaneState) >= 128);
    static_assert(alignof(glyphastore::store::paired::GenerationState) >= 128);
    static_assert(alignof(glyphastore::store::paired::SyncLaneState) >= 128);
    static_assert(alignof(glyphastore::store::paired::ReclamationState) >= 128);
}
