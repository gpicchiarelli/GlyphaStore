#include "glyphastore/store/paired/mutation_state.hpp"
#include "test.hpp"

#include <optional>

using glyphastore::store::paired::CommitKnowledge;
using glyphastore::store::paired::CompletionDecision;
using glyphastore::store::paired::DurableDecision;
using glyphastore::store::paired::MutationLifecycle;
using glyphastore::store::paired::MutationStage;
using glyphastore::store::paired::PublicationDecision;
using glyphastore::store::paired::PublicationState;
using glyphastore::store::paired::commit_knowledge_from;
using glyphastore::store::paired::decide_completion;
using glyphastore::store::paired::durable_outcome_from;

GLYPHA_TEST("mutation_state CommitKnowledge aliases DurableMutationOutcome") {
    GLYPHA_REQUIRE(commit_knowledge_from(glyphastore::DurableMutationOutcome::committed) ==
                   CommitKnowledge::committed);
    GLYPHA_REQUIRE(commit_knowledge_from(glyphastore::DurableMutationOutcome::not_committed) ==
                   CommitKnowledge::known_not_committed);
    GLYPHA_REQUIRE(commit_knowledge_from(glyphastore::DurableMutationOutcome::indeterminate) ==
                   CommitKnowledge::indeterminate);
    GLYPHA_REQUIRE(durable_outcome_from(CommitKnowledge::committed) ==
                   glyphastore::DurableMutationOutcome::committed);
}

GLYPHA_TEST("mutation_state illegal transitions are rejected by table") {
    GLYPHA_REQUIRE(!MutationLifecycle::transition_allowed(MutationStage::not_admitted,
                                                          MutationStage::completed));
    GLYPHA_REQUIRE(!MutationLifecycle::transition_allowed(MutationStage::published,
                                                          MutationStage::rejected));
    GLYPHA_REQUIRE(!MutationLifecycle::transition_allowed(MutationStage::completed,
                                                          MutationStage::admitted));
    GLYPHA_REQUIRE(MutationLifecycle::transition_allowed(MutationStage::not_admitted,
                                                         MutationStage::admitted));
    GLYPHA_REQUIRE(MutationLifecycle::transition_allowed(MutationStage::durable_started,
                                                         MutationStage::authority_committed));
}

GLYPHA_TEST("mutation_state happy durable path reaches completed success") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.stage_for_writer());
    GLYPHA_REQUIRE(life.mark_durable_started());
    glyphastore::DurableMutationResult result{.outcome = glyphastore::DurableMutationOutcome::committed,
                                              .sequence = glyphastore::SequenceNumber{1},
                                              .error = std::nullopt};
    GLYPHA_REQUIRE(life.apply_durable_result(result));
    GLYPHA_REQUIRE(life.stage() == MutationStage::publication_required);
    GLYPHA_REQUIRE(life.mark_publication_staged());
    GLYPHA_REQUIRE(life.mark_published());
    const auto decided = decide_completion(life.durable(), life.publication());
    GLYPHA_REQUIRE(decided.kind == CompletionDecision::Kind::success);
    GLYPHA_REQUIRE(!decided.fail_closed_required);
    GLYPHA_REQUIRE(life.decide(decided));
    GLYPHA_REQUIRE(life.mark_completed());
    GLYPHA_REQUIRE(life.stage() == MutationStage::completed);
}

GLYPHA_TEST("mutation_state known-not-committed never becomes success") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.stage_for_writer());
    GLYPHA_REQUIRE(life.mark_durable_started());
    glyphastore::DurableMutationResult result{
        .outcome = glyphastore::DurableMutationOutcome::not_committed,
        .sequence = std::nullopt,
        .error = glyphastore::Error{glyphastore::ErrorCode::segment_full, "full"}};
    GLYPHA_REQUIRE(life.apply_durable_result(result));
    GLYPHA_REQUIRE(life.stage() == MutationStage::completion_decided);
    GLYPHA_REQUIRE(life.completion().kind == CompletionDecision::Kind::known_not_committed);
    GLYPHA_REQUIRE(!life.mark_published());
}

GLYPHA_TEST("mutation_state committed cannot decide known_not_committed") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.stage_for_writer());
    GLYPHA_REQUIRE(life.mark_durable_started());
    glyphastore::DurableMutationResult result{.outcome = glyphastore::DurableMutationOutcome::committed,
                                              .sequence = glyphastore::SequenceNumber{2},
                                              .error = std::nullopt};
    GLYPHA_REQUIRE(life.apply_durable_result(result));
    GLYPHA_REQUIRE(life.mark_published());
    CompletionDecision illegal{.kind = CompletionDecision::Kind::known_not_committed};
    GLYPHA_REQUIRE(!life.decide(illegal));
}

GLYPHA_TEST("mutation_state completion_decided cannot change outcome") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.expire_pre_store());
    GLYPHA_REQUIRE(life.stage() == MutationStage::completion_decided);
    CompletionDecision again{.kind = CompletionDecision::Kind::success};
    GLYPHA_REQUIRE(!life.decide(again));
}

GLYPHA_TEST("mutation_state exception after durable_started is indeterminate") {
    MutationLifecycle life;
    GLYPHA_REQUIRE(life.admit());
    GLYPHA_REQUIRE(life.stage_for_writer());
    GLYPHA_REQUIRE(life.mark_durable_started());
    GLYPHA_REQUIRE(life.mark_exception_after_durable_start());
    GLYPHA_REQUIRE(life.stage() == MutationStage::indeterminate);
    GLYPHA_REQUIRE(life.durable().knowledge == CommitKnowledge::indeterminate);
    GLYPHA_REQUIRE(life.durable().mutate_entered);
    GLYPHA_REQUIRE(life.publication().state == PublicationState::required);
    const auto decided = decide_completion(life.durable(), life.publication());
    GLYPHA_REQUIRE(decided.kind == CompletionDecision::Kind::indeterminate);
    GLYPHA_REQUIRE(decided.fail_closed_required);
    GLYPHA_REQUIRE(life.decide(decided));
    GLYPHA_REQUIRE(life.mark_completed());
}

GLYPHA_TEST("mutation_state decide_completion characterization table") {
    // known_not_committed → OVERLOADED polarity, no fail-closed.
    {
        DurableDecision d{.knowledge = CommitKnowledge::known_not_committed,
                          .error = glyphastore::Error{glyphastore::ErrorCode::io_error, {}},
                          .mutate_entered = true};
        PublicationDecision p{};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::known_not_committed);
        GLYPHA_REQUIRE(!c.fail_closed_required);
    }
    // clean commit + published → success.
    {
        DurableDecision d{.knowledge = CommitKnowledge::committed, .mutate_entered = true};
        PublicationDecision p{.state = PublicationState::published};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::success);
        GLYPHA_REQUIRE(!c.fail_closed_required);
    }
    // clean commit without publish → indeterminate + fail-closed.
    {
        DurableDecision d{.knowledge = CommitKnowledge::committed, .mutate_entered = true};
        PublicationDecision p{.state = PublicationState::failed};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::indeterminate);
        GLYPHA_REQUIRE(c.fail_closed_required);
    }
    // committed+error + published → success (ACK-after-visibility), fail-closed.
    {
        DurableDecision d{.knowledge = CommitKnowledge::committed,
                          .error = glyphastore::Error{glyphastore::ErrorCode::internal_error, {}},
                          .mutate_entered = true};
        PublicationDecision p{.state = PublicationState::published};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::success);
        GLYPHA_REQUIRE(c.fail_closed_required);
    }
    // indeterminate → indeterminate + drain + fail-closed.
    {
        DurableDecision d{.knowledge = CommitKnowledge::indeterminate, .mutate_entered = true};
        PublicationDecision p{.state = PublicationState::failed};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::indeterminate);
        GLYPHA_REQUIRE(c.fail_closed_required);
        GLYPHA_REQUIRE(c.drain_required);
    }
    // expired pre-store (never entered).
    {
        DurableDecision d{};
        PublicationDecision p{};
        const auto c = decide_completion(d, p);
        GLYPHA_REQUIRE(c.kind == CompletionDecision::Kind::known_not_committed);
        GLYPHA_REQUIRE(!c.fail_closed_required);
    }
}
