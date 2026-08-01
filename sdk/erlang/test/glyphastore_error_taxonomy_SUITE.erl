-module(glyphastore_error_taxonomy_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%% Mirrors tests/fixtures/error_taxonomy_v1.json (GS-PROTO-ERROR-001).
all() -> [wire_status_matrix].

wire_status_matrix(_Config) ->
    lists:foreach(fun(Case) -> check_case(Case) end, cases()).

cases() ->
    [
        {invalid_request, 1, invalid_argument, never, rejected, reconcile_first, false},
        {unsupported, 2, invalid_argument, never, rejected, reconcile_first, false},
        {internal_error, 3, internal, new_attempt, indeterminate, reconcile_first, false},
        {not_found, 4, not_found, new_attempt, rejected, new_attempt, false},
        {overloaded, 5, overloaded, never, rejected, never, false},
        {wrong_owner, 6, protocol, new_attempt, rejected, reconcile_first, true},
        {not_bound, 7, unavailable, never, rejected, never, true},
        {permission_denied, 8, permission_denied, never, rejected, never, false},
        {unknown_wire_status, 99, protocol, new_attempt, indeterminate, reconcile_first, false}
    ].

check_case({Id, Status, Category, ReadRetry, MutOutcome, MutRetry, Unhealthy}) ->
    Err = glyphastore_error:from_status(Status),
    Category = maps:get(category, Err),
    Status = maps:get(wire_status, Err),
    ReadRetry = maps:get(retryability, Err),
    Enriched = glyphastore_error:enrich(Err, #{
        bytes_sent => 1,
        mutation_outcome => MutOutcome
    }),
    MutRetry = maps:get(retryability, Enriched),
    WantUnhealthy = (Status =:= 6) orelse (Status =:= 7),
    Unhealthy = WantUnhealthy,
    ct:pal("ok ~p", [Id]),
    ok.
