-module(glyphastore_client_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("glyphastore_client_internal.hrl").

all() ->
    [
        put_get_ping_erase,
        internal_error_mutation_is_indeterminate,
        disconnect_after_mutation_is_indeterminate,
        pipeline_put_get,
        pipeline_preserves_order,
        pipeline_disconnect_classifies_each_request,
        pipeline_outer_timeout_put_is_indeterminate,
        pipeline_limits_fail_before_transmission,
        batch_multi_worker,
        batch_preserves_sibling_on_rebind_failure,
        worker_pipelines_concurrent,
        rejects_non_positive_timeout,
        per_call_timeout_overrides_config,
        overloaded_retryability_is_never,
        permission_denied_status,
        backup_internal_error_is_reconcile_first,
        backup_validate_failure_is_reconcile_first,
        tls_requires_cert_and_key_pair,
        close_is_synchronous
    ].

put_get_ping_erase(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        Key = <<"binary\000key">>,
        Value = <<"value\000\377">>,
        #{outcome := committed} = glyphastore_client:put(Client, Key, Value),
        {ok, Value} = glyphastore_client:get(Client, Key),
        {ok, <<"hello">>} = glyphastore_client:ping(Client, <<"hello">>),
        #{outcome := committed} = glyphastore_client:erase(Client, Key),
        {error, Err} = glyphastore_client:get(Client, Key),
        not_found = maps:get(category, Err),
        4 = maps:get(wire_status, Err),
        new_attempt = maps:get(retryability, Err),
        <<"get">> = maps:get(operation, Err),
        true = glyphastore_client:healthy(Client),
        9 = glyphastore_client:routing_epoch(Client),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

internal_error_mutation_is_indeterminate(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{internal_error_on_put => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        #{outcome := indeterminate, error := Err} = glyphastore_client:put(Client, <<"key">>, <<"value">>),
        internal = maps:get(category, Err),
        3 = maps:get(wire_status, Err),
        reconcile_first = maps:get(retryability, Err),
        <<"put">> = maps:get(operation, Err),
        true = maps:get(bytes_sent, Err) > 0,
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

backup_internal_error_is_reconcile_first(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{internal_error_on_backup => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {error, Err} = glyphastore_client:backup(Client, <<"/tmp/glyphastore-erl-backup-internal">>),
        internal = maps:get(category, Err),
        3 = maps:get(wire_status, Err),
        indeterminate = maps:get(mutation_outcome, Err),
        reconcile_first = maps:get(retryability, Err),
        <<"backup">> = maps:get(operation, Err),
        true = maps:get(bytes_sent, Err) > 0,
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

backup_validate_failure_is_reconcile_first(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        ok = glyphastore_fake_server:control(Server, {set_wrong_request_id, true}),
        {error, Err} = glyphastore_client:backup(Client, <<"/tmp/glyphastore-erl-backup-wrong-id">>),
        indeterminate = maps:get(mutation_outcome, Err),
        reconcile_first = maps:get(retryability, Err),
        true = maps:get(bytes_sent, Err) > 0,
        <<"backup">> = maps:get(operation, Err),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

disconnect_after_mutation_is_indeterminate(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{drop_after_mutation => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        #{outcome := indeterminate, error := Err} = glyphastore_client:put(Client, <<"key">>, <<"value">>),
        transport = maps:get(category, Err),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

pipeline_put_get(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        Key = <<"pipe">>,
        Value = <<"v">>,
        {ok, Responses} =
            glyphastore_client:execute_pipeline(Client, [
                #{opcode => put, key => Key, value => Value},
                #{opcode => get, key => Key}
            ]),
        2 = length(Responses),
        succeeded = maps:get(outcome, lists:nth(1, Responses)),
        succeeded = maps:get(outcome, lists:nth(2, Responses)),
        Value = maps:get(value, lists:nth(2, Responses)),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

pipeline_preserves_order(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        Requests = lists:flatmap(
            fun(I) ->
                Value = list_to_binary(io_lib:format("pipeline-~B", [I])),
                [
                    #{opcode => put, key => <<"key">>, value => Value},
                    #{opcode => get, key => <<"key">>}
                ]
            end,
            lists:seq(0, 63)
        ),
        {ok, Responses} = glyphastore_client:execute_pipeline(Client, Requests),
        128 = length(Responses),
        lists:foreach(
            fun(I) ->
                Expected = list_to_binary(io_lib:format("pipeline-~B", [I])),
                succeeded = maps:get(outcome, lists:nth(I * 2 + 1, Responses)),
                succeeded = maps:get(outcome, lists:nth(I * 2 + 2, Responses)),
                Expected = maps:get(value, lists:nth(I * 2 + 2, Responses))
            end,
            lists:seq(0, 63)
        ),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

pipeline_disconnect_classifies_each_request(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{drop_after_mutation => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {ok, Responses} =
            glyphastore_client:execute_pipeline(Client, [
                #{opcode => put, key => <<"key">>, value => <<"value">>},
                #{opcode => get, key => <<"key">>},
                #{opcode => erase, key => <<"key">>}
            ]),
        indeterminate = maps:get(outcome, lists:nth(1, Responses)),
        failed = maps:get(outcome, lists:nth(2, Responses)),
        indeterminate = maps:get(outcome, lists:nth(3, Responses)),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

pipeline_outer_timeout_put_is_indeterminate(_Config) ->
    %% Outer gen_server deadline races inner receive: must return per-slot
    %% indeterminate / reconcile_first, not bare {error, transport}.
    {ok, Server} = glyphastore_fake_server:start(#{hold_puts => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {ok, Responses} =
            glyphastore_client:execute_pipeline(
                Client,
                [
                    #{opcode => put, key => <<"key">>, value => <<"value">>},
                    #{opcode => get, key => <<"key">>}
                ],
                #{timeout => 0.05}
            ),
        indeterminate = maps:get(outcome, lists:nth(1, Responses)),
        Err = maps:get(error, lists:nth(1, Responses)),
        reconcile_first = maps:get(retryability, Err),
        indeterminate = maps:get(mutation_outcome, Err),
        true = maps:get(bytes_sent, Err) > 0,
        failed = maps:get(outcome, lists:nth(2, Responses)),
        ok = glyphastore_fake_server:control(Server, release_held),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

pipeline_limits_fail_before_transmission(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{
            port => glyphastore_fake_server:port(Server),
            maximum_pipeline_requests => 1
        }),
        {error, Err} =
            glyphastore_client:execute_pipeline(Client, [
                #{opcode => get, key => <<"key">>},
                #{opcode => get, key => <<"key">>}
            ]),
        invalid_argument = maps:get(category, Err),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

batch_multi_worker(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 2}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        2 = glyphastore_client:worker_count(Client),
        Keys = find_keys_for_workers(2),
        Requests = lists:flatmap(
            fun(Key) ->
                [
                    #{opcode => put, key => Key, value => <<"v-", Key/binary>>},
                    #{opcode => get, key => Key}
                ]
            end,
            Keys
        ),
        {ok, Responses} = glyphastore_client:execute_batch(Client, Requests),
        4 = length(Responses),
        lists:foreach(fun(Resp) -> succeeded = maps:get(outcome, Resp) end, Responses),
        K1 = lists:nth(1, Keys),
        K2 = lists:nth(2, Keys),
        Expected1 = <<"v-", K1/binary>>,
        Expected2 = <<"v-", K2/binary>>,
        Expected1 = maps:get(value, lists:nth(2, Responses)),
        Expected2 = maps:get(value, lists:nth(4, Responses)),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

batch_preserves_sibling_on_rebind_failure(_Config) ->
    %% Match Python/Go: Worker-0 success survives Worker-1 rebind failure; batch
    %% returns the full slot vector (not a bare {error} that discards siblings).
    {ok, Server} = glyphastore_fake_server:start(#{
        workers => 2,
        fail_rebind_workers => [1]
    }),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        Keys = find_keys_for_workers(2),
        K0 = lists:nth(1, Keys),
        K1 = lists:nth(2, Keys),
        Conn1 = worker_conn(Client, 1),
        exit(Conn1, kill),
        wait_until_dead(Conn1, 200),
        {ok, Responses} = glyphastore_client:execute_batch(Client, [
            #{opcode => put, key => K0, value => <<"a">>},
            #{opcode => put, key => K1, value => <<"b">>}
        ]),
        2 = length(Responses),
        succeeded = maps:get(outcome, lists:nth(1, Responses)),
        failed = maps:get(outcome, lists:nth(2, Responses)),
        Err = maps:get(error, lists:nth(2, Responses)),
        rejected = maps:get(mutation_outcome, Err),
        0 = maps:get(bytes_sent, Err),
        {ok, <<"a">>} = glyphastore_client:get(Client, K0),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

worker_pipelines_concurrent(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 2}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        Keys = find_keys_for_workers(2),
        Wave = [
            [
                #{opcode => put, key => lists:nth(1, Keys), value => <<"a">>},
                #{opcode => get, key => lists:nth(1, Keys)}
            ],
            [
                #{opcode => put, key => lists:nth(2, Keys), value => <<"b">>},
                #{opcode => get, key => lists:nth(2, Keys)}
            ]
        ],
        {ok, All} = glyphastore_client:execute_worker_pipelines(Client, Wave),
        2 = length(All),
        succeeded = maps:get(outcome, lists:nth(1, lists:nth(1, All))),
        <<"a">> = maps:get(value, lists:nth(2, lists:nth(1, All))),
        <<"b">> = maps:get(value, lists:nth(2, lists:nth(2, All))),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

rejects_non_positive_timeout(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {error, Err} = glyphastore_client:get(Client, <<"k">>, #{timeout => 0}),
        invalid_argument = maps:get(category, Err),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

per_call_timeout_overrides_config(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{stall_on_get => true}),
    try
        {ok, Client} = glyphastore_client:connect(#{
            port => glyphastore_fake_server:port(Server),
            request_timeout => 5.0
        }),
        {error, Err} = glyphastore_client:get(Client, <<"key">>, #{timeout => 0.05}),
        transport = maps:get(category, Err),
        {error, Err2} = glyphastore_client:get(Client, <<"key">>, #{timeout => 0}),
        invalid_argument = maps:get(category, Err2),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

overloaded_retryability_is_never(_Config) ->
    never = glyphastore_error:retryability_for(overloaded, false, false),
    never = glyphastore_error:retryability_for(overloaded, true, false),
    Err = glyphastore_error:overloaded(<<"server is overloaded">>),
    overloaded = maps:get(category, Err),
    never = maps:get(retryability, Err),
    {ok, Server} = glyphastore_fake_server:start(#{
        status_on_get => glyphastore_protocol:status_overloaded()
    }),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {error, Wire} = glyphastore_client:get(Client, <<"key">>),
        overloaded = maps:get(category, Wire),
        never = maps:get(retryability, Wire),
        5 = maps:get(wire_status, Wire),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

permission_denied_status(_Config) ->
    Err = glyphastore_error:permission_denied(<<"denied">>),
    permission_denied = maps:get(category, Err),
    never = maps:get(retryability, Err),
    {ok, Server} = glyphastore_fake_server:start(#{
        status_on_get => glyphastore_protocol:status_permission_denied()
    }),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        {error, Wire} = glyphastore_client:get(Client, <<"key">>),
        permission_denied = maps:get(category, Wire),
        never = maps:get(retryability, Wire),
        8 = maps:get(wire_status, Wire),
        glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

tls_requires_cert_and_key_pair(_Config) ->
    {error, Err} = glyphastore_client:build_tls_options(
        #{enable => true, cert_file => "/tmp/only-cert.pem"},
        "localhost"
    ),
    invalid_argument = maps:get(category, Err).

close_is_synchronous(_Config) ->
    {ok, Server} = glyphastore_fake_server:start(#{workers => 1}),
    try
        {ok, Client} = glyphastore_client:connect(#{port => glyphastore_fake_server:port(Server)}),
        ok = glyphastore_client:close(Client),
        false = is_process_alive(Client),
        ok = glyphastore_client:close(Client)
    after
        glyphastore_fake_server:stop(Server)
    end.

find_keys_for_workers(Workers) ->
    find_keys_for_workers(Workers, 0, array:new(Workers, {default, undefined})).

find_keys_for_workers(Workers, Candidate, Acc) ->
    case lists:all(fun(I) -> array:get(I, Acc) =/= undefined end, lists:seq(0, Workers - 1)) of
        true ->
            [array:get(I, Acc) || I <- lists:seq(0, Workers - 1)];
        false ->
            Key = list_to_binary(io_lib:format("k-~B", [Candidate])),
            {ok, Owner} = glyphastore_protocol:worker_for(Key, Workers),
            Acc1 =
                case array:get(Owner, Acc) of
                    undefined -> array:set(Owner, Key, Acc);
                    _ -> Acc
                end,
            find_keys_for_workers(Workers, Candidate + 1, Acc1)
    end.

worker_conn(Client, Worker) ->
    State = sys:get_state(Client),
    maps:get(Worker, State#state.workers).

wait_until_dead(_Pid, 0) ->
    error(process_still_alive);
wait_until_dead(Pid, Left) ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(10),
            wait_until_dead(Pid, Left - 1)
    end.
