-module(glyphastore_client_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
        put_get_ping_erase,
        internal_error_mutation_is_indeterminate,
        pipeline_put_get,
        batch_multi_worker,
        worker_pipelines_concurrent,
        rejects_non_positive_timeout
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
