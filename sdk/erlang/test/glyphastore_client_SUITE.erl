-module(glyphastore_client_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
        put_get_ping_erase,
        internal_error_mutation_is_indeterminate,
        pipeline_put_get,
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
