-module(glyphastore_fake_server).
-export([start/1, stop/1, port/1]).

-include("glyphastore_protocol.hrl").

start(Opts) ->
    Parent = self(),
    Pid = spawn(fun() -> server(Parent, Opts) end),
    receive
        {glyphastore_fake_server, ready, Port} ->
            {ok, #{pid => Pid, port => Port}}
    end.

stop(#{pid := Pid}) ->
    exit(Pid, shutdown),
    ok.

port(#{port := Port}) -> Port.

server(Parent, Opts) ->
    Workers = maps:get(workers, Opts, 1),
    Internal = maps:get(internal_error_on_put, Opts, false),
    Drop = maps:get(drop_after_mutation, Opts, false),
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, 0}, {reuseaddr, true}]),
    {ok, {_Addr, Port}} = inet:sockname(Listen),
    Parent ! {glyphastore_fake_server, ready, Port},
    accept_loop(Listen, Workers, Internal, Drop).

accept_loop(Listen, Workers, Internal, Drop) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            spawn(fun() -> client_loop(Socket, Workers, Internal, Drop, undefined, #{}) end),
            accept_loop(Listen, Workers, Internal, Drop);
        {error, closed} ->
            ok
    end.

client_loop(Socket, Workers, Internal, Drop, Bound, Store) ->
    case read_frame(Socket) of
        {ok, Frame} ->
            {ok, Request} = glyphastore_protocol:decode_request(Frame, glyphastore_protocol:max_frame_bytes()),
            handle_request(Socket, Workers, Internal, Drop, Bound, Store, Request);
        _ ->
            catch gen_tcp:close(Socket)
    end.

handle_request(Socket, Workers, Internal, Drop, Bound, Store, Request) ->
    Meta = #{
        worker_count => Workers,
        routing_epoch => 9,
        owner_worker => case Bound of undefined -> 0; _ -> Bound end
    },
    case maps:get(opcode, Request) of
        ?GS_OP_INIT ->
            reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{value => glyphastore_protocol:identity(), owner_worker => glyphastore_protocol:no_worker()}),
            client_loop(Socket, Workers, Internal, Drop, Bound, Store);
        ?GS_OP_BIND ->
            NewBound = maps:get(target_worker, Request),
            reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{owner_worker => NewBound}),
            client_loop(Socket, Workers, Internal, Drop, NewBound, Store);
        ?GS_OP_PUT ->
            case Internal of
                true ->
                    reply(Socket, glyphastore_protocol:status_internal_error(), Request, Meta#{owner_worker => Bound}),
                    client_loop(Socket, Workers, Internal, Drop, Bound, Store);
                false ->
                    Key = maps:get(key, Request),
                    Value = maps:get(value, Request),
                    reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{owner_worker => Bound}),
                    case Drop of
                        true -> catch gen_tcp:close(Socket);
                        false -> client_loop(Socket, Workers, Internal, Drop, Bound, Store#{Key => Value})
                    end
            end;
        ?GS_OP_GET ->
            Key = maps:get(key, Request),
            case maps:get(Key, Store, undefined) of
                undefined ->
                    reply(Socket, glyphastore_protocol:status_not_found(), Request, Meta#{owner_worker => Bound}),
                    client_loop(Socket, Workers, Internal, Drop, Bound, Store);
                Value ->
                    reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{owner_worker => Bound, value => Value}),
                    client_loop(Socket, Workers, Internal, Drop, Bound, Store)
            end;
        ?GS_OP_ERASE ->
            Key = maps:get(key, Request),
            reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{owner_worker => Bound}),
            client_loop(Socket, Workers, Internal, Drop, Bound, maps:remove(Key, Store));
        ?GS_OP_PING ->
            reply(Socket, glyphastore_protocol:status_ok(), Request, Meta#{value => maps:get(value, Request)}),
            client_loop(Socket, Workers, Internal, Drop, Bound, Store);
        _ ->
            reply(Socket, glyphastore_protocol:status_unsupported(), Request, Meta),
            client_loop(Socket, Workers, Internal, Drop, Bound, Store)
    end.

read_frame(Socket) ->
    case gen_tcp:recv(Socket, 4, 5000) of
        {ok, <<Size:32/little>>} ->
            case gen_tcp:recv(Socket, Size - 4, 5000) of
                {ok, Rest} -> {ok, <<Size:32/little, Rest/binary>>};
                Err -> Err
            end;
        Err ->
            Err
    end.

reply(Socket, Status, Request, Fields) ->
    {ok, Frame} = glyphastore_protocol:encode_response(
        Status,
        maps:get(request_id, Request),
        maps:get(value, Fields, <<>>),
        maps:get(owner_worker, Fields),
        maps:get(worker_count, Fields),
        maps:get(routing_epoch, Fields)
    ),
    gen_tcp:send(Socket, Frame).
