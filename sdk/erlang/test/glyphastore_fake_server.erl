-module(glyphastore_fake_server).
-export([start_link/1, stop/1, port/1]).

-record(state, {
    workers,
    internal_error_on_put = false,
    drop_after_mutation = false,
    listen,
    port,
    store = #{},
    accept_pid
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

stop(Pid) ->
    gen_server:stop(Pid).

port(Pid) ->
    gen_server:call(Pid, port).

init(Opts) ->
    Workers = maps:get(workers, Opts, 1),
    Internal = maps:get(internal_error_on_put, Opts, false),
    Drop = maps:get(drop_after_mutation, Opts, false),
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, 0}, {reuseaddr, true}]),
    {port, Port} = inet:sockname(Listen),
    AcceptPid = spawn_link(fun() -> accept_loop(Listen) end),
    {ok,
        #state{
            workers = Workers,
            internal_error_on_put = Internal,
            drop_after_mutation = Drop,
            listen = Listen,
            port = Port,
            accept_pid = AcceptPid
        }}.

handle_call(port, _From, State) ->
    {reply, State#state.port, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

terminate(_Reason, State) ->
    catch gen_tcp:close(State#state.listen),
    ok.

accept_loop(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            spawn(fun() -> handle_client(Socket) end),
            accept_loop(Listen);
        _ ->
            ok
    end.

handle_client(Socket) ->
    Bound = undefined,
    Store = #{},
    handle_client(Socket, Bound, Store).

handle_client(Socket, Bound, Store) ->
    case read_frame(Socket) of
        {ok, Frame} ->
            {ok, Request} = glyphastore_protocol:decode_request(Frame, glyphastore_protocol:max_frame_bytes()),
            case maps:get(opcode, Request) of
                Op when Op =:= glyphastore_protocol:opcode_init() ->
                    reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                        value => glyphastore_protocol:identity(),
                        owner_worker => glyphastore_protocol:no_worker(),
                        worker_count => server_workers(Socket),
                        routing_epoch => 9
                    }),
                    handle_client(Socket, Bound, Store);
                Op when Op =:= glyphastore_protocol:opcode_bind_worker() ->
                    NewBound = maps:get(target_worker, Request),
                    reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                        owner_worker => NewBound,
                        worker_count => server_workers(Socket),
                        routing_epoch => 9
                    }),
                    handle_client(Socket, NewBound, Store);
                Op when Op =:= glyphastore_protocol:opcode_put() ->
                    handle_put(Socket, Bound, Store, Request);
                Op when Op =:= glyphastore_protocol:opcode_get() ->
                    handle_get(Socket, Bound, Store, Request);
                Op when Op =:= glyphastore_protocol:opcode_erase() ->
                    Key = maps:get(key, Request),
                    NewStore = maps:remove(Key, Store),
                    reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                        owner_worker => Bound,
                        worker_count => server_workers(Socket),
                        routing_epoch => 9
                    }),
                    handle_client(Socket, Bound, NewStore);
                Op when Op =:= glyphastore_protocol:opcode_ping() ->
                    reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                        value => maps:get(value, Request),
                        owner_worker => case Bound of undefined -> 0; _ -> Bound end,
                        worker_count => server_workers(Socket),
                        routing_epoch => 9
                    }),
                    handle_client(Socket, Bound, Store);
                _ ->
                    reply(Socket, glyphastore_protocol:status_unsupported(), Request, #{
                        owner_worker => case Bound of undefined -> 0; _ -> Bound end,
                        worker_count => server_workers(Socket),
                        routing_epoch => 9
                    }),
                    handle_client(Socket, Bound, Store)
            end;
        _ ->
            catch gen_tcp:close(Socket)
    end.

handle_put(Socket, Bound, Store, Request) ->
    case server_internal_error(Socket) of
        true ->
            reply(Socket, glyphastore_protocol:status_internal_error(), Request, #{
                owner_worker => Bound,
                worker_count => server_workers(Socket),
                routing_epoch => 9
            }),
            handle_client(Socket, Bound, Store);
        false ->
            Key = maps:get(key, Request),
            Value = maps:get(value, Request),
            NewStore = Store#{Key => Value},
            reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                owner_worker => Bound,
                worker_count => server_workers(Socket),
                routing_epoch => 9
            }),
            case server_drop_after_mutation(Socket) of
                true ->
                    catch gen_tcp:close(Socket);
                false ->
                    handle_client(Socket, Bound, NewStore)
            end
    end.

handle_get(Socket, Bound, Store, Request) ->
    Key = maps:get(key, Request),
    case maps:get(Key, Store, undefined) of
        undefined ->
            reply(Socket, glyphastore_protocol:status_not_found(), Request, #{
                owner_worker => Bound,
                worker_count => server_workers(Socket),
                routing_epoch => 9
            }),
            handle_client(Socket, Bound, Store);
        Value ->
            reply(Socket, glyphastore_protocol:status_ok(), Request, #{
                value => Value,
                owner_worker => Bound,
                worker_count => server_workers(Socket),
                routing_epoch => 9
            }),
            handle_client(Socket, Bound, Store)
    end.

server_workers(_Socket) ->
    1.

server_internal_error(_Socket) ->
    false.

server_drop_after_mutation(_Socket) ->
    false.

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
