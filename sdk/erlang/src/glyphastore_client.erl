-module(glyphastore_client).
-behaviour(gen_server).

-export([
    connect/1,
    close/1,
    healthy/1,
    worker_count/1,
    routing_epoch/1,
    worker_for/2,
    get/2,
    get/3,
    ping/2,
    ping/3,
    put/3,
    put/4,
    erase/2,
    erase/3,
    execute_pipeline/2,
    execute_pipeline/3,
    execute_batch/2,
    execute_batch/3,
    build_tls_options/2
]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-export_type([config/0, client/0, mutation_result/0, pipeline_request/0, pipeline_response/0]).

-type config() :: #{
    host => string() | binary(),
    port => pos_integer(),
    connect_timeout => float(),
    request_timeout => float(),
    maximum_frame_bytes => pos_integer(),
    maximum_pipeline_requests => pos_integer(),
    maximum_pipeline_bytes => pos_integer(),
    tls => map()
}.

-type client() :: pid().
-type mutation_result() :: #{outcome := committed | rejected | indeterminate, error => glyphastore_error:error()}.
-type pipeline_request() :: #{
    opcode := get | put | erase,
    key := binary(),
    value => binary(),
    expire_at_ns => non_neg_integer()
}.
-type pipeline_response() :: #{
    outcome := succeeded | failed | indeterminate,
    value => binary(),
    error => glyphastore_error:error()
}.

-record(state, {
    config :: config(),
    worker_count = 0 :: non_neg_integer(),
    routing_epoch = 0 :: non_neg_integer(),
    request_id = 1 :: non_neg_integer(),
    healthy = true :: boolean(),
    workers = #{} :: #{non_neg_integer() => pid()}
}).

connect(Config0) ->
    Config = glyphastore_util:merge_config(Config0),
    case glyphastore_util:validate_config(Config) of
        ok -> gen_server:start_link(?MODULE, Config, []);
        {error, Err} -> {error, Err}
    end.

close(Client) -> gen_server:cast(Client, close), ok.
healthy(Client) -> gen_server:call(Client, healthy).
worker_count(Client) -> gen_server:call(Client, worker_count).
routing_epoch(Client) -> gen_server:call(Client, routing_epoch).
worker_for(Client, Key) -> gen_server:call(Client, {worker_for, Key}).

get(Client, Key) -> get(Client, Key, #{}).
get(Client, Key, Opts) -> gen_server:call(Client, {read, get, Key, <<>>, Opts}).

ping(Client, Payload) -> ping(Client, Payload, #{}).
ping(Client, Payload, Opts) -> gen_server:call(Client, {read, ping, <<>>, Payload, Opts}).

put(Client, Key, Value) -> put(Client, Key, Value, #{}).
put(Client, Key, Value, Opts) ->
    Expire = maps:get(expire_at_ns, Opts, 0),
    CallOpts = maps:without([expire_at_ns], Opts),
    gen_server:call(Client, {mutate, put, Key, Value, Expire, CallOpts}).

erase(Client, Key) -> erase(Client, Key, #{}).
erase(Client, Key, Opts) -> gen_server:call(Client, {mutate, erase, Key, <<>>, 0, Opts}).

execute_pipeline(Client, Requests) -> execute_pipeline(Client, Requests, #{}).
execute_pipeline(Client, Requests, Opts) -> gen_server:call(Client, {execute_pipeline, Requests, Opts}).

execute_batch(Client, Requests) -> execute_batch(Client, Requests, #{}).
execute_batch(Client, Requests, Opts) -> gen_server:call(Client, {execute_batch, Requests, Opts}).

init(Config) ->
    case bootstrap_all(Config, 1) of
        {ok, Workers, WorkerCount, RoutingEpoch, NextId} ->
            {ok,
                #state{
                    config = Config,
                    workers = Workers,
                    worker_count = WorkerCount,
                    routing_epoch = RoutingEpoch,
                    request_id = NextId
                }};
        {error, Err} ->
            {stop, Err}
    end.

handle_call(healthy, _From, State) -> {reply, State#state.healthy, State};
handle_call(worker_count, _From, State) -> {reply, State#state.worker_count, State};
handle_call(routing_epoch, _From, State) -> {reply, State#state.routing_epoch, State};
handle_call({worker_for, Key}, _From, #state{worker_count = 0} = State) ->
    {reply, {error, glyphastore_error:unavailable(<<"client is not connected">>)}, State};
handle_call({worker_for, Key}, _From, #state{worker_count = WC} = State) ->
    {reply, glyphastore_protocol:worker_for(Key, WC), State};
handle_call({read, Op, Key, Value, Opts}, _From, State) ->
    {Reply, State1} = do_read(Op, Key, Value, Opts, State),
    {reply, Reply, State1};
handle_call({mutate, Op, Key, Value, Expire, Opts}, _From, State) ->
    {Reply, State1} = do_mutate(Op, Key, Value, Expire, Opts, State),
    {reply, Reply, State1};
handle_call({execute_pipeline, Requests, Opts}, _From, State) ->
    {Reply, State1} = do_execute_pipeline(Requests, Opts, State),
    {reply, Reply, State1};
handle_call({execute_batch, Requests, Opts}, _From, State) ->
    {Reply, State1} = do_execute_batch(Requests, Opts, State),
    {reply, Reply, State1};
handle_call(_Req, _From, State) ->
    {reply, {error, glyphastore_error:internal(<<"unexpected call">>)}, State}.

handle_cast(close, State) ->
    maps:foreach(fun(_W, Pid) -> glyphastore_conn:reset(Pid) end, State#state.workers),
    {stop, normal, State#state{healthy = false}}.

terminate(_Reason, State) ->
    maps:foreach(fun(_W, Pid) -> catch gen_server:stop(Pid) end, State#state.workers),
    ok.

bootstrap_all(Config, NextId) ->
    {ok, Conn0} = glyphastore_conn:start_link(0, Config),
    case bootstrap_conn(Conn0, 0, undefined, Config, NextId) of
        {ok, WC, Epoch, NextId1} ->
            bootstrap_rest(1, WC, Epoch, Config, #{0 => Conn0}, NextId1);
        {error, Err} ->
            catch gen_server:stop(Conn0),
            {error, Err}
    end.

bootstrap_rest(W, WC, Epoch, _Config, Workers, NextId) when W >= WC ->
    {ok, Workers, WC, Epoch, NextId};
bootstrap_rest(W, WC, Epoch, Config, Workers, NextId) ->
    {ok, Conn} = glyphastore_conn:start_link(W, Config),
    case bootstrap_conn(Conn, W, {WC, Epoch}, Config, NextId) of
        {ok, WC, Epoch, NextId1} ->
            bootstrap_rest(W + 1, WC, Epoch, Config, Workers#{W => Conn}, NextId1);
        {error, Err} ->
            maps:foreach(fun(_I, Pid) -> catch gen_server:stop(Pid) end, Workers#{W => Conn}),
            {error, Err}
    end.

bootstrap_conn(Conn, Worker, Expected, Config, NextId) ->
    glyphastore_conn:reset(Conn),
    case glyphastore_conn:dial(Conn) of
        ok ->
            Deadline = glyphastore_util:monotonic_seconds() + maps:get(request_timeout, Config),
            InitId = NextId,
            NextAfterInit = advance_id(InitId),
            case encode_init(InitId) of
                {ok, Frame} ->
                    case glyphastore_conn:exchange(Conn, Frame, Deadline) of
                        {ok, Response} ->
                            case validate_init(Response, InitId, Expected) of
                                ok -> bind_worker(Conn, Worker, Response, Deadline, NextAfterInit);
                                {error, Err} ->
                                    glyphastore_conn:reset(Conn),
                                    {error, Err}
                            end;
                        {error, SF = #{send_failure := true}} ->
                            glyphastore_conn:reset(Conn),
                            {error, promote_send(SF)};
                        {error, Err} ->
                            glyphastore_conn:reset(Conn),
                            {error, maybe_unavailable(Err)}
                    end;
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {error, Err}
            end;
        {error, Err} ->
            {error, Err}
    end.

bind_worker(Conn, Worker, InitResponse, Deadline, BindId) ->
    case glyphastore_protocol:encode_request(
        glyphastore_protocol:opcode_bind_worker(), BindId, <<>>, <<>>, 0, Worker
    ) of
        {ok, Frame} ->
            case glyphastore_conn:exchange(Conn, Frame, Deadline) of
                {ok, Bound} ->
                    case validate_bind(Bound, BindId, Worker, InitResponse) of
                        ok ->
                            {ok, maps:get(worker_count, InitResponse), maps:get(routing_epoch, InitResponse),
                                advance_id(BindId)};
                        {error, Err} ->
                            glyphastore_conn:reset(Conn),
                            {error, Err}
                    end;
                {error, SF = #{send_failure := true}} ->
                    glyphastore_conn:reset(Conn),
                    {error, promote_send(SF)};
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {error, maybe_unavailable(Err)}
            end;
        {error, {invalid_argument, Msg}} ->
            {error, glyphastore_error:invalid_argument(Msg)}
    end.

advance_id(Id) ->
    case Id of
        16#FFFFFFFFFFFFFFFF -> 1;
        N -> N + 1
    end.

bump_id(State) ->
    Id = State#state.request_id,
    {Id, State#state{request_id = advance_id(Id)}}.

do_read(Op, Key, Value, Opts, #state{healthy = false} = State) ->
    {{error, glyphastore_error:unavailable(<<"client is closed or routing metadata changed">>)}, State};
do_read(Op, Key, Value, Opts, State) ->
    case resolve_deadline(State, Opts) of
        {ok, Deadline} ->
            {Opcode, OpName, RouteKey} = read_op(Op, Key),
            case worker_index(State, RouteKey) of
                {ok, Worker, Conn} ->
                    do_read_attempt(Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, 2);
                {error, Err} ->
                    {{error, Err}, State}
            end;
        {error, Err} ->
            {{error, Err}, State}
    end.

do_read_attempt(_O, _N, _K, _V, _D, _W, _C, State, 0) ->
    {{error, glyphastore_error:unavailable(<<"request was not attempted">>)}, State};
do_read_attempt(Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, Attempts) ->
    case ensure_connected(Conn, State) of
        {ok, State1} ->
            {RequestId, State2} = bump_id(State1),
            case encode_request(Opcode, RequestId, Key, Value, 0, State2) of
                {ok, Frame, State3} ->
                    case glyphastore_conn:exchange(Conn, Frame, Deadline) of
                        {ok, Response} ->
                            handle_read_response(Response, OpName, RequestId, Worker, Conn, State3);
                        {error, SF = #{send_failure := true}} ->
                            glyphastore_conn:reset(Conn),
                            Err = promote_send_sf(SF, OpName, RequestId, Worker, State3, false),
                            retry_read(Err, Opcode, OpName, Key, Value, Deadline, Worker, Conn, State3, Attempts);
                        {error, Err} ->
                            glyphastore_conn:reset(Conn),
                            Ann = annotate(Err, OpName, RequestId, Worker, State3),
                            retry_read(Ann, Opcode, OpName, Key, Value, Deadline, Worker, Conn, State3, Attempts)
                    end;
                {{error, Err}, State3} ->
                    {{error, Err}, State3}
            end;
        {{error, Err}, State1} ->
            retry_read(Err, Opcode, OpName, Key, Value, Deadline, Worker, Conn, State1, Attempts)
    end.

handle_read_response(Response, OpName, RequestId, Worker, Conn, State) ->
    case validate_response(Response, RequestId, Worker, State) of
        ok ->
            case maps:get(status, Response) of
                S when S =:= glyphastore_protocol:status_ok() ->
                    {{ok, maps:get(value, Response)}, State};
                Status ->
                    Err = status_err(Status, OpName, RequestId, Worker, State),
                    {State1, Final} = apply_unhealthy(Status, Err, State),
                    {{error, Final}, State1}
            end;
        {error, Err} ->
            glyphastore_conn:reset(Conn),
            {{error, annotate(Err, OpName, RequestId, Worker, State)}, State}
    end.

retry_read(Err, Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, Attempts) ->
    case retryable_read(Err, State) of
        true -> do_read_attempt(Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, Attempts - 1);
        false -> {{error, Err}, State}
    end.

do_mutate(Op, Key, Value, Expire, Opts, #state{healthy = false} = State) ->
    {mutation_rejected(Op, glyphastore_error:unavailable(<<"client is closed or routing metadata changed">>), State), State};
do_mutate(Op, Key, Value, Expire, Opts, State) ->
    OpName = atom_to_binary(Op, utf8),
    case resolve_deadline(State, Opts) of
        {ok, Deadline} ->
            case worker_index(State, Key) of
                {ok, Worker, Conn} ->
                    do_mutate_attempt(Op, OpName, Key, Value, Expire, Deadline, Worker, Conn, State, 2);
                {error, Err} ->
                    {mutation_rejected(Op, Err, State), State}
            end;
        {error, Err} ->
            {mutation_rejected(Op, Err, State), State}
    end.

do_mutate_attempt(_Op, OpName, _K, _V, _E, _D, Worker, _C, State, 0) ->
    Err = glyphastore_error:unavailable(<<"could not send mutation">>) |> annotate(OpName, undefined, Worker, State) |> enrich_mutation(rejected),
    {#{outcome => rejected, error => Err}, State};
do_mutate_attempt(Op, OpName, Key, Value, Expire, Deadline, Worker, Conn, State, Attempts) ->
    case ensure_connected(Conn, State) of
        {ok, State1} ->
            {RequestId, State2} = bump_id(State1),
            Opcode = mutate_opcode(Op),
            case encode_request(Opcode, RequestId, Key, Value, Expire, State2) of
                {ok, Frame, State3} ->
                    case glyphastore_conn:exchange(Conn, Frame, Deadline) of
                        {ok, Response} ->
                            handle_mutate_response(Response, OpName, RequestId, Worker, Conn, State3);
                        {error, SF = #{send_failure := true}} ->
                            glyphastore_conn:reset(Conn),
                            handle_mutate_send(SF, OpName, RequestId, Worker, Op, Key, Value, Expire, Deadline, Conn, State3, Attempts);
                        {error, Err} ->
                            glyphastore_conn:reset(Conn),
                            Ann = annotate(Err, OpName, RequestId, Worker, State3) |> enrich_mutation(indeterminate),
                            {#{outcome => indeterminate, error => Ann}, State3}
                    end;
                {{error, Err}, State3} ->
                    {mutation_rejected(Op, annotate(Err, OpName, RequestId, Worker, State3), State3), State3}
            end;
        {{error, Err}, State1} ->
            {mutation_rejected(Op, annotate(Err, OpName, undefined, Worker, State1), State1), State1}
    end.

handle_mutate_send(SF, OpName, RequestId, Worker, Op, Key, Value, Expire, Deadline, Conn, State, Attempts) ->
    case maps:get(bytes_sent, SF, 0) of
        0 when Attempts > 1 ->
            do_mutate_attempt(Op, OpName, Key, Value, Expire, Deadline, Worker, Conn, State, Attempts - 1);
        0 ->
            Err = promote_send_sf(SF, OpName, RequestId, Worker, State, true) |> enrich_mutation(rejected),
            {#{outcome => rejected, error => Err}, State};
        _ ->
            Err = promote_send_sf(SF, OpName, RequestId, Worker, State, true) |> enrich_mutation(indeterminate),
            {#{outcome => indeterminate, error => Err}, State}
    end.

handle_mutate_response(Response, OpName, RequestId, Worker, Conn, State) ->
    case validate_response(Response, RequestId, Worker, State) of
        ok ->
            case maps:get(status, Response) of
                S when S =:= glyphastore_protocol:status_ok() ->
                    case maps:get(value, Response) of
                        <<>> -> {#{outcome => committed}, State};
                        _ ->
                            glyphastore_conn:reset(Conn),
                            Err = glyphastore_error:protocol(<<"mutation response value must be empty">>) |> annotate(OpName, RequestId, Worker, State) |> enrich_mutation(indeterminate),
                            {#{outcome => indeterminate, error => Err}, State}
                    end;
                Status when Status =:= glyphastore_protocol:status_internal_error() ->
                    Err = status_err(Status, OpName, RequestId, Worker, State) |> enrich_mutation(indeterminate),
                    {#{outcome => indeterminate, error => Err}, State};
                Status ->
                    Err = status_err(Status, OpName, RequestId, Worker, State) |> enrich_mutation(rejected),
                    {State1, Final} = apply_unhealthy(Status, Err, State),
                    {#{outcome => rejected, error => Final}, State1}
            end;
        {error, Err} ->
            glyphastore_conn:reset(Conn),
            Ann = annotate(Err, OpName, RequestId, Worker, State) |> enrich_mutation(indeterminate),
            {#{outcome => indeterminate, error => Ann}, State}
    end.

do_execute_pipeline([], _Opts, State) -> {{ok, []}, State};
do_execute_pipeline(_R, _O, #state{healthy = false} = State) ->
    {{error, glyphastore_error:unavailable(<<"client is closed or routing metadata changed">>)}, State};
do_execute_pipeline(Requests, Opts, State) ->
    case resolve_deadline(State, Opts) of
        {ok, Deadline} ->
            case plan_pipeline(Requests, State) of
                {ok, Worker, Conn, Plan, State1} ->
                    run_pipeline(Plan, Deadline, Worker, Conn, State1, length(Requests));
                {error, Err} ->
                    {{error, Err}, State}
            end;
        {error, Err} ->
            {{error, Err}, State}
    end.

do_execute_batch([], _Opts, State) -> {{ok, []}, State};
do_execute_batch(_R, _O, #state{healthy = false} = State) ->
    {{error, glyphastore_error:unavailable(<<"client is closed or routing metadata changed">>)}, State};
do_execute_batch(Requests, Opts, State) ->
    case resolve_deadline(State, Opts) of
        {ok, Deadline} ->
            case group_batch(Requests, State) of
                {ok, Groups} ->
                    run_batch(Groups, Deadline, State, length(Requests));
                {error, Err} ->
                    {{error, Err}, State}
            end;
        {error, Err} ->
            {{error, Err}, State}
    end.

plan_pipeline(Requests, State) ->
    MaxReq = maps:get(maximum_pipeline_requests, State#state.config),
    MaxBytes = maps:get(maximum_pipeline_bytes, State#state.config),
    case length(Requests) > MaxReq of
        true ->
            {error, glyphastore_error:invalid_argument(<<"pipeline exceeds the configured request limit">>)};
        false ->
            plan_pipeline_items(Requests, State, undefined, 0, [], MaxBytes, State)
    end.

plan_pipeline_items([], _State, Worker, _Need, Acc, _MaxBytes, StateAcc) ->
    Conn = maps:get(Worker, StateAcc#state.workers),
    {ok, Worker, Conn, lists:reverse(Acc), StateAcc};
plan_pipeline_items([Req | Rest], State, Worker0, Need, Acc, MaxBytes, StateAcc) ->
    case normalize_pipeline_req(Req) of
        {error, Err} ->
            {error, Err};
        {ok, Norm} ->
            case worker_index(StateAcc, maps:get(key, Norm)) of
                {error, Err} ->
                    {error, Err};
                {ok, Owner, _Conn} ->
                    case Worker0 of
                        undefined -> ok;
                        W when W =:= Owner -> ok;
                        _ -> {error, glyphastore_error:invalid_argument(<<"every pipeline key must route to the same Worker">>)}
                    end,
                    Key = maps:get(key, Norm),
                    Value = maps:get(value, Norm, <<>>),
                    FrameLen = glyphastore_protocol:request_frame_size(Key, Value),
                    MaxFrame = maps:get(maximum_frame_bytes, StateAcc#state.config),
                    case Worker0 =:= undefined orelse Worker0 =:= Owner of
                        false ->
                            {error, glyphastore_error:invalid_argument(<<"every pipeline key must route to the same Worker">>)};
                        true ->
                            case FrameLen =< MaxFrame andalso FrameLen =< MaxBytes - Need of
                                true ->
                                    {RequestId, State1} = bump_id(StateAcc),
                                    case encode_request(
                                        maps:get(opcode, Norm),
                                        RequestId,
                                        Key,
                                        Value,
                                        maps:get(expire_at_ns, Norm, 0),
                                        State1
                                    ) of
                                        {ok, Frame, State2} ->
                                            Item = #{
                                                req => Norm,
                                                request_id => RequestId,
                                                frame => Frame,
                                                begin_offset => Need
                                            },
                                            plan_pipeline_items(
                                                Rest, State, Owner, Need + FrameLen, [Item | Acc], MaxBytes, State2
                                            );
                                        {{error, Err2}, _} ->
                                            {error, Err2}
                                    end;
                                false ->
                                    {error, frame_limit_err(FrameLen, MaxFrame, MaxBytes, Need)}
                            end
                    end
            end
    end.

run_pipeline(Plan, Deadline, Worker, Conn, State, Count) ->
    Responses = [failed_response() || _ <- lists:seq(1, Count)],
    case ensure_connected(Conn, State) of
        {ok, State1} ->
            Output = iolist_to_binary([maps:get(frame, I) || I <- Plan]),
            Metadata = [{maps:get(request_id, I), maps:get(req, I), maps:get(begin_offset, I)} || I <- Plan],
            case glyphastore_conn:send(Conn, Output, Deadline) of
                ok ->
                    collect_pipeline(Metadata, 1, Responses, Deadline, Worker, Conn, State1, byte_size(Output));
                {error, SF = #{send_failure := true}} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, mark_unresolved(Responses, 1, promote_send_sf(SF, undefined, undefined, Worker, State1, false), maps:get(bytes_sent, SF, 0), Metadata, byte_size(Output))}, State1};
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, mark_unresolved(Responses, 1, Err, 0, Metadata, byte_size(Output))}, State1}
            end;
        {{error, Err}, State1} ->
            {{ok, mark_unresolved(Responses, 1, Err, 0, Metadata_from_plan(Plan), byte_size(iolist_to_binary([maps:get(frame, I) || I <- Plan])))}, State1}
    end.

collect_pipeline([], _Idx, Responses, _Deadline, _Worker, _Conn, State, _Sent) ->
    {{ok, Responses}, State};
collect_pipeline([{RequestId, Req, _Begin} | Rest], Idx, Responses, Deadline, Worker, Conn, State, SentBytes) ->
    case glyphastore_conn:receive_response(Conn, Deadline) of
        {ok, Response} ->
            case validate_response(Response, RequestId, Worker, State) of
                ok ->
                    case maps:get(status, Response) of
                        S when S =:= glyphastore_protocol:status_ok() ->
                            case pipeline_ok(Req, Response) of
                                ok ->
                                    Responses1 = set_response(Responses, Idx, #{
                                        outcome => succeeded, value => maps:get(value, Response)
                                    }),
                                    collect_pipeline(Rest, Idx + 1, Responses1, Deadline, Worker, Conn, State, SentBytes);
                                {error, Err} ->
                                    glyphastore_conn:reset(Conn),
                                    {{ok, mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, 0} | Rest], SentBytes)}, State}
                            end;
                        Status ->
                            Err = status_err(Status, pipeline_op(Req), RequestId, Worker, State),
                            Outcome = pipeline_status_outcome(Req, Status),
                            Responses1 = set_response(Responses, Idx, #{outcome => Outcome, error => Err}),
                            State1 = maybe_unhealthy_state(Status, State),
                            collect_pipeline(Rest, Idx + 1, Responses1, Deadline, Worker, Conn, State1, SentBytes)
                    end;
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, 0} | Rest], SentBytes)}, State}
            end;
        {error, Err} ->
            glyphastore_conn:reset(Conn),
            {{ok, mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, 0} | Rest], SentBytes)}, State}
    end.

group_batch(Requests, State) ->
    group_batch(Requests, State, 0, #{}).

group_batch([], _State, _Idx, Groups) ->
    {ok, maps:to_list(Groups)};
group_batch([Req | Rest], State, Idx, Groups) ->
    case normalize_pipeline_req(Req) of
        {ok, Norm} ->
            case worker_index(State, maps:get(key, Norm)) of
                {ok, Worker, _Conn} ->
                    Bucket = maps:get(Worker, Groups, []),
                    MaxReq = maps:get(maximum_pipeline_requests, State#state.config),
                    case length(Bucket) >= MaxReq of
                        true ->
                            {error, glyphastore_error:invalid_argument(<<"batch exceeds the configured per-Worker request limit">>)};
                        false ->
                            group_batch(Rest, State, Idx + 1, Groups#{Worker => [{Idx, Norm} | Bucket]})
                    end;
                {error, Err} ->
                    {error, Err}
            end;
        {error, Err} ->
            {error, Err}
    end.

run_batch(Groups, Deadline, State, Count) ->
    Responses0 = [failed_response() || _ <- lists:seq(1, Count)],
    run_batch_groups(Groups, Deadline, State, Responses0).

run_batch_groups([], _Deadline, State, Responses) ->
    {{ok, Responses}, State};
run_batch_groups([{Worker, Items} | Rest], Deadline, State, Responses0) ->
    case build_group_plan(lists:reverse(Items), State, []) of
        {ok, Plan, State1} ->
            Conn = maps:get(Worker, State1#state.workers),
            {{ok, GroupResponses}, State2} =
                run_pipeline(Plan, Deadline, Worker, Conn, State1, length(Items)),
            Responses1 = merge_group_by_index(Responses0, Items, GroupResponses),
            run_batch_groups(Rest, Deadline, State2, Responses1);
        {error, Err} ->
            {{error, Err}, State}
    end.

build_group_plan([], State, Acc) ->
    {ok, lists:reverse(Acc), State};
build_group_plan([{Idx, Norm} | Rest], State, Acc) ->
    {RequestId, State1} = bump_id(State),
    Key = maps:get(key, Norm),
    Value = maps:get(value, Norm, <<>>),
    Expire = maps:get(expire_at_ns, Norm, 0),
    case encode_request(maps:get(opcode, Norm), RequestId, Key, Value, Expire, State1) of
        {ok, Frame, State2} ->
            Begin = lists:foldl(fun(I, N) -> N + byte_size(maps:get(frame, I)) end, 0, Acc),
            Item = #{
                req => Norm,
                request_id => RequestId,
                frame => Frame,
                begin_offset => Begin,
                index => Idx
            },
            build_group_plan(Rest, State2, [Item | Acc]);
        {{error, Err}, _} ->
            {error, Err}
    end.

merge_group_by_index(Responses, Items, GroupResponses) ->
    lists:foldl(
        fun({{Idx, _Norm}, Resp}, Acc) ->
            set_response(Acc, Idx + 1, Resp)
        end,
        Responses,
        lists:zip(Items, GroupResponses)
    ).

read_op(get, Key) -> {glyphastore_protocol:opcode_get(), <<"get">>, Key};
read_op(ping, _) -> {glyphastore_protocol:opcode_ping(), <<"ping">>, <<>>}.

mutate_opcode(put) -> glyphastore_protocol:opcode_put();
mutate_opcode(erase) -> glyphastore_protocol:opcode_erase().

encode_init(InitId) ->
    case glyphastore_protocol:encode_request(glyphastore_protocol:opcode_init(), InitId, <<>>, <<>>, 0, glyphastore_protocol:no_worker()) of
        {ok, Frame} -> {ok, Frame};
        {error, {invalid_argument, Msg}} -> {error, glyphastore_error:invalid_argument(Msg)}
    end.

encode_request(Opcode, RequestId, Key, Value, Expire, State) ->
    case glyphastore_protocol:encode_request(Opcode, RequestId, Key, Value, Expire, glyphastore_protocol:no_worker()) of
        {ok, Frame} ->
            Max = maps:get(maximum_frame_bytes, State#state.config),
            case byte_size(Frame) =< Max of
                true -> {ok, Frame, State};
                false ->
                    {{error, glyphastore_error:invalid_argument(<<"request exceeds the configured frame limit">>)}, State}
            end;
        {error, {invalid_argument, Msg}} ->
            {{error, glyphastore_error:invalid_argument(Msg)}, State}
    end.

validate_init(Response, InitId, Expected) ->
    Base = maps:get(status, Response) =:= glyphastore_protocol:status_ok()
        andalso maps:get(request_id, Response) =:= InitId
        andalso maps:get(value, Response) =:= glyphastore_protocol:identity()
        andalso maps:get(worker_count, Response) > 0
        andalso maps:get(worker_count, Response) =< 256
        andalso maps:get(routing_epoch, Response) > 0,
    case Base of
        false -> {error, glyphastore_error:protocol(<<"server INIT response is inconsistent">>)};
        true ->
            case Expected of
                undefined -> ok;
                {WC, Epoch} ->
                    case maps:get(worker_count, Response) =:= WC andalso maps:get(routing_epoch, Response) =:= Epoch of
                        true -> ok;
                        false -> {error, glyphastore_error:unavailable(<<"server routing metadata changed during bootstrap">>)}
                    end
            end
    end.

validate_bind(Bound, BindId, Worker, InitResponse) ->
    case maps:get(status, Bound) =:= glyphastore_protocol:status_ok()
        andalso maps:get(request_id, Bound) =:= BindId
        andalso maps:get(owner_worker, Bound) =:= Worker
        andalso maps:get(worker_count, Bound) =:= maps:get(worker_count, InitResponse)
        andalso maps:get(routing_epoch, Bound) =:= maps:get(routing_epoch, InitResponse)
    of
        true -> ok;
        false -> {error, glyphastore_error:protocol(<<"server BIND_WORKER response is inconsistent">>)}
    end.

resolve_deadline(State, Opts) ->
    Budget = maps:get(request_timeout, State#state.config),
    Timeout = maps:get(timeout, Opts, Budget),
    case Timeout > 0 of
        true -> {ok, glyphastore_util:monotonic_seconds() + Timeout};
        false -> {error, glyphastore_error:invalid_argument(<<"request timeout must be positive">>)}
    end.

worker_index(#state{worker_count = 0}, _Key) ->
    {error, glyphastore_error:unavailable(<<"client is not connected">>)};
worker_index(#state{worker_count = WC, workers = Workers}, Key) ->
    case glyphastore_protocol:worker_for(Key, WC) of
        {ok, Worker} -> {ok, Worker, maps:get(Worker, Workers)};
        {error, Err} -> {error, glyphastore_error:invalid_argument(Err)}
    end.

ensure_connected(Conn, State) ->
    case gen_server:call(Conn, connected, 5000) of
        true ->
            {ok, State};
        false ->
            WC = State#state.worker_count,
            Epoch = State#state.routing_epoch,
            Worker = worker_for_conn(Conn, State#state.workers),
            case bootstrap_conn(Conn, Worker, {WC, Epoch}, State#state.config, State#state.request_id) of
                {ok, WC, Epoch, NextId} ->
                    {ok, State#state{request_id = NextId}};
                {error, Err} ->
                    {{error, Err}, State}
            end
    end.

worker_for_conn(Conn, Workers) ->
    maps:fold(fun(W, P, Acc) -> case P =:= Conn of true -> W; false -> Acc end end, 0, Workers).

validate_response(Response, RequestId, Worker, #state{worker_count = WC, routing_epoch = Epoch}) ->
    case maps:get(request_id, Response) =:= RequestId of
        false -> {error, glyphastore_error:protocol(<<"server response request ID does not match">>)};
        true ->
            case maps:get(worker_count, Response) =:= WC andalso maps:get(routing_epoch, Response) =:= Epoch of
                false -> {error, glyphastore_error:unavailable(<<"server routing metadata changed">>)};
                true ->
                    case maps:get(owner_worker, Response) =:= Worker orelse maps:get(status, Response) =:= glyphastore_protocol:status_wrong_owner() of
                        true -> ok;
                        false -> {error, glyphastore_error:protocol(<<"server response came from the wrong Worker">>)}
                    end
            end
    end.

status_err(Status, OpName, RequestId, Worker, State) ->
    annotate(glyphastore_error:from_status(Status), OpName, RequestId, Worker, State).

annotate(Err, OpName, RequestId, Worker, State) ->
    glyphastore_error:enrich(Err, #{
        operation => OpName,
        request_id => RequestId,
        worker => Worker,
        routing_epoch => State#state.routing_epoch
    }).

enrich_mutation(Err, Outcome) ->
    glyphastore_error:enrich(Err, #{mutation_outcome => Outcome}).

promote_send(SF) ->
    maps:get(error, SF).

promote_send_sf(SF, OpName, RequestId, Worker, State, Mutation) ->
    Err = maps:get(error, SF),
    Bytes = maps:get(bytes_sent, SF, 0),
    Outcome = case Mutation of
        true when Bytes =:= 0 -> rejected;
        true -> indeterminate;
        false -> undefined
    end,
    E1 = glyphastore_error:enrich(Err, #{bytes_sent => Bytes, operation => OpName, request_id => RequestId, worker => Worker, routing_epoch => State#state.routing_epoch}),
    case Outcome of
        undefined -> E1;
        O -> enrich_mutation(E1, O)
    end.

maybe_unavailable(Err) ->
    case maps:get(category, Err, undefined) of
        transport -> glyphastore_error:unavailable(maps:get(message, Err));
        _ -> Err
    end.

mutation_rejected(Op, Err, State) ->
    OpName = atom_to_binary(Op, utf8),
    #{outcome => rejected, error => enrich_mutation(annotate(Err, OpName, undefined, undefined, State), rejected)}.

retryable_read(Err, #state{healthy = true}) ->
    Cat = maps:get(category, Err),
    Cat =:= transport orelse Cat =:= unavailable;
retryable_read(_, _) -> false.

apply_unhealthy(Status, Err, State) ->
    case Status of
        S when S =:= glyphastore_protocol:status_wrong_owner(); S =:= glyphastore_protocol:status_not_bound() ->
            {State#state{healthy = false}, Err};
        _ -> {State, Err}
    end.

maybe_unhealthy_state(Status, State) ->
    case Status of
        S when S =:= glyphastore_protocol:status_wrong_owner(); S =:= glyphastore_protocol:status_not_bound() ->
            State#state{healthy = false};
        _ -> State
    end.

normalize_pipeline_req(Req) ->
    Op = maps:get(opcode, Req),
    Key = maps:get(key, Req),
    Value = maps:get(value, Req, <<>>),
    Expire = maps:get(expire_at_ns, Req, 0),
    Opcode = case Op of
        get -> glyphastore_protocol:opcode_get();
        put -> glyphastore_protocol:opcode_put();
        erase -> glyphastore_protocol:opcode_erase();
        _ -> invalid
    end,
    case Opcode of
        invalid -> {error, glyphastore_error:invalid_argument(<<"pipeline request contains an invalid opcode">>)};
        _ ->
            case (Op =:= get orelse Op =:= erase) andalso (Value =/= <<>> orelse Expire =/= 0) of
                true -> {error, glyphastore_error:invalid_argument(<<"GET and ERASE pipeline requests cannot carry PUT fields">>)};
                false -> {ok, #{opcode => Opcode, key => Key, value => Value, expire_at_ns => Expire, op => Op}}
            end
    end.

failed_response() -> #{outcome => failed}.
set_response(List, Idx, Resp) ->
    lists:sublist(List, 1, Idx - 1) ++ [Resp] ++ lists:nthtail(Idx, List).

frame_limit_err(FrameLen, MaxFrame, MaxBytes, Need) ->
    case FrameLen > MaxFrame of
        true -> glyphastore_error:invalid_argument(<<"pipeline request exceeds the configured frame limit">>);
        false -> glyphastore_error:invalid_argument(<<"pipeline exceeds the configured aggregate byte limit">>)
    end.

pipeline_ok(Req, Response) ->
    case maps:get(op, Req) of
        Op when Op =:= put; Op =:= erase ->
            case maps:get(value, Response) of
                <<>> -> ok;
                _ -> {error, glyphastore_error:protocol(<<"mutation response value must be empty">>)}
            end;
        _ -> ok
    end.

pipeline_status_outcome(Req, Status) ->
    case maps:get(op, Req) of
        Op when (Op =:= put orelse Op =:= erase) andalso Status =:= glyphastore_protocol:status_internal_error() ->
            indeterminate;
        _ -> failed
    end.

pipeline_op(Req) ->
    atom_to_binary(maps:get(op, Req), utf8).

mark_unresolved(Responses, Start, Err, BytesSent, Metadata, TotalSent) ->
    lists:zipwith(
        fun(Idx, Resp) ->
            case Idx >= Start of
                true ->
                    case mutation_may_have_arrived(Idx, Metadata, BytesSent, TotalSent) of
                        true -> #{outcome => indeterminate, error => Err};
                        false -> #{outcome => failed, error => Err}
                    end;
                false -> Resp
            end
        end,
        lists:seq(1, length(Responses)),
        Responses
    ).

mutation_may_have_arrived(Idx, Metadata, BytesSent, TotalSent) ->
    lists:any(fun({_Id, Req, Begin}) ->
        maps:get(op, Req) =/= get andalso (BytesSent > Begin orelse TotalSent > Begin)
    end, lists:nthtail(Idx - 1, Metadata)).

Metadata_from_plan(Plan) ->
    [{maps:get(request_id, I), maps:get(req, I), maps:get(begin_offset, I)} || I <- Plan].

build_tls_options(TLS, Host) ->
    case code:ensure_loaded(ssl) of
        {module, ssl} ->
            SN = maps:get(server_name, TLS, Host),
            Verify = case maps:get(insecure_skip_verify, TLS, false) of true -> verify_none; false -> verify_peer end,
            Opts = [{verify, Verify}, {server_name_indication, SN}, {versions, ['tlsv1.3']}],
            case maps:get(ca_file, TLS, undefined) of
                undefined -> {ok, Opts};
                CAFile ->
                    case file:read_file(CAFile) of
                        {ok, PEM} ->
                            case public_key:pem_decode(PEM) of
                                [{_, Cert, _} | _] -> {ok, [{cacerts, [Cert]} | Opts]};
                                _ -> {error, glyphastore_error:invalid_argument(<<"TLS CA file does not contain a usable certificate">>)}
                            end;
                        {error, _} -> {error, glyphastore_error:invalid_argument(<<"cannot read TLS CA file">>)}
                    end
            end;
        _ -> {error, glyphastore_error:unavailable(<<"TLS requires the ssl application">>)}
    end.
