-module(glyphastore_client).
-behaviour(gen_server).

-include("glyphastore_protocol.hrl").

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
    execute_worker_pipelines/2,
    execute_worker_pipelines/3,
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

%% Synchronous close (sibling SDKs wait until sockets are released).
close(Client) ->
    try gen_server:call(Client, close, infinity) of
        ok -> ok
    catch
        exit:{noproc, _} -> ok;
        exit:{{noproc, _}, _} -> ok;
        exit:{normal, _} -> ok;
        exit:{{normal, _}, _} -> ok
    end.
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
execute_pipeline(Client, Requests, Opts) ->
    gen_server:call(Client, {execute_pipeline, Requests, Opts}, infinity).

execute_batch(Client, Requests) -> execute_batch(Client, Requests, #{}).
execute_batch(Client, Requests, Opts) ->
    gen_server:call(Client, {execute_batch, Requests, Opts}, infinity).

%% Run one pipeline vector per Worker concurrently (Perl execute_worker_pipelines parity).
%% Batches must be a list of length worker_count(); empty lists are skipped.
execute_worker_pipelines(Client, Batches) -> execute_worker_pipelines(Client, Batches, #{}).
execute_worker_pipelines(Client, Batches, Opts) ->
    gen_server:call(Client, {execute_worker_pipelines, Batches, Opts}, infinity).

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
handle_call({worker_for, _Key}, _From, #state{worker_count = 0} = State) ->
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
handle_call({execute_worker_pipelines, Batches, Opts}, _From, State) ->
    {Reply, State1} = do_execute_worker_pipelines(Batches, Opts, State),
    {reply, Reply, State1};
handle_call(close, _From, State) ->
    maps:foreach(fun(_W, Pid) -> glyphastore_conn:reset(Pid) end, State#state.workers),
    {stop, normal, ok, State#state{healthy = false}};
handle_call(_Req, _From, State) ->
    {reply, {error, glyphastore_error:internal(<<"unexpected call">>)}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

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

do_read(_Op, _Key, _Value, _Opts, #state{healthy = false} = State) ->
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
    case ensure_connected(Conn, Worker, State) of
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
                ?GS_ST_OK ->
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

%% At-most-one auto-retry (Attempts starts at 2). Preserve the last error — do not
%% collapse a second transport failure into a synthetic "not attempted" unavailable.
retry_read(Err, Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, Attempts) ->
    case Attempts > 1 andalso retryable_read(Err, State) of
        true -> do_read_attempt(Opcode, OpName, Key, Value, Deadline, Worker, Conn, State, Attempts - 1);
        false -> {{error, Err}, State}
    end.

do_mutate(Op, _Key, _Value, _Expire, _Opts, #state{healthy = false} = State) ->
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
    Err = enrich_mutation(annotate(glyphastore_error:unavailable(<<"could not send mutation">>), OpName, undefined, Worker, State), rejected),
    {#{outcome => rejected, error => Err}, State};
do_mutate_attempt(Op, OpName, Key, Value, Expire, Deadline, Worker, Conn, State, Attempts) ->
    case ensure_connected(Conn, Worker, State) of
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
                            Ann = enrich_mutation(annotate(Err, OpName, RequestId, Worker, State3), indeterminate),
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
            Err = enrich_mutation(promote_send_sf(SF, OpName, RequestId, Worker, State, true), rejected),
            {#{outcome => rejected, error => Err}, State};
        _ ->
            Err = enrich_mutation(promote_send_sf(SF, OpName, RequestId, Worker, State, true), indeterminate),
            {#{outcome => indeterminate, error => Err}, State}
    end.

handle_mutate_response(Response, OpName, RequestId, Worker, Conn, State) ->
    case validate_response(Response, RequestId, Worker, State) of
        ok ->
            case maps:get(status, Response) of
                ?GS_ST_OK ->
                    case maps:get(value, Response) of
                        <<>> -> {#{outcome => committed}, State};
                        _ ->
                            glyphastore_conn:reset(Conn),
                            Err = enrich_mutation(
                                annotate(
                                    glyphastore_error:protocol(<<"mutation response value must be empty">>),
                                    OpName,
                                    RequestId,
                                    Worker,
                                    State
                                ),
                                indeterminate
                            ),
                            {#{outcome => indeterminate, error => Err}, State}
                    end;
                ?GS_ST_INTERNAL ->
                    Err = enrich_mutation(status_err(?GS_ST_INTERNAL, OpName, RequestId, Worker, State), indeterminate),
                    {#{outcome => indeterminate, error => Err}, State};
                Status ->
                    Err = enrich_mutation(status_err(Status, OpName, RequestId, Worker, State), rejected),
                    {State1, Final} = apply_unhealthy(Status, Err, State),
                    {#{outcome => rejected, error => Final}, State1}
            end;
        {error, Err} ->
            glyphastore_conn:reset(Conn),
            Ann = enrich_mutation(annotate(Err, OpName, RequestId, Worker, State), indeterminate),
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
                    case Worker0 =:= undefined orelse Worker0 =:= Owner of
                        false ->
                            {error, glyphastore_error:invalid_argument(<<"every pipeline key must route to the same Worker">>)};
                        true ->
                            Key = maps:get(key, Norm),
                            Value = maps:get(value, Norm, <<>>),
                            FrameLen = glyphastore_protocol:request_frame_size(Key, Value),
                            MaxFrame = maps:get(maximum_frame_bytes, StateAcc#state.config),
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
    %% Pre-size with array for O(1) updates (list setnth is O(n²) on long pipelines).
    Responses = array:new(Count, {default, failed_response()}),
    case ensure_connected(Conn, Worker, State) of
        {ok, State1} ->
            %% Keep frames as an iolist — gen_tcp/ssl send accepts iodata without a contig copy.
            Output = [maps:get(frame, I) || I <- Plan],
            TotalBytes = iolist_size(Output),
            Metadata = [{maps:get(request_id, I), maps:get(req, I), maps:get(begin_offset, I)} || I <- Plan],
            case glyphastore_conn:send(Conn, Output, Deadline) of
                ok ->
                    collect_pipeline(Metadata, 0, Responses, Deadline, Worker, Conn, State1, TotalBytes);
                {error, SF = #{send_failure := true}} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, array:to_list(mark_unresolved(Responses, 0, promote_send_sf(SF, undefined, undefined, Worker, State1, false), maps:get(bytes_sent, SF, 0), Metadata))}, State1};
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, array:to_list(mark_unresolved(Responses, 0, Err, 0, Metadata))}, State1}
            end;
        {{error, Err}, State1} ->
            {{ok, array:to_list(mark_unresolved(Responses, 0, Err, 0, metadata_from_plan(Plan)))}, State1}
    end.

collect_pipeline([], _Idx, Responses, _Deadline, _Worker, _Conn, State, _Sent) ->
    {{ok, array:to_list(Responses)}, State};
collect_pipeline([{RequestId, Req, Begin} | Rest], Idx, Responses, Deadline, Worker, Conn, State, SentBytes) ->
    case glyphastore_conn:receive_response(Conn, Deadline) of
        {ok, Response} ->
            case validate_response(Response, RequestId, Worker, State) of
                ok ->
                    case maps:get(status, Response) of
                        ?GS_ST_OK ->
                            case pipeline_ok(Req, Response) of
                                ok ->
                                    Responses1 = array:set(Idx, #{
                                        outcome => succeeded, value => maps:get(value, Response)
                                    }, Responses),
                                    collect_pipeline(Rest, Idx + 1, Responses1, Deadline, Worker, Conn, State, SentBytes);
                                {error, Err} ->
                                    glyphastore_conn:reset(Conn),
                                    {{ok, array:to_list(mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, Begin} | Rest]))}, State}
                            end;
                        Status ->
                            Err = status_err(Status, pipeline_op(Req), RequestId, Worker, State),
                            Outcome = pipeline_status_outcome(Req, Status),
                            Responses1 = array:set(Idx, #{outcome => Outcome, error => Err}, Responses),
                            State1 = maybe_unhealthy_state(Status, State),
                            collect_pipeline(Rest, Idx + 1, Responses1, Deadline, Worker, Conn, State1, SentBytes)
                    end;
                {error, Err} ->
                    glyphastore_conn:reset(Conn),
                    {{ok, array:to_list(mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, Begin} | Rest]))}, State}
            end;
        {error, Err} ->
            glyphastore_conn:reset(Conn),
            {{ok, array:to_list(mark_unresolved(Responses, Idx, Err, SentBytes, [{RequestId, Req, Begin} | Rest]))}, State}
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
    Responses0 = array:new(Count, {default, failed_response()}),
    case prepare_batch_groups(Groups, State, []) of
        {ok, Prepared, State1} ->
            case ensure_workers_connected([W || {W, _, _} <- Prepared], State1) of
                {ok, State2} ->
                    case fanout_prepared(
                        Prepared,
                        Deadline,
                        State2,
                        fun(_Worker, Items, GroupResponses, Acc) ->
                            merge_group_by_index(Acc, Items, GroupResponses)
                        end,
                        Responses0
                    ) of
                        {{ok, Acc}, State3} when is_tuple(Acc) ->
                            %% Acc is an array when Merge builds via merge_group_by_index
                            {{ok, array:to_list(Acc)}, State3};
                        Other ->
                            Other
                    end;
                {{error, Err}, State2} ->
                    {{error, Err}, State2}
            end;
        {error, Err} ->
            {{error, Err}, State}
    end.

prepare_batch_groups([], State, Acc) ->
    {ok, lists:reverse(Acc), State};
prepare_batch_groups([{Worker, Items} | Rest], State, Acc) ->
    Ordered = lists:reverse(Items),
    case build_group_plan(Ordered, State) of
        {ok, Plan, State1} ->
            prepare_batch_groups(Rest, State1, [{Worker, Ordered, Plan} | Acc]);
        {error, Err} ->
            {error, Err}
    end.

do_execute_worker_pipelines(_Batches, _Opts, #state{healthy = false} = State) ->
    {{error, glyphastore_error:unavailable(<<"client is closed or routing metadata changed">>)}, State};
do_execute_worker_pipelines(Batches, Opts, State) when is_list(Batches) ->
    WC = State#state.worker_count,
    case length(Batches) =:= WC of
        false ->
            {{error, glyphastore_error:invalid_argument(<<"worker pipeline vector does not match Worker count">>)}, State};
        true ->
            case resolve_deadline(State, Opts) of
                {ok, Deadline} ->
                    case plan_worker_pipelines(Batches, 0, State, []) of
                        {ok, Prepared, State1} ->
                            case ensure_workers_connected([W || {W, _, _} <- Prepared], State1) of
                                {ok, State2} ->
                                    Empty = [[] || _ <- lists:seq(1, WC)],
                                    fanout_prepared(
                                        Prepared,
                                        Deadline,
                                        State2,
                                        fun(Worker, _Items, GroupResponses, Acc) ->
                                            set_nth(Acc, Worker + 1, GroupResponses)
                                        end,
                                        Empty
                                    );
                                {{error, Err}, State2} ->
                                    {{error, Err}, State2}
                            end;
                        {error, Err} ->
                            {{error, Err}, State}
                    end;
                {error, Err} ->
                    {{error, Err}, State}
            end
    end;
do_execute_worker_pipelines(_Batches, _Opts, State) ->
    {{error, glyphastore_error:invalid_argument(<<"worker pipelines must be a list">>)}, State}.

plan_worker_pipelines([], _Worker, State, Acc) ->
    {ok, lists:reverse(Acc), State};
plan_worker_pipelines([[] | Rest], Worker, State, Acc) ->
    plan_worker_pipelines(Rest, Worker + 1, State, Acc);
plan_worker_pipelines([Requests | Rest], Worker, State, Acc) when is_list(Requests) ->
    case plan_pipeline_for_worker(Requests, Worker, State) of
        {ok, Plan, State1} ->
            plan_worker_pipelines(Rest, Worker + 1, State1, [{Worker, Requests, Plan} | Acc]);
        {error, Err} ->
            {error, Err}
    end;
plan_worker_pipelines([_ | _], _Worker, _State, _Acc) ->
    {error, glyphastore_error:invalid_argument(<<"each worker pipeline must be a list">>)}.

plan_pipeline_for_worker(Requests, ExpectedWorker, State) ->
    MaxReq = maps:get(maximum_pipeline_requests, State#state.config),
    MaxBytes = maps:get(maximum_pipeline_bytes, State#state.config),
    case length(Requests) > MaxReq of
        true ->
            {error, glyphastore_error:invalid_argument(<<"pipeline exceeds the configured request limit">>)};
        false ->
            plan_pipeline_for_worker_items(Requests, ExpectedWorker, State, 0, [], MaxBytes)
    end.

plan_pipeline_for_worker_items([], _ExpectedWorker, StateAcc, _Need, Acc, _MaxBytes) ->
    {ok, lists:reverse(Acc), StateAcc};
plan_pipeline_for_worker_items([Req | Rest], ExpectedWorker, StateAcc, Need, Acc, MaxBytes) ->
    case normalize_pipeline_req(Req) of
        {error, Err} ->
            {error, Err};
        {ok, Norm} ->
            case worker_index(StateAcc, maps:get(key, Norm)) of
                {error, Err} ->
                    {error, Err};
                {ok, Owner, _Conn} when Owner =/= ExpectedWorker ->
                    {error, glyphastore_error:invalid_argument(<<"pipeline key does not route to the expected Worker">>)};
                {ok, Owner, _Conn} ->
                    Key = maps:get(key, Norm),
                    Value = maps:get(value, Norm, <<>>),
                    FrameLen = glyphastore_protocol:request_frame_size(Key, Value),
                    MaxFrame = maps:get(maximum_frame_bytes, StateAcc#state.config),
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
                                    plan_pipeline_for_worker_items(
                                        Rest, Owner, State2, Need + FrameLen, [Item | Acc], MaxBytes
                                    );
                                {{error, Err2}, _} ->
                                    {error, Err2}
                            end;
                        false ->
                            {error, frame_limit_err(FrameLen, MaxFrame, MaxBytes, Need)}
                    end
            end
    end.

ensure_workers_connected([], State) ->
    {ok, State};
ensure_workers_connected([Worker | Rest], State) ->
    Conn = maps:get(Worker, State#state.workers),
    case ensure_connected(Conn, Worker, State) of
        {ok, State1} ->
            ensure_workers_connected(Rest, State1);
        {{error, Err}, State1} ->
            {{error, Err}, State1}
    end.

fanout_prepared([], _Deadline, State, _Merge, Acc) ->
    {{ok, Acc}, State};
fanout_prepared([{Worker, Items, Plan}], Deadline, State, Merge, Acc) ->
    %% Single Worker: stay on the client process (Go ExecuteBatch fast path).
    Conn = maps:get(Worker, State#state.workers),
    {{ok, GroupResponses}, State1} = run_pipeline(Plan, Deadline, Worker, Conn, State, length(Plan)),
    {{ok, Merge(Worker, Items, GroupResponses, Acc)}, State1};
fanout_prepared(Prepared, Deadline, State, Merge, Acc0) ->
    Parent = self(),
    Pending = lists:map(
        fun({Worker, Items, Plan}) ->
            Ref = make_ref(),
            Conn = maps:get(Worker, State#state.workers),
            spawn(fun() ->
                {{ok, GroupResponses}, StateOut} =
                    run_pipeline(Plan, Deadline, Worker, Conn, State, length(Plan)),
                Parent ! {Ref, Worker, Items, GroupResponses, StateOut#state.healthy}
            end),
            {Ref, Worker, Items}
        end,
        Prepared
    ),
    collect_fanout(Pending, Acc0, State, Merge).

collect_fanout([], Acc, State, _Merge) ->
    {{ok, Acc}, State};
collect_fanout(Pending, Acc, State, Merge) ->
    receive
        {Ref, Worker, Items, GroupResponses, Healthy} ->
            case lists:keytake(Ref, 1, Pending) of
                {value, {Ref, Worker, Items}, Rest} ->
                    Acc1 = Merge(Worker, Items, GroupResponses, Acc),
                    State1 =
                        case Healthy of
                            true -> State;
                            false -> State#state{healthy = false}
                        end,
                    collect_fanout(Rest, Acc1, State1, Merge);
                false ->
                    collect_fanout(Pending, Acc, State, Merge)
            end
    end.

set_nth(List, Idx, Value) ->
    lists:sublist(List, 1, Idx - 1) ++ [Value] ++ lists:nthtail(Idx, List).

build_group_plan(Items, State) ->
    build_group_plan(Items, State, 0, []).

build_group_plan([], State, _Need, Acc) ->
    {ok, lists:reverse(Acc), State};
build_group_plan([{Idx, Norm} | Rest], State, Need, Acc) ->
    {RequestId, State1} = bump_id(State),
    Key = maps:get(key, Norm),
    Value = maps:get(value, Norm, <<>>),
    Expire = maps:get(expire_at_ns, Norm, 0),
    case encode_request(maps:get(opcode, Norm), RequestId, Key, Value, Expire, State1) of
        {ok, Frame, State2} ->
            Item = #{
                req => Norm,
                request_id => RequestId,
                frame => Frame,
                begin_offset => Need,
                index => Idx
            },
            build_group_plan(Rest, State2, Need + byte_size(Frame), [Item | Acc]);
        {{error, Err}, _} ->
            {error, Err}
    end.

merge_group_by_index(Responses, Items, GroupResponses) ->
    lists:foldl(
        fun({{Idx, _Norm}, Resp}, Acc) ->
            array:set(Idx, Resp, Acc)
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
        {error, {invalid_argument, Msg}} ->
            {error, glyphastore_error:invalid_argument(Msg)}
    end.

ensure_connected(Conn, Worker, State) ->
    case gen_server:call(Conn, connected, 5000) of
        true ->
            {ok, State};
        false ->
            WC = State#state.worker_count,
            Epoch = State#state.routing_epoch,
            case bootstrap_conn(Conn, Worker, {WC, Epoch}, State#state.config, State#state.request_id) of
                {ok, WC, Epoch, NextId} ->
                    {ok, State#state{request_id = NextId}};
                {error, Err} ->
                    {{error, Err}, State}
            end
    end.

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
        ?GS_ST_WRONG_OWNER ->
            {State#state{healthy = false}, Err};
        ?GS_ST_NOT_BOUND ->
            {State#state{healthy = false}, Err};
        _ ->
            {State, Err}
    end.

maybe_unhealthy_state(Status, State) ->
    case Status of
        ?GS_ST_WRONG_OWNER ->
            State#state{healthy = false};
        ?GS_ST_NOT_BOUND ->
            State#state{healthy = false};
        _ ->
            State
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

frame_limit_err(FrameLen, MaxFrame, _MaxBytes, _Need) ->
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
    case {maps:get(op, Req), Status} of
        {Op, ?GS_ST_INTERNAL} when Op =:= put; Op =:= erase ->
            indeterminate;
        _ ->
            failed
    end.

pipeline_op(Req) ->
    atom_to_binary(maps:get(op, Req), utf8).

%% Match Go/Python: per-request classification — mutation indeterminate only when
%% bytes_sent > that request's begin_offset (zero-byte send failures stay failed).
mark_unresolved(Responses, Start, Err, BytesSent, Metadata) ->
    mark_unresolved_from(Start, Metadata, Responses, Err, BytesSent).

mark_unresolved_from(_Idx, [], Responses, _Err, _BytesSent) ->
    Responses;
mark_unresolved_from(Idx, [{_Id, Req, Begin} | Rest], Responses, Err, BytesSent) ->
    Op = maps:get(op, Req),
    Outcome =
        case (Op =:= put orelse Op =:= erase) andalso BytesSent > Begin of
            true -> indeterminate;
            false -> failed
        end,
    mark_unresolved_from(
        Idx + 1,
        Rest,
        array:set(Idx, #{outcome => Outcome, error => Err}, Responses),
        Err,
        BytesSent
    ).

metadata_from_plan(Plan) ->
    [{maps:get(request_id, I), maps:get(req, I), maps:get(begin_offset, I)} || I <- Plan].

build_tls_options(TLS, Host) ->
    case code:ensure_loaded(ssl) of
        {module, ssl} ->
            SN = maps:get(server_name, TLS, Host),
            Verify = case maps:get(insecure_skip_verify, TLS, false) of true -> verify_none; false -> verify_peer end,
            Opts0 = [{verify, Verify}, {server_name_indication, SN}, {versions, ['tlsv1.3']}],
            case maps:get(ca_file, TLS, undefined) of
                undefined ->
                    finish_tls_options(TLS, Opts0);
                CAFile ->
                    case file:read_file(CAFile) of
                        {ok, PEM} ->
                            case public_key:pem_decode(PEM) of
                                [{_, Cert, _} | _] ->
                                    finish_tls_options(TLS, [{cacerts, [Cert]} | Opts0]);
                                _ ->
                                    {error, glyphastore_error:invalid_argument(<<"TLS CA file does not contain a usable certificate">>)}
                            end;
                        {error, _} ->
                            {error, glyphastore_error:invalid_argument(<<"cannot read TLS CA file">>)}
                    end
            end;
        _ -> {error, glyphastore_error:unavailable(<<"TLS requires the ssl application">>)}
    end.

finish_tls_options(TLS, Opts0) ->
    Cert = maps:get(cert_file, TLS, undefined),
    Key = maps:get(key_file, TLS, undefined),
    case {Cert, Key} of
        {undefined, undefined} ->
            {ok, Opts0};
        {undefined, _} ->
            {error, glyphastore_error:invalid_argument(<<"TLS mTLS requires both cert_file and key_file">>)};
        {_, undefined} ->
            {error, glyphastore_error:invalid_argument(<<"TLS mTLS requires both cert_file and key_file">>)};
        {CertFile, KeyFile} ->
            {ok, [{certfile, CertFile}, {keyfile, KeyFile} | Opts0]}
    end.
