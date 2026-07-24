#!/usr/bin/env escript
%%! -noshell -noinput +S 4:4
%% Reproducible external-server benchmark for the Erlang GlyphaStore SDK.
%% Workload matches Python/Perl/Go/Ruby: ordered PUT/GET pipeline read-after-write.
%% Fixed +S 4:4 (not 1:1) so execute_worker_pipelines fan-out can use multiple
%% schedulers; still capped for cross-machine comparability.
-mode(compile).

main(Args) ->
    add_beam_path(),
    Opts = parse_args(#{
        host => "127.0.0.1",
        workers => 4,
        ops => 100000,
        pipeline => 64,
        warmup => 1,
        repeats => 7,
        concurrent => undefined
    }, Args),
    run(Opts).

add_beam_path() ->
    Script = escript:script_name(),
    Root = filename:dirname(filename:dirname(Script)),
    Beam = filename:join([Root, "_build", "default", "lib", "glyphastore", "ebin"]),
    true = code:add_patha(Beam).

parse_args(Opts, []) ->
    Opts;
parse_args(Opts, ["--host", Host | Rest]) ->
    parse_args(Opts#{host => Host}, Rest);
parse_args(Opts, ["--port", Port | Rest]) ->
    parse_args(Opts#{port => list_to_integer(Port)}, Rest);
parse_args(Opts, ["--workers", N | Rest]) ->
    parse_args(Opts#{workers => list_to_integer(N)}, Rest);
parse_args(Opts, ["--ops", N | Rest]) ->
    parse_args(Opts#{ops => list_to_integer(N)}, Rest);
parse_args(Opts, ["--pipeline", N | Rest]) ->
    parse_args(Opts#{pipeline => list_to_integer(N)}, Rest);
parse_args(Opts, ["--warmup", N | Rest]) ->
    parse_args(Opts#{warmup => list_to_integer(N)}, Rest);
parse_args(Opts, ["--repeats", N | Rest]) ->
    parse_args(Opts#{repeats => list_to_integer(N)}, Rest);
parse_args(Opts, ["--concurrent" | Rest]) ->
    parse_args(Opts#{concurrent => true}, Rest);
parse_args(Opts, ["--no-concurrent" | Rest]) ->
    parse_args(Opts#{concurrent => false}, Rest);
parse_args(_Opts, [Unknown | _]) ->
    io:format(standard_error, "unknown argument: ~s~n", [Unknown]),
    usage(),
    halt(2).

usage() ->
    io:format(standard_error,
        "Usage: client_benchmark.escript --port N [--host H] [--workers N] [--ops N] "
        "[--pipeline N] [--warmup N] [--repeats N] [--concurrent|--no-concurrent]~n",
        []).

run(Opts) ->
    Port = maps:get(port, Opts, undefined),
    Workers = maps:get(workers, Opts),
    Ops = maps:get(ops, Opts),
    Pipeline = maps:get(pipeline, Opts),
    Warmup = maps:get(warmup, Opts),
    Repeats = maps:get(repeats, Opts),
    case Port =:= undefined orelse Workers < 1 orelse Ops < 1 orelse Pipeline < 1
        orelse Warmup < 0 orelse Repeats < 1 of
        true ->
            io:format(standard_error, "numeric arguments are outside benchmark limits~n", []),
            halt(2);
        false ->
            ok
    end,
    UseConcurrent =
        case maps:get(concurrent, Opts) of
            undefined -> Workers > 1;
            Flag -> Flag
        end,
    Batches = material(Ops, Workers, Pipeline),
    BatchFrames = Pipeline * 2,
    Config = glyphastore_util:merge_config(#{
        host => maps:get(host, Opts),
        port => Port,
        maximum_pipeline_requests => BatchFrames,
        request_timeout => 120.0
    }),
    case glyphastore_client:connect(Config) of
        {ok, Client} ->
            try
                case glyphastore_client:worker_count(Client) of
                    Workers ->
                        ok;
                    Other ->
                        io:format(standard_error,
                            "server Worker count ~p does not match --workers ~p~n",
                            [Other, Workers]),
                        halt(1)
                end,
                RunOnce =
                    case UseConcurrent of
                        true -> fun() -> run_concurrent(Client, Batches) end;
                        false -> fun() -> run_sequential(Client, Batches) end
                    end,
                lists:foreach(fun(_) -> ok = RunOnce() end, lists:seq(1, Warmup)),
                Samples = [begin
                    T0 = erlang:monotonic_time(nanosecond),
                    ok = RunOnce(),
                    T1 = erlang:monotonic_time(nanosecond),
                    (T1 - T0) / 1.0e9
                end || _ <- lists:seq(1, Repeats)],
                report(UseConcurrent, Workers, Pipeline, Ops * 2, Samples)
            after
                glyphastore_client:close(Client)
            end;
        {error, Err} ->
            io:format(standard_error, "~p~n", [Err]),
            halt(1)
    end.

material(Operations, Workers, Pipeline) ->
    Quotas0 = [Operations div Workers || _ <- lists:seq(1, Workers)],
    Rem = Operations rem Workers,
    Quotas = [lists:nth(I, Quotas0) + case I =< Rem of true -> 1; false -> 0 end
              || I <- lists:seq(1, Workers)],
    Requests = fill_requests(Quotas, 0, [[] || _ <- lists:seq(1, Workers)]),
    BatchFrames = Pipeline * 2,
    [chunk(WorkerReqs, BatchFrames) || WorkerReqs <- Requests].

fill_requests(Quotas, _Candidate, Acc) ->
    case lists:any(fun(Q) -> Q > 0 end, Quotas) of
        false ->
            Acc;
        true ->
            fill_requests_step(Quotas, 0, Acc)
    end.

fill_requests_step(Quotas, Candidate, Acc) ->
    case lists:any(fun(Q) -> Q > 0 end, Quotas) of
        false ->
            Acc;
        true ->
            Key = list_to_binary(io_lib:format("erlang-bench-~12..0B", [Candidate])),
            {ok, Owner} = glyphastore_protocol:worker_for(Key, length(Quotas)),
            Quota = lists:nth(Owner + 1, Quotas),
            case Quota of
                0 ->
                    fill_requests_step(Quotas, Candidate + 1, Acc);
                _ ->
                    Value = binary:copy(<< (Candidate band 16#FF) >>, 64),
                    Put = #{opcode => put, key => Key, value => Value},
                    Get = #{opcode => get, key => Key},
                    Acc1 = set_nth(Acc, Owner + 1, lists:nth(Owner + 1, Acc) ++ [Put, Get]),
                    Quotas1 = set_nth(Quotas, Owner + 1, Quota - 1),
                    fill_requests_step(Quotas1, Candidate + 1, Acc1)
            end
    end.

chunk(List, Size) ->
    chunk(List, Size, []).

chunk([], _Size, Acc) ->
    lists:reverse(Acc);
chunk(List, Size, Acc) ->
    {Batch, Rest} = split_at(List, Size),
    chunk(Rest, Size, [Batch | Acc]).

split_at(List, N) when length(List) =< N ->
    {List, []};
split_at(List, N) ->
    {lists:sublist(List, N), lists:nthtail(N, List)}.

set_nth(List, Idx, Value) ->
    lists:sublist(List, 1, Idx - 1) ++ [Value] ++ lists:nthtail(Idx, List).

run_sequential(Client, Batches) ->
    lists:foreach(
        fun(WorkerBatches) ->
            lists:foreach(
                fun(Batch) ->
                    validate_batch(Batch, glyphastore_client:execute_pipeline(Client, Batch))
                end,
                WorkerBatches
            )
        end,
        Batches
    ),
    ok.

run_concurrent(Client, Batches) ->
    MaxRounds = lists:max([0 | [length(WB) || WB <- Batches]]),
    lists:foreach(
        fun(Round) ->
            Wave = [
                case Round < length(WB) of
                    true -> lists:nth(Round + 1, WB);
                    false -> []
                end
             || WB <- Batches
            ],
            case lists:all(fun(B) -> B =:= [] end, Wave) of
                true ->
                    ok;
                false ->
                    {ok, All} = glyphastore_client:execute_worker_pipelines(Client, Wave),
                    lists:foreach(
                        fun({Batch, Responses}) ->
                            case Batch of
                                [] -> ok;
                                _ -> validate_batch(Batch, {ok, Responses})
                            end
                        end,
                        lists:zip(Wave, All)
                    )
            end
        end,
        lists:seq(0, MaxRounds - 1)
    ),
    ok.

validate_batch(Batch, {ok, Responses}) ->
    case length(Responses) =:= length(Batch) of
        false ->
            error(pipeline_response_count_mismatch);
        true ->
            lists:foreach(
                fun({Idx, Req}) ->
                    Resp = lists:nth(Idx, Responses),
                    case maps:get(outcome, Resp) of
                        succeeded ->
                            case maps:get(opcode, Req) of
                                get ->
                                    Expected = maps:get(value, lists:nth(Idx - 1, Batch)),
                                    case maps:get(value, Resp) of
                                        Expected -> ok;
                                        _ -> error(pipeline_get_value_mismatch)
                                    end;
                                _ ->
                                    ok
                            end;
                        _ ->
                            error(pipeline_request_failed)
                    end
                end,
                lists:zip(lists:seq(1, length(Batch)), Batch)
            )
    end;
validate_batch(_Batch, {error, Err}) ->
    error({pipeline_failed, Err}).

report(UseConcurrent, Workers, Pipeline, OperationCount, Samples) ->
    Rates = [OperationCount / S || S <- Samples],
    Execution =
        case UseConcurrent of
            true -> "single-process-worker-concurrent";
            false -> "single-process-worker-sequential"
        end,
    Version = binary_to_list(glyphastore_version:version()),
    SortedS = lists:sort(Samples),
    SortedR = lists:sort(Rates),
    io:format("# glyphastore Erlang client benchmark~n"),
    io:format(
        "# sdk_version=~s runtime=sync execution=~s workers=~B pipeline_pairs=~B operations=~B~n",
        [Version, Execution, Workers, Pipeline, OperationCount]
    ),
    io:format(
        "name=erlang_client_pipeline_read_after_write sdk_version=~s runtime=sync execution=~s "
        "workers=~B pipeline_pairs=~B operations=~B samples=~B "
        "median_seconds=~.9f min_seconds=~.9f max_seconds=~.9f "
        "median_ops_per_second=~.3f min_ops_per_second=~.3f max_ops_per_second=~.3f~n",
        [
            Version,
            Execution,
            Workers,
            Pipeline,
            OperationCount,
            length(Samples),
            median(SortedS),
            hd(SortedS),
            lists:last(SortedS),
            median(SortedR),
            hd(SortedR),
            lists:last(SortedR)
        ]
    ).

median([X]) ->
    X;
median(Sorted) ->
    N = length(Sorted),
    case N rem 2 of
        1 ->
            lists:nth(N div 2 + 1, Sorted);
        0 ->
            (lists:nth(N div 2, Sorted) + lists:nth(N div 2 + 1, Sorted)) / 2
    end.
