-module(glyphastore_util).
-export([
    monotonic_seconds/0,
    remaining_timeout/1,
    timeout_ms/1,
    default_config/0,
    merge_config/1,
    validate_config/1
]).

-spec monotonic_seconds() -> float().
monotonic_seconds() ->
    erlang:convert_time_unit(erlang:monotonic_time(), native, 1000000000) / 1000000000.

-spec remaining_timeout(float()) -> {ok, float()} | {error, glyphastore_error:error()}.
remaining_timeout(Deadline) ->
    Left = Deadline - monotonic_seconds(),
    case Left =< 0 of
        true ->
            {error, glyphastore_error:transport(<<"request deadline expired">>)};
        false ->
            {ok, Left}
    end.

-spec timeout_ms(float()) -> non_neg_integer().
timeout_ms(Seconds) when Seconds =< 0 ->
    0;
timeout_ms(Seconds) ->
    max(1, round(Seconds * 1000)).

-spec default_config() -> glyphastore_client:config().
default_config() ->
    #{
        host => "127.0.0.1",
        port => 7379,
        connect_timeout => 3.0,
        request_timeout => 5.0,
        maximum_frame_bytes => glyphastore_protocol:max_frame_bytes(),
        maximum_pipeline_requests => 256,
        maximum_pipeline_bytes => 1024 * 1024,
        tls => #{enable => false}
    }.

-spec merge_config(map() | undefined) -> glyphastore_client:config().
merge_config(undefined) ->
    default_config();
merge_config(Config) ->
    maps:fold(
        fun
            (_, undefined, Acc) -> Acc;
            (port, 0, Acc) -> Acc;
            (host, <<>>, Acc) -> Acc;
            (host, "", Acc) -> Acc;
            (K, V, Acc) -> Acc#{K => V}
        end,
        default_config(),
        Config
    ).

-spec validate_config(glyphastore_client:config()) -> ok | {error, glyphastore_error:error()}.
validate_config(Config) ->
    Host = maps:get(host, Config),
    Port = maps:get(port, Config),
    ConnectTimeout = maps:get(connect_timeout, Config),
    RequestTimeout = maps:get(request_timeout, Config),
    MaxFrame = maps:get(maximum_frame_bytes, Config),
    MaxPipeReq = maps:get(maximum_pipeline_requests, Config),
    MaxPipeBytes = maps:get(maximum_pipeline_bytes, Config),
    HostOk = (is_binary(Host) andalso Host =/= <<>>) orelse (is_list(Host) andalso Host =/= ""),
    case HostOk
        andalso is_integer(Port)
        andalso Port > 0
        andalso Port =< 65535
        andalso ConnectTimeout > 0
        andalso RequestTimeout > 0
        andalso MaxFrame >= glyphastore_protocol:response_header_bytes()
        andalso MaxFrame =< glyphastore_protocol:max_frame_bytes()
        andalso MaxPipeReq > 0
        andalso MaxPipeBytes >= 40
    of
        true ->
            ok;
        false ->
            {error, glyphastore_error:invalid_argument(<<"client configuration is outside protocol limits">>)}
    end.
