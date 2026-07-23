-module(glyphastore_conn).
-behaviour(gen_server).

-export([start_link/2, exchange/3, send/3, receive_response/2, reset/1, dial/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {
    worker :: non_neg_integer(),
    config :: glyphastore_client:config(),
    socket :: term() | undefined,
    use_ssl = false :: boolean(),
    input = <<>> :: binary()
}).

start_link(Worker, Config) ->
    gen_server:start_link(?MODULE, {Worker, Config}, []).

exchange(Pid, Frame, Deadline) ->
    gen_server:call(Pid, {exchange, Frame, Deadline}, infinity).

send(Pid, Frame, Deadline) ->
    gen_server:call(Pid, {send, Frame, Deadline}, infinity).

receive_response(Pid, Deadline) ->
    gen_server:call(Pid, {receive_response, Deadline}, infinity).

reset(Pid) ->
    gen_server:cast(Pid, reset).

dial(Pid) ->
    gen_server:call(Pid, dial, infinity).

init({Worker, Config}) ->
    {ok, #state{worker = Worker, config = Config}}.

handle_call(dial, _From, State) ->
    case do_dial(State) of
        {ok, State1} ->
            {reply, ok, State1};
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call(connected, _From, #state{socket = undefined} = State) ->
    {reply, false, State};
handle_call(connected, _From, State) ->
    {reply, true, State};
handle_call({exchange, Frame, Deadline}, _From, State) ->
    case send_frame(State, Frame, Deadline) of
        {ok, State1} ->
            case receive_one(State1, Deadline) of
                {ok, Response, State2} ->
                    {reply, {ok, Response}, State2};
                {error, Reason, State2} ->
                    {reply, {error, Reason}, State2}
            end;
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call({send, Frame, Deadline}, _From, State) ->
    case send_frame(State, Frame, Deadline) of
        {ok, State1} ->
            {reply, ok, State1};
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call({receive_response, Deadline}, _From, State) ->
    case receive_one(State, Deadline) of
        {ok, Response, State1} ->
            {reply, {ok, Response}, State1};
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, glyphastore_error:internal(<<"unexpected call">>)}, State}.

handle_cast(reset, State) ->
    {noreply, reset_state(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_socket(State),
    ok.

do_dial(State) ->
    Config = State#state.config,
    Host = maps:get(host, Config),
    Port = maps:get(port, Config),
    TimeoutMs = glyphastore_util:timeout_ms(maps:get(connect_timeout, Config)),
    TLS = maps:get(tls, Config, #{enable => false}),
    case maps:get(enable, TLS, false) of
        true ->
            dial_tls(Host, Port, TimeoutMs, TLS, State);
        false ->
            dial_tcp(Host, Port, TimeoutMs, State)
    end.

dial_tcp(Host, Port, TimeoutMs, State) ->
    Opts = [binary, {active, false}, {packet, 0}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, Opts, TimeoutMs) of
        {ok, Socket} ->
            {ok, State#state{socket = Socket, use_ssl = false}};
        {error, Reason} ->
            Msg = iolist_to_binary([
                <<"could not connect to GlyphaStore: ">>,
                atom_to_binary(Reason, utf8)
            ]),
            {error, glyphastore_error:unavailable(Msg), State}
    end.

dial_tls(Host, Port, TimeoutMs, TLS, State) ->
    case code:ensure_loaded(ssl) of
        {module, ssl} ->
            case application:ensure_all_started(ssl) of
                {ok, _} ->
                    do_dial_tls(Host, Port, TimeoutMs, TLS, State);
                _ ->
                    {error,
                        glyphastore_error:unavailable(<<"TLS requires the ssl application">>),
                        State}
            end;
        _ ->
            {error, glyphastore_error:unavailable(<<"TLS requires the ssl application">>), State}
    end.

do_dial_tls(Host, Port, TimeoutMs, TLS, State) ->
    Opts = [binary, {active, false}, {packet, 0}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, Opts, TimeoutMs) of
        {ok, Tcp} ->
            case glyphastore_client:build_tls_options(TLS, Host) of
                {ok, SSLOpts} ->
                    case ssl:connect(Tcp, SSLOpts, TimeoutMs) of
                        {ok, Socket} ->
                            {ok, State#state{socket = Socket, use_ssl = true}};
                        {error, Reason} ->
                            gen_tcp:close(Tcp),
                            {error,
                                glyphastore_error:unavailable(
                                    iolist_to_binary([
                                        <<"TLS handshake failed: ">>,
                                        atom_to_binary(Reason, utf8)
                                    ])
                                ),
                                State}
                    end;
                {error, Err} ->
                    gen_tcp:close(Tcp),
                    {error, Err, State}
            end;
        {error, Reason} ->
            Msg = iolist_to_binary([
                <<"could not connect to GlyphaStore: ">>,
                atom_to_binary(Reason, utf8)
            ]),
            {error, glyphastore_error:unavailable(Msg), State}
    end.

send_frame(#state{socket = undefined} = State, _Frame, _Deadline) ->
    {error, glyphastore_error:transport(<<"socket is not connected">>), State};
send_frame(State, Frame, Deadline) ->
    send_loop(State, Frame, 0, byte_size(Frame), Deadline).

send_loop(State, Frame, Sent, Total, Deadline) ->
    case glyphastore_util:remaining_timeout(Deadline) of
        {ok, Left} ->
            TimeoutMs = glyphastore_util:timeout_ms(Left),
            case send_chunk(State, Frame, Sent, Total, TimeoutMs) of
                {ok, Sent1} when Sent1 >= Total ->
                    {ok, State};
                {ok, Sent1} ->
                    send_loop(State, Frame, Sent1, Total, Deadline);
                {error, closed, Sent1} ->
                    {error, send_failure(Sent1, transport(<<"socket closed during send">>)), reset_state(State)};
                {error, Reason, Sent1} ->
                    {error, send_failure(Sent1, Reason), reset_state(State)}
            end;
        {error, Err} ->
            {error, send_failure(0, Err), State}
    end.

send_chunk(#state{socket = Socket, use_ssl = true}, Frame, Sent, Total, TimeoutMs) ->
    case ssl:send(Socket, binary_part(Frame, Sent, Total - Sent), TimeoutMs) of
        ok -> {ok, Total};
        {error, closed} -> {error, closed, Sent};
        {error, timeout} -> {error, transport(<<"request deadline expired">>), Sent};
        {error, Reason} ->
            {error, transport(iolist_to_binary([<<"request send failed: ">>, atom_to_binary(Reason, utf8)])), Sent}
    end;
send_chunk(#state{socket = Socket, use_ssl = false}, Frame, Sent, Total, TimeoutMs) ->
    case gen_tcp:send(Socket, binary_part(Frame, Sent, Total - Sent)) of
        ok -> {ok, Total};
        {error, closed} -> {error, closed, Sent};
        {error, timeout} -> {error, transport(<<"request deadline expired">>), Sent};
        {error, Reason} ->
            {error, transport(iolist_to_binary([<<"request send failed: ">>, atom_to_binary(Reason, utf8)])), Sent}
    end.

receive_one(State, Deadline) ->
    read_response(State, Deadline).

read_response(State, Deadline) ->
    Input = State#state.input,
    MaxFrame = maps:get(maximum_frame_bytes, State#state.config),
    case frame_ready(Input, MaxFrame) of
        {ready, Frame, Rest} ->
            case glyphastore_protocol:decode_response(Frame, MaxFrame) of
                {ok, Response} ->
                    {ok, Response, State#state{input = Rest}};
                {error, {invalid_argument, Msg}} ->
                    {error, glyphastore_error:protocol(Msg), State}
            end;
        {need, _} ->
            recv_more(State, Deadline);
        {invalid, _} ->
            {error, glyphastore_error:protocol(<<"server response size is outside client limits">>), State}
    end.

frame_ready(Input, MaxFrame) when byte_size(Input) >= 4 ->
    <<Size:32/little, _/binary>> = Input,
    Min = glyphastore_protocol:response_header_bytes(),
    case Size >= Min andalso Size =< MaxFrame of
        true ->
            case byte_size(Input) >= Size of
                true ->
                    <<Frame:Size/binary, Rest/binary>> = Input,
                    {ready, Frame, Rest};
                false ->
                    {need, Size - byte_size(Input)}
            end;
        false ->
            {invalid, Size}
    end;
frame_ready(Input, _MaxFrame) ->
    {need, 4 - byte_size(Input)}.

recv_more(State, Deadline) ->
    case glyphastore_util:remaining_timeout(Deadline) of
        {ok, Left} ->
            TimeoutMs = glyphastore_util:timeout_ms(Left),
            Recv =
                case State#state.use_ssl of
                    true -> ssl:recv(State#state.socket, 0, TimeoutMs);
                    false -> gen_tcp:recv(State#state.socket, 0, TimeoutMs)
                end,
            case Recv of
                {ok, Chunk} ->
                    read_response(
                        State#state{input = <<(State#state.input)/binary, Chunk/binary>>},
                        Deadline
                    );
                {error, closed} ->
                    {error, glyphastore_error:transport(<<"server closed the connection">>), reset_state(State)};
                {error, timeout} ->
                    {error, glyphastore_error:transport(<<"request deadline expired">>), State};
                {error, Reason} ->
                    Msg = iolist_to_binary([
                        <<"response receive failed: ">>,
                        atom_to_binary(Reason, utf8)
                    ]),
                    {error, glyphastore_error:transport(Msg), State}
            end;
        {error, Err} ->
            {error, Err, State}
    end.

send_failure(BytesSent, Err) ->
    #{send_failure => true, bytes_sent => BytesSent, error => Err}.

transport(Msg) ->
    glyphastore_error:transport(Msg).

reset_state(State) ->
    close_socket(State),
    State#state{socket = undefined, input = <<>>}.

close_socket(#state{socket = undefined}) ->
    ok;
close_socket(#state{socket = Socket, use_ssl = true}) ->
    catch ssl:close(Socket);
close_socket(#state{socket = Socket, use_ssl = false}) ->
    catch gen_tcp:close(Socket).
