-module(glyphastore_protocol_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [request_encoder_matches_fixtures, request_decoder_round_trips, response_round_trips, worker_routing, rejects_noncanonical_reserved].

request_encoder_matches_fixtures(Config) ->
    Expected = frames(fixture(Config, "wire_requests_v2.hex")),
    Encoded = [
        enc(glyphastore_protocol:opcode_init(), 1, <<>>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_ping(), 2, <<>>, <<"\0ping\377">>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_get(), 3, <<"get\0key">>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_put(), 4, <<"put\0key">>, <<16, 32, 255>>, 123456789, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_erase(), 5, <<"erase-key">>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_bind_worker(), 6, <<>>, <<>>, 0, 2),
        enc(glyphastore_protocol:opcode_health(), 7, <<>>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_ready(), 8, <<>>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_stats(), 9, <<>>, <<>>, 0, glyphastore_protocol:no_worker())
    ],
    true = Expected =:= Encoded.

request_decoder_round_trips(Config) ->
    Expected = frames(fixture(Config, "wire_requests_v2.hex")),
    Reencoded = [
        begin
            {ok, Decoded} = glyphastore_protocol:decode_request(Frame, glyphastore_protocol:max_frame_bytes()),
            {ok, Out} = glyphastore_protocol:encode_request(
                maps:get(opcode, Decoded),
                maps:get(request_id, Decoded),
                maps:get(key, Decoded),
                maps:get(value, Decoded),
                maps:get(expire_at_ns, Decoded),
                maps:get(target_worker, Decoded)
            ),
            Out
        end
        || Frame <- Expected
    ],
    true = Expected =:= Reencoded.

response_round_trips(Config) ->
    Expected = frames(fixture(Config, "wire_responses_v2.hex")),
    Reencoded = [
        begin
            {ok, Decoded} = glyphastore_protocol:decode_response(Frame, glyphastore_protocol:max_frame_bytes()),
            {ok, Out} = glyphastore_protocol:encode_response(
                maps:get(status, Decoded),
                maps:get(request_id, Decoded),
                maps:get(value, Decoded),
                maps:get(owner_worker, Decoded),
                maps:get(worker_count, Decoded),
                maps:get(routing_epoch, Decoded)
            ),
            Out
        end
        || Frame <- Expected
    ],
    true = Expected =:= Reencoded.

worker_routing(_Config) ->
    Key = <<"session\00042">>,
    {ok, Worker} = glyphastore_protocol:worker_for(Key, 4),
    true = Worker =:= glyphastore_protocol:fnv1a64(Key) rem 4,
    {ok, Worker2} = glyphastore_protocol:worker_for(Key, 4),
    true = Worker =:= Worker2.

rejects_noncanonical_reserved(_Config) ->
    {ok, Frame} = glyphastore_protocol:encode_request(
        glyphastore_protocol:opcode_ping(), 1, <<>>, <<"x">>, 0, glyphastore_protocol:no_worker()
    ),
    <<Head:36/binary, _:32/little, Tail/binary>> = Frame,
    Mutated = <<Head/binary, 1:32/little, Tail/binary>>,
    {error, _} = glyphastore_protocol:decode_request(Mutated, glyphastore_protocol:max_frame_bytes()).

enc(Opcode, RequestId, Key, Value, Expire, Target) ->
    {ok, Frame} = glyphastore_protocol:encode_request(Opcode, RequestId, Key, Value, Expire, Target),
    Frame.

fixture(Config, Name) ->
    Dir = ?config(data_dir, Config),
    Path = filename:join([Dir, "fixtures", Name]),
    {ok, Bin} = file:read_file(Path),
    parse_hex(Bin).

parse_hex(Bin) ->
    Tokens = string:tokens(binary_to_list(Bin), "\n\r\t "),
    list_to_binary([integer(X, 16) || X <- Tokens, X =/= ""]).

frames(Corpus) ->
    frames(Corpus, []).

frames(<<>>, Acc) ->
    lists:reverse(Acc);
frames(Corpus, Acc) ->
    <<Size:32/little, _/binary>> = Corpus,
    <<Frame:Size/binary, Rest/binary>> = Corpus,
    frames(Rest, [Frame | Acc]).
