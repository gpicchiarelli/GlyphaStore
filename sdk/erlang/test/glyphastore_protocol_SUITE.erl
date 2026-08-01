-module(glyphastore_protocol_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
        request_encoder_matches_fixtures,
        request_decoder_round_trips,
        response_round_trips,
        worker_routing,
        keyed_routing_and_init_identity,
        rejects_noncanonical_reserved
    ].

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
        enc(glyphastore_protocol:opcode_stats(), 9, <<>>, <<>>, 0, glyphastore_protocol:no_worker()),
        enc(glyphastore_protocol:opcode_backup(), 10, <<"/tmp/glyphastore-backup">>, <<>>, 0,
            glyphastore_protocol:no_worker())
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

%% Paper vectors from Aumasson & Bernstein, SipHash: a fast short-input PRF.
keyed_routing_and_init_identity(_Config) ->
    K0 = 16#0706050403020100,
    K1 = 16#0f0e0d0c0b0a0908,
    true = glyphastore_protocol:siphash24(<<>>, K0, K1) =:= 16#726fdb47dd0e0e31,
    true = glyphastore_protocol:siphash24(<<0, 1, 2>>, K0, K1) =:= 16#85676696d7fb7e2d,
    Keyed = #{
        algorithm => glyphastore_protocol:routing_alg_siphash24_v1(),
        seed => 16#1111222233334444
    },
    {ok, Digest} = glyphastore_protocol:hash_key_routing(<<"tenant-a/orders/1">>, Keyed),
    true = Digest =:= 16#712fcec57ac84546,
    {ok, 6} = glyphastore_protocol:worker_for(<<"tenant-a/orders/1">>, 8, Keyed),
    {ok, Fnv} = glyphastore_protocol:hash_key_routing(<<"tenant-a/orders/1">>, glyphastore_protocol:default_routing()),
    true = Digest =/= Fnv,
    {ok, Plain} = glyphastore_protocol:encode_init_identity(glyphastore_protocol:default_routing()),
    true = Plain =:= glyphastore_protocol:identity(),
    {ok, DecodedPlain} = glyphastore_protocol:decode_init_identity(Plain),
    true = DecodedPlain =:= glyphastore_protocol:default_routing(),
    ExtendedSeed = 16#ABCDEF0123456789,
    {ok, Extended} = glyphastore_protocol:encode_init_identity(#{
        algorithm => glyphastore_protocol:routing_alg_siphash24_v1(),
        seed => ExtendedSeed
    }),
    true = byte_size(Extended) =:= 26,
    {ok, Decoded} = glyphastore_protocol:decode_init_identity(Extended),
    true = Decoded =:= #{
        algorithm => glyphastore_protocol:routing_alg_siphash24_v1(),
        seed => ExtendedSeed
    },
    {error, _} = glyphastore_protocol:decode_init_identity(<<"GlyphaStore/2", 0, "bad">>),
    {error, _} = glyphastore_protocol:decode_init_identity(<<"GlyphaStore/3">>).

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

fixture(_Config, Name) ->
    SuiteDir = filename:dirname(?FILE),
    Path = filename:join([SuiteDir, "fixtures", Name]),
    {ok, Bin} = file:read_file(Path),
    parse_hex(Bin).

parse_hex(Bin) ->
    Tokens = [X || X <- string:tokens(binary_to_list(Bin), "\n\r\t "), X =/= ""],
    binary:decode_hex(list_to_binary(lists:concat(Tokens))).

frames(Corpus) ->
    frames(Corpus, []).

frames(<<>>, Acc) ->
    lists:reverse(Acc);
frames(Corpus, Acc) ->
    <<Size:32/little, _/binary>> = Corpus,
    <<Frame:Size/binary, Rest/binary>> = Corpus,
    frames(Rest, [Frame | Acc]).
