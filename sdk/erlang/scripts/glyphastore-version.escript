#!/usr/bin/env escript
%%! -noshell -noinput
-mode(compile).

main([]) ->
    io:format("~s", [glyphastore_version:version()]),
    halt(0);
main(_) ->
    halt(1).
