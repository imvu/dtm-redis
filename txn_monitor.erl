-module(txn_monitor).
-export([start/0]).
-compile(export_all).

start() ->
    io:format("starting transaction monitor with pid ~p~n", [self()]),
    loop().

loop() ->
    receive
        Any ->
            io:format("transaction monitor received message ~p~n", [Any]),
            loop()
    end.

