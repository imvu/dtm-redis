-module(txn_monitor).
-export([start/0]).
-compile(export_all).

-include("protocol.hrl").

-record(state, {next_id=1}).

start() ->
    io:format("starting transaction monitor with pid ~p~n", [self()]),
    loop(#state{}).

loop(State) ->
    receive
        #allocate_txn{from=From} ->
            Id = State#state.next_id,
            From ! #txn_id{monitor=self(), id=Id};
        Any ->
            io:format("transaction monitor received message ~p~n", [Any]),
            loop(State)
    end.

