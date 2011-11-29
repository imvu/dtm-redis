-module(txn_monitor).
-export([start/0, allocate/1, finalized/1]).
-compile(export_all).

-record(allocate, {from, buckets}).
-record(txn_id, {monitor, id}).
-record(finalized, {id, bucket}).

-record(state, {next_id=1, transactions}).

start() ->
    io:format("starting transaction monitor with pid ~p~n", [self()]),
    loop(#state{transactions=dict:new()}).

loop(State) ->
    receive
        #allocate{from=From}=Allocate ->
            {Id, Transactions} = handle_allocate(Allocate, State),
            From ! {self(), #txn_id{monitor=self(), id=Id}},
            loop(State#state{next_id=Id+1, transactions=Transactions});
        #finalized{}=Finalize ->
            loop(handle_finalize(Finalize, State));
        Any ->
            io:format("transaction monitor received message ~p~n", [Any]),
            loop(State)
    end.

handle_allocate(#allocate{buckets=Buckets}, #state{}=State) ->
    % TODO: Verify that Buckets is a non-empty set
    Id = State#state.next_id,
    {Id, dict:store(Id, Buckets, State#state.transactions)}.

handle_finalize(#finalized{id=Id, bucket=Bucket}, State) ->
    Buckets = sets:erase(Bucket, dict:fetch(Id, State#state.transactions)),
    % TODO: Check for an empty buckets set and dispose of the transaction when empty
    State#state{transactions=dict:store(Id, Buckets)}.

allocate(Monitors) ->
    Monitor = lists:nth(random:uniform(length(Monitors)), Monitors),
    Monitor ! #allocate{from=self()},
    receive
        #txn_id{}=Id ->
            Id;
        Any ->
            io:format("received unexpected message ~p~n", [Any])
    end.

finalized(#txn_id{}=Id) ->
    Id#txn_id.monitor ! #finalized{id=Id, bucket=self()}.

