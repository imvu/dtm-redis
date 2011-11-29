-module(txn_monitor).
-export([start/0, allocate/1, finalized/1]).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

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
            {Id, NewState} = handle_allocate(Allocate, State),
            From ! {self(), #txn_id{monitor=self(), id=Id}},
            loop(NewState);
        #finalized{}=Finalize ->
            NewState = handle_finalize(Finalize, State),
            loop(NewState);
        Any ->
            io:format("transaction monitor received message ~p~n", [Any]),
            loop(State)
    end.

handle_allocate(#allocate{buckets=Buckets}, #state{}=State) ->
    true = sets:is_set(Buckets),
    Id = State#state.next_id,
    {Id, State#state{next_id=Id+1, transactions=update_transaction(Id, Buckets, State#state.transactions)}}.

handle_allocate_test() ->
    Transactions = dict:store(42, sets:add_element(1234, sets:new()), dict:new()),
    {42, #state{next_id=43, transactions=Transactions}} = handle_allocate(#allocate{buckets=sets:add_element(1234, sets:new())}, #state{next_id=42, transactions=dict:new()}).

handle_finalize(#finalized{id=Id, bucket=Bucket}, #state{}=State) ->
    Buckets = sets:del_element(Bucket, dict:fetch(Id, State#state.transactions)),
    State#state{transactions=update_transaction(Id, Buckets, State#state.transactions)}.

handle_finalize_test() ->
    Result = #state{transactions=dict:store(42, sets:add_element(1234, sets:new()), dict:new())},
    Result = handle_finalize(#finalized{id=42, bucket=4321}, #state{transactions=dict:store(42, sets:add_element(1234, sets:add_element(4321, sets:new())), dict:new())}).

update_transaction(Id, Buckets, Transactions) ->
    case sets:size(Buckets) of
        % TODO: dispose of the transaction from disk
        0 ->
            dict:erase(Id, Transactions);
        _Any -> dict:store(Id, Buckets, Transactions)
    end.

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

