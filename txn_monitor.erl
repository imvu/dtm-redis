-module(txn_monitor).
-export([start/0, allocate/1, finalized/1]).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-record(allocate, {from}).
-record(persist, {from, id, buckets}).
-record(txn_id, {monitor, id}).
-record(finalized, {id, bucket}).

-record(state, {next_id=1, transactions}).
-record(transaction, {session, buckets}).

start() ->
    io:format("starting transaction monitor with pid ~p~n", [self()]),
    loop(#state{transactions=dict:new()}).

loop(State) ->
    receive
        #allocate{from=From} ->
            {Id, NewState} = handle_allocate(State),
            From ! {self(), #txn_id{monitor=self(), id=Id}},
            loop(NewState);
        #persist{}=Persist ->
            NewState = handle_persist(Persist, State),
            loop(NewState);
        #finalized{}=Finalize ->
            NewState = handle_finalize(Finalize, State),
            loop(NewState);
        Any ->
            io:format("transaction monitor received message ~p~n", [Any]),
            loop(State)
    end.

handle_allocate(#state{}=State) ->
    Id = State#state.next_id,
    {Id, State#state{next_id=Id+1}}.

handle_allocate_test() ->
    State = {42, #state{next_id=43, transactions=dict:new()}},
    State = handle_allocate(#state{next_id=42, transactions=dict:new()}).

handle_persist(#persist{from=From, id=Id, buckets=Buckets}, #state{}=State) ->
    true = sets:is_set(Buckets),
    % TODO: write the transaction information to disk
    State#state{transactions=update_transaction(Id, #transaction{session=From, buckets=Buckets}, State#state.transactions)}.

handle_persist_test() ->
    Transactions = dict:store(3, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new()),
    #state{next_id=42, transactions=Transactions} = handle_persist(#persist{from=foo, id=3, buckets=sets:add_element(1234, sets:new())}, #state{next_id=42, transactions=dict:new()}).

handle_finalize(#finalized{id=Id, bucket=Bucket}, #state{}=State) ->
    Transaction = dict:fetch(Id, State#state.transactions),
    Buckets = sets:del_element(Bucket, Transaction#transaction.buckets),
    State#state{transactions=update_transaction(Id, Transaction#transaction{buckets=Buckets}, State#state.transactions)}.

handle_finalize_test() ->
    Result = #state{transactions=dict:store(42, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new())},
    Result = handle_finalize(#finalized{id=42, bucket=4321}, #state{transactions=dict:store(42, #transaction{session=foo, buckets=sets:add_element(1234, sets:add_element(4321, sets:new()))}, dict:new())}).

update_transaction(Id, #transaction{buckets=Buckets}=Transaction, Transactions) ->
    case sets:size(Buckets) of
        % TODO: dispose of the transaction from disk
        0 ->
            dict:erase(Id, Transactions);
        _Any -> dict:store(Id, Transaction, Transactions)
    end.

allocate(Monitors) ->
    Monitor = lists:nth(random:uniform(length(Monitors)), Monitors),
    Monitor ! #allocate{from=self()},
    receive
        {Monitor, #txn_id{}=Id} -> Id
    end.

persist(#txn_id{}=Id, Buckets) ->
    Monitor = Id#txn_id.monitor,
    Monitor ! #persist{from=self(), id=Id, buckets=Buckets},
    receive
        {Monitor, ok} -> ok
    end.

finalized(#txn_id{}=Id) ->
    Id#txn_id.monitor ! #finalized{id=Id, bucket=self()}.

