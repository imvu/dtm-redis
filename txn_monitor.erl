-module(txn_monitor).
-export([start/1, allocate/1, finalized/1]).
-compile(export_all).

-include("dtm_redis.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(allocate, {from}).
-record(persist, {from, id, buckets}).
-record(txn_id, {monitor, id}).
-record(finalized, {id, bucket}).
-record(txn_written, {id}).

-record(state, {next_id=1, transactions, binlog_state}).
-record(transaction, {session, buckets}).

start(#monitor{}=_Config) ->
    io:format("starting transaction monitor with pid ~p~n", [self()]),
    BinlogState = binlog:init(pid_to_list(self())),
    loop(#state{transactions=dict:new(), binlog_state = BinlogState}).

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
	{binlog_data_written, {persist, Id}} ->
	    {ok, Value} = dict:find(Id, State#state.transactions),
	    Value#transaction.session ! {self(), #txn_written{id=Id}},
	    loop(State);
	{binlog_data_written, {delete, _Id}} ->
	    loop(State);
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

handle_persist(#persist{from=From, id=Id, buckets=Buckets}, #state{binlog_state=BinLogState}=State) ->
    true = sets:is_set(Buckets),
    binlog:write(BinLogState, {persist, Id}, 'Write TXN_LOG'),
    State#state{transactions=update_transaction(Id, #transaction{session=From, buckets=Buckets}, State#state.transactions, BinLogState)}.

handle_persist_test() ->
    erlymock:start(),
    Id = 3,
    BinLogState = 'foo',
    erlymock:strict(binlog, write, [BinLogState, {persist, Id}, 'Write TXN_LOG'], [{return, ok}]),
    erlymock:replay(),
    Transactions = dict:store(3, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new()),
    #state{next_id=42, transactions=Transactions} = handle_persist(#persist{from=foo, id=Id, buckets=sets:add_element(1234, sets:new())}, #state{next_id=42, transactions=dict:new(), binlog_state=BinLogState}),
    erlymock:verify().

handle_finalize(#finalized{id=Id, bucket=Bucket}, #state{}=State) ->
    Transaction = dict:fetch(Id, State#state.transactions),
    Buckets = sets:del_element(Bucket, Transaction#transaction.buckets),
    State#state{transactions=update_transaction(Id, Transaction#transaction{buckets=Buckets}, State#state.transactions, State#state.binlog_state)}.

handle_finalize_multiple_buckets_test() ->
    erlymock:start(),
    Id = 3,
    BinLogState = 'foo',
    erlymock:stub(binlog, init, [], []),
    erlymock:replay(),
    Result = #state{transactions=dict:store(Id, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new()), binlog_state=BinLogState},
    Result = handle_finalize(#finalized{id=Id, bucket=4321}, #state{transactions=dict:store(Id, #transaction{session=foo, buckets=sets:add_element(1234, sets:add_element(4321, sets:new()))}, dict:new()), binlog_state=BinLogState}),
    erlymock:verify().

handle_finalize_single_buckets_test() ->
    erlymock:start(),
    Id = 3,
    BinLogState = 'foo',
    erlymock:strict(binlog, write, [BinLogState, {delete, Id}, 'Delete TXN_LOG'], [{return, ok}]),
    erlymock:replay(),
    Result = #state{transactions=dict:new(), binlog_state=BinLogState},
    Result = handle_finalize(#finalized{id=Id, bucket=1234}, #state{transactions=dict:store(Id, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new()), binlog_state=BinLogState}),
    erlymock:verify().

update_transaction(Id, #transaction{buckets=Buckets}=Transaction, Transactions, BinLogState) ->
    case sets:size(Buckets) of
        0 ->
	    binlog:write(BinLogState, {delete, Id}, 'Delete TXN_LOG'),
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
        {Monitor, #txn_written{id=Id}} -> ok
    end.

finalized(#txn_id{}=Id) ->
    Id#txn_id.monitor ! #finalized{id=Id, bucket=self()}.
