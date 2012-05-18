%% Copyright (C) 2011-2012 IMVU Inc.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy of
%% this software and associated documentation files (the "Software"), to deal in
%% the Software without restriction, including without limitation the rights to
%% use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
%% of the Software, and to permit persons to whom the Software is furnished to do
%% so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.

-module(txn_monitor).
-behavior(gen_server).
-export([start_link/1, allocate/1, persist/2, finalized/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("dtm_redis.hrl").

-record(persist, {id, buckets}).
-record(txn_id, {monitor, id}).
-record(finalized, {id, bucket}).

-record(state, {next_id=1, transactions, binlog_state}).
-record(transaction, {session, buckets}).

% API methods

start_link(#monitor{}=Monitor) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Monitor, []).

allocate(Monitors) ->
    Monitor = lists:nth(random:uniform(length(Monitors)), Monitors),
    gen_server:call(Monitor, allocate).

persist(#txn_id{}=Id, Buckets) ->
    Monitor = Id#txn_id.monitor,
    gen_server:call(Monitor, #persist{id=Id, buckets=Buckets}).

finalized(#txn_id{}=Id) ->
    gen_server:cast(Id#txn_id.monitor, #finalized{id=Id, bucket=self()}).

% gen_server callbacks

init(#monitor{binlog=Filename}) ->
    error_logger:info_msg("starting txn_monitor with pid ~p", [self()]),
    {ok, #state{transactions=dict:new(), binlog_state=binlog:init(Filename)}}.

handle_call(allocate, _From, State) ->
    {Id, NewState} = handle_allocate(State),
    {reply, #txn_id{monitor=self(), id=Id}, NewState};
handle_call(#persist{}=Persist, From, State) ->
    NewState = handle_persist(Persist, From, State),
    {noreply, NewState};
handle_call(Message, From, _State) ->
    error_logger:error_msg("txn_monitor:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(#finalized{}=Finalize, State) ->
    NewState = handle_finalize(Finalize, State),
    {noreply, NewState};
handle_cast(Message, _State) ->
    error_logger:error_msg("txn_monitor:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info({binlog_data_written, {persist, Id}}, State) ->
    {ok, Value} = dict:find(Id, State#state.transactions),
    gen_server:reply(Value#transaction.session, ok),
    {noreply, State};
handle_info({binlog_data_written, {delete, _Id}}, State) ->
    {noreply, State};
handle_info(Message, _State) ->
    error_logger:error_msg("txn_monitor:handle_info unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

terminate(Reason, _State) ->
    error_logger:info_msg("terminating txn_monitor because ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

handle_allocate(#state{}=State) ->
    Id = State#state.next_id,
    {Id, State#state{next_id=Id+1}}.

handle_persist(#persist{id=Id, buckets=Buckets}, From, #state{binlog_state=BinLogState}=State) ->
    true = sets:is_set(Buckets),
    binlog:write(BinLogState, {persist, Id}, 'Write TXN_LOG'),
    State#state{transactions=update_transaction(Id, #transaction{session=From, buckets=Buckets}, State#state.transactions, BinLogState)}.

handle_finalize(#finalized{id=Id, bucket=Bucket}, #state{}=State) ->
    Transaction = dict:fetch(Id, State#state.transactions),
    Buckets = sets:del_element(Bucket, Transaction#transaction.buckets),
    State#state{transactions=update_transaction(Id, Transaction#transaction{buckets=Buckets}, State#state.transactions, State#state.binlog_state)}.

update_transaction(Id, #transaction{buckets=Buckets}=Transaction, Transactions, BinLogState) ->
    case sets:size(Buckets) of
        0 ->
	    binlog:write(BinLogState, {delete, Id}, 'Delete TXN_LOG'),
            dict:erase(Id, Transactions);
        _Any -> dict:store(Id, Transaction, Transactions)
    end.

% tests

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

handle_allocate_test() ->
    State = {42, #state{next_id=43, transactions=dict:new()}},
    State = handle_allocate(#state{next_id=42, transactions=dict:new()}).

handle_persist_test() ->
    erlymock:start(),
    Id = 3,
    BinLogState = 'foo',
    erlymock:strict(binlog, write, [BinLogState, {persist, Id}, 'Write TXN_LOG'], [{return, ok}]),
    erlymock:replay(),
    Transactions = dict:store(3, #transaction{session=foo, buckets=sets:add_element(1234, sets:new())}, dict:new()),
    #state{next_id=42, transactions=Transactions} = handle_persist(#persist{id=Id, buckets=sets:add_element(1234, sets:new())}, foo, #state{next_id=42, transactions=dict:new(), binlog_state=BinLogState}),
    erlymock:verify().

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

-endif.
