%% Copyright (C) 2011-2013 IMVU Inc.
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

-module(bucket).
-behavior(gen_server).
-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("dtm_redis.hrl").
-include("protocol.hrl").
-include("operation.hrl").
-include("data_types.hrl").
-include("store.hrl").

-record(state, {name=none, binlog, transactions, store}).
-record(transaction, {session, operations=[], deferred=[], watches=[], locked=false}).

-record(single_operation, {session}).
-record(transaction_operation, {txn_id}).

% API methods

start_link(Name, Binlog, #bucket{}=Bucket) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Binlog, Bucket], []).

% gen_server callbacks

init([Name, Binlog, #bucket{store_host=Host, store_port=Port}]) ->
    error_logger:info_msg("starting storage bucket with pid ~p and storage ~p:~p", [self(), Host, Port]),
    {ok, #state{name=Name, binlog=Binlog, transactions=dict:new(), store=redis_store:connect(Host, Port)}}.

handle_call(#command{}=Command, From, State) ->
    {noreply, handle_command(Command, From, State)};
handle_call(#transact{}=Transact, From, State) ->
    {reply, stored, handle_transact(Transact, From, State)};
handle_call(#watch{}=Watch, From, State) ->
    {reply, ok, handle_watch(Watch, From, State)};
handle_call(Message, From, _State) ->
    error_logger:error_msg("bucket:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(#unwatch{}=Unwatch, State) ->
    {noreply, handle_unwatch(Unwatch, State)};
handle_cast(#lock_transaction{}=Transaction, State) ->
    {noreply, handle_lock_transaction(Transaction, State)};
handle_cast(#rollback_transaction{}=Transaction, State) ->
    {noreply, handle_rollback_transaction(Transaction, State)};
handle_cast(#commit_transaction{}=Transaction, State) ->
    {noreply, handle_commit_transaction(#commit_transaction{}=Transaction, State)};
handle_cast(Message, _State) ->
    error_logger:error_msg("bucket:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info({binlog_data_written, {lock_txn, Id}}, State) ->
    {ok, Transaction} = dict:find(Id, State#state.transactions),
    Transaction#transaction.session ! #transaction_locked{bucket=bucket_id(State), status=Transaction#transaction.locked},
    {noreply, State};
handle_info({binlog_data_written, {delete, _Id}}, State) ->
    {noreply, State};
handle_info(Message, #state{store=Store}=State) ->
    {Results, NewStore} = redis_store:handle_info(Message, Store),
    {noreply, lists:foldl(fun(#store_result{id=Id, result=Result}, S) ->
            handle_store_result(Id, Result, S)
        end, State#state{store=NewStore}, Results)}.

terminate(Reason, _State) ->
    error_logger:info_msg("terminating bucket with reason ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

bucket_id(#state{name=none}) ->
    self();
bucket_id(#state{name=Name}) ->
    Name.

handle_command(#command{session=Session, operation=Operation}, From, #state{transactions=Transactions, store=Store}=State) ->
    Current = get(operation:key(Operation)),
    case key_meta:is_locked(Current, Session) of
        true ->
            #key{locked=Locked} = Current,
            #transaction{deferred=Deferred}=Transaction = dict:fetch(Locked, Transactions),
            NewTransaction = Transaction#transaction{deferred=[{From, Operation}|Deferred]},
            State#state{transactions=dict:store(Locked, NewTransaction, Transactions)};
        false ->
            State#state{store=handle_operation(Store, From, Operation, Current)}
    end.

update_meta(Key, Current) ->
    put(Key, key_meta:update(Current)).

delete_meta(Key, Current) ->
    case key_meta:is_watched(Current) of
        true -> put(Key, key_meta:update(Current));
        false -> erase(Key)
    end.

handle_batch_operation(Store, #get{key=Key}) ->
    redis_store:get(Store, Key);
handle_batch_operation(Store, #set{key=Key, value=Value}) ->
    update_meta(Key, get(Key)),
    redis_store:set(Store, Key, Value);
handle_batch_operation(Store, #delete{key=Key}) ->
    redis_store:delete(Store, Key).

handle_operation(Store, From, #get{key=Key}, _Current) ->
    redis_store:get(#single_operation{session=From}, Store, Key);
handle_operation(Store, From, #set{key=Key, value=Value}, Current) ->
    update_meta(Key, Current),
    redis_store:set(#single_operation{session=From}, Store, Key, Value);
handle_operation(Store, From, #delete{key=Key}, Current) ->
    delete_meta(Key, Current),
    redis_store:delete(#single_operation{session=From}, Store, Key).

add_operation(error, Session, Operation) ->
    add_operation({ok, #transaction{session=Session}}, Session, Operation);
add_operation({ok, #transaction{}=Transaction}, Session, Operation) ->
    Session = Transaction#transaction.session,
    Transaction#transaction{operations=[Operation|Transaction#transaction.operations]}.

handle_transact(#transact{txn_id=TransactionId, session=Session, operation_id=Id, operation=Operation}, _From, #state{transactions=Transactions}=State) ->
    NewTransaction = add_operation(dict:find(TransactionId, Transactions), Session, {Id, Operation}),
    State#state{transactions=dict:store(TransactionId, NewTransaction, Transactions)}.

handle_watch(#watch{txn_id=TransactionId, session=Session, key=Key}, _From, #state{transactions=Transactions}=State) ->
    NewTransaction = add_watch(dict:find(TransactionId, Transactions), Session, Key),
    State#state{transactions=dict:store(TransactionId, NewTransaction, Transactions)}.

add_watch(error, Session, Key) ->
    add_watch({ok, #transaction{session=Session}}, Session, Key);
add_watch({ok, #transaction{watches=Watches}=Transaction}, Session, Key) ->
    Session = Transaction#transaction.session,
    Current = get(Key),
    put(Key, key_meta:watch(Current)),
    Transaction#transaction{watches=[{Key, key_meta:version(Current)}|Watches]}.

handle_unwatch(#unwatch{txn_id=TransactionId, session=Session}, #state{transactions=Transactions}=State) ->
    NewState = State#state{transactions=remove_watch(Transactions, TransactionId, dict:find(TransactionId, Transactions))},
    Session ! {bucket_id(State), ok},
    NewState.

remove_watch(Transactions, _TransactionId, error) ->
    Transactions;
remove_watch(Transactions, TransactionId, {ok, Transaction}) ->
    remove_watches(Transaction#transaction.watches),
    dict:store(TransactionId, Transaction#transaction{watches=[]}, Transactions).

remove_watches([]) ->
    ok;
remove_watches([{Key, _Version}|Remainder]) ->
    NewValue = key_meta:unwatch(get(Key)),
    case key_meta:is_watched(NewValue) of
        true -> put(Key, NewValue);
        false -> erase(Key)
    end,
    remove_watches(Remainder).

handle_lock_transaction(#lock_transaction{txn_id=TransactionId}, #state{binlog=Binlog, transactions=Transactions, store=Store}=State) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    WatchStatus = check_watches(Transaction#transaction.watches),
    LockStatus = lock_keys(WatchStatus, Transaction#transaction.operations, TransactionId),
    {NewTransaction, NewStore} = case LockStatus of
        ok ->
            binlog:write(Binlog, {lock_txn, TransactionId}, Transaction),
            {Transaction#transaction{locked=true}, Store};
        error ->
            Transaction#transaction.session ! #transaction_locked{bucket=bucket_id(State), status=LockStatus},
            unlock_transaction(Store, Transaction#transaction{locked=false})
    end,
    State#state{transactions=dict:store(TransactionId, NewTransaction, State#state.transactions), store=NewStore}.

check_watches([]) ->
    ok;
check_watches([{Key, Version}|Remaining]) ->
    case key_meta:version(get(Key)) =:= Version of
        true -> check_watches(Remaining);
        false -> error
    end.

lock_keys(error, _, _) ->
    error;
lock_keys(ok, [], _TransactionId) ->
    ok;
lock_keys(ok, [{_Id, Operation}|Remainder], TransactionId) ->
    Key = operation:key(Operation),
    Current = get(Key),
    case key_meta:is_locked(Current, TransactionId) of
        true -> error;
        false ->
            put(Key, key_meta:lock(Current, TransactionId)),
            case lock_keys(ok, Remainder, TransactionId) of
                ok -> ok;
                error -> put(Key, Current), error
            end
    end.

handle_rollback_transaction(#rollback_transaction{txn_id=TransactionId}, #state{binlog=Binlog, transactions=Transactions, store=Store}=State) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    {_, NewStore} = unlock_transaction(Store, Transaction),
    remove_watches(Transaction#transaction.watches),
    binlog:write(Binlog, {delete, TransactionId}, "Bucket rollback transaction"),
    State#state{transactions=dict:erase(TransactionId, Transactions), store=NewStore}.

handle_commit_transaction(#commit_transaction{txn_id=TransactionId}, #state{transactions=Transactions, store=Store}=State) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    NewStore = apply_transaction(Store, Transaction, TransactionId),
    State#state{store=NewStore}.

apply_transaction(Store, #transaction{operations=Operations}=Transaction, TransactionId) ->
    Pipe = redis_store:pipeline(Store),
    Pipe2 = lists:foldr(fun({_Order, O}, P) -> handle_batch_operation(P, O) end, Pipe, Operations),
    NewStore = redis_store:commit(#transaction_operation{txn_id=TransactionId}, Pipe2),
    {_Transaction, NewNewStore} = unlock_transaction(NewStore, Transaction),
    NewNewStore.

unlock_transaction(Store, #transaction{deferred=Deferred, locked=false}=Transaction) ->
    NewStore = lists:foldl(fun({Session, Operation}, S) ->
            handle_operation(S, Session, Operation, get(operation:key(Operation)))
        end, Store, lists:reverse(Deferred)),
    {Transaction#transaction{deferred=[]}, NewStore};
unlock_transaction(Store, #transaction{operations=Operations}=Transaction) ->
    unlock_keys(Operations),
    unlock_transaction(Store, Transaction#transaction{locked=false}).

unlock_keys([]) ->
    ok;
unlock_keys([{_Id, Operation}|Remainder]) ->
    Key = operation:key(Operation),
    put(Key, key_meta:unlock(get(Key))),
    unlock_keys(Remainder).

map_result(ok) ->
    ok;
map_result(undefined) ->
    undefined;
map_result(error) ->
    error;
map_result(Result) when is_integer(Result) ->
    Result;
map_result(Result) ->
    {ok, Result}.

map_transaction_results(_Results, _Operations, error) ->
    error;
map_transaction_results([], [], Results) ->
    Results;
map_transaction_results([Result|ResultsTail], [{Order, _Operation}|OperationsTail], Results) ->
    map_transaction_results(ResultsTail, OperationsTail, [{Order, Result}|Results]).

handle_store_result(#single_operation{session=From}, Result, State) ->
    gen_server:reply(From, map_result(Result)),
    State;
handle_store_result(#transaction_operation{txn_id=TransactionId}, Results, #state{binlog=Binlog, transactions=Transactions}=State) ->
    {ok, Transaction} = dict:find(TransactionId, Transactions),
    Transaction#transaction.session ! {bucket_id(State), map_transaction_results(Results, lists:reverse(Transaction#transaction.operations), [])},
    remove_watches(Transaction#transaction.watches),
    txn_monitor:finalized(TransactionId),
    binlog:write(Binlog, {delete, TransactionId}, "Bucket stored transaction"),
    State#state{transactions=dict:erase(TransactionId, Transactions)}.
