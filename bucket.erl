-module(bucket).
-export([start/1]).
-compile(export_all).

-include("dtm_redis.hrl").
-include("protocol.hrl").
-include("data_types.hrl").
-include("store.hrl").

-record(state, {transactions, store, binlog_state}).
-record(transaction, {session, operations=[], deferred=[], watches=[], locked=false}).

-record(single_operation, {session}).
-record(transaction_operation, {txn_id}).

start(#bucket{store_host=Host, store_port=Port}) ->
    io:format("starting storage bucket with pid ~p and storage ~p:~p~n", [self(), Host, Port]),
    BinlogState = binlog:init(pid_to_list(self())),
    loop(#state{transactions=dict:new(), store=redis_store:connect(Host, Port), binlog_state=BinlogState}).

loop(State) ->
    receive
        #command{}=Command ->
            loop(handle_command(State, Command));
        #transact{}=Transact ->
            loop(handle_transact(State, Transact));
        #watch{}=Watch ->
            loop(handle_watch(State, Watch));
        #unwatch{}=Unwatch ->
            loop(handle_unwatch(State, Unwatch));
        #lock_transaction{txn_id=TransactionId} ->
            loop(handle_lock_transaction(State, TransactionId));
        #rollback_transaction{txn_id=TransactionId} ->
            loop(handle_rollback_transaction(State, TransactionId));
        #commit_transaction{txn_id=TransactionId} ->
            loop(handle_commit_transaction(State, TransactionId));
        #store_result{id=Id, result=Result} ->
            loop(handle_store_result(State, Id, Result));
	{binlog_data_written, {lock_txn, Id}} ->
	    {ok, Transaction} = dict:find(Id, State#state.transactions),
	    Transaction#transaction.session ! #transaction_locked{bucket=self(), status=Transaction#transaction.locked},
	    loop(State);
	{binlog_data_written, {delete, _Id}} ->
	    loop(State);
        stop ->
            io:format("storage bucket halting after receiving stop message~n");
        Any ->
            io:format("storage bucket process received message ~p~n", [Any]),
            loop(State)
    end.

handle_command(#state{transactions=Transactions, store=Store}=State, #command{session=Session, operation=Operation}) ->
    Current = get(operation_key(Operation)),
    case key_meta:is_locked(Current, Session) of
        true ->
            #key{locked=Locked} = Current,
            #transaction{deferred=Deferred}=Transaction = dict:fetch(Locked, Transactions),
            NewTransaction = Transaction#transaction{deferred=[{Session, Operation}|Deferred]},
            State#state{transactions=dict:store(Locked, NewTransaction, Transactions)};
        false ->
            handle_operation(Store, Session, Operation, Current),
            State
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

handle_operation(Store, Session, #get{key=Key}, _Current) ->
    redis_store:get(#single_operation{session=Session}, Store, Key);
handle_operation(Store, Session, #set{key=Key, value=Value}, Current) ->
    update_meta(Key, Current),
    redis_store:set(#single_operation{session=Session}, Store, Key, Value);
handle_operation(Store, Session, #delete{key=Key}, Current) ->
    delete_meta(Key, Current),
    redis_store:delete(#single_operation{session=Session}, Store, Key).

add_operation(error, Session, Operation) ->
    add_operation({ok, #transaction{session=Session}}, Session, Operation);
add_operation({ok, #transaction{}=Transaction}, Session, Operation) ->
    Session = Transaction#transaction.session,
    Transaction#transaction{operations=[Operation|Transaction#transaction.operations]}.

handle_transact(#state{transactions=Transactions}=State, #transact{txn_id=TransactionId, session=Session, operation_id=Id, operation=Operation}) ->
    Session ! {self(), stored},
    NewTransaction = add_operation(dict:find(TransactionId, Transactions), Session, {Id, Operation}),
    State#state{transactions=dict:store(TransactionId, NewTransaction, Transactions)}.

handle_watch(#state{transactions=Transactions}=State, #watch{txn_id=TransactionId, session=Session, key=Key}) ->
    Session ! {self(), ok},
    NewTransaction = add_watch(dict:find(TransactionId, Transactions), Session, Key),
    State#state{transactions=dict:store(TransactionId, NewTransaction, Transactions)}.

add_watch(error, Session, Key) ->
    add_watch({ok, #transaction{session=Session}}, Session, Key);
add_watch({ok, #transaction{watches=Watches}=Transaction}, Session, Key) ->
    Session = Transaction#transaction.session,
    Current = get(Key),
    put(Key, key_meta:watch(Current)),
    Transaction#transaction{watches=[{Key, key_meta:version(Current)}|Watches]}.

handle_unwatch(#state{transactions=Transactions}=State, #unwatch{txn_id=TransactionId, session=Session}) ->
    Session ! {self(), ok},
    State#state{transactions=remove_watch(Transactions, TransactionId, dict:find(TransactionId, Transactions))}.

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

operation_key(#get{key=Key}) ->
    Key;
operation_key(#set{key=Key}) ->
    Key;
operation_key(#delete{key=Key}) ->
    Key.

handle_lock_transaction(#state{transactions=Transactions, store=Store}=State, TransactionId) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    WatchStatus = check_watches(Transaction#transaction.watches),
    LockStatus = lock_keys(WatchStatus, Transaction#transaction.operations, TransactionId),
    NewTransaction = case LockStatus of
			 ok ->
			     binlog:write(State#state.binlog_state, {lock_txn, TransactionId}, "Bucket lock transaction"),
			     Transaction#transaction{locked=true};
			 error ->
			     Transaction#transaction.session ! #transaction_locked{bucket=self(), status=LockStatus},
			     unlock_transaction(Store, Transaction#transaction{locked=false})
		     end,
    State#state{transactions=dict:store(TransactionId, NewTransaction, State#state.transactions)}.

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
    Key = operation_key(Operation),
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

handle_rollback_transaction(#state{transactions=Transactions, store=Store}=State, TransactionId) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    unlock_transaction(Store, Transaction),
    remove_watches(Transaction#transaction.watches),
    binlog:write(State#state.binlog_state, {delete, TransactionId}, "Bucket rollback transaction"),
    State#state{transactions=dict:erase(TransactionId, Transactions)}.

handle_commit_transaction(#state{transactions=Transactions, store=Store}=State, TransactionId) ->
    Transaction = dict:fetch(TransactionId, Transactions),
    apply_transaction(Store, Transaction, TransactionId),
    State.

apply_transaction(Store, #transaction{operations=Operations}=Transaction, TransactionId) ->
    Pipe = redis_store:pipeline(Store),
    Pipe2 = lists:foldr(fun({_Order, O}, P) -> handle_batch_operation(P, O) end, Pipe, Operations),
    redis_store:commit(#transaction_operation{txn_id=TransactionId}, Pipe2),
    unlock_transaction(Store, Transaction).

unlock_transaction(Store, #transaction{deferred=Deferred, locked=false}=Transaction) ->
    lists:foreach(fun({Session, Operation}) ->
            handle_operation(Store, Session, Operation, get(operation_key(Operation)))
        end, lists:reverse(Deferred)),
    Transaction#transaction{deferred=[]};
unlock_transaction(Store, #transaction{operations=Operations}=Transaction) ->
    unlock_keys(Operations),
    unlock_transaction(Store, Transaction#transaction{locked=false}).

unlock_keys([]) ->
    ok;
unlock_keys([{_Id, Operation}|Remainder]) ->
    Key = operation_key(Operation),
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

handle_store_result(State, #single_operation{session=Session}, Result) ->
    Session ! {self(), map_result(Result)},
    State;
handle_store_result(#state{transactions=Transactions}=State, #transaction_operation{txn_id=TransactionId}, Results) ->
    {ok, Transaction} = dict:find(TransactionId, Transactions),
    Transaction#transaction.session ! {self(), map_transaction_results(Results, lists:reverse(Transaction#transaction.operations), [])},
    remove_watches(Transaction#transaction.watches),
    txn_monitor:finalized(TransactionId),
    binlog:write(State#state.binlog_state, {delete, TransactionId}, "Bucket stored transaction"),
    State#state{transactions=dict:erase(TransactionId, Transactions)}.
