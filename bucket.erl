-module(bucket).
-export([start/0]).
-compile(export_all).

-include("protocol.hrl").
-include("data_types.hrl").
-include("store.hrl").

-record(state, {transactions, store}).
-record(transaction, {session, operations=[], deferred=[], watches=[], locked=false}).

start() ->
    io:format("starting storage bucket with pid ~p~n", [self()]),
    loop(#state{transactions=dict:new(), store=redis_store:connect("127.0.0.1", 6379)}).

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
        #lock_transaction{session=Session} ->
            loop(handle_lock_transaction(State, Session));
        #rollback_transaction{session=Session} ->
            loop(handle_rollback_transaction(State, Session));
        #commit_transaction{session=Session} ->
            loop(handle_commit_transaction(State, Session));
        #store_result{id=Session, result=Result} ->
            loop(handle_store_result(State, Session, Result));
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

handle_batch_operation(Store, #get{key=Key}) ->
    redis_store:get(Store, Key);
handle_batch_operation(Store, #set{key=Key, value=Value}) ->
    redis_store:set(Store, Key, Value);
handle_batch_operation(Store, #delete{key=Key}) ->
    redis_store:delete(Store, Key).

handle_operation(Store, Session, #get{key=Key}, _Current) ->
    redis_store:get(Session, Store, Key);
handle_operation(Store, Session, #set{key=Key, value=Value}, Current) ->
    put(Key, key_meta:update(Current, Value)),
    redis_store:set(Session, Store, Key, Value);
handle_operation(Store, Session, #delete{key=Key}, Current) ->
    case key_meta:is_watched(Current) of
        true -> put(Key, key_meta:update(Current));
        false -> erase(Key)
    end,
    redis_store:delete(Session, Store, Key);
handle_operation(_Store, _Session, Any, _Current) ->
    io:format("unrecognized operation: ~p~n", [Any]).

add_operation(error, Operation) ->
    add_operation({ok, #transaction{}}, Operation);
add_operation({ok, #transaction{}=Transaction}, Operation) ->
    Transaction#transaction{operations=[Operation|Transaction#transaction.operations]}.

handle_transact(#state{transactions=Transactions}=State, #transact{session=Session, id=Id, operation=Operation}) ->
    Session ! {self(), stored},
    NewTransaction = add_operation(dict:find(Session, Transactions), {Id, Operation}),
    State#state{transactions=dict:store(Session, NewTransaction, Transactions)}.

handle_watch(#state{transactions=Transactions}=State, #watch{session=Session, key=Key}) ->
    Session ! {self(), ok},
    NewTransaction = add_watch(dict:find(Session, Transactions), Key),
    State#state{transactions=dict:store(Session, NewTransaction, Transactions)}.

add_watch(error, Key) ->
    add_watch({ok, #transaction{}}, Key);
add_watch({ok, #transaction{watches=Watches}=Transaction}, Key) ->
    Current = get(Key),
    put(Key, key_meta:watch(Current)),
    Transaction#transaction{watches=[{Key, key_meta:version(Current)}|Watches]}.

handle_unwatch(#state{transactions=Transactions}=State, #unwatch{session=Session}) ->
    Session ! {self(), ok},
    State#state{transactions=remove_watch(Transactions, Session, dict:find(Session, Transactions))}.

remove_watch(Transactions, _Session, error) ->
    Transactions;
remove_watch(Transactions, Session, {ok, Transaction}) ->
    remove_watches(Transaction#transaction.watches),
    dict:store(Session, Transaction#transaction{watches=[]}, Transactions).

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

handle_lock_transaction(#state{transactions=Transactions, store=Store}=State, Session) ->
    Transaction = dict:fetch(Session, Transactions),
    WatchStatus = check_watches(Transaction#transaction.watches),
    LockStatus = lock_keys(WatchStatus, Transaction#transaction.operations, Session),
    Session ! #transaction_locked{bucket=self(), status=LockStatus},
    NewTransaction = case LockStatus of
        ok ->
            Transaction#transaction{locked=true};
        error ->
            unlock_transaction(Store, Transaction#transaction{locked=false})
    end,
    State#state{transactions=dict:store(Session, NewTransaction, State#state.transactions)}.

check_watches([]) ->
    ok;
check_watches([{Key, Version}|Remaining]) ->
    case key_meta:version(get(Key)) =:= Version of
        true -> check_watches(Remaining);
        false -> error
    end.

lock_keys(error, _, _) ->
    error;
lock_keys(ok, [], _Session) ->
    ok;
lock_keys(ok, [{_Id, Operation}|Remainder], Session) ->
    Key = operation_key(Operation),
    Current = get(Key),
    case key_meta:is_locked(Current, Session) of
        true -> error;
        false ->
            put(Key, key_meta:lock(Current, Session)),
            case lock_keys(ok, Remainder, Session) of
                ok -> ok;
                error -> put(Key, Current), error
            end
    end.

handle_rollback_transaction(#state{transactions=Transactions, store=Store}=State, Session) ->
    Transaction = dict:fetch(Session, Transactions),
    unlock_transaction(Store, Transaction),
    remove_watches(Transaction#transaction.watches),
    State#state{transactions=dict:erase(Session, Transactions)}.

handle_commit_transaction(#state{transactions=Transactions, store=Store}=State, Session) ->
    Transaction = dict:fetch(Session, Transactions),
    apply_transaction(Store, Transaction, Session),
    remove_watches(Transaction#transaction.watches),
    State#state{transactions=dict:erase(Session, Transactions)}.

apply_transaction(Store, #transaction{operations=Operations}=Transaction, Session) ->
    Pipe = redis_store:pipeline(Store),
    Pipe2 = lists:foldr(fun(O, P) -> handle_batch_operation(P, O) end, Pipe, Operations),
    redis_store:commit(Session, Pipe2),
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

handle_store_result(State, Session, Result) ->
    Session ! {self(), Result},
    State.

