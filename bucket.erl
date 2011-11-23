-module(bucket).
-export([start/0]).
-compile(export_all).

-include("protocol.hrl").
-include("data_types.hrl").

-record(state, {transactions}).
-record(transaction, {operations=[], deferred=[], watches=[], locked=false}).

start() ->
    io:format("starting storage bucket with pid ~p~n", [self()]),
    loop(#state{transactions=dict:new()}).

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
        #commit_transaction{session=Session, now=Now} ->
            loop(handle_commit_transaction(State, Session, Now));
        stop ->
            io:format("storage bucket halting after receiving stop message~n");
        Any ->
            io:format("storage bucket process received message ~p~n", [Any]),
            loop(State)
    end.

handle_command(#state{transactions=Transactions}=State, #command{session=Session, operation=Operation}) ->
    Value = get(operation_key(Operation)),
    case value:is_locked(Value, Session) of
        true ->
            #data{locked=Locked} = Value,
            #transaction{deferred=Deferred}=Transaction = dict:fetch(Locked, Transactions),
            NewTransaction = Transaction#transaction{deferred=[{Session, Operation}|Deferred]},
            State#state{transactions=dict:store(Locked, NewTransaction, Transactions)};
        false ->
            Session ! {self(), handle_operation(Operation, Value, erlang:now())},
            State
    end.

handle_operation(Operation, Now) ->
    handle_operation(Operation, get(operation_key(Operation)), Now).

handle_operation(#get{}, Current, _Now) ->
    string_value:read(Current);
handle_operation(#set{key=Key, value=Value}, Current, _Now) ->
    put(Key, string_value:update(Current, Value)),
    ok;
handle_operation(#delete{key=Key}, Current, _Now) ->
    case value:is_watched(Current) of
        true -> put(Key, value:update(Current, undefined));
        false -> erase(Key)
    end,
    ok;
handle_operation(Any, _Current, _Now) ->
    io:format("unrecognized operation: ~p~n", [Any]),
    error.

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
    put(Key, value:watch(Current)),
    Transaction#transaction{watches=[{Key, value:version(Current)}|Watches]}.

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
    NewValue = value:unwatch(get(Key)),
    case value:is_watched(NewValue) of
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

handle_lock_transaction(#state{transactions=Transactions}=State, Session) ->
    Transaction = dict:fetch(Session, Transactions),
    WatchStatus = check_watches(Transaction#transaction.watches),
    LockStatus = lock_keys(WatchStatus, Transaction#transaction.operations, Session),
    Session ! #transaction_locked{bucket=self(), status=LockStatus},
    NewTransaction = case LockStatus of
        ok ->
            Transaction#transaction{locked=true};
        error ->
            unlock_transaction(Transaction#transaction{locked=false})
    end,
    State#state{transactions=dict:store(Session, NewTransaction, State#state.transactions)}.

check_watches([]) ->
    ok;
check_watches([{Key, Version}|Remaining]) ->
    case value:version(get(Key)) =:= Version of
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
    case value:is_locked(Current, Session) of
        true -> error;
        false ->
            put(Key, value:lock(Current, Session)),
            case lock_keys(ok, Remainder, Session) of
                ok -> ok;
                error -> put(Key, Current), error
            end
    end.

handle_rollback_transaction(#state{transactions=Transactions}=State, Session) ->
    Transaction = dict:fetch(Session, Transactions),
    unlock_transaction(Transaction),
    remove_watches(Transaction#transaction.watches),
    State#state{transactions=dict:erase(Session, Transactions)}.

handle_commit_transaction(#state{transactions=Transactions}=State, Session, Now) ->
    Transaction = dict:fetch(Session, Transactions),
    apply_transaction(Transaction, Session, Now),
    remove_watches(Transaction#transaction.watches),
    State#state{transactions=dict:erase(Session, Transactions)}.

apply_transaction(#transaction{operations=Operations}=Transaction, Session, Now) ->
    Session ! {self(), [{I, handle_operation(O, Now)} || {I, O} <- lists:reverse(Operations)]},
    unlock_transaction(Transaction).

unlock_transaction(#transaction{deferred=Deferred, locked=false}=Transaction) ->
    lists:foreach(fun({Session, Operation}) ->
            Session ! {self(), handle_operation(Operation, erlang:now())}
        end, lists:reverse(Deferred)),
    Transaction#transaction{deferred=[]};
unlock_transaction(#transaction{operations=Operations}=Transaction) ->
    unlock_keys(Operations),
    unlock_transaction(Transaction#transaction{locked=false}).

unlock_keys([]) ->
    ok;
unlock_keys([{_Id, Operation}|Remainder]) ->
    Key = operation_key(Operation),
    put(Key, value:unlock(get(Key))),
    unlock_keys(Remainder).
