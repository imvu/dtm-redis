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

-module(session).
-behavior(gen_server).
-export([start_link/3]).
-export([call/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("protocol.hrl").
-include("dtm_redis.hrl").

-record(transaction, {
    current,
    buckets :: [bucket_id()]
}).

-record(state, {
    client :: none | shell | gen_tcp:socket(),
    txn_id=none :: none | txn_monitor:transaction_id(),
    buckets :: [#bucket{}],
    monitors :: [#monitor{}],
    transaction=none :: none | #transaction{},
    watches=none :: none | set(),
    stream
}).

% API methods

start_link(#buckets{}=Buckets, Monitors, shell) ->
    gen_server:start_link({local, shell}, ?MODULE, #state{client=shell, buckets=Buckets, monitors=Monitors}, []);
start_link(#buckets{}=Buckets, Monitors, Client) ->
    gen_server:start_link(?MODULE, #state{client=Client, buckets=Buckets, monitors=Monitors, stream=redis_protocol:init()}, []).

-spec call(pid() | atom(), #operation{}) -> reply().
call(Session, Operation) ->
    gen_server:call(Session, Operation).

% gen_server callbacks

init(#state{}=State) ->
    error_logger:info_msg("initializing session with pid ~p", [self()]),
    {ok, State}.

handle_call(#operation{command= <<"WATCH">>, key=Key}, _From, State) ->
    handle_watch(State, Key);
handle_call(#operation{command= <<"UNWATCH">>}, _From, State) ->
    handle_unwatch(State);
handle_call(#operation{command= <<"MULTI">>}, _From, State) ->
    handle_multi(State);
handle_call(#operation{command= <<"EXEC">>}, _From, State) ->
    handle_exec(State);
handle_call(#operation{}=Operation, From, State) ->
    handle_operation(State, From, Operation);
handle_call(Message, From, _State) ->
    error_logger:error_msg("session:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(Message, _State) ->
    error_logger:error_msg("session:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info({tcp, Client, Data}, #state{client=Client}=State) ->
    inet:setopts(Client, [{active, once}]),
    {NewStream, Result} = redis_protocol:parse_stream(State#state.stream, Data),
    NewState = State#state{stream=NewStream},
    if
        is_record(Result, command) ->
            handle_tcp_command(Client, NewState, Result);
        Result == protocol_error ->
            error_logger:info_msg("unexpected data received from client connection, aborting~n", []);
        Result == incomplete ->
            {noreply, NewState}
    end;
handle_info({tcp_closed, Client}, #state{client=Client}=State) ->
    {stop, normal, State};
handle_info({tcp_error, Reason, Client}, #state{client=Client}=State) ->
    error_logger:error_msg("stopping session because of tcp error ~p", [Reason]),
    {stop, Reason, State};
handle_info(Message, _State) ->
    error_logger:error_msg("session:handle_info unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

terminate(Reason, State) ->
    error_logger:info_msg("terminating session with reason ~p", [Reason]),
    close(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

handle_watch(State, Key) ->
    {TransactionId, NewState} = get_txn_id(State),
    Bucket = hash:worker_for_key(Key, NewState#state.buckets),
    ok = gen_server:call(Bucket, #watch{txn_id=TransactionId, session_id=self(), key=Key}),
    send_response(NewState#state{watches=add_watch(NewState#state.watches, Bucket)}, #redis_status{message= <<"OK">>}).

add_watch(none, Bucket) ->
    sets:add_element(Bucket, sets:new());
add_watch(Watches, Bucket) ->
    sets:add_element(Bucket, Watches).

handle_unwatch(State) ->
    send_unwatch(State),
    send_response(State#state{watches=none}, #redis_status{message= <<"OK">>}).

send_unwatch(#state{watches=none}) ->
    ok;
send_unwatch(#state{watches=Watches}=State) ->
    {TransactionId, _State} = get_txn_id(State),
    sets:fold(fun(Bucket, NotUsed) -> gen_server:cast(Bucket, #unwatch{txn_id=TransactionId, session=self()}), NotUsed end, not_used, Watches),
    loop_unwatch(Watches, sets:size(Watches)).

loop_unwatch(_, 0) ->
    ok;
loop_unwatch(Watches, _) ->
    receive
        {Bucket, ok} ->
            NewWatches = sets:del_element(Bucket, Watches),
            loop_unwatch(NewWatches, sets:size(NewWatches))
    end.

handle_multi(#state{transaction=none}=State) ->
    {_TransactionId, NewState} = get_txn_id(State),
    send_response(NewState#state{transaction=#transaction{current=0, buckets=sets:new()}}, #redis_status{message= <<"OK">>});
handle_multi(#state{}=State) ->
    send_response(State, #redis_error{type= <<"ERR">>, message= <<"MULTI calls can not be nested">>}).

-spec send_response(#state{}, reply()) -> {reply, reply(), #state{}} | {noreply, #state{}}.
send_response(#state{client=shell}=State, Reply) ->
    {reply, Reply, State};
send_response(#state{client=Client}=State, Reply) ->
    gen_tcp:send(Client, redis_stream:format_reply(Reply)),
    {noreply, State}.

handle_exec(#state{transaction=none}=State) ->
    send_response(State, #redis_error{type= <<"ERR">>, message= <<"EXEC without MULTI">>});
handle_exec(#state{txn_id=TransactionId, transaction=#transaction{buckets=Buckets}}=State) ->
    send_response(State#state{txn_id=none, transaction=none}, commit_transaction(TransactionId, Buckets)).

get_txn_id(#state{txn_id=none, monitors=Monitors}=State) ->
    TransactionId = txn_monitor:allocate(Monitors),
    {TransactionId, State#state{txn_id=TransactionId}};
get_txn_id(#state{txn_id=TransactionId}=State) ->
    {TransactionId, State}.

close(#state{client=shell}) ->
    ok;
close(#state{client=Client}) ->
    gen_tcp:close(Client).

handle_tcp_command(_Client, _State, {command, _Name, _Parameters}) ->
    ok.

send_operation_response(_From, Response, #state{client=shell}=State) ->
    {reply, Response, State}.

handle_operation(#state{transaction=none, buckets=Buckets}=State, From, #operation{key=Key}=Operation) ->
    Bucket = hash:worker_for_key(Key, Buckets),
    Response = gen_server:call(Bucket, #command{session_id=self(), operation=Operation}),
    send_operation_response(From, Response, State);
handle_operation(#state{txn_id=TransactionId, transaction=Transaction}=State, From, #operation{key=Key}=Operation) ->
    Bucket = hash:worker_for_key(Key, State#state.buckets),
    Response = gen_server:call(Bucket, #transact{txn_id=TransactionId, session_id=self(), operation_id=Transaction#transaction.current, operation=Operation}),
    Current = Transaction#transaction.current + 1,
    Buckets = sets:add_element(Bucket, Transaction#transaction.buckets),
    NewTransaction = Transaction#transaction{current=Current, buckets=Buckets},
    send_operation_response(From, Response, State#state{transaction=NewTransaction}).

commit_transaction(TransactionId, Buckets) ->
    sets:fold(fun(Bucket, NotUsed) -> gen_server:cast(Bucket, #lock_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
    case loop_transaction_lock(Buckets, sets:size(Buckets), false) of
        error ->
            sets:fold(fun(Bucket, NotUsed) -> gen_server:cast(Bucket, #rollback_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
            #redis_bulk{content=none};
        ok ->
            txn_monitor:persist(TransactionId, Buckets),
            sets:fold(fun(Bucket, NotUsed) -> gen_server:cast(Bucket, #commit_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
            loop_transaction_commit(Buckets, [], sets:size(Buckets))
    end.

loop_transaction_lock(_Buckets, 0, false) ->
    ok;
loop_transaction_lock(_Buckets, 0, true) ->
    error;
loop_transaction_lock(Buckets, _Size, Failure) ->
    receive
        #transaction_locked{bucket_id=Bucket, status=Status} ->
            NewBuckets = sets:del_element(Bucket, Buckets),
            loop_transaction_lock(NewBuckets, sets:size(NewBuckets), Failure or (Status =:= error));
        Any ->
            error_logger:info_msg("session got an unexpected message ~p~n", [Any])
    end.

loop_transaction_commit(_Buckets, Results, 0) ->
    [Result || {_, Result} <- lists:sort(fun({Lhs, _}, {Rhs, _}) -> Lhs =< Rhs end, lists:flatten(Results))];
loop_transaction_commit(Buckets, ResultsSoFar, _) ->
    receive
        {Bucket, Results} ->
            NewBuckets = sets:del_element(Bucket, Buckets),
            NewResultsSoFar = [Results|ResultsSoFar],
            loop_transaction_commit(NewBuckets, NewResultsSoFar, sets:size(NewBuckets));
        Any ->
            error_logger:info_msg("session got an unexpected message ~p~n", [Any])
    end.

