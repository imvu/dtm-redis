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
-behavior(ranch_protocol).
-export([start_link/2, start_link/4]).
-export([call_shell/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("protocol.hrl").
-include("dtm_redis.hrl").

-record(transaction, {
    current,
    buckets :: [bucket_id()]
}).

-record(state, {
    client :: none | shell | {module(), gen_tcp:socket(), redis_stream:parse_state()},
    txn_id=none :: none | txn_monitor:transaction_id(),
    buckets :: [#bucket{}],
    monitors :: [#monitor{}],
    transaction=none :: none | #transaction{},
    watches=none :: none | set()
}).

% API methods

-spec start_link(#buckets{}, [#monitor{}]) -> {ok, pid()} | {error, any()}.
start_link(Buckets, Monitors) ->
    gen_server:start_link({local, shell}, ?MODULE, {Buckets, Monitors}, []).

-spec call_shell(#operation{}) -> reply().
call_shell(Operation) ->
    gen_server:call(shell, Operation).

% ranch_protocol callbacks

-spec start_link(pid(), any(), module(), {#buckets{}, [#monitor{}]}) -> {ok, pid()} | {error, any()}.
start_link(ListenerPid, Socket, Transport, {Buckets, Monitors}) ->
    gen_server:start_link(?MODULE, {ListenerPid, Socket, Transport, Buckets, Monitors}, []).

% gen_server callbacks

-spec init({pid(), any(), module(), #buckets{}, [#monitor{}]} | {#buckets{}, [#monitor{}]}) -> {ok, #state{}}.
init({ListenerPid, Socket, Transport, Buckets, Monitors}) ->
    State = #state{client={Transport, Socket, redis_stream:init_request_stream()}, buckets=Buckets, monitors=Monitors},
    gen_server:cast(self(), {init_complete, ListenerPid}),
    {ok, State};
init({Buckets, Monitors}) ->
    {ok, #state{client=shell, buckets=Buckets, monitors=Monitors}}.

-spec handle_call(#operation{}, any(), #state{}) -> {reply, any(), #state{}} | {noreply, #state{}}.
handle_call(#operation{}=Operation, _From, State) ->
    handle_operation(State, Operation).

-spec handle_cast({init_complete, pid()}, #state{}) -> {noreply, #state{}}.
handle_cast({init_complete, ListenerPid}, #state{client={Transport, Socket, _Stream}}=State) ->
    ranch:accept_ack(ListenerPid),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State}.

-spec handle_info({tcp, any(), binary()} | {tcp_closed, any()} | {tcp_error, any(), any()}, #state{}) -> {reply, any(), #state{}} | {noreply, #state{}} | {stop, any(), #state{}}.
handle_info({tcp, Socket, Data}, #state{client={Transport, Socket, _Stream}}=State) ->
    Transport:setopts(Socket, [{active, once}]),
    parse_request(State, Data);
handle_info({tcp_closed, Socket}, #state{client={_Transport, Socket, _Stream}}=State) ->
    {stop, normal, State};
handle_info({tcp_error, Reason, Socket}, #state{client={_Transport, Socket, _Stream}}=State) ->
    error_logger:error_msg("stopping session because of tcp error ~p", [Reason]),
    {stop, Reason, State}.

-spec terminate(any(), #state{}) -> #state{}.
terminate(Reason, State) ->
    error_logger:info_msg("terminating session with reason ~p", [Reason]),
    close(State).

-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

-spec parse_request(#state{}, binary()) -> {noreply, #state{}}.
parse_request(State, <<>>) ->
    {noreply, State};
parse_request(#state{client={Transport, Socket, Parser}}=State, Data) ->
    case redis_stream:parse_request(Parser, Data) of
        {partial, NewParser} ->
            {noreply, State#state{client={Transport, Socket, NewParser}}};
        {Request, Remaining, NewParser} ->
            {noreply, NewState} = handle_operation(State#state{client={Transport, Socket, NewParser}}, redis_command:parse_operation(Request)),
            parse_request(NewState, Remaining)
    end.

-spec handle_operation(#state{}, #operation{} | #redis_error{}) -> {reply, reply(), #state{}} | {noreply, #state{}}.
handle_operation(State, #operation{command= <<"WATCH">>, key=Key}) ->
    handle_watch(State, Key);
handle_operation(State, #operation{command= <<"UNWATCH">>}) ->
    handle_unwatch(State);
handle_operation(State, #operation{command= <<"MULTI">>}) ->
    handle_multi(State);
handle_operation(State, #operation{command= <<"EXEC">>}) ->
    handle_exec(State);
handle_operation(#state{transaction=none, buckets=Buckets}=State, #operation{key=Key}=Operation) ->
    Bucket = hash:worker_for_key(Key, Buckets),
    Response = gen_server:call({global, Bucket}, #command{session_id=self(), operation=Operation}),
    send_response(State, Response);
handle_operation(#state{txn_id=TransactionId, transaction=#transaction{current=CurrentOp}=Transaction}=State, #operation{key=Key}=Operation) ->
    Bucket = hash:worker_for_key(Key, State#state.buckets),
    Response = gen_server:call({global, Bucket}, #transact{txn_id=TransactionId, session_id=self(), operation_id=CurrentOp, operation=Operation}),
    Buckets = sets:add_element(Bucket, Transaction#transaction.buckets),
    NewTransaction = Transaction#transaction{current=CurrentOp + 1, buckets=Buckets},
    send_response(State#state{transaction=NewTransaction}, Response);
handle_operation(State, #redis_error{}=Error) ->
    send_response(State, Error).

handle_watch(State, Key) ->
    {TransactionId, NewState} = get_txn_id(State),
    Bucket = hash:worker_for_key(Key, NewState#state.buckets),
    ok = gen_server:call({global, Bucket}, #watch{txn_id=TransactionId, session_id=self(), key=Key}),
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
    sets:fold(fun(Bucket, NotUsed) -> gen_server:cast({global, Bucket}, #unwatch{txn_id=TransactionId, session=self()}), NotUsed end, not_used, Watches),
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
send_response(#state{client={Transport, Socket, _Stream}}=State, Reply) ->
    Transport:send(Socket, redis_stream:format_reply(Reply)),
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

-spec close(#state{}) -> #state{}.
close(#state{client=none}=State) ->
    State;
close(#state{client=shell}=State) ->
    State;
close(#state{client={Transport, Socket, _Stream}}=State) ->
    Transport:close(Socket),
    State#state{client=none}.

commit_transaction(TransactionId, Buckets) ->
    sets:fold(fun(Bucket, NotUsed) -> gen_server:cast({global, Bucket}, #lock_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
    case loop_transaction_lock(Buckets, sets:size(Buckets), false) of
        error ->
            sets:fold(fun(Bucket, NotUsed) -> gen_server:cast({global, Bucket}, #rollback_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
            #redis_bulk{content=none};
        ok ->
            txn_monitor:persist(TransactionId, Buckets),
            sets:fold(fun(Bucket, NotUsed) -> gen_server:cast({global, Bucket}, #commit_transaction{txn_id=TransactionId}), NotUsed end, not_used, Buckets),
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
    FlatResults = lists:flatten(Results),
    #redis_multi_bulk{count=length(FlatResults), items=[Result || {_, Result} <- lists:sort(fun({Lhs, _}, {Rhs, _}) -> Lhs =< Rhs end, FlatResults)]};
loop_transaction_commit(Buckets, ResultsSoFar, _) ->
    receive
        {Bucket, Results} ->
            NewBuckets = sets:del_element(Bucket, Buckets),
            NewResultsSoFar = [Results | ResultsSoFar],
            loop_transaction_commit(NewBuckets, NewResultsSoFar, sets:size(NewBuckets));
        Any ->
            error_logger:info_msg("session got an unexpected message ~p~n", [Any])
    end.

