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

-module(redis_store).
-export([connect/2]).
-export([get/2, get/3, set/3, set/4, delete/3]).
-export([pipeline/1, transaction/1, commit/2]).
-compile(export_all).

-include("store.hrl").

-record(default, {client}).
-record(transaction, {client, stored=[]}).
-record(pipeline, {client, stored=[]}).

-include_lib("eunit/include/eunit.hrl").

connect(Host, Port) ->
    {ok, Client} = eredis:start_link(Host, Port),
    #default{client=Client}.

get_command_result({ok, undefined}) ->
    undefined;
get_command_result({ok, Result}) ->
    Result;
get_command_result(Result) ->
    Result.

get_operation(Key) ->
    {["GET", Key], fun(Result) -> get_command_result(Result) end }.

get(Id, #default{client=Client}, Key) ->
    dispatch_operation(Id, Client, get_operation(Key)).

get(#transaction{}=State, Key) ->
    store_operation(State, get_operation(Key));
get(#pipeline{}=State, Key) ->
    store_operation(State, get_operation(Key)).

set_command_result(<<"OK">>) ->
    ok;
set_command_result({ok, <<"OK">>}) ->
    ok.

set_operation(Key, Value) ->
    {["SET", Key, Value], fun(Result) -> set_command_result(Result) end}.

set(Id, #default{client=Client}, Key, Value) ->
    dispatch_operation(Id, Client, set_operation(Key, Value)).

set(#transaction{}=State, Key, Value) ->
    store_operation(State, set_operation(Key, Value));
set(#pipeline{}=State, Key, Value) ->
    store_operation(State, set_operation(Key, Value)).

delete_command_result({ok, <<"0">>}) ->
    0;
delete_command_result({ok, <<"1">>}) ->
    1.

delete_operation(Key) ->
    {["DEL", Key], fun(Result) -> delete_command_result(Result) end}.

delete(Id, #default{client=Client}, Key) ->
    dispatch_operation(Id, Client, delete_operation(Key)).

delete(#transaction{}=State, Key) ->
    store_operation(State, delete_operation(Key));
delete(#pipeline{}=State, Key) ->
    store_operation(State, delete_operation(Key)).

transaction(#default{client=Client}) ->
    #transaction{client=Client}.

pipeline(#default{client=Client}) ->
    #pipeline{client=Client}.

map_bulk_results([], [], Results) ->
    Results;
map_bulk_results([Result|ResultsTail], [Handler|HandlersTail], Results) ->
    map_bulk_results(ResultsTail, HandlersTail, [Handler(Result)|Results]).

commit(Id, #transaction{client=Client, stored=Operations}) ->
    Parent = self(),
    {Commands, Handlers} = lists:foldl(fun({C, H}, {C0, H0}) -> {[C|C0], [H|H0]} end, {[], []}, Operations),
    spawn(fun() ->
            {ok, <<"OK">>} = eredis:q(Client, ["MULTI"]),
            lists:foreach(fun(Command) -> {ok, <<"QUEUED">>} = eredis:q(Client, Command) end, Commands),
            MultiResults = eredis:q(Client, ["EXEC"]),
            Results = case MultiResults of
                {ok, ResultList} -> lists:reverse(map_bulk_results(ResultList, Handlers, []));
                _Else -> error
            end,
            Parent ! #store_result{id=Id, result=Results}
        end);
commit(Id, #pipeline{client=Client, stored=Operations}) ->
    Parent = self(),
    {Commands, Handlers} = lists:foldl(fun({C, H}, {C0, H0}) -> {[C|C0], [H|H0]} end, {[], []}, Operations),
    spawn(fun() ->
            Results = lists:reverse(map_bulk_results(eredis:qp(Client, Commands), Handlers, [])),
            Parent ! #store_result{id=Id, result=Results}
        end).

dispatch_operation(Id, Client, Operation) ->
    Parent = self(),
    spawn(fun() ->
            {Command, Handler} = Operation,
            Result = eredis:q(Client, Command),
            Parent ! #store_result{id=Id, result=Handler(Result)}
        end).

store_operation(#transaction{stored=Stored}=Transaction, Operation) ->
    Transaction#transaction{stored=[Operation|Stored]};
store_operation(#pipeline{stored=Stored}=Pipeline, Operation) ->
    Pipeline#pipeline{stored=[Operation|Stored]}.

get_set_test() ->
    C = connect("127.0.0.1", 6379),
    redis_store:set(blah, C, "foo", "bar"),
    redis_store:get(blah, C, "foo"),
    success = test_receive([ok, <<"bar">>]).

transaction_test() ->
    C = connect("127.0.0.1", 6379),
    Trans = transaction(C),
    Trans2 = redis_store:set(Trans, "foo", "bar"),
    Trans3 = redis_store:get(Trans2, "foo"),
    commit(blah, Trans3),
    success = test_receive([[ok, <<"bar">>]]).

pipeline_test() ->
    C = connect("127.0.0.1", 6379),
    Pipe = pipeline(C),
    Pipe2 = redis_store:set(Pipe, "foo", "bar"),
    Pipe3 = redis_store:get(Pipe2, "foo"),
    commit(blah, Pipe3),
    success = test_receive([[ok, <<"bar">>]]).

test_receive([]) ->
    success;
test_receive([Expected|T]) ->
    {store_result, blah, Expected} = receive Any -> Any end,
    test_receive(T).

