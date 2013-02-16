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

-module(redis_store).
-export([connect/2, handle_info/2]).
-export([get/2, get/3, set/3, set/4, delete/2, delete/3]).
-export([get_command_result/1, set_command_result/1, delete_command_result/1]).
-export([pipeline/1, commit/2]).

-include("store.hrl").

-define(CONNECT_TIMEOUT, 5000).  % 5 seconds

% types

-type operation_id() :: any().
-type key() :: any().
-type operation() :: {[any()], atom()}.
-type result() :: {operation_id(), [any()]}.

-record(default, {
    client :: redis_client:redis_state()
}).
-record(pipeline, {
    client :: redis_client:redis_state(),
    stored=[] :: [operation()]
}).

-type default() :: #default{}.
-type pipeline() :: #pipeline{}.

-opaque redis_state() :: default() | pipeline().
-export_type([redis_state/0]).

% public api

-spec connect(inet:ip_address() | inet:hostname(), inet:port_number()) -> {ok, redis_state()} | {error, inet:posix()}.
connect(Host, Port) ->
    case redis_client:connect(Host, Port, ?CONNECT_TIMEOUT) of
        {ok, Client} -> #default{client=Client};
        Error -> Error
    end.

-spec handle_info({tcp, inet:socket(), binary()} | {tcp_error, inet:posix(), inet:socket()} | {tcp_closed, inet:socket()}, redis_state()) -> {[result()], redis_state()}.
handle_info({tcp, _Data, _Socket}=Packet, #default{client=Client}=Default) ->
    {Results, NewClient} = redis_client:recv(Packet, Client),
    {map_results(Results), Default#default{client=NewClient}};
handle_info({tcp, _Data, _Socket}=Packet, #pipeline{client=Client}=Pipeline) ->
    {Results, NewClient} = redis_client:recv(Packet, Client),
    {map_results(Results), Pipeline#pipeline{client=NewClient}};
handle_info({tcp_error, _Reason, _Socket}=Message, #default{client=Client}=Default) ->
    {[], Default#default{client=redis_client:recv(Message, Client)}};
handle_info({tcp_error, _Reason, _Socket}=Message, #pipeline{client=Client}=Pipeline) ->
    {[], Pipeline#pipeline{client=redis_client:recv(Message, Client)}};
handle_info({tcp_closed, _Socket}=Message, #default{client=Client}=Default) ->
    {[], Default#default{client=redis_client:recv(Message, Client)}};
handle_info({tcp_closed, _Socket}=Message, #pipeline{client=Client}=Pipeline) ->
    {[], Pipeline#pipeline{client=redis_client:recv(Message, Client)}}.

-spec get(operation_id(), default(), key()) -> default().
get(Id, #default{}=Default, Key) ->
    dispatch_operation(Id, Default, get_operation(Key)).

-spec get(pipeline(), key()) -> pipeline().
get(#pipeline{}=State, Key) ->
    store_operation(State, get_operation(Key)).

-spec set(operation_id(), default(), key(), any()) -> default().
set(Id, #default{}=Default, Key, Value) ->
    dispatch_operation(Id, Default, set_operation(Key, Value)).

-spec set(pipeline(), key(), any()) -> pipeline().
set(#pipeline{}=State, Key, Value) ->
    store_operation(State, set_operation(Key, Value)).

-spec delete(operation_id(), default(), key()) -> default().
delete(Id, #default{}=Default, Key) ->
    dispatch_operation(Id, Default, delete_operation(Key)).

-spec delete(pipeline(), key()) -> pipeline().
delete(#pipeline{}=State, Key) ->
    store_operation(State, delete_operation(Key)).

-spec pipeline(default()) -> pipeline().
pipeline(#default{client=Client}) ->
    #pipeline{client=Client}.

-spec commit(operation_id(), pipeline()) -> default().
commit(Id, #pipeline{client=Client, stored=Operations}) ->
    {Commands, Handlers, Count} = lists:foldl(fun({C, H}, {C0, H0, Num}) -> {[C | C0], [H | H0], Num + 1} end, {[], [], 0}, Operations),
    {ok, NewClient} = redis_client:send({Id, Handlers}, Commands, Count, Client),
    #default{client=NewClient}.

% private methods

-spec get_operation(key()) -> operation().
get_operation(Key) ->
    {["GET", Key], get_command_result}.

-spec get_command_result({ok, undefined | binary()} | binary()) -> undefined | binary().
get_command_result(Result) ->
    Result.

-spec set_operation(key(), any()) -> operation().
set_operation(Key, Value) ->
    {["SET", Key, Value], set_command_result}.

-spec set_command_result({ok, binary()} | binary()) -> binary().
set_command_result(<<"OK">>) ->
    ok.

-spec delete_operation(key()) -> operation().
delete_operation(Key) ->
    {["DEL", Key], delete_command_result}.

-spec delete_command_result({ok, binary()}) -> 0 | 1.
delete_command_result(<<"0">>) ->
    0;
delete_command_result(<<"1">>) ->
    1.

-spec map_results([redis_client:result()]) -> [#store_result{}].
map_results(Results) ->
    lists:map(fun({{Id, H}, R}) -> handle_result(Id, H, R, []) end, Results).

-spec handle_result(operation_id(), [atom()], [result()], [result()]) -> #store_result{}.
handle_result(Id, [], [], Results) ->
    #store_result{id=Id, result=lists:reverse(Results)};
handle_result(Id, [Handler | HandlersTail], [Result | ResultsTail], Results) ->
    handle_result(Id, HandlersTail, ResultsTail, [redis_store:Handler(Result) | Results]).

-spec dispatch_operation(operation_id(), default(), operation()) -> default().
dispatch_operation(Id, #default{client=Client}=Default, {Commands, Handler}) ->
    {ok, NewClient} = redis_client:send({Id, [Handler]}, Commands, Client),
    Default#default{client=NewClient}.

-spec store_operation(pipeline(), operation()) -> pipeline().
store_operation(#pipeline{stored=Stored}=Pipeline, Operation) ->
    Pipeline#pipeline{stored=[Operation | Stored]}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_default_test() ->
    State = connect(test_host, test_port),
    State2 = get(42, State, "foo"),
    {[#store_result{id=42, result=[<<"bar">>]}], State} = handle_info({tcp, test_socket, <<"$3\r\nbar\r\n">>}, State2).

set_default_test() ->
    State = connect(test_host, test_port),
    State2 = set(42, State, "foo", "bar"),
    {[#store_result{id=42, result=[ok]}], State} = handle_info({tcp, test_socket, <<"+OK\r\n">>}, State2).

delete_default_test() ->
    State = connect(test_host, test_port),
    State2 = delete(42, State, "foo"),
    {[#store_result{id=42, result=[1]}], State} = handle_info({tcp, test_socket, <<":1\r\n">>}, State2).

-endif.

