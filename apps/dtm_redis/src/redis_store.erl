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
-export([connect/2, handle_info/2, handle_operation/3, handle_operation/2]).
-export([pipeline/1, commit/2]).

-include("protocol.hrl").
-include("store.hrl").

-define(CONNECT_TIMEOUT, 5000).  % 5 seconds

% types

-type request_id() :: any().
-type operation() :: {[any()], atom()}.
-type result() :: {request_id(), [any()]}.

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
    {[#store_result{id=Id, result=Result} || {Id, Result} <-Results], Default#default{client=NewClient}};
handle_info({tcp, _Data, _Socket}=Packet, #pipeline{client=Client}=Pipeline) ->
    {Results, NewClient} = redis_client:recv(Packet, Client),
    {Results, Pipeline#pipeline{client=NewClient}};
handle_info({tcp_error, _Reason, _Socket}=Message, #default{client=Client}=Default) ->
    {[], Default#default{client=redis_client:recv(Message, Client)}};
handle_info({tcp_error, _Reason, _Socket}=Message, #pipeline{client=Client}=Pipeline) ->
    {[], Pipeline#pipeline{client=redis_client:recv(Message, Client)}};
handle_info({tcp_closed, _Socket}=Message, #default{client=Client}=Default) ->
    {[], Default#default{client=redis_client:recv(Message, Client)}};
handle_info({tcp_closed, _Socket}=Message, #pipeline{client=Client}=Pipeline) ->
    {[], Pipeline#pipeline{client=redis_client:recv(Message, Client)}}.

-spec handle_operation(request_id(), default(), #operation{}) -> default().
handle_operation(Id, #default{client=Client}=Default, #operation{}=Operation) ->
    {ok, NewClient} = redis_client:send(Id, make_command(Operation), Client),
    Default#default{client=NewClient}.

-spec handle_operation(pipeline(), #operation{}) -> pipeline().
handle_operation(#pipeline{stored=Stored}=Pipeline, #operation{}=Operation) ->
    Pipeline#pipeline{stored=[Operation | Stored]}.

-spec pipeline(default()) -> pipeline().
pipeline(#default{client=Client}) ->
    #pipeline{client=Client}.

-spec commit(request_id(), pipeline()) -> default().
commit(Id, #pipeline{client=Client, stored=Operations}) ->
    {Commands, Count} = lists:foldl(fun(Operation, {Cmds, Cnt}) ->
            {[make_command(Operation) | Cmds], Cnt + 1}
        end, {[], 0}, Operations),
    {ok, NewClient} = redis_client:send(Id, Commands, Count, Client),
    #default{client=NewClient}.

% private methods

-spec make_command(#operation{}) -> [binary()].
make_command(#operation{command=Command, key=none, arguments=[]}) ->
    [Command];
make_command(#operation{command=Command, key=Key, arguments=Arguments}) ->
    [Command, Key | Arguments].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_command_single_test() ->
    [<<"foo">>] = make_command(#operation{command= <<"foo">>, key=none, arguments=[]}).

make_command_with_key_test() ->
    [<<"foo">>, <<"bar">>] = make_command(#operation{command= <<"foo">>, key= <<"bar">>, arguments=[]}).

make_command_with_key_and_args_test() ->
    [<<"foo">>, <<"bar">>, <<"baz">>] = make_command(#operation{command= <<"foo">>, key= <<"bar">>, arguments=[<<"baz">>]}).

get_default_test() ->
    State = connect(test_host, test_port),
    State2 = handle_operation(42, State, #operation{command= <<"GET">>, key= <<"foo">>, arguments=[]}),
    {[#store_result{id=42, result=#redis_bulk{content= <<"bar">>}}], State} = handle_info({tcp, test_socket, <<"$3\r\nbar\r\n">>}, State2).

set_default_test() ->
    State = connect(test_host, test_port),
    State2 = handle_operation(42, State, #operation{command= <<"SET">>, key = <<"foo">>, arguments=[<<"bar">>]}),
    {[#store_result{id=42, result=#redis_status{message= <<"OK">>}}], State} = handle_info({tcp, test_socket, <<"+OK\r\n">>}, State2).

delete_default_test() ->
    State = connect(test_host, test_port),
    State2 = handle_operation(42, State, #operation{command= <<"DEL">>, key= <<"foo">>, arguments=[]}),
    {[#store_result{id=42, result=#redis_integer{value= <<"1">>}}], State} = handle_info({tcp, test_socket, <<":1\r\n">>}, State2).

-endif.

