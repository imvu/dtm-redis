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

-module(redis_client).

-export([connect/3, send/3, send/4, recv/2]).

% Types

-type command_id() :: any().
-type command() :: [any()].
-type reply() :: binary() | [binary()].
-type result() :: {command_id(), [reply()]}.
-type parser() :: any().

-record(request, {
    id :: command_id(),
    expected :: pos_integer(),
    received :: non_neg_integer(),
    replies :: [reply()]
}).

-type request() :: #request{}.

-record(redis_client_state, {
    socket :: gen_tcp:socket(),
    read_state :: parser(),
    pending :: [request()]
}).


-opaque redis_state() :: #redis_client_state{}.

-export_type([redis_state/0, reply/0, result/0]).

% Public API

-spec connect(inet:hostname() | atom(), inet:port_number() | atom(), timeout()) -> {ok, redis_state()} | {error, inet:posix()}.
connect(Host, Port, _Timeout) when is_atom(Host) and is_atom(Port) ->
    {ok, init(test_socket)};
connect(Host, Port, Timeout) ->
    error_logger:info_msg("connecting to redis ~p:~p", [Host, Port]),
    case gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, 0}, {send_timeout, 0}, {send_timeout_close, true}], Timeout) of
        {ok, Socket} -> {ok, init(Socket)};
        Error -> Error
    end.

-spec send(command_id(), command(), redis_state()) -> {ok, redis_state()} | {error, inet:posix()}.
send(Id, Command, #redis_client_state{}=State) ->
    send(Id, [Command], 1, State).

-spec send(command_id(), [command()], integer(), redis_state()) -> {ok, redis_state()} | {error, inet:posix()}.
send(Id, _Commands, Count, #redis_client_state{socket=Socket}=State) when is_atom(Socket) and (Count > 0) ->
    {ok, push_request(Id, Count, State)};
send(Id, Commands, Count, #redis_client_state{socket=Socket}=State) when (Count > 0) ->
    Data = [create_multibulk(Command) || Command <- Commands],
    case gen_tcp:send(Socket, Data) of
        ok -> {ok, push_request(Id, Count, State)};
        Error -> Error
    end.

-spec recv({tcp, gen_tcp:socket(), binary()} | {tcp_error, inet:posix(), gen_tcp:socket()} | {tcp_closed, gen_tcp:socket()}, redis_state()) -> {[result()], redis_state()}.
recv({tcp, Socket, Data}, #redis_client_state{socket=Socket}=State) ->
    parse_reply([], Data, State);
recv({tcp_error, Reason, Socket}, #redis_client_state{socket=Socket}=State) ->
    error_logger:error_msg("redis client socket tpc_error: ~p", [Reason]),
    State;
recv({tcp_closed, Socket}, #redis_client_state{socket=Socket}) ->
    error_logger:info_msg("redis client socket closed", []),
    #redis_client_state{socket=none, read_state=eredis_parser:init(), pending=[]}.

% Private methods

-spec init(gen_tcp:socket() | test_socket) -> redis_state().
init(Socket) ->
    #redis_client_state{socket=Socket, read_state=eredis_parser:init(), pending=[]}.

-spec create_multibulk(command()) -> iolist().
create_multibulk([]) ->
    erlang:throw(empty_command);
create_multibulk(Command) when is_list(Command) ->
    eredis:create_multibulk(Command).

-spec push_request(command_id(), non_neg_integer(), redis_state()) -> redis_state().
push_request(Id, Count, #redis_client_state{pending=Pending}=State) ->
    State#redis_client_state{pending=Pending ++ [#request{id=Id, expected=Count, received=0, replies=[]}]}.

-spec parse_reply([result()], binary(), redis_state()) -> {[result()], redis_state()}.
parse_reply(Replies, <<>>, State) ->
    {lists:reverse(Replies), State};
parse_reply(Results, Data, #redis_client_state{read_state=Parser}=State) ->
    case eredis_parser:parse(Parser, Data) of
        {continue, NewParser} -> {Results, State#redis_client_state{read_state=NewParser}};
        {ok, Reply, NewParser} -> handle_reply(Results, Reply, NewParser, <<>>, State);
        {ok, Reply, Remaining, NewParser} -> handle_reply(Results, Reply, NewParser, Remaining, State)
    end.

-spec handle_reply([result()], reply(), parser(), binary(), redis_state()) -> {[result()], redis_state()}.
handle_reply(Results, Reply, Parser, Remaining, State) ->
    {NewResults, NewState} = push_reply(Results, Reply, State#redis_client_state{read_state=Parser}),
    parse_reply(NewResults, Remaining, NewState).

-spec push_reply([result()], reply(), redis_state()) -> {[result()], redis_state()}.
push_reply(Results, Reply, #redis_client_state{pending=[#request{id=Id, expected=Expected, received=Received, replies=Replies} | Waiting]}=State) when (Expected =:= Received + 1) ->
    {[{Id, lists:reverse([Reply | Replies])} | Results], State#redis_client_state{pending=Waiting}};
push_reply(Results, Reply, #redis_client_state{pending=[#request{received=Received, replies=Replies}=Request | Waiting]}=State) ->
    {Results, State#redis_client_state{pending=[Request#request{received=Received + 1, replies=[Reply | Replies]} | Waiting]}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_state(Pending) ->
    {ok, State} = connect(test_host, test_port, 5),
    lists:foldl(fun({Id, Commands}, S) ->
            {ok, NewState} = send(Id, Commands, length(Commands), S),
            NewState
        end, State, Pending).

test_reply(Data) ->
    {tcp, test_socket, Data}.

verify_partial_and_complete_single_reply(Data, Result) ->
    Expected = {[{42, [Result]}], test_state([{43, ["bar"]}])},
    Expected = lists:foldl(fun(Byte, {[], State}) ->
            recv(test_reply(list_to_binary([Byte])), State)
        end, {[], test_state([{42, ["foo"]}, {43, ["bar"]}])}, binary_to_list(Data)).

receive_status_reply_test() ->
    verify_partial_and_complete_single_reply(<<"+OK\r\n">>, <<"OK">>).

receive_bulk_reply_test() ->
    verify_partial_and_complete_single_reply(<<"$3\r\nbar\r\n">>, <<"bar">>).

receive_multi_bulk_reply_test() ->
    verify_partial_and_complete_single_reply(<<"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n">>, [<<"foo">>, <<"bar">>, <<"baz">>]).

receive_multiple_replies_test() ->
    Expected = {[{42, [<<"OK">>]}, {43, [<<"bar">>]}, {44, [[<<"foo">>, <<"bar">>]]}], test_state([{45, ["fooey"]}])},
    Expected = recv(test_reply(<<"+OK\r\n", "$3\r\nbar\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>), test_state([{42, ["foo"]}, {43, ["bar"]}, {44, ["baz"]}, {45, ["fooey"]}])).

receive_complete_and_partial_them_remainder_reply_test() ->
    {continue, PartialParser} = eredis_parser:parse(eredis_parser:init(), <<"*2\r\n$3\r\nfoo">>),
    State = test_state([{43, ["bar"]}, {44, ["baz"]}]),
    State2 = State#redis_client_state{read_state=PartialParser},
    Expected = {[{42, [<<"foo">>]}], State2},
    Expected = recv(test_reply(<<"$3\r\nfoo\r\n*2\r\n$3\r\nfoo">>), test_state([{42, ["foo"]}, {43, ["bar"]}, {44, ["baz"]}])),
    Expected2 = {[{43, [[<<"foo">>, <<"bar">>]]}], test_state([{44, ["baz"]}])},
    Expected2 = recv(test_reply(<<"\r\n$3\r\nbar\r\n">>), State2).

verify_partial_and_complete_pipeline_test() ->
    Data = <<"+OK\r\n*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nbar\r\n">>,
    Expected = {[{42, [<<"OK">>, [<<"foo">>, <<"bar">>, <<"baz">>], <<"bar">>]}], test_state([{43, ["bar"]}])},
    Expected = lists:foldl(fun(Byte, {[], State}) ->
            recv(test_reply(list_to_binary([Byte])), State)
        end, {[], test_state([{42, [["set", "foo", "bar"], ["smembers", "foobar"], ["get", "foo"]]}, {43, ["bar"]}])}, binary_to_list(Data)).

-endif.

