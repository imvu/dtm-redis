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

-include("protocol.hrl").

-export([connect/3, send/3, send/4, recv/2]).

% Types

-type command_id() :: any().
-type command() :: [any()].
-type result() :: {command_id(), [reply()]}.
-type parse_state() :: any().

-record(single_request, {
    id :: command_id()
}).

-record(multi_request, {
    id :: command_id(),
    expected :: pos_integer(),
    received :: non_neg_integer(),
    replies :: [reply()]
}).

-type request() :: #single_request{} | #multi_request{}.

-record(redis_client_state, {
    socket :: gen_tcp:socket(),
    parse_state :: parse_state(),
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
send(Id, _Command, #redis_client_state{socket=Socket}=State) when is_atom(Socket) ->
    {ok, push_request(Id, State)};
send(Id, Command, #redis_client_state{socket=Socket}=State) ->
    Data = redis_stream:create_multi_bulk(Command),
    case gen_tcp:send(Socket, Data) of
        ok -> {ok, push_request(Id, State)};
        Error -> Error
    end.

-spec send(command_id(), [command()], integer(), redis_state()) -> {ok, redis_state()} | {error, inet:posix()}.
send(Id, _Commands, Count, #redis_client_state{socket=Socket}=State) when is_atom(Socket) and (Count > 0) ->
    {ok, push_request(Id, Count, State)};
send(Id, Commands, Count, #redis_client_state{socket=Socket}=State) when (Count > 0) ->
    Data = [redis_stream:create_multi_bulk(Command) || Command <- Commands],
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
    init(none).

% Private methods

-spec init(none | gen_tcp:socket() | test_socket) -> redis_state().
init(Socket) ->
    #redis_client_state{socket=Socket, parse_state=redis_stream:init_reply_stream(), pending=[]}.

-spec push_request(command_id(), redis_state()) -> redis_state().
push_request(Id, #redis_client_state{pending=Pending}=State) ->
     State#redis_client_state{pending=Pending ++ [#single_request{id=Id}]}.

-spec push_request(command_id(), non_neg_integer(), redis_state()) -> redis_state().
push_request(Id, Count, #redis_client_state{pending=Pending}=State) ->
    State#redis_client_state{pending=Pending ++ [#multi_request{id=Id, expected=Count, received=0, replies=[]}]}.

-spec parse_reply([result()], binary(), redis_state()) -> {[result()], redis_state()}.
parse_reply(Replies, <<>>, State) ->
    {lists:reverse(Replies), State};
parse_reply(Results, Data, #redis_client_state{parse_state=ParseState}=State) ->
    case redis_stream:parse_reply(ParseState, Data) of
        {partial, NewParseState} -> {Results, State#redis_client_state{parse_state=NewParseState}};
        {Reply, Remaining, NewParseState} -> handle_reply(Results, Reply, NewParseState, Remaining, State)
    end.

-spec handle_reply([result()], reply(), parse_state(), binary(), redis_state()) -> {[result()], redis_state()}.
handle_reply(Results, Reply, ParseState, Remaining, State) ->
    {NewResults, NewState} = push_reply(Results, Reply, State#redis_client_state{parse_state=ParseState}),
    parse_reply(NewResults, Remaining, NewState).

-spec push_reply([result()], reply(), redis_state()) -> {[result()], redis_state()}.
push_reply(Results, Reply, #redis_client_state{pending=[#single_request{id=Id} | Waiting]}=State) ->
    {[{Id, Reply} | Results], State#redis_client_state{pending=Waiting}};
push_reply(Results, Reply, #redis_client_state{pending=[#multi_request{id=Id, expected=Expected, received=Received, replies=Replies} | Waiting]}=State) when (Expected =:= Received + 1) ->
    {[{Id, lists:reverse([Reply | Replies])} | Results], State#redis_client_state{pending=Waiting}};
push_reply(Results, Reply, #redis_client_state{pending=[#multi_request{received=Received, replies=Replies}=Request | Waiting]}=State) ->
    {Results, State#redis_client_state{pending=[Request#multi_request{received=Received + 1, replies=[Reply | Replies]} | Waiting]}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_state(Pending) ->
    {ok, State} = connect(test_host, test_port, 5),
    lists:foldl(fun({Id, Commands}, S) ->
            case length(Commands) of
                1 ->
                    {ok, NewState} = send(Id, Commands, S),
                    NewState;
                Len ->
                    {ok, NewState} = send(Id, Commands, Len, S),
                    NewState
            end
        end, State, Pending).

test_reply(Data) ->
    {tcp, test_socket, Data}.

verify_partial_and_complete_single_reply(Data, Result) ->
    Expected = {[{42, Result}], test_state([{43, ["bar"]}])},
    io:format("Expected:~p~n", [Expected]),
    Expected = lists:foldl(fun(Byte, {[], State}) ->
            recv(test_reply(list_to_binary([Byte])), State)
        end, {[], test_state([{42, ["foo"]}, {43, ["bar"]}])}, binary_to_list(Data)).

receive_status_reply_test() ->
    verify_partial_and_complete_single_reply(<<"+OK\r\n">>, #redis_status{message= <<"OK">>}).

receive_bulk_reply_test() ->
    verify_partial_and_complete_single_reply(<<"$3\r\nbar\r\n">>, #redis_bulk{content= <<"bar">>}).

receive_multi_bulk_reply_test() ->
    verify_partial_and_complete_single_reply(<<"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n">>, #redis_multi_bulk{count=3, items=[
        #redis_bulk{content= <<"foo">>},
        #redis_bulk{content= <<"bar">>},
        #redis_bulk{content= <<"baz">>}
    ]}).

receive_multiple_replies_test() ->
    Expected = {[{42, #redis_status{message= <<"OK">>}}, {43, #redis_bulk{content= <<"bar">>}}, {44, #redis_multi_bulk{count=2, items=[
        #redis_bulk{content= <<"foo">>},
        #redis_bulk{content= <<"bar">>}]}}
    ], test_state([{45, ["fooey"]}])},
    Expected = recv(test_reply(<<"+OK\r\n", "$3\r\nbar\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>), test_state([{42, ["foo"]}, {43, ["bar"]}, {44, ["baz"]}, {45, ["fooey"]}])).

receive_complete_and_partial_them_remainder_reply_test() ->
    {partial, Partial} = redis_stream:parse_reply(redis_stream:init_reply_stream(), <<"*2\r\n$3\r\nfoo">>),
    State = test_state([{43, ["bar"]}, {44, ["baz"]}]),
    State2 = State#redis_client_state{parse_state=Partial},
    Expected = {[{42, #redis_bulk{content= <<"foo">>}}], State2},
    Expected = recv(test_reply(<<"$3\r\nfoo\r\n*2\r\n$3\r\nfoo">>), test_state([{42, ["foo"]}, {43, ["bar"]}, {44, ["baz"]}])),
    Expected2 = {[{43, #redis_multi_bulk{count=2, items=[
        #redis_bulk{content= <<"foo">>},
        #redis_bulk{content= <<"bar">>}
    ]}}], test_state([{44, ["baz"]}])},
    Expected2 = recv(test_reply(<<"\r\n$3\r\nbar\r\n">>), State2).

verify_partial_and_complete_pipeline_test() ->
    Data = <<"+OK\r\n", "*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n", "$3\r\nbar\r\n">>,
    Expected = {[{42, [
        #redis_status{message= <<"OK">>},
        #redis_multi_bulk{count=3, items=[
            #redis_bulk{content= <<"foo">>},
            #redis_bulk{content= <<"bar">>},
            #redis_bulk{content= <<"baz">>}
        ]},
        #redis_bulk{content= <<"bar">>}
    ]}], test_state([{43, ["bar"]}])},
    StartState = test_state([{42, [["set", "foo", "bar"], ["smembers", "foobar"], ["get", "foo"]]}, {43, ["bar"]}]),
    Expected = lists:foldl(fun(Byte, {[], State}) ->
            recv(test_reply(<<Byte>>), State)
        end, {[], StartState}, binary_to_list(Data)).

-endif.

