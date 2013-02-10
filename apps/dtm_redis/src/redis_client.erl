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

-export([connect/3, send/3, recv/2]).

% Types

-type command_id() :: any().
-type result() :: {command_id(), binary() | [binary()]}.
-type parser() :: any().

-record(redis_client_state, {
    socket :: gen_tcp:socket(),
    read_state :: parser(),
    pending :: [command_id()]
}).

-type redis_state() :: #redis_client_state{}.

% Public API

-spec connect(inet:hostname(), inet:port_number(), timeout()) -> {ok, redis_state()} | {error, inet:posix()}.
connect(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, 0}, {send_timeout, 0}, {send_timeout_close, true}], Timeout) of
        {ok, Socket} -> {ok, #redis_client_state{socket=Socket, read_state=eredis_parser:init(), pending=[]}};
        Error -> Error
    end.

-spec send([any()], command_id(), redis_state()) -> {ok, redis_state()} | {error, inet:posix()}.
send(Command, Id, #redis_client_state{socket=Socket, pending=Pending}=State) ->
    case gen_tcp:send(Socket, eredis:create_multibulk(Command)) of
        ok -> {ok, State#redis_client_state{pending=Pending ++ [Id]}};
        Error -> Error
    end.

-spec recv({tcp, gen_tcp:socket(), binary()} | {tcp_error, inet:posix(), gen_tcp:socket()} | {tcp_closed, gen_tcp:socket()}, redis_state()) -> {[result()], redis_state()}.
recv({tcp, Socket, Data}, #redis_client_state{socket=Socket}=State) ->
    parse_reply([], Data, State);
recv({tcp_error, Reason, Socket}, #redis_client_state{socket=Socket}) ->
    %TODO: do something other than log this error
    error_logger:error_msg("redis client socket tpc_error: ~p", [Reason]);
recv({tcp_closed, Socket}, #redis_client_state{socket=Socket}) ->
    %TODO: do something other than log that the socket closed
    error_logger:info_msg("redis client socket closed", []).

% Private methods
-spec parse_reply([result()], binary(), redis_state()) -> {[result()], redis_state()}.
parse_reply(Results, <<>>, State) ->
    {lists:reverse(Results), State};
parse_reply(Results, Data, #redis_client_state{read_state=Parser, pending=[Next | _Waiting]}=State) ->
    case eredis_parser:parse(Parser, Data) of
        {continue, NewParser} -> {Results, State#redis_client_state{read_state=NewParser}};
        {ok, Result, NewParser} -> parse_reply(push_result(Next, Result, Results), <<>>, pop_request(NewParser, State));
        {ok, Result, Remaining, NewParser} -> parse_reply(push_result(Next, Result, Results), Remaining, pop_request(NewParser, State))
    end.

-spec push_result(command_id(), binary() | [binary()], [result()]) -> [result()].
push_result(Next, Result, Results) ->
    [{Next, Result} | Results].

-spec pop_request(parser(), redis_state()) -> redis_state().
pop_request(Parser, #redis_client_state{pending=[_Next | Waiting]}=State) ->
    State#redis_client_state{read_state=Parser, pending=Waiting}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_state(Pending) ->
    #redis_client_state{socket=fake_socket, read_state=eredis_parser:init(), pending=Pending}.

test_reply(Data) ->
    {tcp, fake_socket, Data}.

verify_partial_and_complete_reply(Data, Result) ->
    Expected = {[{42, Result}], test_state([43])},
    Expected = lists:foldl(fun(Byte, {[], S}) ->
            recv(test_reply(list_to_binary([Byte])), S)
        end, {[], test_state([42, 43])}, binary_to_list(Data)).

receive_status_reply_test() ->
    verify_partial_and_complete_reply(<<"+OK\r\n">>, <<"OK">>).

receive_bulk_reply_test() ->
    verify_partial_and_complete_reply(<<"$3\r\nbar\r\n">>, <<"bar">>).

receive_multi_bulk_reply_test() ->
    verify_partial_and_complete_reply(<<"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n">>, [<<"foo">>, <<"bar">>, <<"baz">>]).

receive_multiple_replies_test() ->
    Expected = {[{42, <<"OK">>}, {43, <<"bar">>}, {44, [<<"foo">>, <<"bar">>]}], test_state([45])},
    Expected = recv(test_reply(<<"+OK\r\n", "$3\r\nbar\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>), test_state([42, 43, 44, 45])).

receive_complete_and_partial_them_remainder_reply_test() ->
    {continue, PartialParser} = eredis_parser:parse(eredis_parser:init(), <<"*2\r\n$3\r\nfoo">>),
    State = #redis_client_state{socket=fake_socket, read_state=PartialParser, pending=[43, 44]},
    Expected = {[{42, <<"foo">>}], State},
    Expected = recv(test_reply(<<"$3\r\nfoo\r\n*2\r\n$3\r\nfoo">>), test_state([42, 43, 44])),
    Expected2 = {[{43, [<<"foo">>, <<"bar">>]}], test_state([44])},
    Expected2 = recv(test_reply(<<"\r\n$3\r\nbar\r\n">>), State).

-endif.

