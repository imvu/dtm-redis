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

-module(transaction).

-export([execute/2, execute/3, reply/1, reply/2, noreply/1, noreply/2, ok/0]).

% types

-type continue() :: {continue, mfa(), operation()}.
-type done() :: {done, any()}.
-type error() :: {error, any()}.
-type operation() :: fun((any()) -> continue() | done() | error()) | fun(() -> continue() | done() | error()).

-type reply() :: {reply, any(), any()} | {reply, any(), any(), timeout() | hibernate}.
-type noreply() :: {noreply, any()} | {noreply, any(), timeout() | hibernate}.
-type stop() :: {stop, any(), any()} | {stop, any(), any(), any()}.
-type ok() :: {ok, any()} | {ok, any(), timeout() | hibernate}.
-type result() :: reply() | noreply() | stop() | ok().
-type message() :: {pid() | atom(), any()} | {pid() | atom(), any(), [nosuspend | noconnect]}.
-type result_fun() :: fun((any()) -> result()) | fun((any(), [message()]) -> result()).

% Public API

-spec execute(operation(), result_fun()) -> result().
execute(StartFun, ResultFun) ->
    execute(StartFun, [], ResultFun).

-spec execute(operation(), list(), result_fun()) -> result().
execute(StartFun, StartArgs, ResultFun) ->
    Result = try
        case perform(StartFun, StartArgs) of
            {done, Res} -> Res;
            {done, Res, Messages} ->
                lists:foreach(fun({To, Message}) -> erlang:send(To, Message);
                                 ({To, Message, Options}) -> erlang:send(To, Message, Options) end, Messages),
                Res;
            Other -> Other
        end
    catch
        throw:Term ->
            Error = {throw, Term, erlang:get_stacktrace()},
            error_logger:error_msg("exception in transaction: ~p", [Error]),
            {error, Error}
    end,
    ResultFun(Result).

-spec reply(any()) -> result_fun().
reply(OldState) ->
    reply_impl(fun({error, _Reason}=Error) -> {reply, Error, OldState} end).

-spec reply(any(), timeout() | hibernate) -> result_fun().
reply(OldState, Timeout) ->
    reply_impl(fun({error, _Reason}=Error) -> {reply, Error, OldState, Timeout} end).

-spec noreply(any()) -> result_fun().
noreply(OldState) ->
    noreply_impl(fun({error, _Reason}=_Error) -> {noreply, OldState} end).

-spec noreply(any(), timeout() | hibernate) -> result_fun().
noreply(OldState, Timeout) ->
    noreply_impl(fun({error, _Reason}=_Error) -> {noreply, OldState, Timeout} end).

-spec ok() -> result_fun().
ok() ->
    fun({ok, _State}=Result) -> Result;
       ({ok, _State, _Timeout}=Result) -> Result;
       ({error, Reason}) -> {stop, Reason}
    end.

% Internal functions

-spec perform(operation(), list()) -> any().
perform(Operation, Args) ->
    case erlang:apply(Operation, Args) of
        {continue, {M, F, A}, Continue} -> perform(Continue, [erlang:apply(M, F, A)]);
        {continue, {M, F, A}, Continue, Messages} -> perform(Continue, [erlang:apply(M, F, A), Messages]);
        {done, _Result}=Done -> Done;
        {done, _Result, _Messages}=Done -> Done;
        {error, _Reason}=Error -> Error
    end.

-spec reply_impl(fun((error()) -> reply())) -> reply().
reply_impl(ErrorFun) ->
    fun({reply, _Reply, _NewState}=Result) -> Result;
       ({reply, _Reply, _NewState, _Timeout}=Result) -> Result;
       ({stop, _Reason, _Reply, _NewState}=Result) -> Result;
       ({error, _Reason}=Error) -> ErrorFun(Error)
    end.

-spec noreply_impl(fun((error()) -> noreply())) -> noreply().
noreply_impl(ErrorFun) ->
    fun({noreply, _NewState}=Result) -> Result;
       ({noreply, _NewState, _Timeout}=Result) -> Result;
       ({stop, _Reason, _NewState}=Result) -> Result;
       ({error, _Reason}=Error) -> ErrorFun(Error)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

receive_message(Message) ->
    ok = receive
        Message -> ok
    after 0 -> fail
    end.

execute_continue_with_args_test() ->
    Continue = fun([1, 2]) -> {done, {reply, result, new_state}} end,
    Start = fun(param) -> {continue, {lists, sort, [[2, 1]]}, Continue} end,
    {reply, result, new_state} = execute(Start, [param], reply(old_state)).

execute_continue_no_args_test() ->
    Continue = fun([1, 2]) -> {done, {reply, result, new_state}} end,
    Start = fun() -> {continue, {lists, sort, [[2, 1]]}, Continue} end,
    {reply, result, new_state} = execute(Start, reply(old_state)).

execute_done_with_messages_test() ->
    Start = fun() -> {done, {reply, result, new_state}, [{self(), message1}, {self(), message2}]} end,
    {reply, result, new_state} = execute(Start, reply(old_state)),
    receive_message(message1),
    receive_message(message2).

execute_done_with_messages_and_options_test() ->
    Start = fun() -> {done, {reply, result, new_state}, [{self(), message1, [nosuspend]}, {self(), message2, [noconnect]}]} end,
    {reply, result, new_state} = execute(Start, reply(old_state)),
    receive_message(message1),
    receive_message(message2).

execute_error_with_args_test() ->
    Start = fun(param) -> {error, foobar} end,
    {reply, {error, foobar}, old_state} = execute(Start, [param], reply(old_state)).

execute_error_no_args_test() ->
    Start = fun() -> {error, foobar} end,
    {reply, {error, foobar}, old_state} = execute(Start, reply(old_state)).

execute_catches_throws_test() ->
    Start = fun() -> erlang:throw(fooey) end,
    {reply, {error, {throw, fooey, _}}, old_state} = execute(Start, reply(old_state)).

execute_does_not_catch_errors_test() ->
    Start = fun() -> erlang:error(fooey) end,
    ok = try
        execute(Start, reply(old_state))
    catch
        error:fooey -> ok
    end.

execute_does_not_catch_exits_test() ->
    Start = fun() -> erlang:exit(because) end,
    ok = try
        execute(Start, reply(old_state))
    catch
        exit:because -> ok
    end.

call_reply(OldState, Result) ->
    Result = (reply(OldState))(Result).

reply_handles_reply_without_timeout_test() ->
    call_reply(old_state, {reply, foo, state}).

reply_handles_reply_with_timeout_test() ->
    call_reply(old_state, {reply, foo, state, 42}).

reply_handles_stop_test() ->
    call_reply(old_state, {stop, because, foo, state}).

reply_handles_error_without_timeout_test() ->
    {reply, {error, because}, old_state} = (reply(old_state))({error, because}).

reply_handles_error_with_timeout_test() ->
    {reply, {error, because}, old_state, 42} = (reply(old_state, 42))({error, because}).

noreply_handles_error_without_timeout_test() ->
    {noreply, old_state} = (noreply(old_state))({error, because}).

noreply_handles_error_with_timeout_test() ->
    {noreply, old_state, 42} = (noreply(old_state, 42))({error, because}).

ok_handles_ok_test() ->
    {ok, state} = (ok())({ok, state}).

ok_handles_ok_with_timeout_test() ->
    {ok, state, 42} = (ok())({ok, state, 42}).

ok_handles_error_test() ->
    {stop, because} = (ok())({error, because}).

-endif.

