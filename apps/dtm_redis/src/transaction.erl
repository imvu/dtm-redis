-module(transaction).
-export([execute/2, execute/3, reply/1, reply/2, noreply/1, noreply/2]).

% types

-type continue() :: {continue, mfa(), operation()}.
-type done() :: {done, any()}.
-type error() :: {error, any()}.
-type operation() :: fun((any()) -> continue() | done() | error()).

-type reply() :: {reply, any(), any()} | {reply, any(), any(), timeout() | hibernate}.
-type noreply() :: {noreply, any()} | {noreply, any(), timeout() | hibernate}.
-type stop() :: {stop, any(), any()} | {stop, any(), any(), any()}.
-type result() :: reply() | noreply() | stop().
-type result_fun() :: fun((any()) -> result()).

% Public API

-spec execute(operation(), result_fun()) -> result().
execute(StartFun, ResultFun) ->
    execute(StartFun, [], ResultFun).

-spec execute(operation(), list(), result_fun()) -> result().
execute(StartFun, StartArgs, ResultFun) ->
    Result = try
        perform(StartFun, StartArgs)
    catch
        throw:Term -> {error, {throw, Term, erlang:get_stacktrace()}}
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

% Internal functions

-spec perform(operation(), list()) -> any().
perform(Operation, Args) ->
    case erlang:apply(Operation, Args) of
        {continue, {M, F, A}, Continue} -> perform(Continue, [erlang:apply(M, F, A)]);
        {done, Result} -> Result;
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

execute_continue_with_args_test() ->
    Continue = fun([1, 2]) -> {done, {reply, result, new_state}} end,
    Start = fun(param) -> {continue, {lists, sort, [[2, 1]]}, Continue} end,
    {reply, result, new_state} = execute(Start, [param], reply(old_state)).

execute_continue_no_args_test() ->
    Continue = fun([1, 2]) -> {done, {reply, result, new_state}} end,
    Start = fun() -> {continue, {lists, sort, [[2, 1]]}, Continue} end,
    {reply, result, new_state} = execute(Start, reply(old_state)).

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

-endif.

