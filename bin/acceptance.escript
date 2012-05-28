#!/usr/bin/env escript
%%! -sname acceptance -setcookie dtm_redis

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

main([Param]) when is_list(Param) ->
	Node = list_to_atom(Param),
    io:format("Starting dtm-redis acceptance tests running on ~p~n", [Node]),
    find_shell(Node),
    Tests = [
        test_get_set(),
        test_delete(),
        test_transaction(),
        test_watch(),
        test_unwatch()
    ],
    lists:foreach(fun(Test) -> rpc:call(Node, erlang, apply, [Test, []]) end, Tests),
    io:format("All tests passed~n", []);
main(Any) ->
    io:format("Invalid program arugments ~p~n", [Any]),
    halt(1).

find_shell(Node) ->
    find_shell(Node, 10).

find_shell(Node, 0) ->
    io:format("Unable to location shell on node ~p", [Node]),
    halt(1);
find_shell(Node, N) ->
    case rpc:call(Node, erlang, whereis, [shell]) of
        Pid when is_pid(Pid) -> ok;
        _Else ->
            timer:sleep(500),
            find_shell(Node, N - 1)
    end.

test_get_set() ->
    fun() ->
        io:format("### beginning test_get_set~n", []),
        ok = dtm_redis:set(foo, bar),
        {ok, <<"bar">>} = dtm_redis:get(foo),
        io:format("test_get_set passed ###~n", [])
    end.

test_delete() ->
    fun() ->
        io:format("### beginning test_delete~n", []),
        ok = dtm_redis:set(foo, bar),
        1 = dtm_redis:delete(foo),
        undefined = dtm_redis:get(foo),
        io:format("test_delete passed ###~n", [])
    end.

test_transaction() ->
    fun() ->
        io:format("### beginning test_transaction~n", []),
        ok = dtm_redis:set(foo, baz),
        ok = dtm_redis:multi(),
        stored = dtm_redis:get(foo),
        stored = dtm_redis:set(foo, bar),
        {ok, [<<"baz">>, ok]} = dtm_redis:exec(),
        {ok, <<"bar">>} = dtm_redis:get(foo),
        io:format("test_transaction passed ###~n", [])
    end.

test_watch() ->
    fun() ->
        io:format("### beginning test_watch~n", []),
        ok = dtm_redis:watch(foo),
        ok = dtm_redis:set(foo, baz),
        ok = dtm_redis:multi(),
        stored = dtm_redis:set(foo, bar),
        undefined = dtm_redis:exec(),
        {ok, <<"baz">>} = dtm_redis:get(foo),
        io:format("test_watch passed ###~n", [])
    end.

test_unwatch() ->
    fun() ->
        io:format("### beginning test_unwatch~n", []),
        ok = dtm_redis:watch(foo),
        ok = dtm_redis:set(foo, baz),
        ok = dtm_redis:unwatch(),
        ok = dtm_redis:multi(),
        stored = dtm_redis:set(foo, bar),
        {ok, [ok]} = dtm_redis:exec(),
        {ok, <<"bar">>} = dtm_redis:get(foo),
        io:format("test_unwatch passed ###~n", [])
    end.

