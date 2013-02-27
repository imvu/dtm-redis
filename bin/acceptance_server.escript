#!/usr/bin/env escript
%%! -sname acceptance -setcookie dtm_redis

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

main([Param]) when is_list(Param) ->
	Node = list_to_atom(Param),
    io:format("Starting dtm-redis server acceptance tests running on ~p~n", [Node]),
    connect_node(Node),
    Tests = [
        test_get_set(),
        test_delete(),
        test_transaction(),
        test_watch(),
        test_unwatch()
    ],
    Connect = fun() ->
        {ok, Conn} = redis_client:connect("localhost", 6378, 5000),
        Conn
    end,
    Query = fun(Commands, Conn) ->
        QueryReceive = fun(ReceiveConn, Self) ->
            receive
                Message ->
                    case redis_client:recv(Message, ReceiveConn) of
                        {[], NewReceiveConn} -> Self(NewReceiveConn, Self);
                        {[{0, Reply}], NewReceiveConn} -> {Reply, NewReceiveConn}
                    end
            end
        end,
        {ok, NewConn} = redis_client:send(0, Commands, Conn),
        QueryReceive(NewConn, QueryReceive)
    end,
    Close = fun(Conn) ->
        redis_client:close(Conn)
    end,
    lists:foreach(fun(Test) -> ok = rpc:call(Node, erlang, apply, [Test, [Connect, Query, Close]]) end, Tests),
    io:format("All tests passed~n", []);
main(Any) ->
    io:format("Invalid program arugments ~p~n", [Any]),
    halt(1).

connect_node(Node) ->
    connect_node(Node, 10).

connect_node(Node, 0) ->
    io:format("Unable to locate server node ~p", [Node]),
    halt(1);
connect_node(Node, N) ->
    case rpc:call(Node, erlang, whereis, [dtm_redis_sup]) of
        Pid when is_pid(Pid) -> ok;
        _Else ->
            timer:sleep(500),
            connect_node(Node, N - 1)
    end.

test_get_set() ->
    fun(Connect, Query, Close) ->
        Conn = Connect(),
        io:format("### beginning test_get_set~n", []),
        {Ok, Conn2} = Query([<<"set">>, <<"foo">>, <<"bar">>], Conn),
        ok = dtm_redis:show_reply(Ok),
        {Bar, Conn3} = Query([<<"get">>, <<"foo">>], Conn2),
        "bar" = dtm_redis:show_reply(Bar),
        Close(Conn3),
        io:format("test_get_set passed ###~n", [])
    end.

test_delete() ->
    fun(Connect, Query, Close) ->
        io:format("### beginning test_delete~n", []),
        Conn = Connect(),
        {Ok, Conn2} = Query([<<"set">>, <<"foo">>, <<"bar">>], Conn),
        ok = dtm_redis:show_reply(Ok),
        {One, Conn3} = Query([<<"del">>, <<"foo">>], Conn2),
        1 = dtm_redis:show_reply(One),
        {Nil, Conn4} = Query([<<"get">>, <<"foo">>], Conn3),
        nil = dtm_redis:show_reply(Nil),
        Close(Conn4),
        io:format("test_delete passed ###~n", [])
    end.

test_transaction() ->
    fun(Connect, Query, Close) ->
        io:format("### beginning test_transaction~n", []),
        Conn = Connect(),
        {Ok, Conn2} = Query([<<"set">>, <<"foo">>, <<"baz">>], Conn),
        ok = dtm_redis:show_reply(Ok),
        {Ok2, Conn3} = Query([<<"multi">>], Conn2),
        ok = dtm_redis:show_reply(Ok2),
        {Queued, Conn4} = Query([<<"get">>, <<"foo">>], Conn3),
        queued = dtm_redis:show_reply(Queued),
        {Queued2, Conn5} = Query([<<"set">>, <<"foo">>, <<"bar">>], Conn4),
        queued = dtm_redis:show_reply(Queued2),
        {BazOk, Conn6} = Query([<<"exec">>], Conn5),
        ["baz", ok] = dtm_redis:show_reply(BazOk),
        {Bar, Conn7} = Query([<<"get">>, <<"foo">>], Conn6),
        "bar" = dtm_redis:show_reply(Bar),
        Close(Conn7),
        io:format("test_transaction passed ###~n", [])
    end.

test_watch() ->
    fun(Connect, Query, Close) ->
        io:format("### beginning test_watch~n", []),
        Conn = Connect(),
        {Ok, Conn2} = Query([<<"watch">>, <<"foo">>], Conn),
        ok = dtm_redis:show_reply(Ok),
        {Ok2, Conn3} = Query([<<"set">>, <<"foo">>, <<"baz">>], Conn2),
        ok = dtm_redis:show_reply(Ok2),
        {Ok3, Conn4} = Query([<<"multi">>], Conn3),
        ok = dtm_redis:show_reply(Ok3),
        {Queued, Conn5} = Query([<<"set">>, <<"foo">>, <<"bar">>], Conn4),
        queued = dtm_redis:show_reply(Queued),
        {Nil, Conn6} = Query([<<"exec">>], Conn5),
        nil = dtm_redis:show_reply(Nil),
        {Baz, Conn7} = Query([<<"get">>, <<"foo">>], Conn6),
        "baz" = dtm_redis:show_reply(Baz),
        Close(Conn7),
        io:format("test_watch passed ###~n", [])
    end.

test_unwatch() ->
    fun(Connect, Query, Close) ->
        io:format("### beginning test_unwatch~n", []),
        Conn = Connect(),
        {Ok, Conn2} = Query([<<"watch">>, <<"foo">>], Conn),
        ok = dtm_redis:show_reply(Ok),
        {Ok2, Conn3} = Query([<<"set">>, <<"foo">>, <<"baz">>], Conn2),
        ok = dtm_redis:show_reply(Ok2),
        {Ok3, Conn4} = Query([<<"unwatch">>], Conn3),
        ok = dtm_redis:show_reply(Ok3),
        {Ok4, Conn5} = Query([<<"multi">>], Conn4),
        ok = dtm_redis:show_reply(Ok4),
        {Queued, Conn6} = Query([<<"set">>, <<"foo">>, <<"bar">>], Conn5),
        queued = dtm_redis:show_reply(Queued),
        {Ok5, Conn7} = Query([<<"exec">>], Conn6),
        [ok] = dtm_redis:show_reply(Ok5),
        {Bar, Conn8} = Query([<<"get">>, <<"foo">>], Conn7),
        "bar" = dtm_redis:show_reply(Bar),
        Close(Conn8),
        io:format("test_unwatch passed ###~n", [])
    end.


