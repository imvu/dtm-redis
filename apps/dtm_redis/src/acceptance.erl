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

-module(acceptance).
-export([test/0]).
-compile(export_all).

test() ->
    io:format("Starting dtm-redis acceptance tests~n"),
    dtm_redis:start(),
    test_get_set(),
    test_delete(),
    test_transaction(),
    test_watch(),
    test_unwatch(),
    io:format("All tests passed~n").

test_get_set() ->
    io:format("### beginning test_get_set~n"),
    ok = dtm_redis:set(foo, bar),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_get_set passed ###~n").

test_delete() ->
    io:format("### beginning test_delete~n"),
    ok = dtm_redis:set(foo, bar),
    1 = dtm_redis:delete(foo),
    undefined = dtm_redis:get(foo),
    io:format("test_delete passed ###~n").

test_transaction() ->
    io:format("### beginning test_transaction~n"),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:get(foo),
    stored = dtm_redis:set(foo, bar),
    {ok, [<<"baz">>, ok]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_transaction passed ###~n").

test_watch() ->
    io:format("### beginning test_watch~n"),
    ok = dtm_redis:watch(foo),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    undefined = dtm_redis:exec(),
    {ok, <<"baz">>} = dtm_redis:get(foo),
    io:format("test_watch passed ###~n").

test_unwatch() ->
    io:format("### beginning test_unwatch~n"),
    ok = dtm_redis:watch(foo),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:unwatch(),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    {ok, [ok]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_unwatch passed ###~n").
