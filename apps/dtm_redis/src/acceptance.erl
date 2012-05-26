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
-compile([test/0]).

start() ->
    ok = application:start(eredis),
    ok = application:start(dtm_redis).

stop() ->
    ok = application:stop(dtm_redis),
    ok = application:stop(eredis).

test() ->
    error_logger:info_msg("Starting dtm-redis acceptance tests", []),
    start(),
    test_get_set(),
    test_delete(),
    test_transaction(),
    test_watch(),
    test_unwatch(),
    error_logger:info_msg("All tests passed", []),
    stop().

test_get_set() ->
    error_logger:info_msg("### beginning test_get_set", []),
    ok = dtm_redis:set(foo, bar),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    error_logger:info_msg("test_get_set passed ###", []).

test_delete() ->
    error_logger:info_msg("### beginning test_delete", []),
    ok = dtm_redis:set(foo, bar),
    1 = dtm_redis:delete(foo),
    undefined = dtm_redis:get(foo),
    error_logger:info_msg("test_delete passed ###", []).

test_transaction() ->
    error_logger:info_msg("### beginning test_transaction", []),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:get(foo),
    stored = dtm_redis:set(foo, bar),
    {ok, [<<"baz">>, ok]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    error_logger:info_msg("test_transaction passed ###", []).

test_watch() ->
    error_logger:info_msg("### beginning test_watch", []),
    ok = dtm_redis:watch(foo),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    undefined = dtm_redis:exec(),
    {ok, <<"baz">>} = dtm_redis:get(foo),
    error_logger:info_msg("test_watch passed ###", []).

test_unwatch() ->
    error_logger:info_msg("### beginning test_unwatch", []),
    ok = dtm_redis:watch(foo),
    ok = dtm_redis:set(foo, baz),
    ok = dtm_redis:unwatch(),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    {ok, [ok]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    error_logger:info_msg("test_unwatch passed ###", []).

