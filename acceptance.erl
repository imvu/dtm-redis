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
    {ok, <<"OK">>} = dtm_redis:set(foo, bar),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_get_set passed ###~n").

test_delete() ->
    io:format("### beginning test_delete~n"),
    {ok, <<"OK">>} = dtm_redis:set(foo, bar),
    {ok, <<"1">>} = dtm_redis:delete(foo),
    {ok, undefined} = dtm_redis:get(foo),
    io:format("test_delete passed ###~n").

test_transaction() ->
    io:format("### beginning test_transaction~n"),
    {ok, <<"OK">>} = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:get(foo),
    stored = dtm_redis:set(foo, bar),
    {ok, [<<"baz">>, <<"OK">>]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_transaction passed ###~n").

test_watch() ->
    io:format("### beginning test_watch~n"),
    ok = dtm_redis:watch(foo),
    {ok, <<"OK">>} = dtm_redis:set(foo, baz),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    error = dtm_redis:exec(),
    {ok, <<"baz">>} = dtm_redis:get(foo),
    io:format("test_watch passed ###~n").

test_unwatch() ->
    io:format("### beginning test_unwatch~n"),
    ok = dtm_redis:watch(foo),
    {ok, <<"OK">>} = dtm_redis:set(foo, baz),
    ok = dtm_redis:unwatch(),
    ok = dtm_redis:multi(),
    stored = dtm_redis:set(foo, bar),
    {ok, [<<"OK">>]} = dtm_redis:exec(),
    {ok, <<"bar">>} = dtm_redis:get(foo),
    io:format("test_unwatch passed ###~n").

