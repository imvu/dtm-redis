-module(acceptance).
-export([test/0]).
-compile(export_all).

test() ->
    io:format("Starting eredis acceptance tests~n"),
    eredis:start(),
    test_get_set(),
    test_delete(),
    test_transaction(),
    test_watch(),
    test_unwatch(),
    io:format("All tests passed~n").

test_get_set() ->
    io:format("### beginning test_get_set~n"),
    ok = eredis:set(foo, bar),
    {ok, bar} = eredis:get(foo),
    io:format("test_get_set passed ###~n").

test_delete() ->
    io:format("### beginning test_delete~n"),
    ok = eredis:set(foo, bar),
    ok = eredis:delete(foo),
    undefined = eredis:get(foo),
    io:format("test_delete passed ###~n").

test_transaction() ->
    io:format("### beginning test_transaction~n"),
    ok = eredis:set(foo, baz),
    ok = eredis:multi(),
    stored = eredis:get(foo),
    stored = eredis:set(foo, bar),
    {ok, [{ok, baz}, ok]} = eredis:exec(),
    {ok, bar} = eredis:get(foo),
    io:format("test_transaction passed ###~n").

test_watch() ->
    io:format("### beginning test_watch~n"),
    ok = eredis:watch(foo),
    ok = eredis:set(foo, baz),
    ok = eredis:multi(),
    stored = eredis:set(foo, bar),
    error = eredis:exec(),
    {ok, baz} = eredis:get(foo),
    io:format("test_watch passed ###~n").

test_unwatch() ->
    io:format("### beginning test_unwatch~n"),
    ok = eredis:watch(foo),
    ok = eredis:set(foo, baz),
    ok = eredis:unwatch(),
    ok = eredis:multi(),
    stored = eredis:set(foo, bar),
    {ok, [ok]} = eredis:exec(),
    {ok, bar} = eredis:get(foo),
    io:format("test_unwatch passed ###~n").

