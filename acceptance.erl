-module(acceptance).
-export([test/0]).
-compile(export_all).

test() ->
    io:format("### beginning eredis acceptance test~n"),
    eredis:start(),
    ok = eredis:set(foo, bar),
    {ok, bar} = eredis:get(foo),
    ok = eredis:delete(foo),
    undefined = eredis:get(foo),
    ok = eredis:watch(foo),
    ok = eredis:set(foo, baz),
    ok = eredis:multi(),
    stored = eredis:set(foo, baz),
    error = eredis:exec(),
    {ok, baz} = eredis:get(foo),
    io:format("### eredis acceptance test passed~n").

