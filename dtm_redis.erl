-module(dtm_redis).
-export([start/0, start/1]).
-export([get/1, set/2, delete/1]).
-export([watch/1, unwatch/0, multi/0, exec/0]).

-include("dtm_redis.hrl").
-include("protocol.hrl").

start() ->
    start(#config{shell=true, buckets=2}).

start(#config{shell=true}=Config) ->
    register(shell, spawn_link(session, start, [shell, bucket_map(Config#config.buckets), monitor_map(Config#config.monitors)]));
start(Config) ->
    register(server, spawn_link(server, start, [Config, bucket_map(Config#config.buckets), monitor_map(Config#config.monitors)])).

bucket_map(Buckets) ->
    #buckets{bits=hash:bits(Buckets), map=lists:foldl(fun(I, M) -> dict:store(I - 1, spawn_link(bucket, start, []), M) end, dict:new(), lists:seq(1, Buckets))}.

monitor_map(Monitors) ->
    #monitors{map=lists:foldl(fun(I, M) -> dict:store(I - 1, spawn_link(txn_monitor, start, []), M) end, dict:new(), lists:seq(1, Monitors))}.

dispatch(Message) ->
    Shell = whereis(shell),
    Shell ! Message,
    receive
        {Shell, Response} -> Response
    end.

get(Key) -> dispatch({self(), Key, #get{key=Key}}).
set(Key, Value) -> dispatch({self(), Key, #set{key=Key, value=Value}}).
delete(Key) -> dispatch({self(), Key, #delete{key=Key}}).

watch(Key) -> dispatch({self(), watch, Key}).
unwatch() -> dispatch({self(), unwatch}).
multi() -> dispatch({self(), multi}).
exec() -> dispatch({self(), exec}).
