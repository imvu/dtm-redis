-module(dtm_redis).
-export([start/0, server_start/0, start/1]).
-export([get/1, set/2, delete/1]).
-export([watch/1, unwatch/0, multi/0, exec/0]).

-include("dtm_redis.hrl").
-include("protocol.hrl").

start() ->
    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    start(#config{shell=true, buckets=[Bucket, Bucket]}).

server_start() ->
    [Filename|_] = init:get_plain_arguments(),
    {ok, [Config|_]} = file:consult(Filename),
    io:format("starting dtm-redis using configuration ~p~n", [Config]),
    start(Config).

start_bucket(#bucket{nodename=none}=Config) ->
    spawn_link(bucket, start, [Config]);
start_bucket(#bucket{nodename=Node}=Config) ->
    spawn_link(Node, bucket, start, [Config]).

start(#config{shell=true}=Config) ->
    register(shell, spawn_link(session, start, [shell, start_buckets(Config#config.buckets), monitor_list(Config#config.monitors)]));
start(Config) ->
    register(server, spawn_link(server, start, [Config, start_buckets(Config#config.buckets), monitor_list(Config#config.monitors)])).

start_buckets(Buckets) ->
    Map = lists:foldl(fun(Config, M) -> dict:store(dict:size(M), start_bucket(Config), M) end, dict:new(), Buckets),
    #buckets{bits=hash:bits(dict:size(Map)), map=Map}.

monitor_list(Monitors) ->
    [spawn_link(txn_monitor, start, []) || _I <- lists:seq(1, Monitors)].

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
