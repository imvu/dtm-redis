-module(dtm_redis).
-export([start/0, server_start/0, start/1]).
-export([get/1, set/2, delete/1]).
-export([watch/1, unwatch/0, multi/0, exec/0]).

-include("dtm_redis.hrl").
-include("protocol.hrl").

start() ->
    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    Monitor = #monitor{nodename=none, binlog="binlog/monitor.log"},
    start(#config{servers=shell, buckets=[Bucket#bucket{binlog="binlog/bucket0.log"}, Bucket#bucket{binlog="binlog/bucket1.log"}], monitors=[Monitor]}).

server_start() ->
    [Filename|_] = init:get_plain_arguments(),
    {ok, [Config|_]} = file:consult(Filename),
    io:format("starting dtm-redis using configuration ~p~n", [Config]),
    start(Config).

start_bucket(#bucket{nodename=none}=Config) ->
    spawn_link(bucket, start, [Config]);
start_bucket(#bucket{nodename=Node}=Config) ->
    spawn_link(Node, bucket, start, [Config]).

start_monitor(#monitor{nodename=none}=Config) ->
    spawn_link(txn_monitor, start, [Config]);
start_monitor(#monitor{nodename=Node}=Config) ->
    spawn_link(Node, txn_monitor, start, [Config]).

start_server(#server{nodename=none}=Config, Buckets, Monitors) ->
    spawn_link(server, start, [Config, Buckets, Monitors]);
start_server(#server{nodename=Node}=Config, Buckets, Monitors) ->
    spawn_link(Node, server, start, [Config, Buckets, Monitors]).

start(#config{servers=shell}=Config) ->
    register(shell, spawn_link(session, start, [shell, start_buckets(Config#config.buckets), start_monitors(Config#config.monitors)]));
start(#config{}=Config) ->
    Buckets = start_buckets(Config#config.buckets),
    Monitors = start_monitors(Config#config.monitors),
    lists:foreach(fun(Server) -> start_server(Server, Buckets, Monitors) end, Config#config.servers).

start_buckets(Buckets) ->
    Map = lists:foldl(fun(Config, M) -> dict:store(dict:size(M), start_bucket(Config), M) end, dict:new(), Buckets),
    #buckets{bits=hash:bits(dict:size(Map)), map=Map}.

start_monitors(Monitors) ->
    [start_monitor(Config) || Config <- Monitors].

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
