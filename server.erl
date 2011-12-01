-module(server).
-export([start/3]).

-include("dtm_redis.hrl").

start(Config, BucketMap, Monitors) ->
    io:format("starting dtm-redis server with pid ~p and ~p buckets~n", [self(), dict:size(BucketMap#buckets.map)]),
    {ok, Listen} = gen_tcp:listen(Config#config.port, [binary, {backlog, Config#config.backlog}, {active, true}|iface(Config)]),
    loop(Listen, BucketMap, Monitors).

iface(#config{iface=all}) ->
    [];
iface(#config{iface=Iface}) ->
    {ok, Addr} = inet_parse:address(Iface),
    [{ip, Addr}].

loop(Listen, BucketMap, Monitors) ->
    {ok, Client} = gen_tcp:accept(Listen),
    Pid = spawn(session, start, [Client, BucketMap, Monitors]),
    gen_tcp:controlling_process(Client, Pid),
    loop(Listen, BucketMap, Monitors).

