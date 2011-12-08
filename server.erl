-module(server).
-export([start/3]).

-include("dtm_redis.hrl").

start(Config, BucketMap, Monitors) ->
    io:format("starting dtm-redis server with pid ~p and ~p buckets~n", [self(), dict:size(BucketMap#buckets.map)]),
    {ok, Listen} = gen_tcp:listen(Config#server.port, [binary, {backlog, Config#server.backlog}, {active, true}|iface(Config)]),
    loop(Listen, BucketMap, Monitors).

iface(#server{iface=all}) ->
    [];
iface(#server{iface={_,_,_,_}=Iface}) ->
    [{ip, Iface}];
iface(#server{iface=Iface}) ->
    {ok, Addr} = inet_parse:address(Iface),
    [{ip, Addr}].

loop(Listen, BucketMap, Monitors) ->
    {ok, Client} = gen_tcp:accept(Listen),
    Pid = spawn(session, start, [Client, BucketMap, Monitors]),
    gen_tcp:controlling_process(Client, Pid),
    loop(Listen, BucketMap, Monitors).

