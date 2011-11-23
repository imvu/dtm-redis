-module(server).
-export([start/2]).

-include("eredis.hrl").

start(Config, BucketMap) ->
    io:format("starting eredis server with pid ~p and ~p buckets~n", [self(), dict:size(BucketMap#buckets.map)]),
    {ok, Listen} = gen_tcp:listen(Config#config.port, [binary, {backlog, Config#config.backlog}, {active, true}|iface(Config)]),
    loop(Listen, BucketMap).

iface(#config{iface=all}) ->
    [];
iface(#config{iface=Iface}) ->
    {ok, Addr} = inet_parse:address(Iface),
    [{ip, Addr}].

loop(Listen, BucketMap) ->
    {ok, Client} = gen_tcp:accept(Listen),
    Pid = spawn(session, start, [Client, BucketMap]),
    gen_tcp:controlling_process(Client, Pid),
    loop(Listen, BucketMap).
