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

-module(dtm_redis).
-behavior(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start/0, server_start/0, start/1]).
-export([get/1, set/2, delete/1]).
-export([watch/1, unwatch/0, multi/0, exec/0]).

-include("dtm_redis.hrl").
-include("protocol.hrl").

start_link() ->
    gen_server:start_link({local, dtm_redis}, dtm_redis, [], []).

init([]) ->
    error_logger:info_msg("starting dtm_redis with pid ~p", [self()]),
    {ok, none}.

handle_call(Message, From, _State) ->
    error_logger:error_msg("dtm_redis:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(Message, _State) ->
    error_logger:error_msg("dtm_redis:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info(Message, _State) ->
    error_logger:error_msg("dtm_redis:handle_info unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

terminate(Reason, _State) ->
    error_logger:info_msg("terminating dtm_redis because ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start() ->
    binlog:start_link(monitor_binlog, "binlog/monitor.log"),
    binlog:start_link(bucket_binlog, "binlog/bucket.log"),
    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    Monitor = #monitor{nodename=none},
    start(#config{servers=shell, buckets=[Bucket, Bucket], monitors=[Monitor]}).

server_start() ->
    [Filename|_] = init:get_plain_arguments(),
    {ok, [Config|_]} = file:consult(Filename),
    io:format("starting dtm-redis using configuration ~p~n", [Config]),
    start(Config).

start_bucket(#bucket{nodename=none}=Config) ->
    {ok, Bucket} = bucket:start_link(Config),
    Bucket;
start_bucket(#bucket{nodename=Node}=Config) ->
    spawn_link(Node, bucket, start, [Config]).

start_monitor(#monitor{nodename=none}=Config) ->
    {ok, Monitor} = txn_monitor:start_link(Config),
    Monitor;
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

get(Key) -> session:get(shell, Key).
set(Key, Value) -> session:set(shell, Key, Value).
delete(Key) -> session:delete(shell, Key).

watch(Key) -> session:watch(shell, Key).
unwatch() -> session:unwatch(shell).
multi() -> session:multi(shell).
exec() -> session:exec(shell).
