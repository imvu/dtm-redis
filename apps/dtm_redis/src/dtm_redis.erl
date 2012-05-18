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
    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    Monitor = #monitor{nodename=none, binlog="binlog/monitor.log"},
    Config = #config{servers=shell, buckets=[Bucket#bucket{binlog="binlog/bucket0.log"}, Bucket#bucket{binlog="binlog/bucket1.log"}], monitors=[Monitor]},
    gen_server:start_link({local, dtm_redis}, dtm_redis, Config, []).

init(#config{}=Config) ->
    error_logger:info_msg("starting dtm_redis with pid ~p", [self()]),
    register(shell, spawn_link(session, start, [shell, start_buckets(Config#config.buckets), [monitor]])),
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
