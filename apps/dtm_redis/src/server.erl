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

-module(server).
-export([start/3]).

-include("dtm_redis.hrl").

start(Config, BucketMap, Monitors) ->
    error_logger:info_msg("starting dtm-redis server with pid ~p and ~p buckets", [self(), dict:size(BucketMap#buckets.map)]),
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

