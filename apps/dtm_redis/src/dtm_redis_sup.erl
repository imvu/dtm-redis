%% Copyright (C) 2011-2013 IMVU Inc.
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

-module(dtm_redis_sup).
-behavior(supervisor).
-export([start_link/0]).
-export([init/1]).

-include("dtm_redis.hrl").

% API methods

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

% supervisor callbacks

init([]) ->
    error_logger:info_msg("initializing dtm_redis_sup", []),
    init_mode(application:get_env(mode)).

% internal methods

init_mode(undefined) ->
    error_logger:info_msg("no mode specified for dtm_redis, assuming debug", []),
    init_mode({ok, debug});
init_mode({ok, debug}) ->
    error_logger:info_msg("dtm_redis_sup starting in debug mode", []),
    {#buckets{}=Buckets, Monitors, Children} = debug_child_specs(),
    {ok, {{one_for_one, 5, 10}, Children ++ [
        {shell, {session, start_link, [Buckets, Monitors]}, permanent, 5000, worker, [session]}
    ]}};
init_mode({ok, debug_server}) ->
    error_logger:info_msg("dtm_redis_sup starting in debug_server mode", []),
    {#buckets{}=Buckets, Monitors, Children} = debug_child_specs(),

    {ok, {{one_for_one, 5, 10}, Children ++ [
        ranch:child_spec(debug_server, 5, ranch_tcp, [{port, 6378}], session, {Buckets, Monitors})
    ]}};
init_mode({ok, master}) ->
    {ok, {{one_for_one, 5, 10}, [
        {master, {master_sup, start_link, []}, permanent, 5000, supervisor, [master_sup]}
    ]}};
init_mode({ok, slave}) ->
    {ok, {{one_for_one, 5, 10}, [
        {slave, {slave_sup, start_link, []}, permanent, 5000, supervisor, [slave_sup]}
    ]}};
init_mode({ok, Other}) ->
    error_logger:error("dtm_redis application mode '~p' not supported", [Other]),
    {error, mode_not_supported}.

debug_child_specs() ->
    MonitorConfig = [#monitor{nodename=none, binlog="binlog/monitor.log"}],
    Monitors = txn_monitor_sup:local_names(MonitorConfig),

    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    BucketConfig = [Bucket#bucket{binlog="binlog/bucket0.log"}, Bucket#bucket{binlog="binlog/bucket1.log"}],
    BucketMap = bucket_sup:bucket_map(BucketConfig),
    Buckets = #buckets{bits=hash:bits(dict:size(BucketMap)), map=BucketMap},

    {Buckets, Monitors, [
        {txn_monitor_sup, {txn_monitor_sup, start_link, [MonitorConfig]}, permanent, 5000, supervisor, [txn_monitor_sup]},
        {bucket_sup, {bucket_sup, start_link, [BucketConfig]}, permanent, 5000, supervisor, [bucket_sup]}
    ]}.

