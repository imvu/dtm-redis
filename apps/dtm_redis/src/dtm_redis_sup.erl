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

-module(dtm_redis_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).

-include("dtm_redis.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    error_logger:info_msg("initializing dtm_redis_sup", []),
    init_mode(application:get_env(mode)).

init_mode(undefined) ->
    error_logger:info_msg("no mode specified for dtm_redis, assuming debug", []),
    init_mode({ok, debug});
init_mode({ok, debug}) ->
    error_logger:info_msg("dtm_redis_sup starting in debug mode", []),
    Bucket = #bucket{nodename=none, store_host="localhost", store_port=6379},
    BucketMap = dict:from_list([{0, bucket0}, {1, bucket1}]),
    Buckets = #buckets{bits=hash:bits(dict:size(BucketMap)), map=BucketMap},
    {ok, {{one_for_one, 5, 10}, [
        {monitor_binlog, {binlog, start_link, [monitor_binlog, "binlog/monitor.log"]}, permanent, 5000, worker, [binlog]},
        {bucket_binlog, {binlog, start_link, [bucket_binlog, "binlog/bucket.log"]}, permanent, 5000, worker, [binlog]},
        {monitor, {txn_monitor, start_link, [#monitor{}]}, permanent, 5000, worker, [txn_monitor]},
        {bucket0, {bucket, start_link, [bucket0, Bucket]}, permanent, 5000, worker, [bucket]},
        {bucket1, {bucket, start_link, [bucket1, Bucket]}, permanent, 5000, worker, [bucket]},
        {shell, {session, start_link, [shell, Buckets, [txn_monitor]]}, permanent, 5000, worker, [session]},
        {dtm_redis, {dtm_redis, start_link, []}, permanent, 5000, worker, [dtm_rdis]}
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

