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

-module(bucket_sup).
-behavior(supervisor).
-export([bucket_map/1, start_link/1]).
-export([init/1]).

-include("dtm_redis.hrl").

% API methods

bucket_map(Buckets) ->
    lists:foldl(fun({_LocalOrRemote, BucketName, _BinlogName, _Bucket}, Map) ->
            dict:store(dict:size(Map), BucketName, Map)
        end, dict:new(), parse_buckets(Buckets)).

start_link(Buckets) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, local_buckets(parse_buckets(Buckets))).

% supervisor callbacks

init(Buckets) ->
    error_logger:info_msg("initializing bucket_sup", []),
    {ok, {{one_for_one, 0, 1},
        lists:foldl(fun({local, _BucketName, _BinlogName, _Bucket}=Bucket, Acc) ->
                {BinlogSpec, BucketSpec} = binlog_bucket_specs(Bucket),
                [BinlogSpec, BucketSpec | Acc]
            end, [], Buckets)}}.

% internal functions

parse_buckets(Buckets) ->
    {Result, _Count} = lists:mapfoldl(fun(Bucket, Index) ->
            BucketName = list_to_atom(lists:flatten(io_lib:format("bucket~p", [Index]))),
            BinlogName = list_to_atom(lists:flatten(io_lib:format("bucket_binlog~p", [Index]))),
            {{local_or_remote(Bucket), BucketName, BinlogName, Bucket}, Index + 1}
        end, 0, Buckets),
    Result.

local_buckets(Buckets) ->
    lists:foldl(fun({local, _BucketName, _BinlogName, _Bucket}=Bucket, Acc) -> [Bucket | Acc];
                   ({remote, _BucketName, _BinlogName, _Bucket}, Acc) -> Acc
        end, [], Buckets).

local_or_remote(#bucket{nodename=none}) ->
    local;
local_or_remote(#bucket{nodename=Node}) when (Node == node()) ->
    local;
local_or_remote(#bucket{}) ->
    remote.

binlog_bucket_specs({local, BucketName, BinlogName, #bucket{binlog=BinlogFile}=Bucket}) ->
    {{BinlogName, {binlog, start_link, [BinlogName, BinlogFile]}, permanent, 5000, worker, [binlog]},
     {BucketName, {bucket, start_link, [BucketName, BinlogName, Bucket]}, permanent, 5000, worker, [bucket]}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

bucket_map_test() ->
    Map = dict:from_list([{0, bucket0}, {1, bucket1}]),
    Map = bucket_map([#bucket{nodename=other_node}, #bucket{nodename=node()}]).

parse_buckets_test() ->
    Bucket1 = #bucket{nodename=node()},
    Bucket2 = #bucket{nodename=other_node},
    [{local, bucket0, bucket_binlog0, Bucket1}, {remote, bucket1, bucket_binlog1, Bucket2}] = parse_buckets([Bucket1, Bucket2]).

local_buckets_test() ->
    [{local, foo, bar, baz}] = local_buckets([{local, foo, bar, baz}, {remote, foo, bar, baz}]).

local_or_remote_no_node_test() ->
    local = local_or_remote(#bucket{nodename=none}).

local_or_remote_same_node_test() ->
    local = local_or_remote(#bucket{nodename=node()}).

local_or_remote_different_node_test() ->
    remote = local_or_remote(#bucket{nodename=other_node}).

binlog_bucket_spec_test() ->
    BucketConfig = #bucket{binlog=foobar},
    {Binlog, Bucket} = binlog_bucket_specs({local, bucket_name, binlog_name, BucketConfig}),
    Binlog = {binlog_name, {binlog, start_link, [binlog_name, foobar]}, permanent, 5000, worker, [binlog]},
    Bucket = {bucket_name, {bucket, start_link, [bucket_name, binlog_name, BucketConfig]}, permanent, 5000, worker, [bucket]}.

-endif.

