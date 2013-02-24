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

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

% supervisor callbacks

-spec init([]) -> {ok, {{supervisor:strategy(), non_neg_integer(), pos_integer()}, [supervisor:child_spec()]}} | {error, any()}.
init([]) ->
    error_logger:info_msg("initializing dtm_redis_sup", []),
    Mode = case application:get_env(mode) of
        {ok, Any} ->
            Any;
        undefined ->
            error_logger:info_msg("no mode specified for dtm-redis, assuming debug", []),
            debug
    end,
    error_logger:info_msg("dtm-redis starting in ~p mode", [Mode]),
    case init_mode(Mode) of
        {error, _Any}=Error ->
            Error;
        Children ->
            {ok, {{one_for_one, 5, 10}, Children}}
    end.

% internal methods

-spec init_mode(atom()) -> [supervisor:child_spec()] | {error, any()}.
init_mode(debug) ->
    {FakeRedisPort, FakeRedis} = debug_redis_stores(),
    DebugConfig = debug_config(FakeRedisPort),
    {Buckets, Monitors, Children} = init_config(DebugConfig#config{servers=[]}),
    FakeRedis ++ Children ++ [
        {shell, {session, start_link, [Buckets, Monitors]}, permanent, 5000, worker, [session]}
    ];
init_mode(debug_server) ->
    {FakeRedisPort, FakeRedis} = debug_redis_stores(),
    {_Buckets, _Monitors, Children} = init_config(debug_config(FakeRedisPort)),
    FakeRedis ++ Children;
init_mode(cluster) ->
    case application:get_env(config) of
        undefined -> {error, config_not_specified};
        {ok, Filename} -> init_config_file(Filename)
    end;
init_mode(Mode) ->
    error_logger:error_msg("dtm_redis application mode '~p' not supported", [Mode]),
    {error, mode_not_supported}.

-spec init_config_file(string()) -> [supervisor:child_spec()] | {error, any()}.
init_config_file(Filename) ->
    case file:consult(Filename) of
        {ok, [#config{}=Config]} ->
            {_Buckets, _Monitors, Children} = init_config(Config),
            Children;
        {ok, [#config{} | Other]} ->
            error_logger:error_msg("Extra terms found in config file: ~p", [Other]),
            {error, too_many_config_terms};
        {ok, Other} ->
            error_logger:error_msg("Invalid configuration: ~p", [Other]),
            {error, invalid_configuration};
        {error, _Any}=Error ->
            Error
    end.

-spec init_config(#config{}) -> {#buckets{}, [#monitor{}], [supervisor:child_spec()]}.
init_config(#config{servers=ServerConfig, buckets=BucketConfig, monitors=MonitorConfig}=Config) ->
    error_logger:info_msg("starting dtm-redis using configuration ~p", [Config]),

    Monitors = txn_monitor_sup:local_names(MonitorConfig),

    BucketMap = bucket_sup:bucket_map(BucketConfig),
    Buckets = #buckets{bits=hash:bits(dict:size(BucketMap)), map=BucketMap},

    SupervisorConfig = [
        {txn_monitor_sup, [MonitorConfig]},
        {bucket_sup, [BucketConfig]},
        {server_sup, [ServerConfig, {Buckets, Monitors}]}
    ],
    Supervisors = [{Sup, {Sup, start_link, Args}, permanent, 5000, supervisor, [Sup]} || {Sup, Args} <- SupervisorConfig],

    Children = Supervisors ++ [{cluster, {cluster, start_link, [Config]}, permanent, 5000, worker, [cluster]}],

    {Buckets, Monitors, Children}.

-spec debug_redis_stores() -> {gen_tcp:port_number(), [supervisor:child_spec()]}.
debug_redis_stores() ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {packet, raw}]),
    {ok, Port} = inet:port(Listener),
    {Port, [
        {fake_redis0, {fake_redis, start_link, [Listener]}, permanent, 5000, worker, [fake_redis]},
        {fake_redis1, {fake_redis, start_link, [Listener]}, permanent, 5000, worker, [fake_redis]}
    ]}.

-spec debug_config(inet:port_number()) -> #config{}.
debug_config(Port) ->
    MonitorConfig = [#monitor{nodename=none, binlog="binlog/monitor.log"}],

    Bucket = #bucket{nodename=none, store_host="localhost", store_port=Port},
    BucketConfig = [Bucket#bucket{binlog="binlog/bucket0.log"}, Bucket#bucket{binlog="binlog/bucket1.log"}],

    ServerConfig = [#server{nodename=none, port=6378, iface=all}],

    #config{servers=ServerConfig, buckets=BucketConfig, monitors=MonitorConfig}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

init_config_test() ->
    ServerConfig = [
        #server{nodename=node(), port=1234, iface=all},
        #server{nodename=othernode, port=5678, iface=all}
    ],
    BucketConfig = [
        #bucket{nodename=othernode, store_host="otherhost", store_port=8765, binlog="otherbucketbin.log"},
        #bucket{nodename=node(), store_host="thishost", store_port=4321, binlog="thisbucketbin.log"}
    ],
    MonitorConfig = [
        #monitor{nodename=node(), binlog="thismonitorbin.log"},
        #monitor{nodename=othernode, binlog="othermonitorbin.log"}
    ],
    {Buckets, Monitors, Children} = init_config(#config{servers=ServerConfig, buckets=BucketConfig, monitors=MonitorConfig}),

    Buckets = #buckets{bits=1, map=bucket_sup:bucket_map(BucketConfig)},

    Monitors = [txn_monitor0],

    [TxnMonSup, BucketSup, ServerSup, ClusterChild] = Children,
    {txn_monitor_sup, {txn_monitor_sup, start_link, [MonitorConfig]}, _, _, _, _} = TxnMonSup,
    {bucket_sup, {bucket_sup, start_link, [BucketConfig]}, _, _, _, _} = BucketSup,
    {server_sup, {server_sup, start_link, [ServerConfig, {Buckets, Monitors}]}, _, _, _, _} = ServerSup,
    {cluster, {cluster, start_link, _}, _, _, _, _} = ClusterChild.

-endif.

