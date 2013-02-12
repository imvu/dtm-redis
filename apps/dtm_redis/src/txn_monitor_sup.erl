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

-module(txn_monitor_sup).
-behavior(supervisor).
-export([local_names/1, start_link/1]).
-export([init/1]).

-include("dtm_redis.hrl").

% API methods

local_names(Monitors) ->
    lists:map(fun({local, MonitorName, _BinlogName, _Monitor}) -> MonitorName end, local_monitors(parse_monitors(Monitors))).

start_link(Monitors) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, local_monitors(parse_monitors(Monitors))).

% supervisor callbacks

init(Monitors) ->
    error_logger:info_msg("initializing txn_monitor_sup", []),
    {ok, {{one_for_one, 0, 1},
        lists:foldl(fun({local, _MonitorName, _BinlogName, _Monitor}=Monitor, Acc) ->
                {BinlogSpec, MonitorSpec} = binlog_monitor_specs(Monitor),
                [BinlogSpec, MonitorSpec | Acc]
            end, [], Monitors)}}.

% internal functions

parse_monitors(Monitors) ->
    {Result, _Count} = lists:mapfoldl(fun(Monitor, Index) ->
            MonitorName = list_to_atom(lists:flatten(io_lib:format("txn_monitor~p", [Index]))),
            BinlogName = list_to_atom(lists:flatten(io_lib:format("monitor_binlog~p", [Index]))),
            {{local_or_remote(Monitor), MonitorName, BinlogName, Monitor}, Index + 1}
        end, 0, Monitors),
    Result.

local_monitors(Monitors) ->
    lists:foldl(fun({local, _MonitorName, _BinlogName, _Monitor}=Monitor, Acc) -> [Monitor | Acc];
                   ({remote, _MonitorName, _BinlogName, _Monitor}, Acc) -> Acc
        end, [], Monitors).

local_or_remote(#monitor{nodename=none}) ->
    local;
local_or_remote(#monitor{nodename=Node}) when (Node == node()) ->
    local;
local_or_remote(#monitor{}) ->
    remote.

binlog_monitor_specs({local, MonitorName, BinlogName, #monitor{binlog=BinlogFile}}) ->
    {{BinlogName, {binlog, start_link, [BinlogName, BinlogFile]}, permanent, 5000, worker, [binlog]},
     {MonitorName, {txn_monitor, start_link, [MonitorName, BinlogName]}, permanent, 5000, worker, [txn_monitor]}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

local_names_test() ->
    [txn_monitor1] = local_names([#monitor{nodename=other_node}, #monitor{nodename=node()}]).

parse_monitors_test() ->
    Monitor1 = #monitor{nodename=node(), binlog=foo},
    Monitor2 = #monitor{nodename=other_node, binlog=bar},
    [{local, txn_monitor0, monitor_binlog0, Monitor1}, {remote, txn_monitor1, monitor_binlog1, Monitor2}] = parse_monitors([Monitor1, Monitor2]).

local_monitors_test() ->
    [{local, foo, bar, baz}] = local_monitors([{local, foo, bar, baz}, {remote, foo, bar, baz}]).

local_or_remote_no_node_test() ->
    local = local_or_remote(#monitor{nodename=none, binlog=foobar}).

local_or_remote_same_node_test() ->
    local = local_or_remote(#monitor{nodename=node(), binlog=foobar}).

local_or_remote_different_node_test() ->
    remote = local_or_remote(#monitor{nodename=other_node, binlog=foobar}).

binlog_monitor_spec_test() ->
    {Binlog, Monitor} = binlog_monitor_specs({local, monitor_name, binlog_name, #monitor{binlog=foobar}}),
    Binlog = {binlog_name, {binlog, start_link, [binlog_name, foobar]}, permanent, 5000, worker, [binlog]},
    Monitor = {monitor_name, {txn_monitor, start_link, [monitor_name, binlog_name]}, permanent, 5000, worker, [txn_monitor]}.

-endif.

