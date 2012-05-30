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

-module(server_sup).
-behavior(supervisor).
-export([start_link/1]).
-export([init/1]).

-include("dtm_redis.hrl").

% API methods

start_link(Servers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, local_servers(parse_servers(Servers))).

% supervisor callbacks

init(Servers) ->
    error_logger:info_msg("initializing server_sup", []),
    {ok, {{one_for_one, 0, 1},
        lists:map(fun({local, Name, Server}) ->
                {Name, {server, start_link, [Server]}, permanent, 5000, worker, [server]}
            end, Servers)}}.

% internal functions

parse_servers(Servers) ->
    {Result, _Count} = lists:mapfoldl(fun(Server, Index) ->
            Name = list_to_atom(lists:flatten(io_lib:format("server~p", [Index]))),
            {{local_or_remote(Server), Name, Server}, Index + 1}
        end, 0, Servers),
    Result.

local_servers(Servers) ->
    lists:foldl(fun({local, _Name, _Server}=Server, Acc) -> [Server | Acc];
                   ({remote, _Name, _Server}, Acc) -> Acc
        end, [], Servers).

local_or_remote(#server{nodename=none}) ->
    local;
local_or_remote(#server{nodename=Node}) when (Node == node()) ->
    local;
local_or_remote(#server{}) ->
    remote.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_servers_test() ->
    Server1 = #server{nodename=node()},
    Server2 = #server{nodename=other_node},
    [{local, server0, Server1}, {remote, server1, Server2}] = parse_servers([Server1, Server2]).

local_servers_test() ->
    [{local, foo, bar}] = local_servers([{local, foo, bar}, {remote, foo, bar}]).

local_or_remote_no_node_test() ->
    local = local_or_remote(#server{nodename=none}).

local_or_remote_same_node_test() ->
    local = local_or_remote(#server{nodename=node()}).

local_or_remote_different_node_test() ->
    remote = local_or_remote(#server{nodename=other_node}).

-endif.

