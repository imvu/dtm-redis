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

-module(cluster).
-behavior(gen_server).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("dtm_redis.hrl").

-record(state, {
    nodes :: dict()
}).

% Public API

start_link(#config{}=Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, remote_nodes(Config), []).

% gen_server callbacks

-spec init([node()]) -> {ok, #state{}} | ignore.
init(Nodes) ->
    case Nodes of
        [] ->
            error_logger:info_msg("dtm-redis cluster running in local-only mode, not monitoring", []),
            ignore;
        _Any ->
            error_logger:info_msg("starting cluster monitoring nodes ~p", [Nodes]),
            gen_server:cast(self(), init_complete),
            {ok, #state{nodes=dict:from_list([{Node, unknown} || Node <- Nodes])}}
    end.

-spec handle_call(any(), any(), #state{}) -> {stop, unhandled, #state{}}.
handle_call(Message, From, State) ->
    error_logger:error_msg("unhandled call to cluster: ~p from ~p", [Message, From]),
    {stop, unhandled, State}.

-spec handle_cast(any(), #state{}) -> {noreply, #state{}} | {stop, unhandled, #state{}}.
handle_cast(init_complete, #state{nodes=NodeState}=State) ->
    net_kernel:monitor_nodes(true, [{node_type, all}, nodedown_reason]),
    lists:foreach(fun(Node) ->
            case net_kernel:connect_node(Node) of
                true -> error_logger:info_msg("connected to node ~p", [Node]);
                false -> error_logger:warning_msg("failed to connect to node ~p", [Node]);
                ignored -> error_logger:error_msg("connection to node ~p failed because the local node is not alive", [Node])
            end
        end, dict:fetch_keys(NodeState)),
    {noreply, State};
handle_cast(Message,  State) ->
    error_logger:error_msg("unhandled cast to cluster: ~p", [Message]),
    {stop, unhandled, State}.

-spec handle_info(any(), #state{}) -> {noeply, #state{}} | {stop, unhandled, #state{}}.
handle_info({nodeup, Node, Info}, State) ->
    {noreply, handle_node_status(nodeup, Node, Info, State)};
handle_info({nodedown, Node, Info}, State) ->
    {noreply, handle_node_status(nodedown, Node, Info, State)};
handle_info(Message, State) ->
    error_logger:error_msg("unhandled info to cluster: ~p", [Message]),
    {stop, unhandled, State}.

-spec terminate(any(), #state{}) -> ok.
terminate(Reason, #state{}) ->
    error_logger:info_msg("cluster terminating: ~p", [Reason]),
    ok.

-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal functions

-spec remote_nodes(#config{} | [#server{}] | [#bucket{}] | [#monitor{}]) -> [node()].
remote_nodes(#config{servers=Servers, buckets=Buckets, monitors=Monitors}) ->
    ServerNodes = [ServerNode || #server{nodename=ServerNode} <- Servers],
    BucketNodes = [BucketNode || #bucket{nodename=BucketNode} <- Buckets],
    MonitorNodes = [MonitorNode || #monitor{nodename=MonitorNode} <- Monitors],

    AllNodes = lists:usort(lists:append([ServerNodes, BucketNodes, MonitorNodes])),
    lists:filter(fun(node) -> false; (Node) -> Node =/= node() end, AllNodes).

-spec handle_node_status(nodeup | nodedown, node(), any(), #state{}) -> #state{}.
handle_node_status(Type, Node, Info, #state{nodes=NodeState}=State) ->
    error_logger:info_msg("got ~p message from ~p: ~p", [Type, Node, Info]),
    NewNodeState = case dict:is_key(Node, NodeState) of
        true ->
            NewStatus = node_status(Type),
            case dict:fetch(Node, NodeState) of
                NewStatus ->
                    error_logger:error_msg("node ~p already had status ~p", [Node, NewStatus]),
                    NodeState;
                OldStatus ->
                    error_logger:info_msg("node ~p status changed from ~p to ~p", [Node, OldStatus, NewStatus]),
                    dict:store(Node, NewStatus, NodeState)
            end;
        false ->
            error_logger:warning_msg("node ~p is not in a member of the dtm-redis cluster", [Node]),
            NodeState
    end,
    State#state{nodes=NewNodeState}.

-spec node_status(nodeup | nodedown) -> up | down.
node_status(nodeup) ->
    up;
node_status(nodedown) ->
    down.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

local_only_config() ->
    Servers = [#server{nodename=node}, #server{nodename=node}],
    Buckets = [#bucket{nodename=node}, #bucket{nodename=node}],
    Monitors = [#monitor{nodename=node}, #monitor{nodename=node}],
    #config{servers=Servers, buckets=Buckets, monitors=Monitors}.

remote_nodes_local_only_test() ->
    [] = remote_nodes(local_only_config()).

some_remote_nodes_test() ->
    Servers = [#server{nodename=node()}, #server{nodename=foo}],
    Buckets = [#bucket{nodename=bar}, #bucket{nodename=node()}],
    Monitors = [#monitor{nodename=node()}, #monitor{nodename=baz}],
    [bar, baz, foo] = remote_nodes(#config{servers=Servers, buckets=Buckets, monitors=Monitors}).

init_local_only_test() ->
    ignore = init([]).

handle_node_status_in_cluster_new_state_test() ->
    Expected = #state{nodes=dict:store(foobar, up, dict:new())},
    {noreply, Expected} = handle_info({nodeup, foobar, someinfo}, #state{nodes=dict:store(foobar, down, dict:new())}).

handle_node_status_in_cluster_same_state_test() ->
    State = #state{nodes=dict:store(foobar, down, dict:new())},
    {noreply, State} = handle_info({nodedown, foobar, someinfo}, State).

handle_node_status_not_in_cluster_test() ->
    State = #state{nodes=dict:store(foobar, fooey, dict:new())},
    {noreply, State} = handle_info({nodeup, someothernode, someinfo}, State).

-endif.

