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

-module(server).
-behavior(gen_server).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("dtm_redis.hrl").

-record(state, {listen=none}).

-define(ACCEPT_TIMEOUT, 100).
-define(SYSTEM_LIMIT_WAIT, 50).

% API methods

start_link(#server{}=Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

% gen_server callbacks

init(#server{port=Port, backlog=Backlog}=Config) ->
    error_logger:info_msg("starting server with pid ~p listening on port ~p", [self(), Port]),
    {ok, Listen} = gen_tcp:listen(Port, [binary, {backlog, Backlog}, {active, false} | iface(Config)]),
    {ok, #state{listen=Listen}, 0}.

handle_call(Message, From, _State) ->
    error_logger:error_msg("server:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(Message, _State) ->
    error_logger:error_msg("server:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info(timeout, #state{listen=Listen}=State) ->
    handle_accept(gen_tcp:accept(Listen, ?ACCEPT_TIMEOUT), State);
handle_info(Message, _State) ->
    error_logger:error_msg("server:handle_info unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

terminate(Reason, State) ->
    error_logger:info_msg("terminating server with reason ~p", [Reason]),
    close(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

iface(#server{iface=all}) ->
    [];
iface(#server{iface={_,_,_,_}=Iface}) ->
    [{ip, Iface}];
iface(#server{iface=Iface}) ->
    {ok, Addr} = inet_parse:address(Iface),
    [{ip, Addr}].

handle_accept({ok, Client}, #state{}=State) ->
    {ok, Session} = session_sup:start_session(Client),
    gen_tcp:controlling_process(Client, Session),
    inet:setopts(Client, [{active, once}]),
    {noreply, State, 0};
handle_accept({error, timeout}, #state{}=State) ->
    {noreply, State, 0};
handle_accept({error, system_limit}, #state{}=State) ->
    error_logger:error_msg("maximum port count exceeded, not accepting new connections for ~p ms", [?SYSTEM_LIMIT_WAIT]),
    {noreply, State, ?SYSTEM_LIMIT_WAIT};
handle_accept({error, Reason}, #state{}=State) ->
    error_logger:error_msg("stopping server because of accept error ~p", [Reason]),
    {stop, Reason, State}.

close(#state{listen=none}) ->
    ok;
close(#state{listen=Listen}) ->
    gen_tcp:close(Listen).

