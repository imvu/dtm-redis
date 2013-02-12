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

-module(session_sup).
-behavior(supervisor).
-export([start_link/2, start_session/1]).
-export([init/1]).

-include("dtm_redis.hrl").

% API methods

start_link(#buckets{}=BucketMap, Monitors) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [BucketMap, Monitors]).

start_session(Client) ->
    supervisor:start_child(?MODULE, [Client]).

% supervisor callbacks

init([#buckets{}=BucketMap, Monitors]) ->
    error_logger:info_msg("initializing session_sup", []),
    {ok, {{simple_one_for_one, 0, 1},
        [{session, {session, start_link, [BucketMap, Monitors]}, temporary, 1000, worker, [session]}]}}.

