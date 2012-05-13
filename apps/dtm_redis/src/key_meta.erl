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

-module(key_meta).
-export([version/1, is_watched/1, watch/1, unwatch/1, is_locked/2, lock/2, unlock/1, update/1]).

-include("data_types.hrl").

version(undefined) ->
    0;
version(#key{version=Version}) ->
    Version.

is_locked(undefined, _Session) ->
    false;
is_locked(#key{locked=false}, _Session) ->
    false;
is_locked(#key{locked=Locked}, Session) ->
    Locked =/= Session.

lock(undefined, Session) ->
    #key{locked=Session};
lock(#key{}=Key, Session) ->
    Key#key{locked=Session}.

unlock(#key{}=Key) ->
    Key#key{locked=false}.

is_watched(undefined) ->
    false;
is_watched(#key{watches=Watches}) ->
    Watches =:= 0.

watch(undefined) ->
    #key{watches=1};
watch(#key{watches=Watches}=Key) ->
    Key#key{watches=Watches+1}.

unwatch(undefined) ->
    undefined;
unwatch(#key{watches=Watches}=Key) when Watches > 0 ->
    Key#key{watches=Watches-1};
unwatch(#key{watches=0}=Key) ->
    Key.

update(undefined) ->
    #key{version=1};
update(#key{version=Version}=Key) ->
    Key#key{version=Version+1}.

