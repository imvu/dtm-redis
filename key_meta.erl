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

