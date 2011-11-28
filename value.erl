-module(value).
-export([is_expired/1, version/1, expire/2, persist/1, is_watched/1, watch/1, unwatch/1, is_locked/2, lock/2, unlock/1, update/2]).

-include("data_types.hrl").

is_expired(undefined) ->
    false;
is_expired(#data{expires=none}) ->
    false;
is_expired(#data{expires={From, Ttl}}) ->
    (timer:now_diff(erlang:now(), From) / 1000000) > Ttl.

version(undefined) ->
    0;
version(#data{version=Version}) ->
    Version.

expire(#data{} = Data, Ttl) when is_integer(Ttl) andalso Ttl > 0 ->
    Data#data{expires={erlang:now(), Ttl}};
expire(_, _) ->
    error.

persist(#data{} = Data) ->
    Data#data{expires=none}.

is_locked(undefined, _Session) ->
    false;
is_locked(#data{locked=false}, _Session) ->
    false;
is_locked(#data{locked=Locked}, Session) ->
    Locked =/= Session.

lock(undefined, Session) ->
    #data{locked=Session};
lock(#data{}=Data, Session) ->
    Data#data{locked=Session}.

unlock(#data{}=Data) ->
    Data#data{locked=false}.

is_watched(undefined) ->
    false;
is_watched(#data{watches=Watches}) ->
    Watches =:= 0.

watch(undefined) ->
    #data{watches=1};
watch(#data{watches=Watches}=Data) ->
    Data#data{watches=Watches+1}.

unwatch(undefined) ->
    undefined;
unwatch(#data{watches=Watches}=Data) when Watches > 0 ->
    Data#data{watches=Watches-1}.

update(#data{version=Version}=Data, Value) ->
    Data#data{expires=none, version=Version + 1, value=Value}.
