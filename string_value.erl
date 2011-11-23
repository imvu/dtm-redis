-module(string_value).
-export([read/1, update/2]).

-include("data_types.hrl").

read(undefined) ->
    undefined;
read(#data{value=#string{text=Text}} = Data) ->
    case value:is_expired(Data) of
        true -> undefined;
        false -> {ok, Text}
    end;
read(#data{}) ->
    undefined.

update(undefined, Text) ->
    #data{version=1, value=#string{text=Text}};
update(#data{} = Old, Text) ->
    value:update(Old, #string{text=Text}).
