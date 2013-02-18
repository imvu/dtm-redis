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

-module(redis_command).

-include("protocol.hrl").

-export([meta/1, parse_operation/1]).

% types

-type type() :: key | str_arg.
-type component() :: type() | {optional, type()}.

-record(command_meta, {
    name :: atom(),
    normalized :: binary(),
    format :: [component()],
    mutation :: boolean()
}).

% Public API

-spec meta(binary()) -> none | #command_meta{}.
meta(Command) when is_binary(Command) ->
    meta(string:to_lower(binary_to_list(Command)));
meta("watch") ->
    #command_meta{name=watch, normalized= <<"WATCH">>, format=[key]};
meta("unwatch") ->
    #command_meta{name=unwatch, normalized= <<"UNWATCH">>, format=[]};
meta("multi") ->
    #command_meta{name=multi, normalized= <<"MULTI">>, format=[]};
meta("exec") ->
    #command_meta{name=exec, normalized= <<"EXEC">>, format=[]};
meta("get") ->
    #command_meta{name=get, normalized= <<"GET">>, format=[key]};
meta("set") ->
    #command_meta{name=set, normalized= <<"SET">>, format=[key, str_arg]};
meta("del") ->
    #command_meta{name=del, normalized= <<"DEL">>, format=[key]};
meta(_Any) ->
    none.

-spec parse_operation(#redis_multi_bulk{}) -> #operation{} | #redis_error{}.
parse_operation(#redis_multi_bulk{items=[#redis_bulk{content=Command} | Remaining]}) ->
    case meta(Command) of
        none -> #redis_error{type= <<"ERR">>, message= <<"unknown command '", Command/binary, "'">>};
        #command_meta{normalized=Normalized, format=Format} ->
            parse_operation(#operation{command=Normalized, key=none, arguments=[]}, Remaining, Format)
    end.

% Internal methods

-spec parse_operation(#operation{}, [#redis_bulk{}], [component()]) -> #operation{} | #redis_error{}.
parse_operation(#operation{arguments=Arguments}=Operation, [], []) ->
    Operation#operation{arguments=lists:reverse(Arguments)};
parse_operation(#operation{key=none}=Operation, [#redis_bulk{content=Key} | Remaining], [key | Format]) ->
    parse_operation(Operation#operation{key=Key}, Remaining, Format);
parse_operation(#operation{arguments=Arguments}=Operation, [#redis_bulk{content=Argument} | Remaining], [str_arg | Format]) ->
    parse_operation(Operation#operation{arguments=[Argument | Arguments]}, Remaining, Format);
parse_operation(#operation{command=Command, key=none}, [], _Format) ->
    #redis_error{type= <<"ERR">>, message= <<"not enough arguments for command '", Command/binary, "'">>};
parse_operation(#operation{command=Command}, _Remaining, []) ->
    #redis_error{type= <<"ERR">>, message= <<"too many arguments for command '", Command/binary, "'">>}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_multi_bulk(Commands) ->
    #redis_multi_bulk{count=length(Commands), items=[#redis_bulk{content=Item} || Item <- Commands]}.

command_operation(Command) ->
    #operation{command=Command, key=none, arguments=[]}.
command_operation(Command, Key) ->
    #operation{command=Command, key=Key, arguments=[]}.

verify_parse_command(Command) ->
    Expected = command_operation(Command),
    Expected = parse_operation(make_multi_bulk([Command])).
verify_parse_command(Command, Key) ->
    Expected = command_operation(Command, Key),
    Expected = parse_operation(make_multi_bulk([Command, Key])).
verify_parse_command(Command, Key, Arg) ->
    Expected = #operation{command=Command, key=Key, arguments=[Arg]},
    Expected = parse_operation(make_multi_bulk([Command, Key, Arg])).

parse_watch_test() ->
    verify_parse_command(<<"WATCH">>, <<"foo">>).

parse_unwatch_test() ->
    verify_parse_command(<<"UNWATCH">>).

parse_multi_test() ->
    verify_parse_command(<<"MULTI">>).

parse_exec_test() ->
    verify_parse_command(<<"EXEC">>).

parse_get_test() ->
    verify_parse_command(<<"GET">>, <<"foo">>).

parse_set_test() ->
    verify_parse_command(<<"SET">>, <<"foo">>, <<"bar">>).

parse_del_test() ->
    verify_parse_command(<<"DEL">>, <<"foo">>).

parse_lowercase_command_test() ->
    Expected = command_operation(<<"GET">>, <<"foo">>),
    Expected = parse_operation(make_multi_bulk([<<"get">>, <<"foo">>])).

parse_mixed_case_test() ->
    Expected = command_operation(<<"MULTI">>),
    Expected = parse_operation(make_multi_bulk([<<"mUlTi">>])).

parse_command_only_test() ->
    Expected = command_operation(<<"foo">>),
    Expected = parse_operation(Expected, [], []).

parse_key_test() ->
    Expected = #operation{command= <<"foo">>, key= <<"bar">>, arguments=[]},
    Expected = parse_operation(command_operation(<<"foo">>), [#redis_bulk{content= <<"bar">>}], [key]).

parse_key_and_one_argument_test() ->
    Expected = #operation{command= <<"foo">>, key= <<"bar">>, arguments=[<<"baz">>]},
    Expected = parse_operation(command_operation(<<"foo">>), [#redis_bulk{content= <<"bar">>}, #redis_bulk{content= <<"baz">>}], [key, str_arg]).

parse_no_key_argument_test() ->
    Expected = #operation{command= <<"foo">>, key=none, arguments=[<<"bar">>]},
    Expected = parse_operation(command_operation(<<"foo">>), [#redis_bulk{content= <<"bar">>}], [str_arg]).

parse_multiple_arguments_test() ->
    Expected = #operation{command= <<"foo">>, key=none, arguments=[<<"bar">>, <<"baz">>]},
    Expected = parse_operation(command_operation(<<"foo">>), [#redis_bulk{content= <<"bar">>}, #redis_bulk{content= <<"baz">>}], [str_arg, str_arg]).

too_many_arguments_command_only_test() ->
    Error = parse_operation(command_operation(<<"foo">>), [#redis_bulk{content= <<"bar">>}], []),
    <<"too many arguments for command 'foo'">> = Error#redis_error.message.

not_enough_arguments_key_expected_test() ->
    Error = parse_operation(command_operation(<<"foo">>), [], [key]),
    <<"not enough arguments for command 'foo'">> = Error#redis_error.message.

-endif.

