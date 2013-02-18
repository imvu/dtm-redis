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

-module(redis_stream).

-export([make_binary/1, create_multi_bulk/1, format_reply/1, init/0, parse/2]).

-include("protocol.hrl").

-record(partial_status, {
    previous :: binary()
}).
-record(partial_error, {
}).
-record(partial_integer, {
    previous :: binary()
}).
-record(partial_bulk, {
    previous :: binary(),
    len :: pos_integer(),
    seen :: non_neg_integer(),
    buffers :: [binary()]
}).
-record(partial_multi_bulk, {
    previous :: binary(),
    count :: pos_integer(),
    seen :: non_neg_integer(),
    current :: none | #partial_status{} | #partial_error{} | #partial_integer{} | #partial_bulk{},
    replies :: [simple_reply()]
}).

-type parse_state() :: none | #partial_status{} | #partial_error{} | #partial_integer{} | #partial_bulk{} | #partial_multi_bulk{}.
-type parse_result() :: {partial, parse_state()} | {reply(), binary(), parse_state()}.

% Public API

-spec make_binary(binary() | list() | integer() | atom()) -> binary().
make_binary(X) when is_binary(X) ->
    X;
make_binary(X) when is_list(X) ->
    list_to_binary(X);
make_binary(X) when is_integer(X) ->
    make_binary(integer_to_list(X));
make_binary(X) when is_atom(X) ->
    make_binary(atom_to_list(X)).

-spec create_multi_bulk([binary() | list() | atom() | integer()]) -> iolist().
create_multi_bulk(Items) ->
    {Multi, Count} = lists:foldl(fun(Item, {M, C}) ->
            {[<<"\r\n">>, make_binary(Item), <<"\r\n">>, list_to_binary(integer_to_list(byte_size(Item))), <<"$">> | M], C + 1}
        end, {[], 0}, Items),
    [<<"*">>, make_binary(Count), <<"\r\n">> | lists:reverse(Multi)].

-spec format_reply(reply()) -> iolist().
format_reply(#status_reply{message=Message}) ->
    <<"+", Message/binary, "\r\n">>;
format_reply(#error_reply{type=Type, message=Message}) ->
    <<"-", Type/binary, " ", Message/binary, "\r\n">>;
format_reply(#integer_reply{value=Value}) ->
    <<":", Value/binary, "\r\n">>;
format_reply(#bulk_reply{content=none}) ->
    <<"$-1\r\n">>;
format_reply(#bulk_reply{content=Content}) ->
    Len = list_to_binary(integer_to_list(byte_size(Content))),
    <<"$", Len/binary, "\r\n", Content/binary, "\r\n">>;
format_reply(#multi_bulk_reply{count=Count, items=Items}) ->
    [<<"*">>, list_to_binary(integer_to_list(Count)), <<"\r\n">> | [format_reply(Item) || Item <- Items]].

-spec init() -> none.
init() ->
    none.

-spec parse(parse_state(), binary()) -> parse_result().
parse(State, <<>>) ->
    {partial, State};
parse(none, Data) ->
    parse_type(Data);
parse(#partial_status{}=Partial, Data) ->
    parse_status(Partial, Data);
parse(#partial_integer{}=Partial, Data) ->
    parse_integer(Partial, Data);
parse(#partial_bulk{}=Partial, Data) ->
    parse_bulk(Partial, Data);
parse(#partial_multi_bulk{}=Partial, Data) ->
    parse_multi_bulk(Partial, Data).

% private methods

-spec parse_type(binary()) -> parse_result().
parse_type(<<$+, Remaining/binary>>) ->
    parse(#partial_status{previous= <<>>}, Remaining);
parse_type(<<$:, Remaining/binary>>) ->
    parse(#partial_integer{previous= <<>>}, Remaining);
parse_type(<<$$, Remaining/binary>>) ->
    parse(#partial_bulk{previous= <<>>, len=none, seen=0, buffers=[]}, Remaining);
parse_type(<<$*, Remaining/binary>>) ->
    parse(#partial_multi_bulk{previous= <<>>, count=none, seen=0, current=none, replies=[]}, Remaining).

-spec parse_status(#partial_status{}, binary()) -> parse_result().
parse_status(#partial_status{previous=Previous}, Data) ->
    Combined = <<Previous/binary, Data/binary>>,
    case binary:split(Combined, <<"\r\n">>) of
        [Message, Remaining] -> {#status_reply{message=Message}, Remaining, none};
        [Combined] -> {partial, #partial_status{previous=Combined}}
    end.

-spec parse_integer(#partial_integer{}, binary()) -> parse_result().
parse_integer(#partial_integer{previous=Previous}, Data) ->
    Combined = <<Previous/binary, Data/binary>>,
    case binary:split(Combined, <<"\r\n">>) of
        [Integer, Remaining] -> {#integer_reply{value=Integer}, Remaining, none};
        [Combined] -> {partial, #partial_integer{previous=Combined}}
    end.

-spec parse_bulk(#partial_bulk{}, binary()) -> parse_result().
parse_bulk(#partial_bulk{previous=Previous, len=none}=Partial, Data) ->
    Combined = <<Previous/binary, Data/binary>>,
    case binary:split(Combined, <<"\r\n">>) of
        [Integer, Remaining] ->
            IntVal = list_to_integer(binary_to_list(Integer)),
            case IntVal of
                -1 -> {#bulk_reply{content=none}, Remaining, none};
                _Any -> parse_bulk(Partial#partial_bulk{previous=none, len=IntVal}, Remaining)
            end;
        [Combined] -> {partial, Partial#partial_bulk{previous=Combined}}
    end;
parse_bulk(#partial_bulk{len=Len, seen=Seen, buffers=Buffers}, Data) when (Seen >= Len + 2) ->
    <<Content:Len/binary, _CrLf:2/binary, Remaining/binary>> = lists:foldl(fun(Buffer, Acc) ->
            <<Buffer/binary, Acc/binary>>
        end, <<>>, Buffers),
    {#bulk_reply{content=Content}, <<Remaining/binary, Data/binary>>, none};
parse_bulk(#partial_bulk{}=Partial, <<>>) ->
    {partial, Partial};
parse_bulk(#partial_bulk{seen=Seen, buffers=Buffers}=Partial, Data) ->
    parse_bulk(Partial#partial_bulk{seen=Seen + byte_size(Data), buffers=[Data | Buffers]}, <<>>).

-spec parse_multi_bulk(#partial_multi_bulk{}, binary()) -> parse_result().
parse_multi_bulk(#partial_multi_bulk{previous=Previous, count=none}=Partial, Data) ->
    Combined = <<Previous/binary, Data/binary>>,
    case binary:split(Combined, <<"\r\n">>) of
        [Integer, Remaining] -> parse_multi_bulk(Partial#partial_multi_bulk{previous=none, count=list_to_integer(binary_to_list(Integer))}, Remaining);
        [Combined] -> {partial, Partial#partial_multi_bulk{previous=Combined}}
    end;
parse_multi_bulk(#partial_multi_bulk{count=Count, seen=Seen, replies=Replies}, Data) when (Count =:= Seen) ->
    {#multi_bulk_reply{count=Count, items=lists:reverse(Replies)}, Data, none};
parse_multi_bulk(Partial, <<>>) ->
    {partial, Partial};
parse_multi_bulk(#partial_multi_bulk{current=none}=Partial, Data) ->
    parse_multi_bulk_type(Data, Partial);
parse_multi_bulk(#partial_multi_bulk{seen=Seen, current=Current, replies=Replies}=Partial, Data) ->
    case parse(Current, Data) of
        {partial, NewCurrent} -> {partial, Partial#partial_multi_bulk{current=NewCurrent}};
        {Reply, Remaining, none} -> parse_multi_bulk(Partial#partial_multi_bulk{seen=Seen + 1, current=none, replies=[Reply | Replies]}, Remaining)
    end.
 
-spec parse_multi_bulk_type(binary(), #partial_multi_bulk{}) -> parse_result().
parse_multi_bulk_type(<<$+, Remaining/binary>>, Partial) ->
    parse_multi_bulk(Partial#partial_multi_bulk{current=#partial_status{previous= <<>>}}, Remaining);
parse_multi_bulk_type(<<$:, Remaining/binary>>, Partial) ->
    parse_multi_bulk(Partial#partial_multi_bulk{current=#partial_integer{previous= <<>>}}, Remaining);
parse_multi_bulk_type(<<$$, Remaining/binary>>, Partial) ->
    parse_multi_bulk(Partial#partial_multi_bulk{current=#partial_bulk{previous= <<>>, len=none, seen=0, buffers=[]}}, Remaining).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_binary_binary_test() ->
    <<"foo">> = make_binary(<<"foo">>).

make_binary_string_test() ->
    <<"foo">> = make_binary("foo").

make_binary_integer_test() ->
    <<"42">> = make_binary(42).

make_binary_atom_test() ->
    <<"foo">> = make_binary(foo).

create_multi_bulk_test() ->
    <<"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n">> = lists:foldr(fun(Bin, Acc) ->
            <<Bin/binary, Acc/binary>>
        end, <<>>, create_multi_bulk([<<"foo">>, <<"bar">>, <<"baz">>])).

format_status_reply_test() ->
    <<"+OK\r\n">> = format_reply(#status_reply{message= <<"OK">>}).

format_error_reply_test() ->
    <<"-ERR foo bar baz\r\n">> = format_reply(#error_reply{type= <<"ERR">>, message= <<"foo bar baz">>}).

format_integer_reply_test() ->
    <<":42\r\n">> = format_reply(#integer_reply{value= <<"42">>}).

format_bulk_reply_test() ->
    <<"$3\r\nfoo\r\n">> = format_reply(#bulk_reply{content= <<"foo">>}).

format_empty_bulk_reply_test() ->
    <<"$-1\r\n">> = format_reply(#bulk_reply{content=none}).

format_multi_bulk_reply_test() ->
    <<"*3\r\n$3\r\nfoo\r\n+OK\r\n:42\r\n">> = lists:foldr(fun(Bin, Acc) ->
            <<Bin/binary, Acc/binary>>
        end, <<>>, format_reply(#multi_bulk_reply{count=3, items=[
            #bulk_reply{content= <<"foo">>},
            #status_reply{message= <<"OK">>},
            #integer_reply{value= <<"42">>}
    ]})).

parse_initial_empty_test() ->
    {partial, none} = parse(none, <<>>).

verify_partial_and_complete(Expected, Data) ->
    Expected = lists:foldl(fun(Byte, {partial, State}) ->
            parse(State, <<Byte>>)
        end, {partial, none}, Data).

parse_status_test() ->
    verify_partial_and_complete({#status_reply{message= <<"OK">>}, <<>>, none}, "+OK\r\n").

parse_integer_test() ->
    verify_partial_and_complete({#integer_reply{value= <<"42">>}, <<>>, none}, ":42\r\n").

parse_bulk_test() ->
    verify_partial_and_complete({#bulk_reply{content= <<"foo">>}, <<>>, none}, "$3\r\nfoo\r\n").

parse_empty_bulk_test() ->
    verify_partial_and_complete({#bulk_reply{content=none}, <<>>, none}, "$-1\r\n").

parse_multi_bulk_test() ->
    verify_partial_and_complete({#multi_bulk_reply{count=3, items=[
        #status_reply{message= <<"STORED">>},
        #integer_reply{value= <<"42">>},
        #bulk_reply{content= <<"foo">>}
    ]}, <<>>, none}, "*3\r\n+STORED\r\n:42\r\n$3\r\nfoo\r\n").

parse_multiple_test() ->
    Expected = #status_reply{message= <<"OK">>},
    {Expected, Remaining, State} = parse(none, <<"+OK\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n">>),
    Expected2 = #multi_bulk_reply{count=2, items=[
        #bulk_reply{content= <<"foo">>},
        #bulk_reply{content= <<"bar">>}
    ]},
    {Expected2, Remaining2, State2} = parse(State, Remaining),
    Expected3 = #integer_reply{value= <<"42">>},
    {Expected3, <<>>, none} = parse(State2, Remaining2).

-endif.

