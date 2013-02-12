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

-module(redis_protocol).
-export([init/0, format_response/1, parse_stream/2]).
-compile(export_all).

-define(NEWLINE, "\r\n").

-record(stream, {parsed= <<>>, unparsed= <<>>, state=parsing_line_count, lines_remaining, command=none, parameters=[]}).

init() ->
    #stream{parsed= <<>>, unparsed= <<>>}.

format_response(Response) when is_list(Response) ->
    MultiStart = list_to_binary(lists:append(["*", integer_to_list(length(Response)), "\r\n"])),
    list_to_binary([MultiStart|lists:foldr(fun(E, A) -> [format_response(E)|A] end, [], Response)]);
format_response(Response) when is_integer(Response) ->
    list_to_binary(lists:append([":", integer_to_list(Response), "\r\n"]));
format_response(undefined) ->
    <<"$-1\r\n">>;
format_response(error) ->
    <<"-ERROR\r\n">>;
format_response(stored) ->
    <<"+QUEUED\r\n">>;
format_response(ok) ->
    <<"+OK\r\n">>;
format_response(Response) ->
    list_to_binary(lists:append(["$", integer_to_list(byte_size(Response)), "\r\n", binary_to_list(Response), "\r\n"])).

parse_stream(#stream{parsed=Parsed, unparsed=Unparsed}=Stream, NewData) ->
    parse_stream(Stream#stream{parsed= <<>>, unparsed= <<Parsed/binary, Unparsed/binary, NewData/binary>>}).

parse_stream(#stream{state=error}=Stream) ->
    {Stream, protocol_error};
parse_stream(#stream{unparsed=Unparsed, state=complete, command=Command, parameters=Parameters}) ->
    {#stream{unparsed=Unparsed}, {command, Command, lists:reverse(Parameters)}};
parse_stream(#stream{unparsed= <<>>}=Stream) ->
    {Stream, incomplete};
parse_stream(#stream{state=State}=Stream) ->
    NewStream = case State of
        parsing_line_count ->
            parse_line_count(Stream);
        parsing_command ->
            parse_command(Stream);
        parsing_parameters ->
            parse_parameter(Stream)
    end,
    parse_stream(NewStream).

parse_line_count(#stream{unparsed=Unparsed, state=parsing_line_count}=Stream) ->
    NewLine = find_newline(Unparsed),
    case NewLine of
        none ->
            Stream#stream{parsed=Unparsed, unparsed= <<>>};
        _Else ->
            <<Line:NewLine/binary, ?NEWLINE, NewUnparsed/binary>> = Unparsed,
            parse_line_count_value(Stream, NewUnparsed, Line)
    end.

parse_line_count_value(Stream, NewUnparsed, <<$*, Line/binary>>) ->
    Stream#stream{parsed= <<>>, unparsed=NewUnparsed, state=parsing_command, lines_remaining=list_to_integer(binary_to_list(Line))};
parse_line_count_value(_Stream, _NewUnparsed, _Any) ->
    #stream{state=error}.

parse_command(#stream{unparsed=Unparsed, lines_remaining=Remaining}=Stream) ->
    Line = parse_line(Unparsed),
    case Line of
        {ok, Command, NewUnparsed} ->
            NewRemaining = Remaining - 1,
            Stream#stream{unparsed=NewUnparsed, state=new_state(NewRemaining), lines_remaining=NewRemaining, command=Command};
        incomplete ->
            Stream#stream{parsed=Unparsed, unparsed= <<>>};
        error ->
            #stream{state=error}
    end.

parse_parameter(#stream{unparsed=Unparsed, lines_remaining=Remaining, parameters=Parameters}=Stream) ->
    Line = parse_line(Unparsed),
    case Line of
        {ok, Parameter, NewUnparsed} ->
            NewRemaining = Remaining - 1,
            Stream#stream{unparsed=NewUnparsed, state=new_state(NewRemaining), lines_remaining=NewRemaining, parameters=[Parameter|Parameters]};
        incomplete ->
            Stream#stream{parsed=Unparsed, unparsed= <<>>};
        error ->
            #stream{state=error}
    end.

parse_line(Unparsed) ->
    NewLine = find_newline(Unparsed),
    case NewLine of
        none ->
            incomplete;
        _Else ->
            <<Line:NewLine/binary, ?NEWLINE, NewUnparsed/binary>> = Unparsed,
            parse_line(Line, NewUnparsed)
    end.

parse_line(Line, Unparsed) ->
    Length = parse_line_length(Line),
    case Length of
        {ok, ValueLength} ->
            parse_line_value(ValueLength, Unparsed);
        error ->
            error
    end.

parse_line_length(<<$$, Length/binary>>) ->
    {ok, list_to_integer(binary_to_list(Length))};
parse_line_length(_Any) ->
    error.

parse_line_value(ValueLength, Unparsed) ->
    UnparsedLength = byte_size(Unparsed),
    if
        UnparsedLength >= ValueLength + 2 ->
            <<Line:ValueLength/binary, ?NEWLINE, NewUnparsed/binary>> = Unparsed,
            {ok, Line, NewUnparsed};
        true -> incomplete
    end.

new_state(0) ->
    complete;
new_state(_Remaining) ->
    parsing_parameters.

find_newline(B) ->
    case re:run(B, ?NEWLINE) of
        {match, [{Pos, _}]} -> Pos;
        nomatch -> none
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

format_response_test() ->
    <<"$3\r\nfoo\r\n">> = format_response(<<"foo">>),
    <<"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">> = format_response([<<"foo">>, <<"bar">>]),
    <<"+OK\r\n">> = format_response(ok),
    <<"$-1\r\n">> = format_response(undefined),
    <<"-ERROR\r\n">> = format_response(error),
    <<"+QUEUED\r\n">> = format_response(stored),
    <<":42\r\n">> = format_response(42).

parse_stream_test() ->
    First = <<"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>,
    Second = <<"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>,
    {#stream{unparsed=Second}, {command, <<"get">>, [<<"foo">>]}} = parse_stream(init(), First),
    {#stream{}, {command, <<"set">>, [<<"foo">>, <<"bar">>]}} = parse_stream(#stream{unparsed=Second}, <<>>),
    {#stream{}, incomplete} = parse_stream(init(), <<>>),
    S = #stream{parsed= <<"*2">>},
    {S, incomplete} = parse_stream(S, <<>>),
    {#stream{}, {command, <<"foo">>, [<<"bar">>]}} = parse_stream(S, <<"\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>),
    {#stream{state=error}, protocol_error} = parse_stream(init(), <<"*2\r\nfoo\r\nbar">>).

parse_line_count_test() ->
    #stream{unparsed= <<"$3\r\nfoo\r\n$3bar\r\n">>, state=parsing_command, lines_remaining=2} = parse_line_count(#stream{unparsed= <<"*2\r\n$3\r\nfoo\r\n$3bar\r\n">>, state=parsing_line_count}),
    #stream{parsed= <<"foobarbaz">>, state=parsing_line_count} = parse_line_count(#stream{unparsed= <<"foobarbaz">>, state=parsing_line_count}),
    #stream{state=error} = parse_line_count(#stream{unparsed= <<"foobarbaz\r\n">>, state=parsing_line_count}).

parse_command_test() ->
    #stream{unparsed= <<"$3\r\nbar\r\n">>, state=parsing_parameters, lines_remaining=1, command= <<"foo">>} = parse_command(#stream{unparsed= <<"$3\r\nfoo\r\n$3\r\nbar\r\n">>, state=parsing_command, lines_remaining=2}).

parse_line_test() ->
    {ok, <<"foo">>, <<"bar">>} = parse_line(<<"$3\r\nfoo\r\nbar">>),
    incomplete = parse_line(<<"$3\r\nfo">>),
    incomplete = parse_line(<<"$3">>),
    error = parse_line(<<"3\r\nfoo">>).

parse_line_length_test() ->
    {ok, 42} = parse_line_length(<<"$42">>),
    error = parse_line_length(<<"42">>).

parse_line_value_test() ->
    {ok, <<"foo">>, <<"bar">>} = parse_line_value(3, <<"foo\r\nbar">>),
    incomplete = parse_line_value(3, <<"fo">>).

find_newline_test() ->
    3 = find_newline(<<"foo\r\nbar">>),
    none = find_newline(<<"foobar">>).

-endif.

