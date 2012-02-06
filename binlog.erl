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

-module(binlog).
-export([init/1, write/3]).

-record(state, {listener, file, filename}).
-record(binlog_state, {pid}).

-include_lib("eunit/include/eunit.hrl").

start(Listener, FileName) ->
    {ok, FD} = file:open(FileName, [append, raw, binary, read]),
    loop(#state{listener=Listener, file=FD, filename=FileName}).

write_to_file(#state{listener=Listener, file=FD}, Messages, OpIds) ->
    file:write(FD, Messages),
    file:sync(FD),
    lists:foreach(fun(OpId) ->
			  Listener ! {binlog_data_written, OpId}
		  end,
		  OpIds),
    done.

convert_data(Message) ->
    BinData = term_to_binary(Message, [compressed]),
    BinDataSize = byte_size(BinData),
    [<<BinDataSize:32/integer-unsigned>>, BinData].

process(State, Messages, OpIds) when is_list(Messages); is_list(OpIds) ->
    case listen(State, 0) of
	stop ->
	    stop;
	none ->
	    write_to_file(State, lists:reverse(Messages), lists:reverse(OpIds));
        {Data, OpId} ->
	    process(State, [Data|Messages], [OpId|OpIds])
    end.

listen(#state{listener=Listener, file=FD, filename= FileName}, Timeout) ->
    receive
	{read, Listener, OpId} ->
	    {ok, FD2} = file:open(FileName, [raw, binary, read]),
	    {ok, <<BinDataSize:32/integer-unsigned>>} = file:read(FD2, 4),
	    {ok, BinData} = file:read(FD2, BinDataSize),
	    Data = binary_to_term(BinData),
	    Listener ! {binlog_data_read, OpId, Data},
	    done;
        {write, Listener, Data, OpId} ->
	    {convert_data(Data), OpId};
	{delete, Listener, OpId} ->
	    file:close(FD),
	    file:delete(FileName),
	    Listener ! {binlog_data_deleted, OpId, FileName},
	    done;
        stop ->
            io:format("Binlog halting after receiving stop message~n"),
	    stop;
	Any ->
	    io:format("My unknown message ~p~n", [Any])
    after Timeout ->
	    none
    end.

loop(State) ->
    case listen(State, infinity) of
	stop ->
	    stop;
	{Data, OpId} ->
	    Ret = process(State, [Data], [OpId]),
	    if stop /= Ret ->
		    loop(State)
	    end;
	done ->
	    loop(State)
    end.

init() ->
    random:seed(now()),
    init(io_lib:format('/tmp/dtm-redis.test.binlog~p', [random:uniform(10000)])).

init(Filename) ->
    MyPid = self(),
    Pid = spawn(fun() -> start(MyPid, Filename) end),
    #binlog_state{pid=Pid}. 

write(#binlog_state{pid = Pid}, OpId, Message) ->
    Pid ! {write, self(), Message, OpId}.

read(#binlog_state{pid = Pid}, OpId) ->
    Pid ! {read, self(), OpId}.

delete(#binlog_state{pid = Pid}, OpId) ->
    Pid ! {delete, self(), OpId}.

testloop(_State) ->
    receive
	{binlog_data_written, OpId} ->
	    io:format('Data written ~p~n', [OpId]),
	    OpId;
	{binlog_data_read, OpId, Data} ->
	    io:format('Data read ~p: ~p~n', [OpId, Data]),
	    {OpId, Data};
	{binlog_data_deleted, OpId, Filename} ->
	    io:format('File deleted ~p ~p~n', [OpId, Filename]),
	    {OpId, Filename};
	Any ->
	    io:format('Unknown data received ~p~n', [Any])
    end.


file_deleted_test() ->
    State = init(),
    OpId = random:uniform(10000),
    write(State, OpId, 'My Message'),
    OpId = testloop(State),
    delete(State, OpId),
    {OpId, FileName} = testloop(State),
    {error, enoent} = file:read_file_info(FileName).

single_write_test() ->
    State = init(),
    OpId = random:uniform(10000),
    write(State, OpId, 'My Message'),
    OpId = testloop(State),
    delete(State, OpId),
    {OpId, _} = testloop(State).

single_write_tuple_test() ->
    State = init(),
    OpId = random:uniform(10000),
    write(State, OpId, {'Foo', 'Bar', ['boo', 123]}),
    OpId = testloop(State),
    delete(State, OpId),
    {OpId, _} = testloop(State).


single_multiple_at_once_test() ->
    State = init(),
    OpId1 = random:uniform(10000),
    OpId2 = random:uniform(10000),
    write(State, OpId1, 'My Message'),
    write(State, OpId2, 'My Message2'),
    OpId1 = testloop(State),
    OpId2 = testloop(State),
    delete(State, OpId1),
    {OpId1, _} = testloop(State).

read_first_string_test() ->
    State = init(),
    OpId = random:uniform(10000),
    Message = 'Test1234',
    write(State, OpId, Message),
    OpId = testloop(State),
    read(State, OpId),
    {OpId, Message} = testloop(State),
    delete(State, OpId),
    {OpId, _} = testloop(State).

read_first_complex_test() ->
    State = init(),
    OpId = random:uniform(10000),
    Message = {'Foo', 'Bar', ['boo', 123]},
    write(State, OpId, Message),
    OpId = testloop(State),
    read(State, OpId),
    {OpId, Message} = testloop(State),
    delete(State, OpId),
    {OpId, _} = testloop(State).

