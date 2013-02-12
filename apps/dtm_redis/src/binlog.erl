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

-module(binlog).
-behavior(gen_server).
-export([start_link/2, read/2, write/3, delete/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(operation, {requestor, id}).
-record(state, {file, filename, operations, data}).
-record(read, {operation}).
-record(write, {operation, data}).
-record(delete, {operation}).

% API methods

start_link(Name, Filename) ->
    gen_server:start_link({local, Name}, ?MODULE, Filename, []).

read(Name, OpId) ->
    gen_server:cast(Name, #read{operation=#operation{requestor=self(), id=OpId}}).

write(Name, OpId, Data) ->
    gen_server:cast(Name, #write{operation=#operation{requestor=self(), id=OpId}, data=Data}).

delete(Name, OpId) ->
    gen_server:cast(Name, #delete{operation=#operation{requestor=self(), id=OpId}}).

stop(Name) ->
    gen_server:call(Name, stop).

% gen_server callbacks

init(Filename) ->
    error_logger:info_msg("starting binlog with pid ~p, writing to file ~p", [self(), Filename]),
    {ok, FD} = file:open(Filename, [append, raw, binary, read]),
    {ok, #state{file=FD, filename=Filename, operations=[], data=[]}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(Message, From, _State) ->
    error_logger:error_msg("binlog:handle_call unhandled message ~p from ~p", [Message, From]),
    erlang:throw({error, unhandled}).

handle_cast(#read{operation=Operation}, #state{}=State) ->
    NewState = handle_read(Operation, State),
    {noreply, NewState};
handle_cast(#write{operation=Operation, data=Data}, #state{}=State) ->
    NewState = handle_write(Operation, Data, State),
    {noreply, NewState, 0};
handle_cast(#delete{operation=Operation}, #state{}=State) ->
    NewState = handle_delete(Operation, State),
    {noreply, NewState};
handle_cast(Message, _State) ->
    error_logger:error_msg("binlog:handle_cast unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

handle_info(timeout, #state{file=FD, operations=Operations, data=Data}=State) ->
    write_to_file(FD, lists:reverse(Operations), lists:reverse(Data)),
    {noreply, State#state{operations=[], data=[]}};
handle_info(Message, _State) ->
    error_logger:error_msg("binlog:handle_info unhandled message ~p", [Message]),
    erlang:throw({error, unhandled}).

terminate(Reason, #state{file=FD}) ->
    error_logger:info_msg("terminating binlog because ~p", [Reason]),
    file:close(FD),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internal methods

handle_read(#operation{requestor=Requestor, id=OpId}, #state{filename=Filename}=State) ->
    {ok, FD} = file:open(Filename, [raw, binary, read]),
    {ok, <<BinDataSize:32/integer-unsigned>>} = file:read(FD, 4),
    {ok, BinData} = file:read(FD, BinDataSize),
    Data = binary_to_term(BinData),
    Requestor ! {binlog_data_read, OpId, Data},
    State.

handle_write(#operation{}=Operation, Data, #state{operations=CurOperations, data=CurData}=State) ->
    State#state{operations=[Operation|CurOperations], data=[convert_data(Data)|CurData]}.

convert_data(Data) ->
    BinData = term_to_binary(Data, [compressed]),
    BinDataSize = byte_size(BinData),
    <<BinDataSize:32/integer-unsigned, BinData/binary>>.

handle_delete(#operation{requestor=Requestor, id=OpId}, #state{file=FD, filename=Filename}=State) ->
    file:close(FD),
    file:delete(Filename),
    Requestor ! {binlog_data_deleted, OpId, Filename},
    State#state{file=closed}.

write_to_file(FD, Operations, Data) ->
    file:write(FD, Data),
    file:sync(FD),
    lists:foreach(fun(#operation{requestor=Requestor, id=OpId}) ->
            Requestor ! {binlog_data_written, OpId}
        end,
        Operations).

% tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_start() ->
    random:seed(now()),
    start_link(test_binlog, lists:flatten(io_lib:format('/tmp/dtm-redis.test.binlog~p', [random:uniform(10000)]))).

test_receive() ->
    receive Any -> Any end.

file_deleted_test() ->
    test_start(),
    OpId = random:uniform(10000),
    write(test_binlog, OpId, 'My Message'),
    {binlog_data_written, OpId} = test_receive(),
    delete(test_binlog, OpId),
    {binlog_data_deleted, OpId, Filename} = test_receive(),
    {error, enoent} = file:read_file_info(Filename),
    stop(test_binlog).

single_write_test() ->
    test_start(),
    OpId = random:uniform(10000),
    write(test_binlog, OpId, 'My Message'),
    {binlog_data_written, OpId} = test_receive(),
    delete(test_binlog, OpId),
    {binlog_data_deleted, OpId, _Filename} = test_receive(),
    stop(test_binlog).

single_write_tuple_test() ->
    test_start(),
    OpId = random:uniform(10000),
    write(test_binlog, OpId, {'Foo', 'Bar', ['boo', 123]}),
    {binlog_data_written, OpId} = test_receive(),
    delete(test_binlog, OpId),
    {binlog_data_deleted, OpId, _Filename} = test_receive(),
    stop(test_binlog).

single_multiple_at_once_test() ->
    test_start(),
    OpId1 = random:uniform(10000),
    OpId2 = random:uniform(10000),
    write(test_binlog, OpId1, 'My Message'),
    write(test_binlog, OpId2, 'My Message2'),
    {binlog_data_written, OpId1} = test_receive(),
    {binlog_data_written, OpId2} = test_receive(),
    delete(test_binlog, OpId1),
    {binlog_data_deleted, OpId1, _Filename} = test_receive(),
    stop(test_binlog).

read_first_string_test() ->
    test_start(),
    OpId = random:uniform(10000),
    Message = 'Test1234',
    write(test_binlog, OpId, Message),
    {binlog_data_written, OpId} = test_receive(),
    read(test_binlog, OpId),
    {binlog_data_read, OpId, Message} = test_receive(),
    delete(test_binlog, OpId),
    {binlog_data_deleted, OpId, _Filename} = test_receive(),
    stop(test_binlog).

read_first_complex_test() ->
    test_start(),
    OpId = random:uniform(10000),
    Message = {'Foo', 'Bar', ['boo', 123]},
    write(test_binlog, OpId, Message),
    {binlog_data_written, OpId} = test_receive(),
    read(test_binlog, OpId),
    {binlog_data_read, OpId, Message} = test_receive(),
    delete(test_binlog, OpId),
    {binlog_data_deleted, OpId, _Filename} = test_receive(),
    stop(test_binlog).

-endif.

