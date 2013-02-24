-module(fake_redis).
-behavior(gen_server).
-export([start_link/0, port/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("protocol.hrl").

-record(state, {
    listener :: gen_tcp:socket(),
    port :: inet:port_number(),
    keys :: dict(),
    client :: none | gen_tcp:socket(),
    stream :: none | redis_stream:parse_state()
}).

% Public API

-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec port() -> inet:port_number().
port() ->
    gen_server:call(?MODULE, port).

-spec stop() -> no_return().
stop() ->
    gen_server:call(?MODULE, stop).

% gen_server callbacks

-spec init([]) -> {ok, #state{}}.
init([]) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {packet, raw}]),
    {ok, Port} = inet:port(Listener),
    accept(),
    {ok, #state{listener=Listener, port=Port, keys=dict:new(), client=none, stream=none}}.

-spec handle_call(port | stop, any(), #state{}) -> {reply, any(), #state{}} | {noreply, #state{}}.
handle_call(port, _From, #state{port=Port}=State) ->
    {reply, Port, State};
handle_call(stop, _FROM, State) ->
    {stop, normal, State}.

-spec handle_cast(accept, #state{}) -> {noreply, #state{}} | {stop, any(), #state{}}.
handle_cast(accept, #state{listener=Listener}=State) ->
    case gen_tcp:accept(Listener, 50) of
        {ok, Client} ->
            {noreply, State#state{client=Client, stream=redis_stream:init_request_stream()}};
        {error, timeout} ->
            accept(),
            {noreply, State};
        {error, Reason} ->
            error_logger:error_msg("error ~p accepting new client in fake_redis", [Reason]),
            {stop, Reason, State}
    end.

-spec handle_info(any(), #state{}) -> {noreply, #state{}}.
handle_info({tcp, Client, Data}, #state{client=Client}=State) ->
    {Replies, NewState} = handle_tcp(Data, [], State),
    gen_tcp:send(Client, [redis_stream:format_reply(Reply) || Reply <- Replies]),
    {noreply, NewState};
handle_info({tcp_closed, Client}, #state{client=Client}=State) ->
    accept(),
    {noreply, State#state{client=none}};
handle_info({tcp_error, Client, _Reason}, #state{client=Client}=State) ->
    accept(),
    {noreply, State#state{client=none}}.

-spec terminate(any(), #state{}) -> ok.
terminate(_Reason, #state{}) ->
    ok.

-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% Internal methods

-spec accept() -> ok.
accept() ->
    gen_server:cast(self(), accept).

-spec handle_tcp(binary(), [any()], #state{}) -> {noreply, #state{}}.
handle_tcp(<<>>, Replies, State) ->
    {lists:reverse(Replies), State};
handle_tcp(Data, Replies, #state{keys=Keys, stream=Stream}=State) ->
    case redis_stream:parse_request(Stream, Data) of
        {partial, NewStream} ->
            handle_tcp(<<>>, Replies, State#state{stream=NewStream});
        {Request, Remaining, NewStream} ->
            {Reply, NewKeys} = handle_request(normalize_request(Request), Keys),
            handle_tcp(Remaining, [Reply | Replies], State#state{keys=NewKeys, stream=NewStream})
    end.

-spec handle_request(#redis_multi_bulk{}, dict()) -> {redis_stream:reply(), dict()}.
handle_request(#redis_multi_bulk{items=[#redis_bulk{content= <<"get">>}, #redis_bulk{content=Key}]}, Keys) ->
    Value = case dict:find(Key, Keys) of
        {ok, Any} -> Any;
        error -> none
    end,
    {#redis_bulk{content=Value}, Keys};
handle_request(#redis_multi_bulk{items=[#redis_bulk{content= <<"set">>}, #redis_bulk{content=Key}, #redis_bulk{content=Value}]}, Keys) ->
    {#redis_status{message= <<"OK">>}, dict:store(Key, Value, Keys)};
handle_request(#redis_multi_bulk{items=[#redis_bulk{content= <<"del">>}, #redis_bulk{content=Key}]}, Keys) ->
    {Result, NewKeys} = case dict:is_key(Key, Keys) of
        true -> {<<"1">>, dict:erase(Key, Keys)};
        false -> {<<"0">>, Keys}
    end,
    {#redis_integer{value=Result}, NewKeys}.

-spec normalize_request(#redis_multi_bulk{}) -> #redis_multi_bulk{}.
normalize_request(#redis_multi_bulk{items=[#redis_bulk{content=Command} | Rest]}=Request) ->
    Request#redis_multi_bulk{items=[#redis_bulk{content=list_to_binary(string:to_lower(binary_to_list(Command)))} | Rest]}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

fake_state() ->
    #state{listener=fake_listener, port=fake_port, keys=dict:new(), client=none, stream=none}.

handle_call_port_test() ->
    State = fake_state(),
    {reply, fake_port, State} = handle_call(port, fake_from, State).

handle_call_stop_test() ->
    State = fake_state(),
    {stop, normal, State} = handle_call(stop, fake_from, State).

handle_tcp_empty_test() ->
    State = fake_state(),
    {[], State} = handle_tcp(<<>>, [], State).

handle_tcp_partial_test() ->
    State = (fake_state())#state{stream=redis_stream:init_request_stream()},
    Data = <<"*2\r\n$3\r\nGET\r\n$3\r\nfo">>,
    {partial, ExpectedStream} = redis_stream:parse_request(State#state.stream, Data),
    Expected = {[], State#state{stream=ExpectedStream}},
    Expected = handle_tcp(Data, [], State).

handle_tcp_single_test() ->
    State = (fake_state())#state{stream=redis_stream:init_request_stream()},
    Expected = {[#redis_bulk{content=none}], State},
    Expected = handle_tcp(<<"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n">>, [], State).

handle_tcp_multiple_test() ->
    State = (fake_state())#state{stream=redis_stream:init_request_stream()},
    Expected = {[#redis_status{message= <<"OK">>}, #redis_bulk{content= <<"bar">>}], State#state{keys=dict:store(<<"foo">>, <<"bar">>, dict:new())}},
    Expected = handle_tcp(<<"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n">>, [], State).

handle_get_have_key_test() ->
    Keys = dict:store(<<"foo">>, <<"bar">>, dict:new()),
    Expected = {#redis_bulk{content= <<"bar">>}, Keys},
    Expected = handle_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"get">>}, #redis_bulk{content= <<"foo">>}]}, Keys).

handle_get_missing_key_test() ->
    Keys = dict:new(),
    Expected = {#redis_bulk{content=none}, Keys},
    Expected = handle_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"get">>}, #redis_bulk{content= <<"foo">>}]}, Keys).

handle_set_test() ->
    Expected = {#redis_status{message= <<"OK">>}, dict:store(<<"foo">>, <<"bar">>, dict:new())},
    Expected = handle_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"set">>}, #redis_bulk{content= <<"foo">>}, #redis_bulk{content= <<"bar">>}]}, dict:new()).

handle_del_have_key_test() ->
    Expected = {#redis_integer{value= <<"0">>}, dict:new()},
    Expected = handle_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"del">>}, #redis_bulk{content= <<"foo">>}]}, dict:new()).

handle_del_missing_key_test() ->
    Expected = {#redis_integer{value= <<"1">>}, dict:new()},
    Keys = dict:store(<<"foo">>, <<"bar">>, dict:new()),
    Expected = handle_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"del">>}, #redis_bulk{content= <<"foo">>}]}, Keys).

normalize_uppercase_command_test() ->
    Expected = #redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"foo">>}, #redis_bulk{content= <<"bar">>}]},
    Expected = normalize_request(#redis_multi_bulk{count=2, items=[#redis_bulk{content= <<"FOO">>}, #redis_bulk{content= <<"bar">>}]}).

-endif.

