-module(redis_store).
-export([connect/2]).
-export([get/3, set/4]). %, delete/3]).
-export([pipeline/1, multi/1, exec/2]).
-compile(export_all).

-include("store.hrl").

-record(connection, {client, state, stored}).

connect(Host, Port) ->
    {ok, Client} = eredis:start_link(Host, Port),
    #connection{client=Client, state=default, stored=[]}.

get(Id, #connection{}=Connection, Key) ->
    handle_command(Id, Connection, ["GET", Key]).

set(Id, #connection{}=Connection, Key, Value) ->
    handle_command(Id, Connection, ["SET", Key, Value]).

multi(#connection{state=State}=Connection) ->
    case State of
        default -> Connection#connection{state=transaction}
    end.

pipeline(#connection{state=State}=Connection) ->
    case State of
        default -> Connection#connection{state=pipeline}
    end.

exec(Id, #connection{state=State}=Connection) ->
    case State of
        transaction -> dispatch_transaction(Id, Connection);
        pipeline -> dispatch_pipeline(Id, Connection)
    end.

handle_command(Id, #connection{client=Client, state=State, stored=Stored}=Connection, Command) ->
    case State of
        default ->
            dispatch_command(Id, Client, Command),
            Connection;
        transaction ->
            Connection#connection{stored=[Command|Stored]};
        pipeline ->
            Connection#connection{stored=[Command|Stored]}
    end.

dispatch_command(Id, Client, Command) ->
    Parent = self(),
    spawn(fun() -> Result = eredis:q(Client, Command), Parent ! #store_result{id=Id, result=Result} end).

dispatch_pipeline(Id, #connection{client=Client, state=pipeline, stored=Commands}) ->
    Parent = self(),
    Ordered = lists:reverse(Commands),
    spawn(fun() -> Result = eredis:qp(Client, Ordered), Parent ! #store_result{id=Id, result=Result} end).

dispatch_transaction(Id, #connection{client=Client, state=transaction, stored=Commands}) ->
    Parent = self(),
    Ordered = lists:reverse(Commands),
    spawn(fun() ->
        {ok, <<"OK">>} = eredis:q(Client, ["MULTI"]),
        lists:foreach(fun(Command) -> {ok, <<"QUEUED">>} = eredis:q(Client, Command) end, Ordered),
        Result = eredis:q(Client, ["EXEC"]),
        Parent ! #store_result{id=Id, result=Result}
    end).

driver() ->
    C = connect("127.0.0.1", 6379),
    io:format("connected~n"),
    C2 = redis_store:set(blah, C, "foo", "bar"),
    io:format("called set~n"),
    redis_store:get(blah, C2, "foo"),
    io:format("called get~n"),
    receive
        Any -> io:format("got a response: ~p~n", [Any])
    end,
    receive
        Any2 -> io:format("got a response: ~p~n", [Any2])
    end.

