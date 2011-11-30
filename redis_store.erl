-module(redis_store).
-export([connect/2]).
-export([get_async/2, set_async/3]). %, delete/2]).
%-export([pipeline/0, multi/0, exec/0]).
-compile(export_all).

-record(connection, {client, state, stored}).

connect(Host, Port) ->
    {ok, Client} = eredis_client:start_link(Host, Port, 0, "", 100),
    #connection{client=Client, state=default, stored=[]}.

get_async(#connection{}=Connection, Key) ->
    send_command(Connection, ["GET", Key]).

set_async(#connection{}=Connection, Key, Value) ->
    send_command(Connection, ["SET", Key, Value]).
    
send_command(#connection{client=Client, state=State, stored=Stored}=Connection, Command) ->
    case State of
        default ->
            gen_server:cast(Client, Command),
            Connection;
        transaction ->
            Connection#connection{stored=[Command|Stored]};
        pipeline ->
            Connection#connection{stored=[Command|Stored]}
    end.

driver() ->
    C = connect("127.0.0.1", 6379),
    C2 = set_async(C, "foo", "bar"),
    get_async(C2, "foo"),
    receive
        Any -> io:format("got a response: ~p~n", [Any])
    end.

