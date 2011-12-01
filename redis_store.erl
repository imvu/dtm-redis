-module(redis_store).
-export([connect/2]).
-export([get/2, get/3, set/3, set/4, delete/3]).
-export([pipeline/1, transaction/1, commit/2]).
-compile(export_all).

-include("store.hrl").

-record(default, {client}).
-record(transaction, {client, stored=[]}).
-record(pipeline, {client, stored=[]}).

-include_lib("eunit/include/eunit.hrl").

connect(Host, Port) ->
    {ok, Client} = eredis:start_link(Host, Port),
    #default{client=Client}.

get_command(Key) ->
    ["GET", Key].

get(Id, #default{client=Client}, Key) ->
    dispatch_command(Id, Client, get_command(Key)).

get(#transaction{}=State, Key) ->
    store_command(State, get_command(Key));
get(#pipeline{}=State, Key) ->
    store_command(State, get_command(Key)).

set_command(Key, Value) ->
    ["SET", Key, Value].

set(Id, #default{client=Client}, Key, Value) ->
    dispatch_command(Id, Client, set_command(Key, Value)).

set(#transaction{}=State, Key, Value) ->
    store_command(State, set_command(Key, Value));
set(#pipeline{}=State, Key, Value) ->
    store_command(State, set_command(Key, Value)).

delete_command(Key) ->
    ["DEL", Key].

delete(Id, #default{client=Client}, Key) ->
    dispatch_command(Id, Client, delete_command(Key)).

delete(#transaction{}=State, Key) ->
    store_command(State, delete_command(Key));
delete(#pipeline{}=State, Key) ->
    store_command(State, delete_command(Key)).

transaction(#default{client=Client}) ->
    #transaction{client=Client}.

pipeline(#default{client=Client}) ->
    #pipeline{client=Client}.

commit(Id, #transaction{client=Client, stored=Commands}) ->
    Parent = self(),
    Ordered = lists:reverse(Commands),
    spawn(fun() ->
        {ok, <<"OK">>} = eredis:q(Client, ["MULTI"]),
        lists:foreach(fun(Command) -> {ok, <<"QUEUED">>} = eredis:q(Client, Command) end, Ordered),
        Result = eredis:q(Client, ["EXEC"]),
        Parent ! #store_result{id=Id, result=Result}
    end);
commit(Id, #pipeline{client=Client, stored=Commands}) ->
    Parent = self(),
    Ordered = lists:reverse(Commands),
    spawn(fun() -> Result = eredis:qp(Client, Ordered), Parent ! #store_result{id=Id, result=Result} end).

dispatch_command(Id, Client, Command) ->
    Parent = self(),
    spawn(fun() -> Result = eredis:q(Client, Command), Parent ! #store_result{id=Id, result=Result} end).

store_command(#transaction{stored=Stored}=Transaction, Command) ->
    Transaction#transaction{stored=[Command|Stored]};
store_command(#pipeline{stored=Stored}=Pipeline, Command) ->
    Pipeline#pipeline{stored=[Command|Stored]}.

get_set_test() ->
    C = connect("127.0.0.1", 6379),
    redis_store:set(blah, C, "foo", "bar"),
    redis_store:get(blah, C, "foo"),
    success = test_receive([{ok, <<"OK">>}, {ok, <<"bar">>}]).

transaction_test() ->
    C = connect("127.0.0.1", 6379),
    Trans = transaction(C),
    Trans2 = redis_store:set(Trans, "foo", "bar"),
    Trans3 = redis_store:get(Trans2, "foo"),
    commit(blah, Trans3),
    success = test_receive([{ok, [<<"OK">>, <<"bar">>]}]).

pipeline_test() ->
    C = connect("127.0.0.1", 6379),
    Pipe = pipeline(C),
    Pipe2 = redis_store:set(Pipe, "foo", "bar"),
    Pipe3 = redis_store:get(Pipe2, "foo"),
    commit(blah, Pipe3),
    success = test_receive([[{ok, <<"OK">>}, {ok, <<"bar">>}]]).

test_receive([]) ->
    success;
test_receive([Expected|T]) ->
    {store_result, blah, Expected} = receive Any -> Any end,
    test_receive(T).

