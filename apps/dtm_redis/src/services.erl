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

-module(services).

-export([call/4, real/0, fake/0]).

% types

-record(fake_services, {
    map :: dict()
}).

-record(fake, {
    module :: atom(),
    state :: any()
}).

-type type() :: real_services | #fake_services{}.

% Public API

-spec call(module(), atom(), [any()], type()) -> {any(), type()}.
call(Module, Function, Args, real_services) when is_atom(Module) andalso is_atom(Function) andalso is_list(Args) ->
    {erlang:apply(Module, Function, Args), real_services};
call(Module, Function, Args, #fake_services{}=Services) when is_atom(Module) andalso is_atom(Function) andalso is_list(Args) ->
    call_fake(Module, Function, Args, Services);
call(_Module, _Function, _Args, _Services) ->
    erlang:error(badarg).

-spec real() -> type().
real() ->
    real_services.

-spec fake() -> type().
fake() ->
    #fake_services{map=dict:new()}.

% Internal functions

-spec call_fake(module(), atom(), [any()], #fake_services{}) -> {any(), #fake_services{}}.
call_fake(Module, Function, Args, #fake_services{}=Services) ->
    {#fake{module=FakeModule, state=State}=Fake, Services2} = get_fake(Module, Services),
    {Result, NewState, Services3} = FakeModule:call(Function, Args, State, Services2),
    {Result, set_fake(Module, Fake#fake{state=NewState}, Services3)}.

-spec get_fake(module(), #fake_services{}) -> #fake{}.
get_fake(Module, #fake_services{map=Fakes}=Services) ->
    get_fake(dict:find(Module, Fakes), Module, Services).

-spec get_fake({ok, #fake{}} | error, module(), #fake_services{}) -> {#fake{}, #fake_services{}}.
get_fake({ok, Fake}, _Module, #fake_services{}=Services) ->
    {Fake, Services};
get_fake(error, Module, #fake_services{}=Services) ->
    create_fake(Module, Services).

-spec create_fake(module(), #fake_services{}) -> {#fake{}, #fake_services{}}.
create_fake(Module, #fake_services{}=Services) ->
    FakeModule = list_to_atom("fake_" ++ atom_to_list(Module)),
    {State, NewServices} = FakeModule:init(Services),
    {#fake{module=FakeModule, state=State}, NewServices}.

-spec set_fake(module(), #fake{}, #fake_services{}) -> #fake_services{}.
set_fake(Module, #fake{}=Fake, #fake_services{map=Fakes}=Services) ->
    Services#fake_services{map=dict:store(Module, Fake, Fakes)}.

