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

-module(hash).

-export([is_power_of_two/1, bits/1, worker_for_key/2]).

-include("dtm_redis.hrl").

% Public API

-spec is_power_of_two(non_neg_integer()) -> boolean().
is_power_of_two(Pow) when is_integer(Pow) andalso (Pow >= 0) ->
    (Pow > 0) andalso ((Pow band (Pow - 1)) =:= 0);
is_power_of_two(_Pow) ->
    erlang:error(badarg).

-spec bits(non_neg_integer()) -> non_neg_integer().
bits(0) ->
    0;
bits(Pow) when is_integer(Pow) andalso (Pow > 0) ->
    bit_count(Pow - 1);
bits(_Pow) ->
    erlang:error(badarg).

-spec worker_for_key(binary(), #buckets{}) -> pid().
worker_for_key(Key, Buckets) when is_binary(Key) andalso is_record(Buckets, buckets) ->
    Bucket = bucket(Key, Buckets#buckets.bits),
    {ok, Pid} = dict:find(Bucket, Buckets#buckets.map),
    Pid;
worker_for_key(_Key, _Buckets) ->
    erlang:error(badarg).

% Internal methods

-spec bit_count(non_neg_integer()) -> non_neg_integer().
bit_count(0) ->
    0;
bit_count(Pow) when is_integer(Pow) and (Pow > 0) ->
    1 + bit_count(Pow bsr 1).

-spec bucket(binary(), non_neg_integer()) -> non_neg_integer().
bucket(Key, NumBits) when is_binary(Key) andalso is_integer(NumBits) andalso (NumBits >= 0) ->
    Int = hash_to_int(binary_to_list(erlang:md5(Key)), bytes_from_bits(NumBits)),
    Int band ((1 bsl NumBits) - 1).

-spec bytes_from_bits(non_neg_integer()) -> non_neg_integer().
bytes_from_bits(0) ->
    0;
bytes_from_bits(NumBits) when NumBits < 8 ->
    1;
bytes_from_bits(NumBits) ->
    (NumBits div 8) + bytes_from_bits(NumBits rem 8).

-spec hash_to_int([byte()], non_neg_integer(), non_neg_integer()) -> non_neg_integer().
hash_to_int([], _Byte, _Total) ->
    0;
hash_to_int(_Hash, Total, Total) ->
    0;
hash_to_int([H|T], Byte, Total) ->
    (H * (1 bsl (Byte * 8))) + hash_to_int(T, Byte + 1, Total).

-spec hash_to_int([byte()], non_neg_integer()) -> non_neg_integer().
hash_to_int(Hash, Bytes) ->
    hash_to_int(Hash, 0, Bytes).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

throws_badarg(Fun) ->
    {'EXIT', {badarg, _}} = (catch Fun()).

is_power_of_two_test() ->
    lists:foreach(fun(I) -> false = is_power_of_two(I) end, [0, 3, 5, 6, 7, 9]),
    lists:foreach(fun(I) -> true = is_power_of_two(I) end, [1, 2, 4, 8]).

is_power_of_two_badarg_test() ->
    throws_badarg(fun() -> is_power_of_two(-1) end).

bits_test() ->
    [0, 0, 1, 2, 2, 3, 3] = [bits(X) || X <- lists:seq(0, 6)].

bits_badarg_test() ->
    throws_badarg(fun() -> bits(-1) end).

worker_for_key_test() ->
    Buckets = #buckets{bits=2, map=dict:store(0, fake_pid0, dict:store(1, fake_pid1, dict:store(2, fake_pid2, dict:store(3, fake_pid3, dict:new()))))},
    [fake_pid0, fake_pid3, fake_pid3] = [worker_for_key(Key, Buckets) || Key <- [<<"foo">>, <<"bar">>, <<"baz">>]].

worker_for_key_badarg_key_test() ->
    throws_badarg(fun() -> worker_for_key(foo, #buckets{}) end).

worker_for_key_badarg_buckets_test() ->
    throws_badarg(fun() -> worker_for_key(<<"foo">>, bar) end).

bit_count_test() ->
    [0, 1, 2, 2, 3, 3] = [bit_count(X) || X <- lists:seq(0, 5)].

bytes_from_bits_test() ->
    [0, 1, 1, 1, 2, 2, 2, 3] = [bytes_from_bits(X) || X <- [0, 1, 7, 8, 9, 15, 16, 17]].

hash_to_int_test() ->
    Data = [170, 85, 204, 51],
    [0, 170, 21930, 13391274, 869029290, 869029290] = [hash_to_int(Data, X) || X <- [0, 1, 2, 3, 4, 5]].

bucket_test() ->
    [0, 0, 0, 4, 12, 12, 44, 44, 172] = [bucket(<<"foo">>, X) || X <- lists:seq(0, 8)],
    [428, 428, 1452, 3500, 7596, 15788, 15788, 48556] = [bucket(<<"foo">>, X) || X <- lists:seq(9, 16)].

-endif.

