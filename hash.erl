-module(hash).
-export([is_power_of_two/1, bits/1, worker_for_key/2]).
-compile(export_all).

-include("eredis.hrl").

-include_lib("eunit/include/eunit.hrl").

%% is_power_Of_two/1
is_power_of_two(Pow) ->
    (Pow > 0) andalso ((Pow band (Pow - 1)) =:= 0).

is_power_of_two_test() ->
    lists:foreach(fun(I) -> false = is_power_of_two(I) end, [0, 3, 5, 6, 7, 9]),
    lists:foreach(fun(I) -> true = is_power_of_two(I) end, [1, 2, 4, 8]).

%% bit_count/1
bit_count(Pow) when Pow < 1 ->
    0;
bit_count(Pow) ->
    1 + bit_count(Pow bsr 1).

bit_count_test() ->
    [0, 0, 1, 2, 2, 3, 3] = [bit_count(X) || X <- lists:seq(-1, 5)].

%% bits/1
bits(Pow) ->
    bit_count(Pow - 1).

bits_test() ->
    [0, 0, 1, 2, 2, 3, 3] = [bits(X) || X <- lists:seq(0, 6)].

%% bytes_from_bits/1
bytes_from_bits(0) ->
    0;
bytes_from_bits(NumBits) when NumBits < 8 ->
    1;
bytes_from_bits(NumBits) ->
    (NumBits div 8) + bytes_from_bits(NumBits rem 8).

bytes_from_bits_test() ->
    [0, 1, 1, 1, 2, 2, 2, 3] = [bytes_from_bits(X) || X <- [0, 1, 7, 8, 9, 15, 16, 17]].

%% hash_to_int/3
hash_to_int([], _Byte, _Total) ->
    0;
hash_to_int(_Hash, Total, Total) ->
    0;
hash_to_int([H|T], Byte, Total) ->
    (H * (1 bsl (Byte * 8))) + hash_to_int(T, Byte + 1, Total).
hash_to_int(Hash, Bytes) ->
    hash_to_int(Hash, 0, Bytes).

hash_to_int_test() ->
    Data = [170, 85, 204, 51],
    [0, 170, 21930, 13391274, 869029290, 869029290] = [hash_to_int(Data, X) || X <- [0, 1, 2, 3, 4, 5]].

%% bucket/2
bucket(Key, NumBits) when is_atom(Key) ->
    bucket(atom_to_list(Key), NumBits);
bucket(Key, NumBits) ->
    Int = hash_to_int(binary_to_list(erlang:md5(Key)), bytes_from_bits(NumBits)),
    Int band ((1 bsl NumBits) - 1).

bucket_test() ->
    [0, 0, 0, 4, 12, 12, 44, 44, 172] = [bucket("foo", X) || X <- lists:seq(0, 8)],
    [428, 428, 1452, 3500, 7596, 15788, 15788, 48556] = [bucket("foo", X) || X <- lists:seq(9, 16)].

%% worker_for_key/2
worker_for_key(Key, Buckets) ->
    Bucket = bucket(Key, Buckets#buckets.bits),
    {ok, Pid} = dict:find(Bucket, Buckets#buckets.map),
    Pid.
