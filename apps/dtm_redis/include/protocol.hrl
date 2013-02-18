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

% types

-type session_id() :: pid().
-type transaction_id() :: binary().
-type operation_id() :: non_neg_integer().
-type key() :: binary().
-type bucket_id() :: pid().

% request records

-record(operation, {
    command :: binary(),
    key :: key() | none,
    arguments :: [binary()]
}).

-record(transact, {
    txn_id :: transaction_id(),
    session_id :: session_id(),
    operation_id :: operation_id(),
    operation :: #operation{}
}).

-record(watch, {
    txn_id :: transaction_id(),
    session_id :: session_id(),
    key :: key()
}).

-record(unwatch, {
    txn_id :: transaction_id(),
    session :: session_id()
}).

-record(command, {
    session_id :: session_id(),
    operation :: #operation{}
}).

% reply records

-record(status_reply, {
    message :: binary()
}).

-record(error_reply, {
    type :: binary(),
    message :: binary()
}).

-record(integer_reply, {
    value :: binary()
}).

-record(bulk_reply, {
    content :: none | binary()
}).

-type simple_reply() :: #status_reply{} | #error_reply{} | #integer_reply{} | #bulk_reply{}.

-record(multi_bulk_reply, {
    count :: binary(),
    items :: [simple_reply()]
}).

-type reply() :: simple_reply() | #multi_bulk_reply{}.

% transaction management records

-record(lock_transaction, {
    txn_id :: transaction_id(),
    session_id :: session_id()
}).

-record(transaction_locked, {
    bucket_id :: bucket_id(),
    status :: ok | error
}).

-record(commit_transaction, {
    txn_id :: transaction_id(),
    session_id :: session_id()
}).

-record(rollback_transaction, {
    txn_id :: transaction_id(),
    session_id :: session_id()
}).

