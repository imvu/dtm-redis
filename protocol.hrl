-record(allocate_txn, {from}).
-record(txn_id, {monitor, id}).

-record(command, {session, operation}).
-record(transact, {session, id, operation}).
-record(watch, {session, key}).
-record(unwatch, {session}).

-record(lock_transaction, {session}).
-record(transaction_locked, {bucket, status}).
-record(commit_transaction, {session, now}).
-record(rollback_transaction, {session}).

-record(get, {key}).
-record(set, {key, value}).
-record(delete, {key}).
