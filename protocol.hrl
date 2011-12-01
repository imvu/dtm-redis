-record(command, {session, operation}).
-record(transact, {txn_id, session, operation_id, operation}).
-record(watch, {txn_id, session, key}).
-record(unwatch, {txn_id, session}).

-record(lock_transaction, {txn_id, session}).
-record(transaction_locked, {bucket, status}).
-record(commit_transaction, {txn_id, session}).
-record(rollback_transaction, {txn_id, session}).

-record(get, {key}).
-record(set, {key, value}).
-record(delete, {key}).
