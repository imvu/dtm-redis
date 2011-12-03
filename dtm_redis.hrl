-record(config, {shell=false, buckets, monitors, iface=all, port=6379, backlog=20}).
-record(bucket, {nodename, store_host, store_port, binlog}).
-record(monitor, {nodename, binlog}).

-record(buckets, {bits=0, map}).
-record(monitors, {map}).

