-record(config, {servers, buckets, monitors}).
-record(bucket, {nodename, store_host, store_port, binlog}).
-record(monitor, {nodename, binlog}).

-record(server, {nodename, iface=all, port=6379, backlog=20}).
-record(buckets, {bits=0, map}).
-record(monitors, {map}).

