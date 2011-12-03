-record(config, {shell=false, buckets, monitors=1, iface=all, port=6379, backlog=20}).
-record(bucket, {nodename, store_host, store_port}).

-record(buckets, {bits=0, map}).
-record(monitors, {map}).

