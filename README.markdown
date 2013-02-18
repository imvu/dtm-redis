dtm-redis
=========
The dtm-redis project is an erlang based implementation of [redis](https://github.com/antirez/redis). Unlike the clustering solution planned by the redis team, this implementation implements distributed transactions.

This project has so far been developed as a prototype. Only a small subset of the redis command set is implemented:
[GET](http://redis.io/commands/get)
[SET](http://redis.io/commands/set)
[DEL](http://redis.io/commands/del)
[WATCH](http://redis.io/commands/watch)
[UNWATCH](http://redis.io/commands/unwatch)
[MULTI](http://redis.io/commands/multi)
[EXEC](http://redis.io/commands/exec)

Building
========

<pre>
$ git clone https://github.com/imvu/dtm-redis
$ cd dtm-redis
$ git submodule init
$ git submodule update
$ make
</pre>

Running Tests
=============

Unit tests can be run like so:
<pre>
$ make test-unit
</pre>

Acceptance tests can be run like so:
<pre>
$ make test-acceptance
</pre>

Run both unit tests and acceptance tests like so:
<pre>
$ make test-all
</pre>

Testing from the command line
=============================

The following command will start dtm-redis and start the erlang interpreter:

<pre>
$ make debug
bin/rebar compile
==> Entering directory `/home/eric/git/dtm-redis/lib'
==> lib (compile)
==> Leaving directory `/home/eric/git/dtm-redis/lib'
==> Entering directory `/home/eric/git/dtm-redis/apps'
==> Entering directory `/home/eric/git/dtm-redis/apps/dtm_redis'
==> dtm_redis (compile)
==> Leaving directory `/home/eric/git/dtm-redis/apps/dtm_redis'
==> apps (compile)
==> Leaving directory `/home/eric/git/dtm-redis/apps'
==> Entering directory `/home/eric/git/dtm-redis/rel'
==> rel (compile)
==> Leaving directory `/home/eric/git/dtm-redis/rel'
==> dtm-redis (compile)
rm -f -rf rel/dtm_redis
bin/rebar generate
==> Entering directory `/home/eric/git/dtm-redis/lib'
==> Leaving directory `/home/eric/git/dtm-redis/lib'
==> Entering directory `/home/eric/git/dtm-redis/apps'
==> Entering directory `/home/eric/git/dtm-redis/apps/dtm_redis'
==> Leaving directory `/home/eric/git/dtm-redis/apps/dtm_redis'
==> Leaving directory `/home/eric/git/dtm-redis/apps'
==> Entering directory `/home/eric/git/dtm-redis/rel'
==> rel (generate)
==> Leaving directory `/home/eric/git/dtm-redis/rel'
mkdir rel/dtm_redis/binlog
rel/dtm_redis/bin/dtm_redis console
Exec: /home/eric/git/dtm-redis/rel/dtm_redis/erts-5.9.1/bin/erlexec -boot /home/eric/git/dtm-redis/rel/dtm_redis/releases/0.4/dtm_redis -mode embedded -config /home/eric/git/dtm-redis/rel/dtm_redis/releases/0.4/sys.config -args_file /home/eric/git/dtm-redis/rel/dtm_redis/releases/0.4/vm.args -- console
Root: /home/eric/git/dtm-redis/rel/dtm_redis
Erlang R15B01 (erts-5.9.1) [source] [64-bit] [async-threads:0] [kernel-poll:false]


=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting dtm_redis application
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
initializing dtm_redis_sup
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
no mode specified for dtm_redis, assuming debug
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
dtm_redis_sup starting in debug mode
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
initializing txn_monitor_sup
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting binlog with pid <0.51.0>, writing to file "binlog/monitor.log"
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting txn_monitor with pid <0.52.0>
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
initializing bucket_sup
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting binlog with pid <0.54.0>, writing to file "binlog/bucket0.log"
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting storage bucket with pid <0.55.0> and storage "localhost":6379
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
connecting to redis "localhost":6379
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting binlog with pid <0.58.0>, writing to file "binlog/bucket1.log"
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
starting storage bucket with pid <0.59.0> and storage "localhost":6379
=INFO REPORT==== 18-Feb-2013::13:40:29 ===
connecting to redis "localhost":6379
=INFO REPORT==== 18-Feb-2013::13:40:30 ===
initializing session with pid <0.60.0>Eshell V5.9.1  (abort with ^G)
(dtm_redis@eric-sandbox)1>
</pre>

Now you should be able to issue any of the supported commands directly in the erlang shell using the dtm_redis module like so:

<pre>
(dtm_redis@eric-sandbox)1> dtm_redis:set(foo, bar).
ok
(dtm_redis@eric-sandbox)2> dtm_redis:get(foo).
"bar"
(dtm_redis@eric-sandbox)3>
</pre>

Starting a local test cluster
=============================

The following command will start a single host dtm-redis cluster using the configuration in config/single:

<pre>
$ make debug_server
</pre>

Now you should be able to connect to dtm-redis like so:

<pre>
$ redis-cli -h localhost -p 6378
redis> set foo bar
1) OK
redis> get foo
1) "bar"
redis>
</pre>

Running a distributed cluster
=============================

The config/ directory contains some default cluster configuration files. The config/single file contains documentation for the layout of the cluster configuration. config/single is also the configuration used by "make debug_server". The file config/cluster contains the configuration for running a 4-host cluster with 32 buckets and one listening server and one monitor per host. The hosts in the cluster operate in a master/slave configuration. The only real difference between master and slave is that the startup script is run on the master. The startup script reads the configuration and starts the dtm-redis remote processes on the slaves.

On each slave host that will participate in the cluster, run the following command:

<pre>
$ slave.sh
</pre>

On the master, edit master.sh and change the path to the configuration file (if you don't want it to be config/cluster). Otherwise, edit config/cluster and replace the placeholder hostnames (host1, host2, etc.) with the names of the hosts that will participate in the cluster. Also, you will want to configure the host and port for the redis backing store instances. The default cluster configuration uses the standard redis port (6379) on different localhost addresses (127.0.0.X). The localhost address is relative to the host where the bucket process is running. Then run the command:

<pre>
$ master.sh
</pre>

Now from a separate shell (on any host) connect to the dtm-redis cluster like so:

<pre>
$ redis-cli -h host1 -p 6379
redis> set foo bar
1) OK
redis> get foo
1) "bar"
redis> watch foo
1) OK
redis> set foo baz
1) OK
redis> multi
1) OK
redis> set foo bar
1) QUEUED
redis> exec
1) (nil)
redis> get foo
1) "baz"
redis>
</pre>

Benchmarking
============

After starting any size cluster, the cluster can be benchmarked in a separate shell using dtm-bench:

<pre>
$ bin/dtm-bench
usage: dtm-bench &lt;host:port[,host:port[,...]]&gt; &lt;clients&gt; &lt;time&gt; &lt;method&gt;
$ bin/dtm-bench localhost:6378 5 5 get_set
creating 5 clients connecting to each of 1 hosts
starting clients
clients running for 5 seconds
stopping clients
total requests:  62832
total latency:   24.939632701
avarage latency: 0.000397
max latency:     0.001858505
elapsed time:    5.000936914
requests/sec:    12564
</pre>

The first parameter is a comma separated list of host:port combinations.
The second parameter is the number of clients to start per host.
The third parameter is the number of seconds to perform the benchmark test.
The available methods for the fourth parameter are "get_set" and "trans".

