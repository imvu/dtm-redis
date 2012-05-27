#!/bin/bash

erl -pa lib/eredis/ebin/ lib/erlymock/ebin/ boot start_clean -kernel inet_dist_listen_min 50000 inet_dist_listen_max 50009 -name redis -setcookie dtmredis

