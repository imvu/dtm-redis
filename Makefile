# Copyright (C) 2011-2012 IMVU Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

.SUFFIXES: .erl .beam .yrl .c .o

ERL = erl -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ boot start_clean

CC = gcc
CFLAGS = -std=gnu99 -g -O2 -fPIC -Ilib/hiredis/
LFLAGS = -lpthread -lrt
COMPILE = $(CC) $(CFLAGS)

SRC_DIRS = .
SOURCES = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.erl))
MODULES = $(patsubst %.erl, %, $(SOURCES))

all: compile

compile: rebar dtm-bench

rebar:
	rebar compile

%.o : %.c
	$(CC) $(CFLAGS) -c $<

dtm-bench: dtm-bench.o
	$(CC) dtm-bench.o lib/hiredis/libhiredis.a -o dtm-bench -lrt $(LFLAGS)

clean:
	${RM} apps/dtm_redis/ebin/*.beam *.o dtm-bench

debug: compile
	${ERL} -s dtm_redis start

debug_server: compile
	${ERL} -s dtm_redis server_start -extra config/single

test: compile
	mkdir -p binlog
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'eunit:test(hash,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'eunit:test(txn_monitor,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'eunit:test(binlog,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'eunit:test(redis_store,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'eunit:test(redis_protocol,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ apps/dtm_redis/ebin/ -eval 'acceptance:test()' -s init stop

