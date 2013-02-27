# Copyright (C) 2011-2013 IMVU Inc.
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

REBAR = bin/rebar

compile:
	${REBAR} compile

generate: compile
	${RM} -rf rel/dtm_redis
	${REBAR} generate

binlog: generate
	mkdir rel/dtm_redis/binlog

build: generate binlog

lib/hiredis2/libhiredis.a:
	make -C lib/hiredis2

hiredis: lib/hiredis2/libhiredis.a

bin/dtm-bench: hiredis
	make -C dtm-bench

dtm-bench: bin/dtm-bench

all: build dtm-bench

clean:
	${REBAR} clean
	make -C lib/hiredis2 clean
	make -C dtm-bench clean

test-unit: compile
	${REBAR} eunit skip_deps=true

test-acceptance: build
	rel/dtm_redis/bin/dtm_redis start
	bin/acceptance_shell.escript dtm_redis@`hostname`
	rel/dtm_redis/bin/dtm_redis stop
	rel/dtm_redis/bin/dtm_redis start -dtm_redis mode debug_server
	bin/acceptance_server.escript dtm_redis@`hostname`
	rel/dtm_redis/bin/dtm_redis stop

test-all: test-acceptance test-unit

test: test-unit

debug: build
	rel/dtm_redis/bin/dtm_redis console

debug-server: build
	rel/dtm_redis/bin/dtm_redis start -dtm_redis mode debug_server

stop:
	rel/dtm_redis/bin/dtm_redis stop

