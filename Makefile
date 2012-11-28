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

REBAR = bin/rebar

build:
	${REBAR} compile

all: test-all bin/dtm-bench

test: test-unit

test-all: test-unit test-acceptance

test-unit:
	${REBAR} eunit

test-acceptance: generate
	rel/dtm_redis/bin/dtm_redis start
	bin/acceptance.escript dtm_redis@`hostname`
	rel/dtm_redis/bin/dtm_redis stop

generate: build
	${RM} -rf rel/dtm_redis
	${REBAR} generate
	mkdir rel/dtm_redis/binlog

bin/dtm-bench:
	make -C dtm-bench

clean:
	${RM} apps/dtm_redis/.eunit/*
	${RM} apps/dtm_redis/ebin/*.beam
	${RM} -rf rel/dtm_redis*
	make -C dtm-bench clean

debug: generate
	rel/dtm_redis/bin/dtm_redis console

debug-server: generate
	rel/dtm_redis/bin/dtm_redis start -dtm_redis mode debug_server

stop:
	rel/dtm_redis/bin/dtm_redis stop

