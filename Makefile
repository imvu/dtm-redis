.SUFFIXES: .erl .beam .yrl .c .o

.erl.beam:
	erlc -W $<

.yrl.erl:
	erlc -W $<

ERL = erl -pa lib/eredis/ebin/ lib/erlymock/ebin/ boot start_clean

CC = gcc
CFLAGS = -std=gnu99 -g -O2 -fPIC -Ilib/hiredis/
LFLAGS = -lpthread -lrt
COMPILE = $(CC) $(CFLAGS)

SRC_DIRS = .
SOURCES = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.erl))
MODULES = $(patsubst %.erl, %, $(SOURCES))

all: compile

compile: ${MODULES:%=%.beam} dtm-bench

%.o : %.c
	$(CC) $(CFLAGS) -c $<

dtm-bench: dtm-bench.o
	$(CC) $(LFLAGS) dtm-bench.o lib/hiredis/libhiredis.a -o dtm-bench

clean:
	${RM} *.beam *.o dtm-bench

debug: compile
	${ERL} -s dtm_redis start

debug_server: compile
	${ERL} -s dtm_redis server_start -extra config/single

test: compile
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(hash,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(txn_monitor,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(binlog,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(redis_store,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(redis_protocol,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'acceptance:test()' -s init stop

