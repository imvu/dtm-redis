.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

.yrl.erl:
	erlc -W $<

ERL = erl -pa lib/eredis/ebin/ lib/erlymock/ebin/ boot start_clean

SRC_DIRS = .
SOURCES = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.erl))
MODULES = $(patsubst %.erl, %, $(SOURCES))

all: compile

compile: ${MODULES:%=%.beam}

clean:
	${RM} *.beam

debug: compile
	${ERL} -s dtm_redis start

test: compile
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(hash,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(txn_monitor,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(binlog,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(redis_store,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'eunit:test(redis_protocol,[verbose])' -s init stop
	erl -noshell -pa lib/eredis/ebin/ lib/erlymock/ebin/ -eval 'acceptance:test()' -s init stop

