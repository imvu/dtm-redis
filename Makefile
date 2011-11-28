.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

.yrl.erl:
	erlc -W $<

ERL = erl -boot start_clean

SRC_DIRS = .
SOURCES = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.erl))
MODULES = $(patsubst %.erl, %, $(SOURCES))

all: compile

compile: ${MODULES:%=%.beam}

clean:
	${RM} *.beam

debug: compile
	${ERL} -s eredis start

test: compile
	erl -noshell -pa hash -eval 'eunit:test(hash,[verbose])' -s init stop
	erl -noshell -pa hash -eval 'acceptance:test()' -s init stop

