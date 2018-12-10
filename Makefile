REBAR3=$(shell which rebar3 || echo ./rebar3)

CT_TEST_FLAGS ?=

space-separate = $(subst ${comma},${space},$(strip $1))

ifdef SUITE
CT_TEST_CLAGS += --suite=$(call comma-separate,$(foreach suite,$(call space-separate,${SUITE}),${suite}_SUITE))
unexport SUITE
endif

ifdef GROUP
CT_TEST_FLAGS += --group=$(GROUP)
unexport GROUP
endif

ifdef TEST
CT_TEST_FLAGS += --case=$(TEST)
EUNIT_TEST_FLAGS += --module=$(TEST)
unexport TEST
endif

ifdef VERBOSE
CT_TEST_FLAGS += --verbose
unexport VERBOSE
endif

ifdef REPEAT
CT_TEST_FLAGS += --repeat=?(REPEAT)
unexport REPEAT
endif

.PHONY: all check test clean run

all:
	$(REBAR3) compile

docs:
	$(REBAR3) doc

check:
	$(REBAR3) dialyzer

ct:
	$(REBAR3) ct
eunit:
	$(REBAR3) eunit $(EUNIT_TEST_FLAGS)

test: eunit ct

conf_clean:
	@:

clean:
	$(REBAR3) clean
	$(RM) doc/*

run:
	ERL_FLAGS="-pz _build/test/lib/mnesia_leveled/test" $(REBAR3) as test shell
