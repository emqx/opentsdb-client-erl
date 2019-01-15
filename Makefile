PROJECT = gen_tsdb
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = jsx
dep_jsx = git https://github.com/talentdeficit/jsx v2.9.0

LOCAL_DEPS = inets

COVER = true

include erlang.mk
