PROJECT = gen_tsdb
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = jsx hackney
dep_jsx = git https://github.com/talentdeficit/jsx v2.9.0
dep_hackney = git https://github.com/benoitc/hackney

COVER = true

include erlang.mk
