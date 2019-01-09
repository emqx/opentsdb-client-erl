PROJECT = gen_tsdb
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = jsx
dep_jsx = git https://github.com/talentdeficit/jsx v2.9.0

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqx/cuttlefish emqx30

LOCAL_DEPS = inets

NO_AUTOPATCH = cuttlefish

COVER = true

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/gen_tsdb.conf -i priv/gen_tsdb.schema -d data
