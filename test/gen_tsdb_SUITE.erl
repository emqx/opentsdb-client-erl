-module(gen_tsdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [t_put, t_async_put].

init_per_suite(Config) ->
    start_apps(gen_tsdb, {local_path("priv/gen_tsdb.schema"),
                          local_path("etc/gen_tsdb.conf")}),
    Config.

end_per_suite(_Config) ->
    [application:stop(App) || App <- [gen_tsdb]].

get_base_dir() ->
    {file, Here} = code:is_loaded(?MODULE),
    filename:dirname(filename:dirname(Here)).

local_path(RelativePath) ->
    filename:join([get_base_dir(), RelativePath]).

start_apps(App, {SchemaFile, ConfigFile}) ->
    read_schema_configs(App, {SchemaFile, ConfigFile}),
    application:ensure_all_started(App).

read_schema_configs(App, {SchemaFile, ConfigFile}) ->
    Schema = cuttlefish_schema:files([SchemaFile]),
    Conf = conf_parse:file(ConfigFile),
    NewConfig = cuttlefish_generator:map(Schema, Conf),
    Vals = proplists:get_value(App, NewConfig, []),
    [application:set_env(App, Par, Value) || {Par, Value} <- Vals].

t_put(_) ->
    {ok, Pid} = gen_tsdb:start_link(),
    {ok, 200, _} = gen_tsdb:put(Pid, <<"sys.cpu.usage">>, 13, #{city => hangzhou}),
    {ok, 200, _} = gen_tsdb:put(Pid, <<"sys.cpu.usage">>, gen_tsdb:unix_timestamp(), <<"13">>, #{city => hangzhou}),
    {ok, 200, _} = gen_tsdb:put(Pid, [#{metric => <<"sys.cpu.nice">>,
                                          value => 20,
                                          tags => #{host => web01}},
                                      #{metric => <<"sys.cpu.nice">>,
                                          timestamp => gen_tsdb:unix_timestamp(),
                                          value => 13,
                                          tags => #{host => <<"web02">>}},
                                      #{metric => <<"sys.cpu.nice">>,
                                          timestamp => integer_to_binary(gen_tsdb:unix_timestamp()),
                                          value => 12,
                                          tags => #{host => <<"web03">>}}]),
    {error, bad_metric} = gen_tsdb:put(Pid, "sys.cpu.usage", 13, #{city => hangzhou}),
    {error, bad_timestamp} = gen_tsdb:put(Pid, <<"sys.cpu.usage">>, integer_to_list(gen_tsdb:unix_timestamp()), 13, #{city => hangzhou}),
    {error, bad_value} = gen_tsdb:put(Pid, <<"sys.cpu.nice">>, "20", #{city => hangzhou}),
    {error, bad_tags} = gen_tsdb:put(Pid, <<"sys.cpu.nice">>, 20, [{city, hangzhou}]),
    {error, missing_metric} = gen_tsdb:put(Pid, #{value => 20, tags => #{city => hangzhou}}),
    {error, missing_value} = gen_tsdb:put(Pid, #{metric => <<"sys.cpu.nice">>, tags => #{city => hangzhou}}),
    {error, missing_tag} = gen_tsdb:put(Pid, #{metric => <<"sys.cpu.nice">>, value => 20}).

t_async_put(_) ->
    {ok, Pid} = gen_tsdb:start_link(),
    ok = gen_tsdb:async_put(Pid, <<"sys.cpu.usage">>, 13, #{city => hangzhou}),
    ok = gen_tsdb:async_put(Pid, <<"sys.cpu.usage">>, gen_tsdb:unix_timestamp(), 13, #{city => hangzhou}),
    ok = gen_tsdb:async_put(Pid, #{metric => <<"sys.cpu.nice">>, value => 20, tags => #{city => hangzhou}}),
    ok = gen_tsdb:async_put(Pid, [#{metric => <<"sys.cpu.nice">>,
                                    value => 20,
                                    tags => #{city => hangzhou}},
                                  #{metric => <<"sys.cpu.nice">>,
                                    value => 21,
                                    tags => #{city => kunming}}]).
