-module(opentsdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [t_put].

init_per_suite(Config) ->
    application:ensure_all_started(opentsdb),
    Config.

end_per_suite(_Config) ->
    application:stop(opentsdb).

t_put(_) ->
    {ok, Pid} = opentsdb:start_link(),
    {ok, 200, Result} = opentsdb:put(Pid, [#{metric => <<"sys.cpu.nice">>,
                                             value => 20,
                                             tags => #{host => web01}},
                                           #{metric => <<"sys.cpu.nice">>,
                                             timestamp => opentsdb:unix_timestamp(),
                                             value => 13,
                                             tags => #{host => <<"web02">>}},
                                           #{metric => <<"sys.cpu.nice">>,
                                             timestamp => integer_to_binary(opentsdb:unix_timestamp()),
                                             value => 12,
                                             tags => #{host => <<"web03">>}}]),
    ?assertEqual(3, maps:get(success, Result, undefined)),
    {ok, 204, _} = opentsdb:put(Pid, #{metric => <<"sys.cpu.nice">>,
                                       value => <<"20">>,
                                       tags => #{host => web01}},
                                     [{details, {present, false}},
                                      {summary, {present, false}}]),
    {error, 400, _} = opentsdb:put(Pid, #{metric => <<"sys.cpu.nice">>,
                                          value => <<"20a">>,
                                          tags => #{host => web01}}).