-module(gen_tsdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [t_put, t_async_put].

init_per_suite(Config) ->
    application:ensure_all_started(gen_tsdb),
    Config.

end_per_suite(_Config) ->
    application:stop(gen_tsdb).

t_put(_) ->
    {ok, Pid} = gen_tsdb:start_link(),
    {ok, 200, Result} = gen_tsdb:put(Pid, [#{metric => <<"sys.cpu.nice">>,
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
    ?assertEqual(3, maps:get(success, Result, undefined)),
    {ok, 204, _} = gen_tsdb:put(Pid, #{metric => <<"sys.cpu.nice">>,
                                       value => <<"20">>,
                                       tags => #{host => web01}},
                                     [{details, {present, false}},
                                      {summary, {present, false}}]),
    {error, 400, _} = gen_tsdb:put(Pid, #{metric => <<"sys.cpu.nice">>,
                                          value => <<"20a">>,
                                          tags => #{host => web01}}).    

t_async_put(_) ->
    {ok, Pid} = gen_tsdb:start_link(),
    ok = gen_tsdb:async_put(Pid, [#{metric => <<"sys.cpu.nice">>,
                                    value => 20,
                                    tags => #{city => hangzhou}},
                                  #{metric => <<"sys.cpu.nice">>,
                                    value => 21,
                                    tags => #{city => kunming}}]).
