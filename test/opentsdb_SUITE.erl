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

wait_until_up(Server) ->
    [Host, Port0] = string:split(Server, ":"),
    Port = list_to_integer(Port0),
    do_wait_until_up({Host, Port}, 100).

do_wait_until_up({Host, Port} = Server, N) ->
    case gen_tcp:connect(Host, Port, [], 1_000) of
        {error, _} when N > 0 ->
            ct:sleep(1_000),
            do_wait_until_up(Server, N - 1);
        {error, Reason} ->
            error({server_not_up, Reason});
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok
    end.

t_put(_) ->
    Server = os:getenv("OPENTS_SERVER", "localhost:4242"),
    wait_until_up(Server),
    Opts = [{server, Server}],
    ct:pal("using opts ~p", [Opts]),
    {ok, Pid} = opentsdb:start_link(Opts),
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
    {error, {400, _}} = opentsdb:put(Pid, #{metric => <<"sys.cpu.nice">>,
                                            value => <<"20a">>,
                                            tags => #{host => web01}}).
