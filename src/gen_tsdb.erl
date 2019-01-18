-module(gen_tsdb).
-behaviour(gen_server).

%% API.
-export([start_link/0, start_link/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([put/2, put/3, put/4, put/5]).

-export([async_put/2, async_put/3, async_put/4, async_put/5]).

-export([unix_timestamp/0, unix_timestamp/1]).

-type(url() :: string()).

-type(present() :: {present, boolean()}).

-type(milliseconds() :: integer()).

-type(metric() :: binary() | atom()).

-type(value() :: integer() | float()).

-type(tags() :: map()).

-define(APP, gen_tsdb).

-record(state, {
    url :: string(),

    summary = {present, true} :: present(),

    details = {present, false} :: present(),

    sync = true :: boolean(),

    sync_timeout = 0 :: milliseconds(),

    max_batch_size = 20 :: integer()
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec(put(pid(), metric(), value(), tags()) -> {ok, integer(), map()} | {erro, atom()} | {error, integer(), map()}).
put(Pid, Metric, Value, Tags) ->
    gen_tsdb:put(Pid, make_data_point({Metric, Value, Tags})).

-spec(put(pid(), metric(), integer(), value(), tags()) -> {ok, integer(), map()} | {erro, atom()} | {error, integer(), map()}).
put(Pid, Metric, Timestamp, Value, Tags) ->
    gen_tsdb:put(Pid, make_data_point({Metric, Timestamp, Value, Tags})).

put(Pid, DataPoints) ->
    put(Pid, undefined, DataPoints).

-spec(put(pid(), url() | undefined, list() | map()) -> {ok, integer(), map()} | {erro, atom()} | {error, integer(), map()}).
put(Pid, Url, DataPoints) ->
    try preprocess(DataPoints) of
        DataPoints1 ->
            gen_server:call(Pid, {put, Url, DataPoints1})
    catch
        error : Reason ->
            {error, Reason}
    end.

-spec(async_put(pid(), metric(), value(), tags()) -> ok | {erro, atom()}).
async_put(Pid, Metric, Value, Tags) ->
    async_put(Pid, make_data_point({Metric, Value, Tags})).

-spec(async_put(pid(), metric(), integer(), value(), tags()) -> ok | {erro, atom()}).
async_put(Pid, Metric, Timestamp, Value, Tags) ->
    async_put(Pid, make_data_point({Metric, Timestamp, Value, Tags})).

async_put(Pid, DataPoints) ->
    async_put(Pid, undefined, DataPoints).

-spec(async_put(pid(), url() | undefined, list() | map()) -> ok | {erro, atom()}).
async_put(Pid, Url, DataPoints) ->
    try preprocess(DataPoints) of
        DataPoints1 ->
            gen_server:cast(Pid, {async_put, Url, DataPoints1})
    catch
        error : Reason ->
            {error, Reason}
    end.

unix_timestamp() ->
    unix_timestamp(seconds).

unix_timestamp(seconds) ->
    {MegaSecs, Secs, _MicroSecs} = erlang:timestamp(),
    MegaSecs * 1000000 + Secs;
unix_timestamp(milliseconds) ->
    {MegaSecs, Secs, MicroSecs} = erlang:timestamp(),
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.

%% gen_server.

init([Opts]) ->
    State = #state{url            = proplists:get_value(url, Opts, "http://127.0.0.1:4242"),
                   summary        = {present, proplists:get_value(summary, Opts, true)},
                   details        = {present, proplists:get_value(details, Opts, false)},
                   sync           = true,
                   sync_timeout   = 0,
                   max_batch_size = proplists:get_value(max_batch_size, Opts, 20)
            },
	{ok, State}.

handle_call({put, Url, DataPoints}, _From, State) ->
    {reply, put_(Url, DataPoints, State), State};
handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast({async_put, Url, DataPoints}, State = #state{max_batch_size = MaxBatchSize}) ->
    DataPoints1 = case DataPoints of
                      DataPoint when is_map(DataPoint) ->
                          [DataPoint];
                      _ when is_list(DataPoints) ->
                          DataPoints
                  end ++ drain_put(MaxBatchSize, []),
    put_(Url, DataPoints1, State),
    {noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

make_data_point({Metric, Value, Tags}) ->
    #{metric => Metric, value => Value, tags => Tags};
make_data_point({Metric, Timestamp, Value, Tags}) ->
    #{metric => Metric, timestamp => Timestamp, value => Value, tags => Tags}.

preprocess(DataPoint) when is_map(DataPoint) ->
    case run_misc_steps([fun check_metric/1,
                         fun preprocess_timestamp/1,
                         fun check_value/1,
                         fun check_tags/1], DataPoint) of
        {ok, DataPoint1} ->
            DataPoint1;
        {error, Reason} ->
            error(Reason)
    end;
preprocess(DataPoints) when is_list(DataPoints) ->
    lists:map(fun(DataPoint) ->
                  preprocess(DataPoint)
              end, DataPoints).


check_metric(#{metric := Metric}) when is_binary(Metric) orelse is_atom(Metric) ->
	ok;
check_metric(#{metric := _Metric}) ->
	{error, bad_metric};
check_metric(_) ->
    {error, missing_metric}.

preprocess_timestamp(#{timestamp := Timestamp}) when is_integer(Timestamp) orelse is_binary(Timestamp) ->
	ok;
preprocess_timestamp(#{timestamp := _Timestamp}) ->
    {error, bad_timestamp};
preprocess_timestamp(DataPoint) ->
    {ok, DataPoint#{timestamp => unix_timestamp()}}.

check_value(#{value := Value}) when is_number(Value) orelse is_binary(Value) ->
    ok;
check_value(#{value := _}) ->
    {error, bad_value};
check_value(_) ->
    {error, missing_value}.

check_tags(#{tags := Tags}) when is_map(Tags) ->
    case maps:size(Tags) of
        0 -> {error, missing_tag};
        _ -> ok
    end;
check_tags(#{tags := _}) ->
    {error, bad_tags};
check_tags(_) ->
    {error, missing_tag}.

run_misc_steps([], DataPoint) ->
    {ok, DataPoint};
run_misc_steps([Step | Steps], DataPoint) ->
    case Step(DataPoint) of
        ok ->
            run_misc_steps(Steps, DataPoint);
        {ok, DataPoint1} ->
            run_misc_steps(Steps, DataPoint1);
        {error, Reason} ->
            {error, Reason}
    end.

put_(undefined, DataPoints, State = #state{url = Url}) ->
    put_(Url, DataPoints, State);
put_(Url, DataPoints, State) ->
    request_api(post,
                make_url(Url, ["api/put"], make_query_params(put, State)),
                jsx:encode(DataPoints)).

request_api(Method, Url, Body) ->
    case httpc:request(Method, {Url, [], "application/json", Body}, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, ResponseBody}}
            when Code =:= 200 orelse Code =:= 204 ->
            {ok, Code, json_text_to_map(ResponseBody)};
        {ok, {{_, Code, _}, _, ResponseBody}} ->
            {error, Code, json_text_to_map(ResponseBody)}
    end.

make_query_params(put, #state{summary      = Summary,
                              details      = Details,
                              sync         = Sync,
                              sync_timeout = SyncTimeout}) ->
    make_query_params([{summary,      Summary},
                       {details,      Details},
                       {sync,         Sync},
                       {sync_timeout, SyncTimeout}]);
make_query_params(_, _State) ->
    [].

make_query_params(Options) ->
    lists:foldl(fun({Flag, {present, true}}, QueryParams) ->
                    QueryParams ++ [Flag];
                   ({_Flag, {present, false}}, QueryParams) ->
                    QueryParams;
                   ({K, V}, QueryParams) ->
                    QueryParams ++ [{K, V}]
                end, [], Options).

make_url(Url, Paths, QueryParams) ->
    case list_to_binary(Url) of
        <<"http://", _/binary>> -> Url;
        <<"https://", _/binary>> -> Url;
        _ -> "http://" ++ Url
    end ++ "/" ++ filename:join(Paths) ++ "?" ++ serialize(QueryParams).

serialize(QueryParams) ->
    lists:foldl(fun({K, V}, String) when is_atom(K) ->
                    Query = erlang:atom_to_list(K) ++ "=" ++ v2str(V),
                    case String of
                        "" -> Query;
                        _ -> String ++ "&" ++ Query
                    end;
                   (Flag, String) when is_atom(Flag) ->
                    case String of
                        "" -> erlang:atom_to_list(Flag);
                        _ -> String ++ "&" ++ erlang:atom_to_list(Flag)
                    end
                end, "", QueryParams).

v2str(V) when is_atom(V) ->
    erlang:atom_to_list(V);
v2str(V) when is_integer(V) ->
    erlang:integer_to_list(V).

json_text_to_map("") ->
    #{};
json_text_to_map(JsonText) when is_list(JsonText) ->
    jsx:decode(list_to_binary(JsonText), [return_maps]).

drain_put(0, Acc) ->
    Acc;
drain_put(Cnt, Acc) ->
    receive
        {async_put, DataPoint} when is_map(DataPoint) ->
            drain_put(Cnt - 1, [DataPoint] ++ [Acc]);
        {async_put, DataPoints} when is_list(DataPoints) ->
            drain_put(Cnt - 1, DataPoints ++ [Acc])
    after 0 ->
        Acc
    end.
