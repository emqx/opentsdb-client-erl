-module(gen_tsdb_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	{ok, { {one_for_one, 10, 100}, 
        [{
            gen_tsdb,
            {gen_tsdb, start_link, []},
            permanent,
            5000,
            worker,
            [gen_tsdb]
        }]} }.
