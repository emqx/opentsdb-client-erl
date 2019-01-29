
# gen_tsdb

**gen_tsdb** is an client which connects and pushes metrics to opentsdb server. 

Both synchronous and asynchronous modes of operation are supported. 

We will support more methods in opentsdb and more kinds of tsdb soon.



## Basic usage

### Start gen_tsdb

To start in the console run:

`$ erl -pa ebin -pa deps/*/ebin`

To start gen_tsdb:

```erlang
application:ensure_all_started(gen_tsdb).
Opts = [{url, "http://127.0.0.1:4242"},
		{summary, true},
		{details, false},
		{max_batch_size, 20}]
{ok, Pid} = gen_tsdb:start_link(Opts).
```



### Sync put one or more metric 

```erlang
1> {ok, StatusCode, BodyMap} = gen_tsdb:put(Pid, <<"sys.cpu.usage">>, 13, #{city => hangzhou}).
2> {ok, StatusCode, BodyMap} = gen_tsdb:put(Pid, <<"sys.cpu.usage">>, 13, #{city => hangzhou}).
3> {ok, StatusCode, BodyMap} = gen_tsdb:put(Pid, [#{metric => <<"sys.cpu.nice">>, 
													value => 19, 
													tags => #{host => web01}},
												  #{metric => <<"sys.cpu.nice">>, 
													timestamp => gen_tsdb:unix_timestamp(),
													value => 13, 
													tags => #{host => <<"web02">>}}]).
```

**StatusCode** is HTTP status code returned by opentsdb server.

The format of **BodyMap** is similar to:

```erlang
#{<<"failed">> => 0, <<"success">> => 1}
```



### Async put one or more metric

Just replace `gen_tsdb:put` to `gen_tsdb:async_put`.