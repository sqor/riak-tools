-module(sqorentities).

-compile(export_all).
%-export([postcommit/1]).

-define('DBTABLE',              <<"entities">>).
-define('RIAK_BUCKET',          <<"entities">>).


table() ->
    Env = plugin_util:environment(),
    <<?DBTABLE/binary, $-, Env/binary>>.

bucket() ->
    ?RIAK_BUCKET.

sync() ->
    Table = table(),
    Bucket = bucket(),
    io:fwrite("Synchronize Riak bucket=~p to DynamoDB Table=~p ~n", [Bucket, Table] ),
    spawn( sqorentities, full_sync, [Table, Bucket] ).

full_sync(Table, Bucket) ->
    {ok, C} = riak:local_client(),
    {ok, KeyList} = C:list_keys(Bucket),
    KeyCount = length(KeyList),
    [ write_object(Table, Bucket, C, Key) || Key <- KeyList ],
    io:fwrite("Synchronize Complete! DynamoDB Table=~p, KeyCount=~p ~n", [Table, KeyCount] ).

write_object(Table, Bucket, C, Key) ->
    try
        {ok, RObj} = C:get(Bucket, Key),
        dynamohook:postcommitEntity(Table, RObj)
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Table=~p, Bucket=~p, Key = ~p, Error=~p, Trace=~p, Reason=~p ~n",
            [Table, Bucket, Key, Error, Trace, Reason])
    end.

rawvalue(Key) ->
    RObj = keyget(Key),
    plugin_util:raw_value(RObj).

value(Key) ->
    RObj = keyget(Key),
    plugin_util:raw_value(RObj).

keyget(Key) ->
    {ok, C} = riak:local_client(),
    Bucket = bucket(),
    RObj = dynamohook:get_object(Bucket, C, Key),
    RObj.

keycommit(Key) ->
    {ok, C} = riak:local_client(),
    Bucket = bucket(),
    RObj = dynamohook:get_object(Bucket, C, Key),
    postcommit(RObj).

postcommit(RObj) ->

    Table = table(),
    dynamohook:postcommitEntity(Table, RObj),
    RObj.


%% ----------------------------------------------------------------------------------------
