-module(riak_plugin).

-compile(export_all).
%-export([postcommit_s3/1]).

-include_lib("riak_plugin.hrl").

configure() ->
   erlcloud_s3:configure(?ACCESS_KEY, ?SECRET_ACCESS_KEY).

crdt_key_value(RObj) ->
    Key = riak_object:key(RObj),
    {map,V1} = riak_kv_crdt:value(RObj),
    V2 = [{X,Y} || {{X,_Z},Y} <- V1],
    V3 = jsx:encode(V2),
    {Key, V3}.

basic_key_value(RObj) ->
    Key = riak_object:key(RObj),
    Value = riak_object:get_value(RObj),
    {Key, Value}.

postcommit_s3(RObj) ->

    lager:error("postcommit_s3: ~p~n", [RObj]),
    case riak_kv_crdt:is_crdt(RObj) of
        true ->
            {Key,Value} = crdt_key_value(RObj),
            s3_write(Key, Value),
            RObj;
        false ->
            {Key,Value} = basic_key_value(RObj),
            s3_write(Key, Value),
            RObj
    end.

s3_write(Key, Value) ->
    KeyList = binary_to_list(Key),
    A = erlcloud_s3:configure(?ACCESS_KEY, ?SECRET_ACCESS_KEY),
    lager:error("~p:~p Settng up AWS ~p to S3 ~n", [?MODULE, ?LINE, A]),
    %R = erlcloud_s3:put_object(?BUCKET, Key, Value, [], [{"Content-type", "application/x-gzip"}]),
    R = erlcloud_s3:put_object(?BUCKET, KeyList, Value),
    lager:error("postcommit_s3 s3_write is complete: Key= ~p, R= ~p ~n", [Key, R]),

    {ok, R}.


%% ----------------------------------------------------------------------------------------
