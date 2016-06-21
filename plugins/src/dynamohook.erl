-module(dynamohook).

-compile(export_all).
-compile([{parse_transform, lager_transform}]).
%-export([postcommit/1]).

-include_lib("dynamohook.hrl").


configure() ->
   erlcloud_ddb2:configure(?ACCESS_KEY, ?SECRET_ACCESS_KEY).

bucket_key_value(Bucket, Key) ->
    {ok, C} = riak:local_client(),
    RObj = get_object(Bucket, C, Key),
    {Bucket, Key, Value} = plugin_util:bucket_key_value(RObj),
    {Bucket, Key, Value}.

full_sync(Table, Bucket) ->
    {ok, C} = riak:local_client(),
    {ok, KeyList} = C:list_keys(Bucket),
    KeyCount = length(KeyList),
    [ write_object(Table, Bucket, C, Key) || Key <- KeyList ],
    io:fwrite("Synchronize Complete! DynamoDB Table=~p, KeyCount=~p ~n", [Table, KeyCount] ).

get_object(Bucket, C, Key) ->
    {ok, RObj} = C:get(Bucket, Key),
    RObj.

write_object(Table, Bucket, C, Key) ->
    try
        {ok, RObj} = C:get(Bucket, Key),
        postcommitPost(Table, RObj)
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Table=~p, Bucket=~p, Key = ~p, Error=~p, Trace=~p, Reason=~p ~n",
            [Table, Bucket, Key, Error, Trace, Reason])
    end.

keycommit(Table, Bucket, Key) ->
    {ok, C} = riak:local_client(),
    write_object(Table, Bucket, C, Key).

keyvalue(_Table, Bucket, Key) ->
    {ok, C} = riak:local_client(),
    RObj = get_object(Bucket, C, Key),
    {_Bucket, _Key1, Value} = plugin_util:bucket_key_value(RObj),
    Value.

keyrawvalue(_Table, Bucket, Key) ->
    {ok, C} = riak:local_client(),
    RObj = get_object(Bucket, C, Key),
    plugin_util:raw_value(RObj).

postcommitEntity(Table, RObj) ->

    {Bucket, Key1, V1} = plugin_util:bucket_key_value(RObj),
    try
        lager:info("DynamoDB update fun=postcommit, Table=~p, Bucket=~p Key=~p, Value=~p ~n", [Table, Bucket, Key1, V1]),
        % Map deleted objects to key=0
        Key = maybe_delete(Table, RObj, Key1),
        % Remove empty lists.  Remove empty values.
        Value = tree(V1),
        table_put(Table, Key, Value)
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Key = ~p, Error=~p, Reason=~p ~n", [Key1, Error, Reason]),
        lager:error("Exception PartOne: Key = ~p, Trace=~p ~n", [Key1, Trace]),
        lager:error("Exception PartTwo: Table=~p, Bucket=~p Key=~p, RawValue=~p ~n", [Table, Bucket, Key1, V1])
    end,
    RObj.

is_item(Value) ->
    not is_list(Value) and not is_tuple(Value).  

tree([]) ->
    [];  %% Delete may contain empty data
tree(Tree) ->
    Head = hd(Tree),
    case is_list(Head) of
        true -> tree(Head);
        false ->
            case is_tuple(Head) of
                true ->
                    %% io:fwrite("Tree=~p ~n", [Tree] ),
                    V1 = lists:filter(fun({_X, Y}) -> case Y of [<<>>] -> false; <<>> -> false; [] -> false; _ -> true end end, Tree),
                    %V2 = lists:map(fun({X,Y}) -> case X of <<"player">> -> {X, <<"TBD">>}; _ -> {X,Y} end end, V1),
                    %V3 = lists:map(fun({X,Y}) -> case X of <<"player_leagues">> -> {X, <<"TBD">>}; _ -> {X,Y} end end, V2),
                    V4 = lists:map(fun({X,Y}) -> case X of <<"details">> -> {<<"details_json">>, jsx:encode(Y)}; _ -> {X,Y} end end, V1),
                    V5 = lists:map(fun({X,Y}) -> case X of <<"game_event">> -> {<<"game_event_json">>, jsx:encode(Y)}; _ -> {X,Y} end end, V4),
                    lists:map(fun({X,Y}) -> case is_list(Y) of true -> tree(X,Y); _ -> {X,Y} end end, V5);
                false ->
                    Tree
            end
    end.

tree(X,Y) ->
    %% io:fwrite("X=~p, Y=~p ~n", [X, Y] ),
    Head = hd(Y),
    case is_tuple(Head) of
        true -> {X, {m,tree(Y)}};
        false -> {X, {ss, clean_list(Y)}}
    end.

clean_list(L0) ->
    L1 = lists:filter(fun(X) -> case X of [<<>>] -> false; _ -> true end end, L0),
    L2 = lists:filter(fun(X) -> case X of <<>> -> false; _ -> true end end, L1),
    L3 = lists:filter(fun(X) -> case X of <<" ">> -> false; _ -> true end end, L2),
    L4 = lists:filter(fun(X) -> case X of <<"[]">> -> false; _ -> true end end, L3),
    L4.


postcommitPost(Table, RObj) ->

    {Bucket, Key1, V1} = plugin_util:bucket_key_value(RObj),
    try
        lager:info("DynamoDB update post, Bucket=~p Key=~p, Value=~p ~n", [Bucket, Key1, V1]),
        % Map deleted objects to key=0
        Key = maybe_delete(Table, RObj, Key1),
        Value = post_value(V1),

        table_put(Table, Key, Value)
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Key = ~p, Error=~p, Trace=~p, Reason=~p ~n", [Key1, Error, Trace, Reason])
    end,
    RObj.

post_value(RawValue) ->
    V1 = lists:filter(fun({_X,Y}) -> case Y of [<<>>] -> false; _ -> true end end, RawValue),
    V2 = lists:filter(fun({_X,Y}) -> case Y of <<>> -> false; _ -> true end end, V1),
    V3 = lists:map(fun({X,Y}) -> case X of <<"metadata">> -> {<<"metadata_json">>, jsx:encode(Y)}; _ -> {X,Y} end end, V2),
    V4 = lists:map(fun({X,Y}) -> case X of <<"game_event">> -> {<<"game_event_json">>, jsx:encode(Y)}; _ -> {X,Y} end end, V3),
    V5 = lists:map(fun({X,Y}) -> case X of <<"content_raw_ig">> -> {<<"content_raw_ig_json">>, jsx:encode(Y)}; _ -> {X,Y} end end, V4),
    % Remove deprecatd keys

    ListKeys = listkeys(V5),
    F = fun ddbify/2,
    Value = foreach( F, V5, ListKeys),
    Value.


foreach(F, List, [H|T]) ->
    NewList = F(H, List),
    foreach(F, NewList, T);
foreach(_F, List, []) ->
    List.

% List all tuple Keys which have a List value
listkeys(List) ->
    L1 = lists:filter(fun({_X,Y}) -> L = is_list(Y), case L of true -> true; _ -> false end end, List),
    proplists:get_keys(L1).

% Transmorfigy the json data, into the DynamoDB format.
ddbify(Key, List) ->
    case proplists:is_defined(Key, List) of
        true ->
            V1 = proplists:get_value(Key, List),
            % Remove empty values
            V2 = lists:filter(fun(X) -> case X of <<>> -> false; _ -> true end end, V1),
            V3 = {Key, {ss, V2}},
            L1 = proplists:delete(Key, List),
            L2 = [ V3 | L1 ],
            L2;
        false -> List
    end.


table_put(_Table, <<>>, _Value) ->
    {ok};  % ignore empty value
table_put(Table, Key, Value) ->

%% R = erlcloud_ddb2:put_item(Table, Value,
%%     [{expected, [{<<"id">>, null}, {<<"author">>, null}, {<<"published_dt">>, null}]}]),
    configure(),
    lager:info("DynamoDB PRE_DD2 put_item: Table=~p, Key = ~p, Value= ~p ~n", [Table, Key, Value]),
    R = erlcloud_ddb2:put_item(Table, Value, []),

    lager:info("DynamoDB put_item: Table=~p, Key = ~p, R=~p, Value= ~p ~n", [Table, Key, R, Value]),

    {ok, R}.

maybe_delete(Table, RObj, Key) ->
    [Metadata| _T] = riak_object:get_metadatas(RObj),
    Deleted = dict:is_key(<<"X-Riak-Deleted">>, Metadata),
    case Deleted of
        true -> table_delete(Table, Key), <<>>;
        false -> Key
    end.

table_delete(Table, Key) ->
 
    configure(),
    Value = [{<<"id">>, Key}],
    R = erlcloud_ddb2:delete_item(Table, Value, []),

    lager:info("DynamoDB delete_item: Table= ~p, Key = ~p, R= ~p, Value= ~p ~n", [Table, Key, R, Value]),

    {ok, R}.

t1() ->
[{<<"author">>,<<"89354">>},
 {<<"avatar">>,
  <<"https://rest-stage.sqor.com/images/entities/89354">>},
 {<<"content">>,<<"hey #Brett Favre">>},
 {<<"hashtags">>,[<<"Brett">>]},
 {<<"id">>,<<"8cd8edf6-0064-4e51-bd69-6022fc0a8272">>},
 {<<"name">>,<<"Brain Train">>},
 {<<"published_dt">>,<<"2016-02-23T00:13:48Z">>},
 {<<"tags">>,[<<" ">>,<<"Sports Personality">>,<<"[]">>]},
 {<<"tags_set">>,[<<>>]},
 {<<"type">>,<<"sqor">>}].

t2() ->
[
 {<<"author">>,<<"89354">>},
 {<<"avatar">>,
  <<"https://rest-stage.sqor.com/images/entities/89354">>},
 {<<"content">>,<<"hey #Brett Favre">>},
 {<<"hashtags">>,[<<"Brett">>]},
 {<<"id">>,<<"8cd8edf6-0064-4e51-bd69-6022fc0a8272">>},
 {<<"name">>,<<"Brain Train">>},
 {<<"published_dt">>,<<"2016-02-23T00:13:48Z">>},
 {<<"tags">>,[<<" ">>,<<"Sports Personality">>,<<"[]">>]},
 {<<"tags_set">>,[<<>>]},
 {<<"type">>,<<"sqor">>},
 {<<"details">>,[
     [{<<"type">>,<<"sqor">>},{<<"type2">>,<<"sqor">>}],[{<<"mype">>,<<"sqor">>},{<<"mype2">>,<<"sqor">>}]
     ] }
].


%% ----------------------------------------------------------------------------------------
