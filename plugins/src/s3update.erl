-module(s3update).

-compile(export_all).
-compile([{parse_transform, lager_transform}]).
%-export([postcommit/1]).

-include_lib("riak_plugin.hrl").

-ifndef(RIAK_PLUGIN).
-define('VERSION',              "1.0.0").
-define('ACCESS_KEY',           "AxxxxxxxxxxxxxxxxxxB").
-define('SECRET_ACCESS_KEY',    "cXXXXXXXXXXXXXXXXXXXXXXXXXXXy").
-define('S3BUCKET',             "sqor-riak").
-endif.

%-include_lib("erlcloud.hrl").
%-include_lib("erlcloud_aws.hrl").



configure() ->
   erlcloud_s3:configure(?ACCESS_KEY, ?SECRET_ACCESS_KEY).

spawn_sync() ->
    io:fwrite("Sychronize Riak bucket to S3 Bucket=~p ~n", [?S3BUCKET] ),
    spawn( s3update, full_sync, [] ).

full_sync() ->
    {ok, C} = riak:local_client(),
    {ok, KeyList} = C:list_keys({<<"map">>,<<"articles">>}),
    KeyCount = length(KeyList),
    [ write_object(C, Key) || Key <- KeyList ],
    io:fwrite("Synchronize Complete! S3 Bucket=~p, KeyCount=~p ~n", [?S3BUCKET, KeyCount] ).

get_object(C, Key) ->
    {ok, RObj} = C:get({<<"map">>,<<"articles">>}, Key),
    RObj.

write_object(C, Key) ->
    try
        {ok, RObj} = C:get({<<"map">>,<<"articles">>}, Key),
        postcommit(RObj)
    catch Error:Reason ->
        lager:error("Caught Exception=~p, Key=~p ~n",[{Error, Reason}, Key])
    end.



postcommit(RObj) ->

    lager:info("s3update fun=postcommit ~n", []),
    {Bucket, Key1, Value1} = plugin_util:bucket_key_value(RObj),
    lager:info("s3update fun=postcommit, Bucket=~p Key=~p, Value=~p ~n", [Bucket, Key1, Value1]),
    % Map deleted objects to key=0
    Key = deleted_object_mask(RObj, Key1),
    % Remove empty lists.  Remove empty values.  Remove Like_count.
    Value2 = lists:filter(fun({_X,Y}) -> case Y of [<<>>] -> false; _ -> true end end, Value1),
    Value3 = lists:filter(fun({_X,Y}) -> case Y of <<>> -> false; _ -> true end end, Value2),
    Value4 = lists:filter(fun({X,_Y}) -> case X of <<"like_count">> -> false; _ -> true end end, Value3),  
    Value5 = lists:filter(fun({X,_Y}) -> case X of <<"metadata">> -> false; _ -> true end end, Value4), 
    Value = jsx:encode(Value5),
    case Bucket of
        {_Type, Name} ->
            % S3 keys are converted to folderized key names
            PathKey = plugin_util:path_key(Name, Key),
            s3_write(PathKey, Value);
        Name ->
            PathKey = plugin_util:path_key(Name, Key),
            s3_write(PathKey, Value)
    end,
    RObj.

deleted_object_mask(RObj, Mask) ->
    Metadata = riak_object:get_metadata(RObj),
    Deleted = dict:is_key(<<"X-Riak-Deleted">>, Metadata),
    case Deleted of
        true -> <<"0">>;
        false -> Mask
    end.


s3_write(_Key, <<>>) ->
    {ok};  % ignore empty value
s3_write(Key, Value) ->
    lager:info("Entering: Key=~p, Value=~p ~n", [Key, Value]),
    try
        KeyList = binary_to_list(Key),
        _A = configure(),
        %R = erlcloud_s3:put_object(?S3BUCKET, Key, Value, [], [{"Content-type", "application/x-gzip"}]),
        R = s3_retry(KeyList, Value, 10),
        lager:info("Write to S3 is complete: Key= ~p, Value= ~p, R=~p ~n", [Key, Value, R]),

        {ok, R}
    catch Error:Reason ->
        lager:error("Caught Exception=~p, Key=~p, Value~p ~n",[{Error, Reason}, Key, Value])
    end.

s3_retry(Key, Value, 0) ->
    erlcloud_s3:put_object(?S3BUCKET, Key, Value);
s3_retry(Key, Value, RetriesLeft) ->
    try
        R = erlcloud_s3:put_object(?S3BUCKET, Key, Value),
        case R of
            % handle socket timeout
            {error,{socket_error,timeout}} ->
                lager:error("catch and handle reply: {error,{socket_error,timeout}} ~n"),
                % retry sending message, after 10 second
                timer:sleep(10000),
                s3_retry(Key, Value, RetriesLeft - 1);
            [{version_id,_}] ->
                R;
            {ok,_} ->
                R;
            _ ->
                lager:error("catch exception: Key=~p, Value=~p, R=~p ~n", [Key, Value, R]),
                R
        end
    catch Error:Reason ->
        timer:sleep(10000),
        lager:error("Caught and handle Exception=~p, Key=~p, Value~p ~n",[{Error, Reason}, Key, Value]),
        s3_retry(Key, Value, 0)
    end.  

%% ----------------------------------------------------------------------------------------
