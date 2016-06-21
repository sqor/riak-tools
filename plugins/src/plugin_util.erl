-module(plugin_util).

-compile(export_all).
-compile([{parse_transform, lager_transform}]).

environment() ->
    S = os:cmd("/bin/hostname"),
    case S of
        [$d, $e, $v, $- | _T] -> <<"dev">>;
        [$s, $t, $a, $g, $e, $- | _T] -> <<"stage">>;
        [$p, $r, $o, $d, $- | _T] -> <<"prod">>
    end.

path_key(Bucket, Key) ->
    % N1 = node(),
    % N2 = atom_to_list(N1),
    % Node = list_to_binary(N2),

    % dislike this cookie kludge, but we don't have an alternative such as cluster_name
    % C1 = os:getenv(<<"CLUSTERNAME">>).
    C1 = erlang:get_cookie(),
    C2 = atom_to_list(C1),
    ClusterName = list_to_binary(C2),
    <<ClusterName/binary, "/",  Bucket/binary, "/", Key/binary>>.

is_crdt(RObj) ->
    riak_kv_crdt:is_crdt(RObj) andalso has_crdt_first_byte(RObj).

has_crdt_first_byte(RObj) ->                               
    try                                                
        <<69,_Rest/binary>> = riak_object:get_value(RObj),
        true                                             
    catch                                              
        error:{badmatch, _} ->                                  
            false         
    end.

raw_value(RObj) ->
    CRDT = riak_kv_crdt:is_crdt(RObj),
    case CRDT of
        true -> raw_crdt_value(RObj);
        false -> raw_basic_value(RObj)
    end.

raw_crdt_value(RObj) ->
    {_Type, Value} = riak_kv_crdt:value(RObj),
    Value.

raw_basic_value(RObj) ->
    V1 = riak_object:get_values(RObj),
    [V2] = V1,
    V3 = jsx:decode(V2),
    V3.
                              

bucket_key_value(RObj) ->
    CRDT = riak_kv_crdt:is_crdt(RObj),
    case CRDT of
        true -> crdt_key_value(RObj);
        false -> basic_key_value(RObj)
    end.

is_item(Value) ->
    not is_list(Value) and not is_tuple(Value). 

tree([]) -> [];
tree(Tree) ->
    List = is_list(Tree),
    Tuple = is_tuple(Tree),
    case {List, Tuple} of
        {true, false} ->
            Head = hd(Tree),
            case is_item(Head) of
                true ->
                    Tree;
                false ->
                    [ tree(Item) || Item <- Tree ]
            end;
        {false, true} ->
            {X,Y} = Tree,
            tree(X,Y);
        _ ->
            Tree
    end.

tree(X, Y) ->
    case is_tuple(X) of
        false -> {X, tree(Y)};
        true ->
            {P, _Q} = X,
            {P, tree(Y)}
    end.


crdt_key_value(RObj) ->
    lager:debug("Entering crdt_key_value",[]),
    try 
        {_Type, Bucket} = riak_object:bucket(RObj),
        Key = riak_object:key(RObj),
        {map,V1} = riak_kv_crdt:value(RObj),
        lager:info("crdt_key_value, RawValue=~p ~n",[V1]),
        % V2 = lists:filter(fun({{_X,Y},_Z}) -> case Y of riak_dt_map -> false; _ -> true end end, V1),
        % V3 = [{X,Y} || {{X,_Z},Y} <- V2],
        V3 = tree(V1),
        lager:info("CRDTValue = ~p ~n",[{Bucket, Key, V3}]),
        {Bucket, Key, V3}
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Error=~p, Reason=~p, Trace=~p, RObj=~p ~n",[Error, Reason, Trace, RObj])
    end.

basic_key_value(RObj) ->
    lager:debug("Entering basic_key_value",[]),
    Bucket = riak_object:bucket(RObj),
    Key = riak_object:key(RObj),
    try riak_object:get_values(RObj) of
        Values ->
            [V2] = Values,
            V3 = jsx:decode(V2),
            lager:info("BasicValue = ~p ~n",[{Bucket, Key, V3}]),
            {Bucket, Key, V3}
    catch Error:Reason ->
        Trace = erlang:get_stacktrace(),
        lager:error("Caught Exception: Error=~p, Reason=~p, Trace=~p, RObj=~p ~n",[Error, Reason, Trace, RObj])
    end.
