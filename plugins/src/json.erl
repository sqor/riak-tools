-module(json).

-export([verify/1]).

verify(Obj) ->
    case jsx:is_json(riak_object:get_value(Obj)) of
	true ->
	    Obj;
	false ->
	    {fail, "Invalid JSON: " ++ binary_to_list(list_to_binary(io_lib:format("~p", [Obj])))}
    end.
