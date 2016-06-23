-module(jwt).
-author("vierling").

-compile(export_all).

-define(CLIENT_APP_ID, <<"66noXXXXXXXXXXXXXXXX">>).

configuration_json_string() ->
    "{\"keys\":[{\"alg\":\"RS256\",\"e\":\"AQAB\",\"kid\":\"RgCoizVgG64sga+6H+0S547Adpz9uqtnl3xxllVk6qI=\",\"kty\":\"RSA\",\"n\":\"1MZA7llnqW4qlsbE95FR1u0k4fXSKzVE7xmF_ziPr-x4vCXhKxhEti1nyluc6oX2jPqPd7_MEHHxhCyRn2RyyohX3BSwrGeGpv-kDQA9D57DK13Y29eQibyGgJa1qoxCPVM2JLqrzHffcu_MLqVhQ1gNlLVovwjsDefRA3rzfdB5AgaRHvGyjVJrqQWbX0KkG0BAO-NOtMQmX5XE4NG2YH301igXRZFGrWe3LUycAGuEFvNGtV50H_Des_j93rTixUSR2wG1h91AnFsF3IuYrjXPS8UVIItCJ6TdnidXzdYMr-eSMAk4H8D7Erzu9llSiFMpFSCQvViYugWvYK6n5w\",\"use\":\"sig\"},{\"alg\":\"RS256\",\"e\":\"AQAB\",\"kid\":\"/o+KoQkC04usrr3zgeE5R5jqA6TKwxTq98m2615FXuA=\",\"kty\":\"RSA\",\"n\":\"gz41BjQKdQvBUKD4gfY1IweUnASxxPnAb6xDpV_gI-xcl-pi0ZQE9gcTXOBMJfS4K7YTwzX8nkDGCQas88_rW7pCnPsmm5MZ5QD_9DobZHeR96VFypJvxGmxZXr50Q9iruMMcvSROpURGbaukf5s1Wnao9GzhC1CuhIVmd09X_Cz83E1fRDGC_GRSkfCVVrZl5-xYl_sLoFrj-LNrCmOTxJsJfAf47yEuFVS0Tbx4ofGC1yBmX-2W68LHGoZ6Jt8IVz4ctLkBUdx1ACM320HCC49qwQWY0iYnS38sXpu-sJ28b64Puya7Hyi6V4tqd45EZQjD8SQLLlpsuoXu-GnlQ\",\"use\":\"sig\"}]}".

configuration_url() ->
    "https://cognito-idp.us-east-1.amazonaws.com/us-east-YOUR-ID-HERE/.well-known/jwks.json".

test_jwt_token() ->
    <<"eyJraWQiOiJSZ0NvaXpWZ0c2NHNnYSs2SCswUzU0N0FkcHo5dXF0bmwzeHhsbFZrNnFJPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJmMGE2N2E5OC0xODA1LTRkNDItYWY3Mi05Y2ZmNjg0YzI0ZjAiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tXC91cy1lYXN0LTFfWXBYSmFmeW94IiwiY3VzdG9tOnVzZXJfaWQiOiIyNjMyNDQiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOmZhbHNlLCJnaXZlbl9uYW1lIjoiamluZyIsImF1ZCI6IjY2bm9iNGYzY2xzYnJzbXNjM21lNTJzcmViIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE0NjE5NjQ3OTgsInBob25lX251bWJlciI6IisxNjUwOTQ4NTk2NCIsImV4cCI6MTQ2MTk2ODM5OCwiaWF0IjoxNDYxOTY0Nzk4LCJlbWFpbCI6ImppbmcubGluQHNxb3IuY29tIn0.dmSxSRGs2Q0A7lsruuQOHOGV8FCpctICywG_2HW0pjn_KPlsG46UGiWoiqp-YqhRO1ZaOBPKhWYE7_PViQmpdlRy6rfDl8FbjOP8k7HHg-BuCBhbvgUT0_fEL-llJjDPBmloXViOw2BiHY4C7ehPeym7iypI9rN5IkjPuvjOTr8kuIA6wh-9DqJRYmxHBxPXhtzm7MDEaL9ch4-Hs6Q5AGNoKXLQY-xZ-l4JJB6DocA1jzUAdweUnFncEFDg8lRSPF_nHyKLcpk0OhXUTx-_nc-bJVh2C4VQhUC5LMqZIj8v-f6nLeHreHDByLQooJ9NFzVmNhboTrGuSVsllGurlQ">>.

app_key() ->
    <<"koepccYOUR-APP-KEY">>.


pad(Width, Binary) ->
    case ((Width - size(Binary) rem Width) rem Width) of
        0 -> Binary;
        N -> <<Binary/binary, 0:(N*8)>>
    end.

is_authorized(Req, State) ->
    case cowboy_req:method(Req) of
        <<"GET">> -> {true, Req, undefined};
        _ ->
            case cowboy_req:header(<<"access-token">>, Req) of
                undefined ->
                    {{false, <<"foo">>}, Req, State};
                Token ->
                    is_valid_check(Token, Req, State)
            end
    end.

is_valid_check(Token, Req, State) ->
    try
        is_valid(Token, Req, State)
    catch Error:Reason ->
        lager:info("is_valid_check: Unauthorized exception: ~p~n",[{Error, Reason}]),
        PrintError = atom_to_binary(Error, utf8),
        PrintReason = term_to_binary(Reason),
        {{false, <<PrintError/binary, <<" ">>/binary, PrintReason/binary>>}, Req, State}
    end.


is_jwt(Token) ->
    try
        sfr_auth:split_token(Token),
        true
    catch _Error:_Reason ->
        false
    end.

is_valid(Token, Req, State) ->

    [Header, Payload, Signature] = sfr_auth:split_token(Token),
    %% Assert Payload contains a valid AUD
    ?CLIENT_APP_ID = maps:get(<<"aud">>, Payload),

    %% Assert expiry is still valid
    %% ToDo Enable.  Leave this off during our Alpha testing period so we can use expired
    %%      tokens.  Uncomment, when we go into production.

    % Exp = maps:get(<<"exp">>,Payload),
    % true = (Exp > erlang:system_time(seconds)),

    Key = sfr_auth:key(Header),
    #{<<"n">> := N0, <<"e">> := E0} = Key,
    N1 = base64url:decode(N0),
    E1 = base64url:decode(E0),
    N = binary:decode_unsigned(N1),
    E = binary:decode_unsigned(E1),
    [H, P, _S] = binary:split(Token, <<".">>, [global]),
    Msg = iolist_to_binary([H, <<".">>, P]),
    true = crypto:verify(rsa, sha256, Msg, Signature, [E, N]),
    {true, Req, State}.

split_token(Token) ->
    [H, P, S] = binary:split(Token, <<".">>, [global]),
    Header = jsx:decode(base64url:decode(H), [return_maps]),
    Payload = jsx:decode(base64url:decode(P), [return_maps]),
    Signature = base64url:decode(S),
    [Header, Payload, Signature].

modulus_exponent(Key) ->
    #{<<"n">> := N0, <<"e">> := E0} = Key,
    N1 = base64url:decode(N0),
    E1 = base64url:decode(E0),
    N = binary:decode_unsigned(N1),
    E = binary:decode_unsigned(E1),
    [N, E].

get_kid(Header) ->
    #{<<"kid">> := KId} = Header,
    KId.

key(Header) ->
    KId = get_kid(Header),
    #{<<"keys">> := Keys} = configuration(),
    [Key] = lists:filter(
            fun(Key) ->
                #{<<"kid">> := K} = Key,
                K =:= KId
            end, Keys),
    Key.

configuration() ->
    %% ConfigurationJson = list_to_binary(configuration_json()),
    %% Technically we should get this data from AWS at boot time, and somehow cache the value.
    %% But per Amazon it will never change...  So we hardcode the retrieved value for optimal performance.
    ConfigurationJson = list_to_binary(configuration_json_string()),
    Configuration = jsx:decode(ConfigurationJson, [return_maps]),
    Configuration.

configuration_json() ->
    {ok, { {_, 200, _}, _, ConfigurationJson}} = httpc:request(configuration_url()),
    ConfigurationJson.

configuration_jsonX() ->
    sfr_auth:get(infinity, query_key, fun() ->
        {ok, { {_, 200, _}, _, ConfigurationJson}} = httpc:request(configuration_url()),
        ConfigurationJson
    end).


%%%-------------------------------------------------------------------
