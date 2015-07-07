-module('riak-tools').

%-export([file_link/2, file_link_targets/2, main/1, backup_script/0, multipart_upload/4, s3_upload/2, read_file/2, tar/1, tar/2, tar_riakdb/0, s3_small_upload/2, get_gzip_name/0, get_month_name/0, get_weekly_name/0, get_wday_name/0]).
-compile(export_all). 

-define('VERSION',              "1.0.0").
-define('ACCESS_KEY',           "AAAAAAAAAAAAAAAAAAAAA").
-define('SECRET_ACCESS_KEY',    "XXXXXXXXXXXXXXXXXXXXX").
-define('S3_BUCKET',            "my-s3-bucket").
-define('RIAK_DB',              ["/etc/riak","/var/lib/riak"]).
-define('CHUNK_SIZE',           (15 * 1024 * 1024)).
-define('RIAK_RUNNING',         "riak is running").
-define('RIAK_STOPPED',         "riak is stopped").
-define('AWS_ELB',              "my-elb").
-define('BACKUP_DIR',           ".").

-define (USAGE_TEMPLATE, "usage: riak-tools [backup|get|elb-register|elb-deregister|riak-start|riak-stop]~n").
-define (USAGE_DATA, []).

backup_script() ->
    io:format("~s backup dir=~p ~n", [ts(),yaml_get("BACKUP_DIR",?BACKUP_DIR)]),
    file:set_cwd(yaml_get("BACKUP_DIR",?BACKUP_DIR)),
    io:format("~s pwd=~p ~n", [ts(),file:get_cwd()]),
    assert_elb(),
    TGZName = get_gzip_name(),
    stop_riak(),
    tar_riakdb(),
    start_riak(),
    TargetList = [get_wday_name(), get_month_name()],
    file_link_targets( TGZName, TargetList ),

    s3_upload( TGZName, TGZName ).

stop_riak() ->
    assert_running(),
    deregister_with_elb(),
    io:format("~s stopping riak ~n", [ts()]),
    os:cmd("/usr/sbin/riak stop"),
    assert_stopped().

start_riak() ->
    io:format("~s starting riak ~n", [ts()]),
    os:cmd("/usr/sbin/riak start"),
    assert_running(),
    register_with_elb(),
    assert_elb(),
    io:format("~s riak running ~n", [ts()]).

register_with_elb() ->
    I = get_instanceid(),
    io:format("~s register elb=~s InstanceId=~s ~n", [ts(), yaml_get("AWS_ELB",?AWS_ELB), I]),
    erlcloud_elb:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    R = erlcloud_elb:register_instance(yaml_get("AWS_ELB",?AWS_ELB),I),
    io:format("~s register reply=~p ~n", [ts(), R]).

deregister_with_elb() ->
    I = get_instanceid(),
    io:format("~s deregister elb=~s InstanceId=~s ~n", [ts(), yaml_get("AWS_ELB",?AWS_ELB), I]),
    erlcloud_elb:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    R = erlcloud_elb:deregister_instance(yaml_get("AWS_ELB",?AWS_ELB),I),
    io:format("~s deregister reply=~p ~n", [ts(), R]).

get_instanceid() ->
    Config = erlcloud_aws:default_config(),
    case erlcloud_aws:http_body(
           erlcloud_httpc:request(
             "http://169.254.169.254/latest/meta-data/instance-id/",
             get, [], <<>>, 10000, Config)) of
        {error, Reason} ->
            {error, Reason};
        {ok, Body} ->
            binary_to_list(Body)
    end.

ts() ->
    TS = {_,_,Micro} = os:timestamp(),
    {{Year,Month,Day},{Hour,Minute,Second}} = 
    calendar:now_to_universal_time(TS),
    Mstr = element(Month,{"Jan","Feb","Mar","Apr","May","Jun","Jul",
              "Aug","Sep","Oct","Nov","Dec"}),
    io_lib:format("~2w ~s ~4w ~2w:~2..0w:~2..0w.~6..0w",
          [Day,Mstr,Year,Hour,Minute,Second,Micro]).

riak_status() ->
    os:cmd("/etc/init.d/riak status").

status_running(S) ->
    case contains(S,"such file") of
        [] -> false;
        _ -> true
    end.

assert_elb() ->
    % Validate this instance is registered to the ELB.
    erlcloud_elb:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    R = erlcloud_elb:describe_load_balancer("SQOR-RIAK-DEV-VPC"),
    ELB = lists:flatten(io_lib:format("~p", [R])),
    I = get_instanceid(),
    assert_string(ELB,I),
    io:format("~s ELB registered with InstanceId=~s ~n", [ts(), I]).

assert_running() ->
    Status = riak_status(),
    R = contains(Status,?RIAK_RUNNING),
    case R of
        [] -> erlang:error(riak_not_running);
        _ -> true
    end.

assert_stopped() ->
    Status = riak_status(),
    R = contains(Status,?RIAK_STOPPED),
    case R of
        [] -> erlang:error(riak_not_stopped);
        _ -> true
    end.

assert_string(S1,S2) ->
    I = string:str(S1,S2),
    case I of
        0 -> erlang:error(not_registered_with_elb);
        _ -> true
    end.

starts_with(S1,S2) ->
    %io:format("sw ~5s ~s ~n", [S1,S2]),
    lists:prefix(S2,S1).

contains(S1,S2) ->
    %io:format("begin ~p ~p ~n", [length(S1),length(S2)]),
    contains(S1,S2,1,[]).
 
%% While S1 is at least as long as S2 we check if S2 is its prefix,                                                                                                                                                                                                                   
%% storing the result if it is. When S1 is shorter than S2, we return                                                                                                                                                                                                                 
%% the result. NB: this will work on any arbitrary list in erlang                                                                                                                                                                                                                     
%% since it makes no distinction between strings and lists.                                                                                                                                                                                                                           
contains([_|T]=S1,S2,N,Acc) when length(S2) =< length(S1) ->
    %io:format("contains ~p ~n", [N]),
    case starts_with(S1,S2) of
        true ->
            contains(T,S2,N+1,[N|Acc]);
        false ->
            contains(T,S2,N+1,Acc)
    end;
contains(_S1,_S2,_N,Acc) ->
    Acc.

hostname() ->
    H = os:cmd("hostname"),
    string:strip(H,right,$\n).

% nodename() ->
%     atom_to_list(node()).

get_gzip_name() ->
    S1 = hostname(),
    S1 ++ "-riakdb.tgz".

get_month_name() ->
    {{_,Month,_},{_,_,_}} = erlang:localtime(),
    get_gzip_name() ++ ".month." ++ erlang:integer_to_list(Month).

get_weekly_name() ->
    {_,Week} = calendar:iso_week_number(),
    get_gzip_name() ++ ".week." ++ erlang:integer_to_list(Week).

get_wday_name() ->
    WeekDay = calendar:day_of_the_week(date()),
    get_gzip_name() ++ ".wday." ++ erlang:integer_to_list(WeekDay).


s3_small_upload(Key, File) ->    
    R = file:read_file(File),
    {ok, Binary} = R,
    s3_upload_single(Key, Binary).

s3_upload_single(Key, Value) ->
    A = erlcloud_s3:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    error_logger:info_msg("~p:~p Settng up AWS ~p to S3 ~n", 
              [?MODULE, ?LINE, A]),
    %R = erlcloud_s3:put_object(yaml_get("S3_BUCKET",?S3_BUCKET), Key, Value, [], [{"Content-type", "application/x-gzip"}]),
    R = erlcloud_s3:put_object(yaml_get("S3_BUCKET",?S3_BUCKET), Key, Value),

    error_logger:info_msg("~p:~p Uploaded File ~p to S3 ~n", [?MODULE, ?LINE, R]),
    {ok, R}.

s3_upload(Key, Path) ->
    {ok, IoDevice} = file:open(Path,[raw]),
    multipart_upload(Key, IoDevice, "Config", ?CHUNK_SIZE).

read_file(IoDevice,Count) ->
    case file:read(IoDevice, Count) of
        eof ->
            {eof, 1};
        _ ->
            {eof, 2}
    end.

multipart_upload(Key, IoDevice, Config, Count) ->
    erlcloud_s3:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    {_,[{_,UploadId}]} = erlcloud_s3:start_multipart(yaml_get("S3_BUCKET",?S3_BUCKET), Key),
    io:format("~s S3 UploadId=~p ~n", [ts(), UploadId]),
    upload_parts(Key, IoDevice, UploadId, Config, 1, Count, []).

upload_parts(Key, _IoDevice, UploadId, _Config, PartCount, 0, Parts) ->
    io:format("~s S3 Multipart Complete. S3Key=~p PartCount=~p ~n", [ts(), Key,PartCount]),
    A = erlcloud_s3:complete_multipart(yaml_get("S3_BUCKET",?S3_BUCKET), Key, UploadId, lists:reverse(Parts)),
    {ok,A};
upload_parts(Key, _Device, UploadId, _Config, PartCount, -1, _Parts) ->
    io:format("~s Error. S3 Abort Multipart. S3Key=~p Part=~p ~n", [ts(), Key,PartCount]),
    A = erlcloud_s3:abort_multipart(yaml_get("S3_BUCKET",?S3_BUCKET), Key, UploadId),
    {err,A};
upload_parts(Key, IoDevice, UploadId, Config, PartCount, Count, Parts) ->
    io:format("~s S3 upload_part. S3Key=~p Part=~p ~n", [ts(), Key,PartCount]),
    case file:read(IoDevice, Count) of
        {ok,Data} ->
            %error_logger:info_msg("OK ~n"),
            %A = erlcloud_s3:upload_part(yaml_get("S3_BUCKET",?S3_BUCKET), Key, UploadId, PartCount, Data),
            A = s3_part_retry(yaml_get("S3_BUCKET",?S3_BUCKET), Key, UploadId, PartCount, Data, 5),
            {ok,[{etag,PartEtag}]} = A,
            upload_parts(Key, IoDevice, UploadId, Config, PartCount + 1, Count, [{PartCount, PartEtag} | Parts]);
        eof ->
            %error_logger:info_msg("eof ~n"),
            upload_parts(Key, IoDevice, UploadId, Config, PartCount, 0, Parts);
        _ ->
            %error_logger:info_msg("_ ~n"),
            upload_parts(Key, IoDevice, UploadId, Config, PartCount + 1, -1, Parts)
    end.

s3_part_retry(S3_BUCKET, Key, UploadId, PartCount, Data, 0) ->
    erlcloud_s3:upload_part(S3_BUCKET, Key, UploadId, PartCount, Data);
s3_part_retry(S3_BUCKET, Key, UploadId, PartCount, Data, RetriesLeft) ->
    R = erlcloud_s3:upload_part(S3_BUCKET, Key, UploadId, PartCount, Data),
    case R of
        % handle socket timeout
        {error,{socket_error,timeout}} ->
            io:format("catch: error: {error,{socket_error,timeout}} ~n"), % dbg only
            % retry sending message, after 1 second
            timer:sleep(1000),
            s3_part_retry(S3_BUCKET, Key, UploadId, PartCount, Data, RetriesLeft - 1);
        {ok,_} ->
            R;
        _ ->
            io:format("catch: unknown error: ~p ~n", [R]),
            R
    end.     

tar_riakdb() ->
    io:format("~s tar leveldb=~p ~n", [ts(), ?RIAK_DB]),
    Tar_Name = get_gzip_name(),
    tar(?RIAK_DB,Tar_Name).
 
tar(File) ->
    TarName = File ++ ".tgz",
    DelCmd = "rm " ++ TarName,
    os:cmd(DelCmd),
	erl_tar:create(TarName,[File],[compressed]).

tar(FileList,TarName) ->
    DelCmd = "rm " ++ TarName,
    os:cmd(DelCmd),
	erl_tar:create(TarName,FileList,[compressed]).


file_link(Src,Target) ->
    Cmd = "ln -f " ++ Src ++ " " ++ Target,
    os:cmd(Cmd),
    {ok}.

file_link_targets(Src,TargetList) ->
    lists:foreach(fun(E) ->
                    file_link(Src,E)
                  end, TargetList).
%
% ToDo: Implement fetch multiple chunks download via Range HTTP header
%
s3_get(Key) ->
    io:format("s3get key=~p ~n", [Key]),
    erlcloud_s3:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    R = erlcloud_s3:get_object(yaml_get("S3_BUCKET",?S3_BUCKET), Key),
    {content,Data} = lists:keyfind(content,1,R),
    file:write_file(Key, Data).

yaml_get(Key) ->
    io:format("~s get yaml key=~p ~n", [ts(), Key]),
    [Yaml] = yamerl_constr:file(".riak-tools.yaml"),
    YamlDict = dict:from_list(Yaml),
    {ok,Value} = dict:find(Key,YamlDict),
    Value.

yaml_get(Key,Default) ->
  try yaml_get(Key) of
    Value -> Value
  catch
    _:_ -> Default
  end.

handle_backup([]) ->
    io:format("~s start riak leveldb backup host=~p ~n", [ts(), hostname()]),
    backup_script(),
    io:format("~s backup_complete host=~p ~n", [ts(), hostname()]);
handle_backup([_Arg|_Args]) ->
    handle_backup([]).

handle_get([]) ->
    s3_get(get_gzip_name());
handle_get([Arg|Args]) ->
    io:format("Arg=~p Args=~p  ~n", [Arg,Args]),
    s3_get(Arg).

handle_status() ->
    os:cmd("chmod 0600 .riak-tools.yaml"),
    Key = "S3_BUCKETX",
    Value = yaml_get(Key,?S3_BUCKET),
    io:format("Yaml ~p ~n", [Value]).
    %R = riak_status(),
    %io:format("Status ~p ~n", [R]),
    %I = get_instanceid(),
    %io:format("InstanceId ~p ~n", [I]),
    %assert_elb().

handle_elb() ->
    erlcloud_elb:configure(yaml_get("ACCESS_KEY",?ACCESS_KEY), yaml_get("SECRET_ACCESS_KEY",?SECRET_ACCESS_KEY)),
    R = erlcloud_elb:describe_load_balancer(yaml_get("AWS_ELB",?AWS_ELB)),
    ELB = lists:flatten(io_lib:format("~p", [R])),
    I = get_instanceid(),
    io:format("InstanceId ~s ~n", [I]),
    %io:format("ELB ~s ~n", [ELB]),
    assert_string(ELB,I),
    io:format("elb!~n").


%% escript --------------------------------------------------------------------------------

main([]) ->
    usage();
main([MapType|Args]) ->
    ssl:start(),
    erlcloud:start(),
    application:start(yamerl),
    os:cmd("chmod 0600 .riak-tools.yaml"),
    case MapType of
        "backup" ->
            handle_backup(Args);
        "get" ->
            handle_get(Args);
        "hello" ->
            io:format("Args ~p ~n", [Args]);
        "status" ->
            handle_status();
        "elb-register" ->
            register_with_elb();
        "elb-deregister" ->
            deregister_with_elb();
        "riak-start" ->
            start_riak();
        "riak-stop" ->
            stop_riak();
        "version" ->
            io:format("riak-tools ~p ~n", [?VERSION]);
        _ ->
            usage()
    end.

usage() ->
    io:format(?USAGE_TEMPLATE, ?USAGE_DATA).


%% ----------------------------------------------------------------------------------------
