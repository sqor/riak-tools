#BUILD STEP
#OS X has issues with -A flag for declare, fyi...
declare -A ENV_SHORT=( ["development"]="dev" ["stage"]="stage" ["production"]="prod" )
#Create git hash
#git rev-parse HEAD > priv/gitrev.txt
cd riak_archive;
make clean;
make;
make docs;
make eunit;
make escript;
#the below has bug ;(
#mv riak_archive riak_archive_${ENV_SHORT}
mv riak_archive riak_archive_dev
#
#S3 Copy
#the below has bug ;(
#s3cmd -c /var/lib/jenkins/.s3cmd put riak_archive_${ENV_SHORT} s3://erlang_releasess
s3cmd -c /var/lib/jenkins/.s3cmd put riak_archive_dev s3://erlang_releasess
