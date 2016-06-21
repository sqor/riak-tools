# riak_plugins

Riak supports a code plugin architecture.

Commit hooks on named buckets allow custom Erlang code to be run, on a key/value precommit or postcommit.  This allows us to trigger, replicate and mirror Riak key/value data to another completely different storage system.  Replication is currently one-way, from Riak to DynamoDB.  For example, we can replicate bucket data into S3 or DynamoDB.

##Prerequisites
Our Riak is Erlang

##Build
`make`

`make tar`

##Deploy
./dev_tar.sh

./dev_configure.sh

##Test
`make eunit`

##Jenkins Jobs
http://build.sqor.com/job/DEV_riak_plugin/

##Riak Debug
On a Riak node:

riak attach

Key = <<"ff44713d-867a-4c73-ad8f-a31045046b10">>.

K2 = <<"feff656b-98ef-4598-b5e9-c4c69b6da16b">>.

sqorposts:keycommit(Key).

##Sychronize a bucket

sqorposts:sync().

sqorentities:sync().



