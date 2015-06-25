PROJECT = riak-tools
DEPS = erlcloud erlexec yamerl
dep_erlcloud = git https://github.com/Amplify-Social/erlcloud.git
dep_erlexec = git https://github.com/saleyn/erlexec.git
dep_yamerl = git https://github.com/sqor/yamerl.git
include erlang.mk
clean::
	@rm -f riak-tools
