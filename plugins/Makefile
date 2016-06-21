PACKAGE=riak_plugin
EBIN_DIR=ebin
INCLUDE_DIRS=include
DEPS_DIR=deps
DEPS ?= erlcloud
DEPS_EZ=$(foreach DEP, $(DEPS), $(DEPS_DIR)/$(DEP).ez)
RABBITMQ_HOME ?= .

all: compile

tools: erlang16 rebar3

erlang16:
	sudo apt-get update	>> /dev/null
	cd /tmp; wget -nc https://packages.erlang-solutions.com/erlang/esl-erlang/FLAVOUR_1_general/esl-erlang_16.b.2-1~ubuntu~saucy_amd64.deb
	sudo dpkg -E --force depends -i /tmp/esl-erlang_16.b.2-1~ubuntu~saucy_amd64.deb
	sudo apt-get -y -f install
	sudo dpkg -E --force depends -i /tmp/esl-erlang_16.b.2-1~ubuntu~saucy_amd64.deb

rebar3:
	cd /usr/local/bin; wget -nc https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3

clean:
	rebar3 clean
	rm -rf $(EBIN_DIR)
	rm -rf _build
	rm -rf *.tgz
	rm rebar.lock

distclean: clean
	rm -rf $(DEPS_DIR)

tar:
	tar --exclude .git --exclude _build --exclude ebin --exclude .gitignore -cvzf riak_plugins.tgz ../riak_plugins/

package: compile
	mkdir -p ebin
	find ./_build/ -name "*.beam" -exec cp {} ./ebin/  \;
	find ./_build/ -name "*.app" -exec cp {} ./ebin/  \;

install: package
	cp advanced.config /etc/riak/advanced.config
	mkdir -p /var/lib/riak/sqorplugins
	chmod 0777 /var/lib/riak/sqorplugins
	cp ebin/* /var/lib/riak/sqorplugins

# $(DEPS_DIR):
#	./rebar get-deps

$(DEPS_EZ): 
	cd $(DEPS_DIR); $(foreach DEP, $(DEPS), zip -r $(DEP).ez $(DEP);)

compile:
	rm -f ebin/*.app
	rebar3 compile
