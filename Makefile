TEST_FILES=$(wildcard test/*.js)

test:
	NODE_PATH=lib/ node_modules/whiskey/bin/whiskey --real-time --scope-leaks --tests "$(TEST_FILES)"

cov:
	NODE_PATH=lib-cov/ node_modules/whiskey/bin/whiskey --coverage --tests "$(TEST_FILES)"

.PHONY: test cov