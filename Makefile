TEST_FILES=$(wildcard test/*.js)

test:
	NODE_PATH=lib/ node_modules/whiskey/bin/whiskey --real-time --scope-leaks --tests "$(TEST_FILES)"

test-cov:
	NODE_PATH=lib-cov/ node_modules/whiskey/bin/whiskey --coverage --tests "$(TEST_FILES)"

doc:
	rm -rf ./doc && node_modules/JSDoc/jsdoc -p -r ./lib -d ./doc

.PHONY: test test-cov doc