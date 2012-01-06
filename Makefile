NODE_PATH="./node_modules/whiskey/bin/"

test:
	whiskey --real-time --scope-leaks --tests test/suite.js

cov:
	node_modules/whiskey/bin/whiskey --coverage --tests test/suite.js

.PHONY: test cov