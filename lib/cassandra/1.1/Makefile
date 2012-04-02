SED=sed "s/require('thrift')/require('helenus-thrift')/"

compile: gen-nodejs Cassandra.js cassandra_types.js clean_compile

gen-nodejs:
	thrift --gen js:node cassandra.thrift

cassandra_types.js:
	$(SED) gen-nodejs/cassandra_types.js > cassandra_types.js

Cassandra.js:
	$(SED) gen-nodejs/Cassandra.js > Cassandra.js

clean_compile:
	rm -rf gen-nodejs

clean: clean_compile
	rm Cassandra.js cassandra_types.js

.PHONY: clean
