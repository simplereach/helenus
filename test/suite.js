var config = {
  host       : 'localhost:9160',
  keyspace   : 'system',
  user       : 'test',
  password   : 'test1233',
  timeout    : 3000
};

var helenus, pool;    

/**
 * Tests connecting to the server using the system keyspace
 * also ensures the connection is available for the other tests
 */
exports.setUp = function(test, assert) {
  helenus = require('../');
  pool = new helenus.ConnectionPool(config);
  
  //connect to the local machine
  pool.connect(function(err, keyspace){
    assert.ifError(err, 'askdhgdfsj');
    test.finish();
  });
};

//exports['test create keyspace']

/**
 * Closes the connection and tests it doesn't throw.
 */
exports.tearDown = function(test, assert){  
  assert.doesNotThrow(function(){ pool.close(); });
  test.finish();
};