var config = {
  host       : 'localhost:9160',
  keyspace   : 'system',
  user       : 'test',
  password   : 'test1233',
  timeout    : 3000
};

var helenus;

/**
 * Tests connecting to the server using the system keyspace
 * also ensures the connection is available for the other tests
 */
exports.setUp = function(test, assert){
  helenus = require('helenus');
  
  //export our connection for use in the other tests
  exports.connection = new helenus.ConnectionPool(config);
  
  //connect to the local machine
  exports.connection.connect(function(err, keyspace){
    assert.ifError(err);
    test.finish();
  });  
};


/**
 * Closes the connection and tests it doesn't throw.
 */
exports.tearDown = function(test, assert){  
  assert.doesNotThrow(function(){ exports.connection.close(); });
  test.finish();
};