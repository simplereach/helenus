var sysConfig = {
  host       : 'localhost:9160',
  keyspace   : 'system',
  user       : 'test',
  password   : 'test1233',
  timeout    : 3000
};

var config = {
  host       : 'localhost:9160',
  keyspace   : 'helenus_test',
  user       : 'test',
  password   : 'test1233',
  timeout    : 3000
};

var helenus, sysConnection;

/**
 * Tests connecting to the server using the system keyspace
 * also creates a test keyspace for use and a new connection to that keyspace
 * also ensures the connection is available for the other tests
 */
exports.setUp = function(test, assert){
  helenus = require('helenus');

  //export our connection for use in the other tests
  sysConnection = new helenus.ConnectionPool(sysConfig);
  //connect to the local machine
  sysConnection.connect(function(err, keyspace){
    assert.ifError(err);
    sysConnection.createKeyspace(config.keyspace, function(err, response){
      assert.ifError(err);
      exports.connection = new helenus.ConnectionPool(config);
      exports.connection.connect(function(err, keyspace){
        assert.ifError(err);
        test.finish();
      });
    });
  });
};

/**
 * Closes the connection and tests it doesn't throw.
 */
exports.tearDown = function(test, assert){
  assert.doesNotThrow(function(){ exports.connection.close(); });
  
  sysConnection.dropKeyspace(config.keyspace, function(err){
    assert.ifError(err);    
    assert.doesNotThrow(function(){ sysConnection.close(); });
    test.finish();
  });
};