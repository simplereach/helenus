var config = require('./helpers/pool'), Helenus, conn;

var ConnectionPoolTest = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    conn = new Helenus.ConnectionPool(config);
    test.finish();
  },
  
  'test connect':function(test, assert){
    conn.connect(function(err, keyspace){
      assert.ifError(err);
      assert.ok(keyspace.definition.name === 'system');
      test.finish();
    });
  },
  
  'test createKeyspace':function(test, assert){
    conn.createKeyspace('helenus_connection_test', function(err){
      assert.ifError(err);
      test.finish();
    });
  },
  
  'test dropKeyspace':function(test, assert){
    conn.dropKeyspace('helenus_connection_test', function(err){
      assert.ifError(err);
      test.finish();
    });
  },
  
  'test disconnect':function(test, assert){
    assert.doesNotThrow(function(){ conn.close(); });
    test.finish();
  },
  
  'tearDown':function(test, assert){
    test.finish();
  }
};
module.exports = ConnectionPoolTest;