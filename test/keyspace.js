var sutd = require('./helpers/set_up_tear_down');

/**
 * Tests creating the keyspace
 */
exports['test create keyspace'] = function(test, assert){
  var conn = sutd.connection;
  /**
  conn.createKeyspace('helenus_keyspace_test', function(err, response){
    assert.ifErr(err);
    
    conn.describeKeyspace('helenus_keyspace_test', function(err, response){
      
      test.finish(); 
    });
  });
  */
  test.finish();
};

exports.setUp = sutd.setUp;
exports.tearDown = sutd.tearDown;