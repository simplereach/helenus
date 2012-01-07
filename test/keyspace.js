var sutd = require('./helpers/set_up_tear_down');

/**
 * Tests creating the keyspace
 */
exports['test create default keyspace'] = function(test, assert){
  var conn = sutd.connection;

  conn.createKeyspace('helenus_keyspace_test', function(err, response){
    assert.ifError(err);
    test.finish();
  });
};

/**
 * Tests dropping the keyspace
 */
exports['test drop keyspace'] = function(test, assert){
  var conn = sutd.connection;

  conn.dropKeyspace('helenus_keyspace_test', function(err, response){
    assert.ifError(err);
    test.finish();
  });
};

exports.setUp = sutd.setUp;
exports.tearDown = sutd.tearDown;