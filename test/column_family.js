var sutd = require('./helpers/set_up_tear_down');

/**
 * Tests creating the the oclumn family
 */
exports['test create column family'] = function(test, assert){
  var conn = sutd.connection;

  test.finish();
};

exports.setUp = sutd.setUp;
exports.tearDown = sutd.tearDown;