var Helenus = require('helenus');

module.exports = function(poolConfig, callback){
  var conn = new Helenus.Connection(poolConfig);
  conn.connect(function(err){
    var canSelect = !(err && err.toString().indexOf('set_cql_version') !== -1);
    conn.on('close', function(){
      callback(canSelect);
    });
    conn.close();
  });
};
