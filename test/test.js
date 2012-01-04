var helenus = require('../'),
    pool = new helenus.ConnectionPool({
      hosts      : ['localhost:9160'],
      keyspace   : 'helenus_test',
      user       : 'test',
      password   : 'test1233',
      timeout    : 3000
    });

pool.on('error', function(err){
  throw(err);
});
    
//connect to the local machine
pool.connect(function(err, keyspace){
  if(err){
    throw(err);
  } else {    
    //test standard

    keyspace.standard_test.insert('foo-bar', { 'one':'1', 'two':2, 'three':3, 'four':4 }, {}, function(err){
      if(err){
        throw(err);
      }
      var cql = "SELECT %s FROM '%s' WHERE key='%s'",
          fields = "*",
          options = [fields, 'standard_test', 'key1'];
      
      pool.cql(cql, options, function(err, results){
        if(err){
          throw(err);
        }
        
        results[0].forEach(function(name, value, timestamp, ttl){
          console.log(name, value, timestamp, ttl);
        });
        
        pool.close();
      });
    });
  }
});