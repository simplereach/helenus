var cassie = require('../'),
    pool = new cassie.ConnectionPool(['localhost:9160','localhost:9161']);

pool.on('error', function(err){
  console.log('ERROR:' + err);
});
    
//connect to the local machine
pool.connect('helenus_test', function(err, keyspace){
  if(err){
    throw(err);
  } else {    
    keyspace.test.insert('foo:bar', { 'hello:world':'cccc' }, {}, function(err, something){
      console.log(err, something);
    });
    
    keyspace.composite_test.insert('1325541869796:bar', { '1325541869796:world':132 }, {}, function(err, something){
      console.log(err, something);
    });
  }
});