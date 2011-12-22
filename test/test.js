var cassie = require('../'),
    pool = new cassie.ConnectionPool(['localhost:9160','localhost:9161']);

pool.on('error', function(err){
  console.log('ERROR:' + err);
});
    
//connect to the local machine
pool.connect('node_cassandra_test', function(err, keyspace){
  if(err){
    throw(err);
  } else {    
    keyspace.composite_test.insert('abcd', { '\x00\x05hello\x00\x00\x05world\x00':'cccc' }, {}, function(err, something){
      console.log(err, something);
    });
    
    //keyspace.standard_test.set('abcd', { 'twiszzl':132 }, {}, function(err, something){
    //  console.log(err, something);
    //});
  }
});