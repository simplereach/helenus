/**
 * Basically here we are benchmarking helenus
 */
var Helenus = require('../');

/**
 * Some information about our tests
 * create column family bench_test with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
 */
var times = 100000, completed = 0, i = 0, vals,
    cqlInsert = "UPDATE bench_test USING CONSISTENCY ANY SET '%s'='%s' WHERE key='%s'";

/**
 * Create our helenus connection
 */    
var conn = new Helenus.Connection({
      host:'localhost:9160',
      keyspace:'helenus_test'
    });

/**
 * First bench helenus
 */
function bench(callback){
  conn.connect(function(err, keyspace){
    if(err){
      throw(err);
    }

    console.time('Helenus ' + times + ' writes');
    
    function cb(err, results){
      if(err){
        console.log('Error encountered at: ' + completed);
        throw(err);
      }
      completed += 1;
      
      if(completed === times){
        console.timeEnd('Helenus ' + times + ' writes')  ;
        conn.close();
        callback();
      }
    }
    
    /**
     * lets run a test for writes
     */
    for(; i < times; i += 1){
      vals = ['Column'+i, i.toString(16), (i % 100).toString(16)];
      conn.cql(cqlInsert, vals, { gzip:true }, cb);
    }
  }); 
}

bench(function(){
  console.log('Done');
});