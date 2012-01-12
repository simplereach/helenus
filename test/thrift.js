var config = require('./helpers/thrift'),
    system = require('./helpers/connection'),
    Helenus, conn, ks, cf_standard;

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    conn = new Helenus.ConnectionPool(system);
    test.finish();
  },

  'test pool.connect':function(test, assert){
    conn.connect(function(err, keyspace){
      assert.ifError(err);
      assert.ok(keyspace.definition.name === 'system');
      test.finish();
    });
  },

  'test pool.createKeyspace':function(test, assert){
    conn.createKeyspace(config.keyspace, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test pool.use':function(test, assert){
    conn.use(config.keyspace, function(err, keyspace){
      assert.ifError(err);
      assert.ok(keyspace instanceof Helenus.Keyspace);
      ks = keyspace;
      test.finish();
    });
  },

  'test standard keyspace.createColumnFamily':function(test, assert){
    ks.createColumnFamily(config.cf_standard, config.cf_standard_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test standard keyspace.get':function(test, assert){
    ks.get(config.cf_standard, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      cf_standard = columnFamily;
      test.finish();
    });
  },

  'test standard cf.insert':function(test, assert){
    cf_standard.insert(config.standard_row_key, config.standard_insert_values, function(err, results){
      assert.ifError(err);
      test.finish();
    });
  },

  'test standard cf.get':function(test, assert){        
    cf_standard.get(config.standard_row_key, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.count === 4);
      assert.ok(row.key === config.standard_row_key);
      assert.ok(row.get('one').value === 'a');
      assert.ok(row.get('two').value === 'b');
      assert.ok(row.get('three').value === 'c');
      assert.ok(row.get('four').value === '');
      
      test.finish();
    });  
  },

  'test standard cf.get with options':function(test, assert){    
    cf_standard.get(config.standard_row_key, config.standard_get_options, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.count === 1);
      assert.ok(row.key === config.standard_row_key);
      assert.ok(row.get('one').value === 'a');
      
      test.finish();
    });
  },
  
  'test standard cf.get with error':function(test, assert){    
    cf_standard.get(config.standard_row_key, config.standard_get_options_error, function(err, row){
      assert.ok(err instanceof Error);
      assert.ok(err.name === 'HelenusInvalidRequestException');
      assert.ok(err.message === 'range finish must come after start in the order of traversal');
      test.finish();
    });
  },
  
  'test keyspace.dropColumnFamily':function(test, assert){
    ks.dropColumnFamily(config.cf_standard, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test pool.dropKeyspace':function(test, assert){
    conn.dropKeyspace(config.keyspace, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test pool.close':function(test, assert){
    assert.doesNotThrow(function(){ conn.close(); });
    test.finish();
  },

  'tearDown':function(test, assert){
    test.finish();
  }
};