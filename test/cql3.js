var poolConfig = require('./helpers/connection'), Helenus, conn,
    config = require('./helpers/cql3');

// CQL3 introduces 4 different types of ColumnFamilies, see:
// https://issues.apache.org/jira/secure/attachment/12511286/create_cf_syntaxes.txt

function testResultless(query){
  return function(test, assert){
    conn.cql(query, function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);
      test.finish();
    });
  };
}

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    poolConfig.cqlVersion = '3.0.0';
    conn = new Helenus.ConnectionPool(poolConfig);

    conn.connect(function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test cql create keyspace':testResultless(config['create_ks#cql']),
  'test cql use keyspace':testResultless(config['use#cql']),

  'test cql static CF create column family':testResultless(config['static_create_cf#cql']),
  'test cql static CF update':testResultless(config['static_update#cql']),
  'test cql static CF update with no callback':function(test, assert){
    conn.cql(config['static_update#cql']);

    //just wait to see if anything bad happens
    setTimeout(function(){
      test.finish();
    }, 100);
  },

  'test cql static CF select':function(test, assert){
    conn.cql(config['static_select#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('foo').value === 'bar');
      test.finish();
    });
  },

  'test cql static CF select with bad user input':function(test, assert){
    var select = "SELECT foo FROM cql_test WHERE id='?'";

    conn.cql(select, ["'foobar"], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].key === "'foobar");
      assert.ok(res[0].count === 0);
      test.finish();
    });
  },

  'test cql static CF count':function(test, assert){
    conn.cql(config['static_count#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('count').value === 1);
      test.finish();
    });
  },

  'test cql static CF error':function(test, assert){
    conn.cql(config['error#cql'], function(err, res){
      assert.ok(err instanceof Error);
      assert.ok(res === undefined);
      assert.ok(err.name === 'HelenusInvalidRequestException');
      assert.ok(err.message.length > 0);
      test.finish();
    });
  },

  'test cql static CF count with gzip':function(test, assert){
    conn.cql(config['static_count#cql'], {gzip:true}, function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('count').value === 1);
      test.finish();
    });
  },

  'test cql static CF delete':function(test, assert){
    conn.cql(config['static_delete#cql'], function(err, res){
      assert.ifError(err);
      assert.strictEqual(res, undefined);
      // after the delete check that all the columns have been deleted,
      // we cant use a count here because the row will still remain until compaction occurs
      // see http://www.datastax.com/docs/1.0/dml/about_writes#about-deletes
      //
      // Since CQL 3.0.0 not only row keys remain as ghosts, but since they
      // are named now and treated the same way as column keys, also column
      // keys remain as ghosts. We can only check that they are all null.
      // More specifically: We're expecting that there's a column 'id' with
      // the value 'foobar' (that's the row ghost) and a column 'foo' with
      // the value null (that's the column ghost).
      conn.cql(config['static_select2#cql'], config['static_select2#vals'], function(err, res){
        assert.ifError(err);
        assert.strictEqual(res.length, 1);
        var row = res[0];
        assert.ok(row instanceof Helenus.Row);
        assert.strictEqual(row.count, 2);
        assert.strictEqual(row.get('id').value, 'foobar');
        assert.strictEqual(row.get('foo').value, null);
        test.finish();
      });
    });
  },

  'test cql static CF drop static column family':testResultless(config['static_drop_cf#cql']),

  'test cql drop keyspace':testResultless(config['drop_ks#cql']),

  'tearDown':function(test, assert){
    conn.close();
    test.finish();
  }
};
