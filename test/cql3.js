var poolConfig = require('./helpers/connection'), Helenus, conn,
    config = require('./helpers/cql3');

// CQL3 introduces 4 different types of ColumnFamilies, see:
// https://issues.apache.org/jira/secure/attachment/12511286/create_cf_syntaxes.txt

function testCql(){
  var args = Array.prototype.slice.call(arguments);
  var tests = args.pop();
  return function(test, assert){
    args.push(function(err, res){
      assert.ifError(err);
      tests(test, assert, err, res);
      test.finish();
    });
    conn.cql.apply(conn, args);
  };
}

function testResultless(){
  var args = Array.prototype.slice.call(arguments);
  args.push(function(test, assert, err, res) {
    assert.ok(res === undefined);
  });
  return testCql.apply(testCql, args);
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

  'test cql static CF select':testCql(config['static_select#cql'], function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].get('foo').value === 'bar');
  }),

  'test cql static CF select with bad user input':testCql("SELECT foo FROM cql_test WHERE id='?'", ["'foobar"], function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].key === "'foobar");
    assert.ok(res[0].count === 0);
  }),

  'test cql static CF count':testCql(config['static_count#cql'], function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].get('count').value === 1);
  }),

  'test cql static CF error':function(test, assert){
    conn.cql(config['error#cql'], function(err, res){
      assert.ok(err instanceof Error);
      assert.ok(res === undefined);
      assert.ok(err.name === 'HelenusInvalidRequestException');
      assert.ok(err.message.length > 0);
      test.finish();
    });
  },

  'test cql static CF count with gzip':testCql(config['static_count#cql'], {gzip:true}, function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].get('count').value === 1);
  }),

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

  'test cql dynamic CF create column family':testResultless(config['dynamic_create_cf#cql']),
  'test cql dynamic CF update 1':testResultless(config['dynamic_update#cql'], config['dynamic_update#vals1']),
  'test cql dynamic CF update 2':testResultless(config['dynamic_update#cql'], config['dynamic_update#vals2']),
  'test cql dynamic CF update 3':testResultless(config['dynamic_update#cql'], config['dynamic_update#vals3']),
  'test cql dynamic CF select by row':testCql(config['dynamic_select1#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 2);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[1] instanceof Helenus.Row);
    assert.strictEqual(res[0].get('ts').value.getTime(), new Date('2012-03-01').getTime());
    assert.strictEqual(res[1].get('ts').value.getTime(), new Date('2012-03-02').getTime());
  }),
  'test cql dynamic CF by row and column':testCql(config['dynamic_select2#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('userid').value, 10);
    assert.strictEqual(res[0].get('url').value, 'www.foo.com');
    assert.strictEqual(res[0].get('ts').value.getTime(), new Date('2012-03-02').getTime());
  }),

  'test cql dense composite CF create column family':testResultless(config['dense_create_cf#cql']),
  'test cql dense composite CF update 1':testResultless(config['dense_update#cql'], config['dense_update#vals1']),
  'test cql dense composite CF update 2':testResultless(config['dense_update#cql'], config['dense_update#vals2']),
  'test cql dense composite CF update 3':testResultless(config['dense_update#cql'], config['dense_update#vals3']),
  'test cql dense composite CF select by row':testCql(config['dense_select1#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 2);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[1] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 2);
    assert.strictEqual(res[0].get('ts').value.getTime(), new Date('2012-03-02').getTime());
    assert.strictEqual(res[0].get('port').value, 1337);
    assert.strictEqual(res[1].length, 2);
    assert.strictEqual(res[1].get('ts').value.getTime(), new Date('2012-03-01').getTime());
    assert.strictEqual(res[1].get('port').value, 8080);
  }),
  'test cql dense composite CF by row and column':testCql(config['dense_select2#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 4);
    assert.strictEqual(res[0].get('userid').value, 10);
    assert.strictEqual(res[0].get('ip').value, '192.168.1.1');
    assert.strictEqual(res[0].get('port').value, 1337);
    assert.strictEqual(res[0].get('ts').value.getTime(), new Date('2012-03-02').getTime());
  }),

  'test cql sparse composite CF create column family':testResultless(config['sparse_create_cf#cql']),
  'test cql sparse composite CF update 1':testResultless(config['sparse_update#cql'], config['sparse_update#vals1']),
  'test cql sparse composite CF update 2':testResultless(config['sparse_update#cql'], config['sparse_update#vals2']),
  'test cql sparse composite CF update 3':testResultless(config['sparse_update#cql'], config['sparse_update#vals3']),
  'test cql sparse composite CF select by row':testCql(config['sparse_select1#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 2);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[1] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('posted_at').value.getTime(), new Date('2012-03-01').getTime());
    assert.strictEqual(res[0].get('body').value, 'body text 1');
    assert.strictEqual(res[0].get('posted_by').value, 'author1');
    assert.strictEqual(res[1].length, 3);
    assert.strictEqual(res[1].get('posted_at').value.getTime(), new Date('2012-03-02').getTime());
    assert.strictEqual(res[1].get('body').value, 'body text 3');
    assert.strictEqual(res[1].get('posted_by').value, 'author3');
  }),
  'test cql sparse composite CF by row and column':testCql(config['sparse_select2#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 2);
    assert.strictEqual(res[0].get('body').value, 'body text 3');
    assert.strictEqual(res[0].get('posted_by').value, 'author3');
  }),

  'test cql drop keyspace':testResultless(config['drop_ks#cql']),

  'tearDown':function(test, assert){
    conn.close();
    test.finish();
  }
};
