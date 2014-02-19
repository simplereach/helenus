var poolConfig = require('./helpers/connection'),
    badConfig = require('./helpers/bad_connection'), Helenus, conn,
    config = require('./helpers/cql3'),
    canSelectCqlVersion = require('./helpers/can_select_cql_version'),
    createKeySpace;

// CQL3 introduces 4 different types of ColumnFamilies, see:
// https://issues.apache.org/jira/secure/attachment/12511286/create_cf_syntaxes.txt

function testCqlFail() {
  var args = Array.prototype.slice.call(arguments);

  return function(test, assert){
    args.push(function(err, res){
      assert.isDefined(err);
      test.finish();
    });

    conn.cql.apply(conn, args);
  };
}

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

function testCreateKeyspace(){
  var args = Array.prototype.slice.call(arguments);
  var tests = function(test, assert, err, res) {
    assert.ok(res === undefined);
  };

  return function(test, assert){
    if(conn.clients[0].version[0] === '19' && conn.clients[0].version[1] < '34'){
      args.push(config['create_ks#cql_v1']);
    } else {
      args.push(config['create_ks#cql']);
    }

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

function makeDates(dates_array){
  var ret = [], i = 0, len = dates_array.length;
  for(i;i<len;i++){
    ret.push(new Date(dates_array[i]));
  }
  return ret;
}

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    poolConfig.cqlVersion = '3.0.0';

    function connect(){
      conn = new Helenus.ConnectionPool(poolConfig);
      conn.connect(function(err){
        assert.ifError(err);
        test.finish();
      });
    }

    canSelectCqlVersion(poolConfig, function(canSelect){
      if (!canSelect){
        console.error('set_cql_version not supported, skipping CQL3 tests');
        process.exit();
      }
      connect();
    });
  },

  'test a bad connection will return an error':function(test, assert){
     var badConn = new Helenus.ConnectionPool(badConfig);
     // Add error handler to avoid uncaught exception.
     badConn.on('error', function (err) { assert.isDefined(err); });
     badConn.connect(function(err) {
        assert.isDefined(err);

        badConn.cql(createKeySpace, function(err, res){
           assert.isDefined(err);
           badConn.close();
           test.finish();
         });
     });
  },

  'test cql create keyspace':testCreateKeyspace(),
  'test cql use keyspace':testResultless(config['use#cql']),

  'test cql static CF create column family':testResultless(config['static_create_cf#cql']),
  'test cql static count CF create column family':testResultless(config['static_create_cnt_cf#cql']),
  'test cql static CF update':testResultless(config['static_update#cql']),
  'test cql static CF update with no callback':function(test, assert){
    conn.cql(config['static_update#cql']);

    //just wait to see if anything bad happens
    setTimeout(function(){
      test.finish();
    }, 100);
  },

  'test cql static counter CF update':testResultless(config['static_update_cnt#cql']),

  'test cql static CF select':testCql(config['static_select#cql'], function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].get('foo').value === 'bar');
  }),

  // using consistency quorum should fail if query-level consistency is working, as we test against a single node
  // with RF=3
  'test cql static CF select with consistency': testCqlFail(config['static_select#cql'], {'consistencyLevel': 2}),

  'test cql static CF select *':testCql(config['static_select*#cql'], function(test, assert, err, res){
    assert.ok(res.length === 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.ok(res[0].get('foo').value === 'bar');
  }),

  'test cql static counter CF select':testCql(config['static_select_cnt#cql'], function(test, assert, err, res){
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('cnt').value === 10);
  }),

  'test cql static counter CF incr and select':function(test, assert){
    conn.cql(config['static_update_cnt#cql'], function(err, res){
      assert.ifError(err);

      conn.cql(config['static_select_cnt#cql'], function(err, res){
        assert.ifError(err);
        assert.ok(res.length === 1);
        assert.ok(res[0] instanceof Helenus.Row);
        assert.ok(res[0].get('cnt').value === 20);
        test.finish();
      });
    });
  },

  'test cql static CF select with bad user input':testCql("SELECT foo FROM cql_test WHERE id='?'", ["'foobar"], function(test, assert, err, res){
    assert.ok(res.length === 0);
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
      //this test will fail on C* 1.1.0 as they returned ghosts in the CQL query
      //As of 1.1.1 the ghosts no longer exist
      conn.cql(config['static_select2#cql'], config['static_select2#vals'], function(err, res){
        assert.ifError(err);
        assert.ok(res.length === 0);
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
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('posted_at').value.getTime(), new Date('2012-03-02').getTime());
    assert.strictEqual(res[0].get('body').value, 'body text 3');
    assert.strictEqual(res[0].get('posted_by').value, 'author3');
  }),
  'test cql sparse composite CF by row and column':testCql(config['sparse_select2#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 2);
    assert.strictEqual(res[0].get('body').value, 'body text 1');
    assert.strictEqual(res[0].get('posted_by').value, 'author1');
  }),
  
  'test cql integers CF create column family':testResultless(config['integers_create_cf#cql']),
  'test cql integers CF update 1':testResultless(config['integers_update#cql'], config['integers_update#vals1']),
  'test cql integers CF update 2':testResultless(config['integers_update#cql'], config['integers_update#vals2']),
  'test cql integers CF update 3':testResultless(config['integers_update#cql'], config['integers_update#vals3']),
  'test cql integers CF select positive numbers':testCql(config['integers_select1#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('number').value, 1);
    assert.strictEqual(res[0].get('longnumber').value, 25);
    assert.strictEqual(res[0].get('varnumber').value, 36);
  }),
  'test cql integers CF select negative numbers':testCql(config['integers_select2#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('number').value, -1);
    assert.strictEqual(res[0].get('longnumber').value, -25);
    assert.strictEqual(res[0].get('varnumber').value, -36);
  }),
  'test cql integers CF select negative numbers with 3 byte varint':testCql(config['integers_select3#cql'], function(test, assert, err, res){
    assert.strictEqual(res.length, 1);
    assert.ok(res[0] instanceof Helenus.Row);
    assert.strictEqual(res[0].length, 3);
    assert.strictEqual(res[0].get('number').value, -2);
    assert.strictEqual(res[0].get('longnumber').value, -25);
    assert.strictEqual(res[0].get('varnumber').value, -8388607);//test a 3 byte-long variable integer
  }),

  'test cql timestamp create table':testResultless(config['timestamp_create']),
  'test cql timestamp update1':testResultless(config['timestamp_update#cql'], makeDates(config['timestamp_update#vals1'])),
  'test cql timestamp update2':testResultless(config['timestamp_update#cql'], makeDates(config['timestamp_update#vals2'])),
  'test cql timestamp update3':testResultless(config['timestamp_update#cql'], makeDates(config['timestamp_update#vals3'])),
  'test cql timestamp invalid date': function(test, assert) {
    assert.throws(function(){
      conn.cql(config['timestamp_invalid#cql'], makeDates(config['timestamp_invalid#vals']))
    }, Error)
    test.finish();
  },

  'test cql drop keyspace':testResultless(config['drop_ks#cql']),

  'tearDown':function(test, assert){
    conn.close();
    test.finish();
  }
};
