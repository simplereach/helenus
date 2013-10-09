var poolConfig = require('./helpers/connection'),
    badConfig = require('./helpers/bad_connection'), Helenus, conn,
    config = require('./helpers/cql2'),
    canSelectCqlVersion = require('./helpers/can_select_cql_version');

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    poolConfig.cqlVersion = '2.0.0';

    function connect(){
      conn = new Helenus.ConnectionPool(poolConfig);
      conn.connect(function(err){
        assert.ifError(err);
        test.finish();
      });
    }

    canSelectCqlVersion(poolConfig, function(canSelect){
      if (!canSelect){
        console.error('set_cql_version not supported, unsetting cqlVersion');
        delete poolConfig.cqlVersion;
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
        badConn.cql(config['create_ks#cql'], function(err, res){
           assert.isDefined(err);
           badConn.close();
           test.finish();
         });
     });
  },

  'test cql create keyspace':function(test, assert){
     conn.cql(config['create_ks#cql'], function(err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql use keyspace':function(test, assert){
     conn.cql(config['use#cql'], function(err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql create column family':function(test, assert){
     conn.cql(config['create_cf#cql'], function(err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql create counter column family':function(test, assert){
     conn.cql(config['create_counter_cf#cql'], function (err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql create reversed column family':function(test, assert){
     conn.cql(config['create_reversed_cf#cql'], function (err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql create uuid column family':function(test, assert){
     conn.cql(config['create_uuid_cf#cql'], function (err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
     });
  },

  'test cql update':function(test, assert){
    conn.cql(config['update#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);
      test.finish();
    });
  },

  'test cql update with boolean value':function(test, assert){
    conn.cql(config['update_uuid#cql'], [true, 'bb85f040-30c3-11e3-aa6e-0800200c9a66'], function(err, res){
      assert.ifError(err);
      conn.cql(config['select_uuid#cql'], ['bb85f040-30c3-11e3-aa6e-0800200c9a66'], function(err, res){
        assert.ifError(err);
        assert.ok(res[0].get('body').value === 'true');
        test.finish();
      });
    });
  },

  'test cql update reversed':function(test, assert){
    conn.cql(config['update_reversed#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);
      test.finish();
    });
  },

  'test cql update with no callback':function(test, assert){
    conn.cql(config['update#cql']);

    //just wait to see if anything bad happens
    setTimeout(function(){
      test.finish();
    }, 100);
  },

  'test cql incr':function(test, assert){
    conn.cql(config['incr#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);
      test.finish();
    });
  },

  'test cql select':function(test, assert){
    conn.cql(config['select#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('foo').value === 'bar');
      test.finish();
    });
  },

  'test cql select reversed':function(test, assert){
    conn.cql(config['select_reversed#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('foo').value === 'bar');

      test.finish();
    });
  },

  'test cql select *': function(test, assert){
    conn.cql(config['select*#cql'], function (err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('foo').value === 'bar');
      test.finish();
    });
  },

  'test cql select counter':function(test, assert){
    conn.cql(config['select_counter#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('foo').value === 10);
      test.finish();
    });
  },

  'test cql incr and select':function(test, assert){
    conn.cql(config['incr#cql'], function(err, res){
      assert.ifError(err);
      conn.cql(config['select_counter#cql'], function(err, res){
        assert.ifError(err);
        assert.ok(res.length === 1);
        assert.ok(res[0] instanceof Helenus.Row);
        assert.ok(res[0].get('foo').value === 20);
        test.finish();
      });
    });
  },

  'test cql select with bad user input':function(test, assert){
    var select = "SELECT foo FROM cql_test WHERE KEY='?'";

    conn.cql(select, ["'foobar"], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].key === "'foobar");
      assert.ok(res[0].count === 0);
      test.finish();
    });
  },

  'test cql count':function(test, assert){
    conn.cql(config['count#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('count').value === 1);
      test.finish();
    });
  },

  'test cql error':function(test, assert){
    conn.cql(config['error#cql'], function(err, res){
      assert.ok(err instanceof Error);
      assert.ok(res === undefined);
      assert.ok(err.name === 'HelenusInvalidRequestException');
      assert.ok(err.message.length > 0);
      test.finish();
    });
  },

  'test cql count with gzip':function(test, assert){
    conn.cql(config['count#cql'], {gzip:true}, function(err, res){
      assert.ifError(err);
      assert.ok(res.length === 1);
      assert.ok(res[0] instanceof Helenus.Row);
      assert.ok(res[0].get('count').value === 1);
      test.finish();
    });
  },

  'test cql delete':function(test, assert){
    conn.cql(config['delete#cql'], function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);
      //after the delete check that all the columns have been deleted,
      //we cant use a count here because the row will still remain until compaction occurs
      //see http://www.datastax.com/docs/1.0/dml/about_writes#about-deletes
      conn.cql(config['select2#cql'], config['select2#vals'], function(err, res){
        assert.ifError(err);
        assert.ok(res.length === 1);
        assert.ok(res[0] instanceof Helenus.Row);
        assert.ok(res[0].count === 0);
        test.finish();
      });
    });
  },

  'test cql uuid key decode':function(test, assert){
    conn.cql(config['update_uuid#cql'], ["body", "07ad2230-ee44-11e2-91e2-0800200c9a66"],function(err, res){
      assert.ifError(err);
      assert.ok(res === undefined);

      conn.cql(config['select_uuid#cql'], ["07ad2230-ee44-11e2-91e2-0800200c9a66"], function(err, res){
        assert.ifError(err);
        assert.ok(res.length === 1);
        assert.ok(res[0] instanceof Helenus.Row);
        assert.ok(res[0].key.toString() === "07ad2230-ee44-11e2-91e2-0800200c9a66");
        test.finish();
      });
    });
  },

  'test cql drop column family':function(test, assert){
    conn.cql(config['drop_cf#cql'], function(err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
    });
  },

  'test cql drop keyspace':function(test, assert){
    conn.cql(config['drop_ks#cql'], function(err, res){
       assert.ifError(err);
       assert.ok(res === undefined);
       test.finish();
    });
  },

  'test too many cql params':function(test, assert){
    assert.throws(function(){
      conn.cql(config['select2#cql'], [1,2,3,4,5,6]);
    }, function(err){
      return err.message === 'Too Many Parameters Given';
    });

    test.finish();
  },

  'test too few cql params':function(test, assert){
    assert.throws(function(){
      conn.cql(config['select2#cql'], []);
    }, function(err){
      return err.message === 'Too Few Parameters Given';
    });

    test.finish();
  },

  'tearDown':function(test, assert){
    conn.close();
    test.finish();
  }
};