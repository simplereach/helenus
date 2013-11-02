var config = require('./helpers/thrift'),
    system = require('./helpers/connection'),
    badSystem = require('./helpers/bad_connection'),
    Helenus, conn, ks, cf_standard, row_standard, cf_composite, cf_counter,
    cf_reversed, cf_composite_nested_reversed;

var has_microtime = false;
try {
  require('microtime');
  has_microtime = true;
} catch(e) { }

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    test.finish();
  },

  'test pool.connect without keyspace':function(test, assert){
    conn = new Helenus.ConnectionPool(system);
    conn.connect(function(err, keyspace){
      assert.ifError(err);
      assert.strictEqual(keyspace, undefined);
      test.finish();
    });
  },

  'test pool.close without keyspace':function(test, assert){
    conn.on('close', function(){
      test.finish();
    });
    assert.doesNotThrow(function(){
      conn.close();
    });
  },

  'test pool.connect with keyspace':function(test, assert){
    system.keyspace = 'system';
    conn = new Helenus.ConnectionPool(system);
    conn.connect(function(err, keyspace){
      assert.ifError(err);
      assert.ok(keyspace.definition.name === 'system');
      test.finish();
    });
  },

  'test bad pool connect':function(test, assert) {
    var badConn = new Helenus.ConnectionPool(badSystem);
    // Add error handler to avoid uncaught exception.
    badConn.on('error', function (err) { assert.isDefined(err); });
    badConn.connect(function(err, keyspace) {
      assert.isDefined(err);
      badConn.createKeyspace(config.keyspace, function(err){
        assert.isDefined(err);
        badConn.dropKeyspace(config.keyspace, function(err){
          assert.isDefined(err);
          badConn.close();
          test.finish();
        });
      });
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

  'test keyspace.createColumnFamily':function(test, assert){
    ks.createColumnFamily(config.cf_standard, config.cf_standard_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.createColumnFamily with composite type':function(test, assert){
    ks.createColumnFamily(config.cf_standard_composite, config.cf_standard_composite_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.createSuperColumnFamily':function(test, assert){
    ks.createColumnFamily(config.cf_supercolumn, config.cf_supercolumn_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.createCounterFamily':function(test, assert){
    ks.createColumnFamily(config.cf_counter, config.cf_counter_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.createColumFamily reversed':function(test, assert){
    ks.createColumnFamily(config.cf_reversed, config.cf_reversed_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.createColumFamily compositeNestedReversed':function(test, assert){
    ks.createColumnFamily(config.cf_composite_nested_reversed, config.cf_composite_nested_reversed_options, function(err){
      assert.ifError(err);
      test.finish();
    });
  },

  'test keyspace.get':function(test, assert){
    ks.get(config.cf_standard, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      assert.ok(columnFamily.isSuper === false);
      test.finish();
    });
  },

  'test keyspace.get composite':function(test, assert){
    ks.get(config.cf_standard_composite, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      cf_composite = columnFamily;
      test.finish();
    });
  },

  'test keyspace.get supercolumn':function(test, assert){
    ks.get(config.cf_supercolumn, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      assert.ok(columnFamily.isSuper === true);
      test.finish();
    });
  },

  'test keyspace.get counter':function(test, assert){
    ks.get(config.cf_counter, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      assert.ok(columnFamily.isCounter === true);
      cf_counter = columnFamily;
      test.finish();
    });
  },

  'test keyspace.get from cache':function(test, assert){
    ks.get(config.cf_standard, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      cf_standard = columnFamily;
      test.finish();
    });
  },

  'test keyspace.get from index':function(test, assert){
    ks.get(config.cf_standard, function(err, columnFamily){
      assert.ifError(err);
      assert.ok(columnFamily instanceof Helenus.ColumnFamily);
      cf_standard = columnFamily;
      test.finish();
    });
  },

  'test keyspace.get invalid cf':function(test, assert){
    ks.get(config.cf_invalid, function(err, columnFamily){
      assert.ok(err instanceof Error);
      assert.ok(err.name === 'HelenusNotFoundError');
      assert.ok(err.message === 'ColumnFamily cf_invalid_test Not Found');
      test.finish();
    });
  },

  'test standard cf.insert':function(test, assert){
    cf_standard.insert(config.standard_row_key, config.standard_insert_values, function(err, results){
      assert.ifError(err);
      test.finish();
    });
  },

  'test standard cf.insert with custom CL':function(test, assert){
    cf_standard.insert(config.standard_row_key, config.standard_insert_values,
                      { consistency : Helenus.ConsistencyLevel.ANY }, function(err, results){
      assert.ifError(err);
      test.finish();
    });
  },

  'test standard cf.insert into composite cf':function(test, assert){
    var values = [
      new Helenus.Column([12345678912345, new Date(1326400762701)], 'some value')
    ],
    key = [ 'åbcd', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ];

    cf_composite.setColumnValidator(values[0].name, 'UTF8Type');

    cf_composite.insert(key, values, function(err, results){
      assert.ifError(err);
      test.finish();
    });
  },

  'test cf.insert default microsecond timestamp':function(test, assert){
    if (!has_microtime) {
      test.finish();
      return;
    }

    //try to tease out same-ms collision with 50 attempts
    var finished = 0, ok = true;
    var callback = function() {
      finished++;
      if (finished % 2 == 0) {
        cf_standard.get(config.standard_row_key, function(err, row){
          assert.ifError(err);

          ok = ok && (row.get("one").value === "a");

          if (finished == 100) {
            assert.ok(ok);
            assert.ifError(err);
            test.finish();
          } else {
            setTimeout(try_race, 0);
          }
        });
      }
    };

    var try_race = function() {
      cf_standard.insert(config.standard_row_key, {"one": "b"}, function(err, results){
        assert.ifError(err);
        callback();
      });

      cf_standard.insert(config.standard_row_key, {"one": "a"}, function(err, results){
        assert.ifError(err);
        callback();
      });
    };

    try_race();
  },

  'test counter cf.incr':function(test, assert){
    var key = 'åbcd';

    cf_counter.incr(key, {'1234': 1337, 'count2': 2}, function (err, results){
      assert.ifError(err);
      cf_counter.incr(key, {'count2': 1}, function (err, results){
        assert.ifError(err);
        test.finish();
      });
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

      var ts = new Date().getTime();
      //ensure the timestamp is within 1 second
      //test to ensure issue #4 doesn't happen again

      assert.ok(row.get('one').timestamp <= ts);
      assert.ok(row.get('one').timestamp >= ts - 1000);

      row_standard = row;
      test.finish();
    });
  },

  'test standard cf.get for composite column family':function(test, assert){
    var key = [ 'åbcd', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ];

    cf_composite.get(key, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.get([12345678912345, new Date(1326400762701)]).value === 'some value');


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

  'test standard cf.get count':function(test, assert){
    cf_standard.count(config.standard_row_key, config.standard_get_options, function(err, val){
      assert.ifError(err);
      assert.ok(typeof val === 'number');
      assert.ok(val === 1);

      test.finish();
    });
  },


  'test standard cf with composite column slice':function(test, assert){
    var values = [
        new Helenus.Column([1, new Date(1)], 'a'),
        new Helenus.Column([2, new Date(2)], 'b'),
        new Helenus.Column([3, new Date(3)], 'c'),
        new Helenus.Column([4, new Date(4)], 'd'),
        new Helenus.Column([5, new Date(5)], 'e'),
        new Helenus.Column([6, new Date(6)], 'f'),
        new Helenus.Column([7, new Date(7)], 'g')
      ],
      key = [ 'comp_range_1', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ];

    cf_composite.insert(key, values, function(err){
      assert.ifError(err);
      var options = {
        start: [3],
        end: [5]
      };

      cf_composite.get(key, options, function(err, row){
        assert.ifError(err);
        assert.ok(row.count === 3);
        assert.ok(row[0].name[0] === 3);
        assert.ok(row[1].name[0] === 4);
        assert.ok(row[2].name[0] === 5);
        test.finish();
      });
    });
  },

  'test standard cf with exclusive composite column slice':function(test, assert){
    var values = [
        new Helenus.Column([1, new Date(1)], 'a'),
        new Helenus.Column([2, new Date(2)], 'b'),
        new Helenus.Column([3, new Date(3)], 'c'),
        new Helenus.Column([4, new Date(4)], 'd'),
        new Helenus.Column([5, new Date(5)], 'e'),
        new Helenus.Column([6, new Date(6)], 'f'),
        new Helenus.Column([7, new Date(7)], 'g')
      ],
      key = [ 'comp_range_1', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ];

    cf_composite.insert(key, values, function(err){
      assert.ifError(err);
      var options = {
        start: [[3, false]],
        end: [[5, false]]
      };

      cf_composite.get(key, options, function(err, row){
        assert.ifError(err);
        assert.ok(row.count === 1);
        assert.ok(row[0].name[0] === 4);
        test.finish();
      });
    });
  },

  'test standard cf.get with custom CL':function(test, assert){
    cf_standard.get(config.standard_row_key,
                    { consistency : Helenus.ConsistencyLevel.ONE }, function(err, row){

      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.key === config.standard_row_key);
      assert.ok(row.get('one').value === 'a');

      test.finish();
    });
  },

  'test standard cf.get with columns names':function(test, assert){
    cf_standard.get(config.standard_row_key, config.standard_get_names_options, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.count === 2);
      assert.ok(row.key === config.standard_row_key);
      assert.ok(row.get('one').value === 'a');
      assert.ok(row.get('three').value === 'c');
      test.finish();
    });
  },

  'test composite cf.get with columns names':function(test, assert){
    var key = [ 'åbcd', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ],
        cols = [[12345678912345, new Date(1326400762701)]];

    cf_composite.get(key, {columns: cols}, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.get([12345678912345, new Date(1326400762701)]).value === 'some value');
      test.finish();
    });
  },

  'test counter cf.get with columns names':function(test, assert){
    var key  = 'åbcd',
        cols = ['1234', 'count2'];

    cf_counter.get(key, {columns : cols}, function(err, row){
      assert.ifError(err);
      assert.ok(row instanceof Helenus.Row);
      assert.ok(row.get('1234').value === 1337);
      assert.ok(row.get('count2').value === 3);
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

  'test standard cf with Index':function(test, assert){
    var key = config.standard_row_key + '-utf8',
        query = { fields: [{ column:'index-test', operator:'EQ', value:'åbcd'}] },
        opts = { 'index-test' : 'åbcd' };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.getIndexed(query, function(err, rows){
        assert.ifError(err);
        assert.ok(Array.isArray(rows));
        assert.ok(rows.length === 1);
        var col = rows[0].get('index-test');
        assert.ok(typeof col.value === 'string');
        assert.ok(col.value === 'åbcd');
        test.finish();
      });
    });
  },

  'test standard cf with BytestType':function(test, assert){
    var key = config.standard_row_key + '-bytes',
        opts = { 'bytes-test' : new Buffer([0, 0xFF, 0x20]) };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('bytes-test');
        assert.ok(col.value instanceof Buffer);
        assert.ok(col.value.toString('hex') === opts['bytes-test'].toString('hex'));
        test.finish();
      });
    });
  },

  'test standard cf with LongType':function(test, assert){
    var key = config.standard_row_key + '-long',
        opts = { 'long-test' : 123456789012345 };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('long-test');
        assert.ok(typeof col.value === 'number');
        assert.ok(col.value === 123456789012345);
        test.finish();
      });
    });
  },

  'test standard cf with IntegerType':function(test, assert){
    var key = config.standard_row_key + '-integer',
        opts = { 'integer-test' : 1234 };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('integer-test');
        assert.ok(typeof col.value === 'number');
        assert.ok(col.value === 1234);
        test.finish();
      });
    });
  },

  'test standard cf with UTF8Type':function(test, assert){
    var key = config.standard_row_key + '-utf8',
        opts = { 'utf8-test' : 'åbcd' };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('utf8-test');
        assert.ok(typeof col.value === 'string');
        assert.ok(col.value === 'åbcd');
        test.finish();
      });
    });
  },

  'test standard cf with AsciiType':function(test, assert){
    var key = config.standard_row_key + '-ascii',
        opts = { 'ascii-test' : 'abcd' };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('ascii-test');
        assert.ok(typeof col.value === 'string');
        assert.ok(col.value === 'abcd');
        test.finish();
      });
    });
  },

  'test standard cf with LexicalUUIDType':function(test, assert){
    var key = config.standard_row_key + '-lexicaluuid',
        opts = { 'lexicaluuid-test' : new Helenus.UUID() };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('lexicaluuid-test');
        assert.ok(col.value instanceof Helenus.UUID);
        assert.ok(col.value.hex.length === 36);
        test.finish();
      });
    });
  },

  'test standard cf with TimeUUIDType':function(test, assert){
    var key = config.standard_row_key + '-timeuuid',
        opts = { 'timeuuid-test' : new Helenus.TimeUUID() };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('timeuuid-test');
        assert.ok(col.value instanceof Helenus.TimeUUID);
        assert.ok(col.value.hex.length === 36);
        test.finish();
      });
    });
  },

  'test standard cf with FloatType':function(test, assert){
    var key = config.standard_row_key + '-float',
        opts = { 'float-test' : 1234.1234130859375 };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('float-test');
        assert.ok(typeof col.value === 'number');
        assert.ok(col.value === 1234.1234130859375);
        test.finish();
      });
    });
  },

  'test standard cf with DoubleType':function(test, assert){
    var key = config.standard_row_key + '-double',
        opts = { 'double-test' : 123456789012345.1234 };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('double-test');
        assert.ok(typeof col.value === 'number');
        assert.ok(col.value === 123456789012345.1234);
        test.finish();
      });
    });
  },

  'test standard cf with DateType':function(test, assert){
    var key = config.standard_row_key + '-date',
        opts = { 'date-test' : new Date(1326400762701) };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('date-test');
        assert.ok(col.value instanceof Date);
        assert.ok(col.value.getTime() === 1326400762701);
        test.finish();
      });
    });
  },

  'test standard cf with BooleanType':function(test, assert){
    var key = config.standard_row_key + '-boolean',
        opts = { 'boolean-test' : true };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('boolean-test');
        assert.ok(typeof col.value === 'boolean');
        assert.ok(col.value === true);
        test.finish();
      });
    });
  },

  'test standard cf with UUIDType':function(test, assert){
    var key = config.standard_row_key + '-lexicaluuid',
        opts = { 'uuid-test' : new Helenus.UUID() };

    cf_standard.insert(key, opts, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        var col = row.get('uuid-test');
        assert.ok(col.value instanceof Helenus.UUID);
        assert.ok(col.value.hex.length === 36);
        test.finish();
      });
    });
  },

  'test row.nameSlice':function(test, assert){
    var row = row_standard.nameSlice('a','s');
    assert.ok(row instanceof Helenus.Row);
    assert.ok(row.count === 2);
    assert.ok(row.key === config.standard_row_key);
    assert.ok(row.get('one').value === 'a');
    assert.ok(row.get('four').value === '');
    test.finish();
  },

  'test row.slice':function(test, assert){
    var row = row_standard.slice(1, 3);
    assert.ok(row instanceof Helenus.Row);
    assert.ok(row.count === 2);
    assert.ok(row.key === config.standard_row_key);
    assert.ok(row.get('one').value === 'a');
    assert.ok(row.get('three').value === 'c');
    test.finish();
  },

  'test row.toString and row.inspect':function(test, assert){
    var str = row_standard.toString();
    assert.ok(typeof str === 'string');
    assert.ok(str === "<Row: Key: 'standard_row_1', ColumnCount: 4, Columns: [ 'four,one,three,two' ]>");
    test.finish();
  },

  'test row.forEach':function(test, assert){
    var i = 0, vals = {
      '0': { name:'four', value:'' },
      '1': { name:'one',  value:'a' },
      '2': { name:'three',value:'c' },
      '3': { name:'two',  value:'b' }
    };

    row_standard.forEach(function(name, value, timestamp, ttl){
      assert.ok(vals[i].name === name);
      assert.ok(vals[i].value === value);
      assert.ok(timestamp instanceof Date);
      assert.ok(ttl === null);
      i += 1;
    });

    test.finish();
  },

  'test standard cf remove column':function(test, assert) {
    cf_standard.remove(config.standard_row_key, 'one', {timestamp:new Date()}, function(err){
      assert.ifError(err);
      cf_standard.get(config.standard_row_key, function(err, row){
        assert.ifError(err);
        assert.ok(row.count === 3);
        test.finish();
      });
    });
  },

  'test standard cf remove default microsecond timestamp':function(test, assert) {
    if (!has_microtime) {
      test.finish();
      return;
    }

    //try to tease out same-ms collision with 50 attempts
    var finished = 0, ok = true;
    var callback = function() {
      finished++;
      if (finished % 2 == 0) {
        cf_standard.get(config.standard_row_key, function(err, row){
          assert.ifError(err);

          ok = ok && (row.count === 3);

          if (finished == 100) {
            assert.ok(ok);
            assert.ifError(err);
            test.finish();
          } else {
            setTimeout(try_race, 0);
          }
        });
      }
    };

    var try_race = function() {
      cf_standard.insert(config.standard_row_key, {"one": "a"}, function(err, results){
        assert.ifError(err);
        callback();
      });

      cf_standard.remove(config.standard_row_key, "one", function(err, results){
        assert.ifError(err);
        callback();
      });
    };

    try_race();
  },

  'test composite cf remove column':function(test, assert) {
    var key = [ 'åbcd', new Helenus.UUID('e491d6ac-b124-4795-9ab3-c8a0cf92615c') ],
        col = [12345678912345, new Date(1326400762701)];
    cf_standard.remove(key, col, {timestamp:new Date()}, function(err){
      assert.ifError(err);
      cf_standard.get(key, function(err, row){
        assert.ifError(err);
        assert.ok(row.count === 0);
        test.finish();
      });
    });
  },

  'test standard cf remove row':function(test, assert) {
    cf_standard.remove(config.standard_row_key, {timestamp:new Date()}, function(err){
      assert.ifError(err);
      cf_standard.get(config.standard_row_key, function(err, row){
        assert.ifError(err);
        assert.ok(row.count === 0);
        test.finish();
      });
    });
  },

  'test standard cf truncate': function(test, assert){
    cf_standard.truncate(function(err){
      assert.ifError(err);
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
    conn.on('close', function(){
      test.finish();
    });
    assert.doesNotThrow(function(){
      conn.close();
    });
  },

  'tearDown':function(test, assert){
    test.finish();
  }
};
