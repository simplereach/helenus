var util = require('util'),
    Marshal = require('./marshal'),
    Column = require('./column'),
    Row = require('./row'),
    ttype = require('./cassandra/1.0/cassandra_types');

/**
 * NO-Operation for deault callbacks
 * @private
 * @memberOf ColumnFamily
 */
var NOOP = function(){};

/**
 * Default read consistency level
 * @private
 * @constant
 * @memberOf ColumnFamily
 */
var DEFAULT_READ_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;

/**
 * Default write consistency level
 * @private
 * @constant
 * @memberOf ColumnFamily
 */
var DEFAULT_WRITE_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;

/**
 * Representation of a Column Family
 *
 * @param {Object} definition The Column Family definition
 * @constructor
 */
var ColumnFamily = function(keyspace, definition){
//  ttype.CfDef.call(this, definition);
  this.isSuper = this.column_type === 'Super';
  this.keyspace = keyspace;
  this.connection = keyspace.connection;
  this.definition = definition;
  this.name = definition.name;
  this.columnMarshaller = new Marshal(definition.comparator_type);
  this.valueMarshaller = new Marshal(definition.default_validation_class);
  this.keyMarshaller = new Marshal(definition.key_validation_class);
};
//util.inherits(ColumnFamily, ttype.CfDef);

/**
 * Performs a set command to the cluster
 *
 * @param {String} key The key for the row
 * @param {Object} value The value for the columns as represented by JSON
 * @param {Object} options The options for the insert
 * @param {Function} callback The callback to call once complete
 */
ColumnFamily.prototype.insert = function(key, values, options, callback){
  if (typeof options === 'function'){
    callback = options;
    options = {};
  }

  var mutations = [], batch = {}, i = 0, value, prop,
      keys = Object.keys(values), keylen = keys.length,
      ts = new Date(),
      consistency = options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;

  if (this.isSuper) {
    // TODO: Implement SuperColumns
    throw(new Error('SuperColumns Are Evil!!!'));
  } else {
    // standard
    for (; i < keylen; i += 1) {
      prop = keys[i];
      value = values[prop];

      if(value === null || value === undefined){
        value = '';
      }

      var col = new Column(prop, value, ts, options.ttl);

      mutations.push(new ttype.Mutation({
        column_or_supercolumn: new ttype.ColumnOrSuperColumn({
          column: col.toThrift(this.columnMarshaller, this.valueMarshaller)
        })
      }));
    }
  }

  batch[key] = {};
  batch[key][this.definition.name] = mutations;

  this.connection.execute('batch_mutate', batch, consistency, callback);
};

/**
 * Get a column in a row by its key
 * @param {String} key The key to get
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>start: the from part of the column name</li>
 *     <li>end: the to part of the column name</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 * @param {Function} callback The callback to invoke once the response has been received
 */
ColumnFamily.prototype.get = function(key, options, callback){
  if (typeof options === 'function'){
    callback = options;
    options = undefined;
  }

  callback = callback || NOOP;

  var consistency = options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
      self = this, predicate, parent;

  parent = new ttype.ColumnParent({
    column_family: this.name
  });

  predicate = new ttype.SlicePredicate({
    slice_range: new ttype.SliceRange({
      start:options.start,
      finish:options.finish,
      reversed:options.reversed,
      count:options.max
    })
  });

  function onComplete(err, val){
    if(err){
      callback(err);
      return;
    }

    callback(null, Row.fromThrift(key, val, self));
  }
  this.connection.execute('get_slice', key, parent, predicate, consistency, onComplete);
};

module.exports = ColumnFamily;