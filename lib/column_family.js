
var util = require('util'),    
    ttype = require('./cassandra/1.0/cassandra_types');
    
/**
 * Default read consistency level
 */
var DEFAULT_READ_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;

/**
 * Default write consistency level
 */
var DEFAULT_WRITE_CONSISTENCY = ttype.ConsistencyLevel.QUORUM;

/**
 * Representation of a Column Family
 * 
 * @param {Object} definition The Column Family definition
 * @constructor
 */
var ColumnFamily = function(pool, definition){
  ttype.CfDef.call(this, definition);
  this.isSuper = this.column_type === 'Super';
  this.pool = pool;
};
util.inherits(ColumnFamily, ttype.CfDef);

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
      ts = new Date().getTime(),
      consistency = options.consistencyLevel || DEFAULT_WRITE_CONSISTENCY;

  if (this.isSuper) {
    // TODO: Implement SuperColumns
    throw(new Error('SuperColumns Are Evil!!!'));
  } else {
    // standard
    for (; i < keylen; i += 1) {
      prop = keys[i];
      value = values[prop];

      mutations.push(new ttype.Mutation({
        column_or_supercolumn: new ttype.ColumnOrSuperColumn({
          column: new ttype.Column({
            name: prop,
            value: '' + value,
            timestamp: ts,
            ttl: options.ttl
          })
        })
      }));
    }
  }

  batch[key] = {};
  batch[key][this.name] = mutations;
  this.pool.execute('batch_mutate', batch, consistency, callback);
};

module.exports = ColumnFamily;