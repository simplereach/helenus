var util = require('util'),
    Marshal = require('./marshal'),
    Column = require('./column'),
    ttype = require('./cassandra/1.0/cassandra_types');

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
  this.columnMarshaller = new Marshal(definition.comparator_type);
  this.valueMarshaller = new Marshal(definition.validation_class);
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

      var col = new Column({
        name: prop,
        value: value,
        timestamp: ts,
        ttl: options.ttl
      });

      mutations.push(new ttype.Mutation({
        column_or_supercolumn: new ttype.ColumnOrSuperColumn({
          column: col.toThrift(this.columnMarshaller, this.valueMarshaller)
        })
      }));
    }
  }

  /**
   * TODO: Build into the Cassandra thrift interface, the ability to have
   * Keys as composites.  This means that the object format is out the window...maybe
   */
  //if(this.keyMarshaller.isComposite){
  //  key =  key.split(':');
  //}
  //key = this.keyMarshaller.serialize(key);

  batch[key] = {};
  batch[key][this.definition.name] = mutations;

  this.connection.execute('batch_mutate', batch, consistency, callback);
};

module.exports = ColumnFamily;