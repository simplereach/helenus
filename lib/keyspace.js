var ColumnFamily = require('./column_family'),
    ttypes = require('./cassandra/1.0/cassandra_types');


/**
 * A No-Operation default for callbacks
 * @private
 * @memberOf Keyspace
 * @constant
 */
var NOOP = function(){};
 
/**
 * Creates an instance of a keyspace
 * @constructor
 */
var Keyspace = function(connection, definition){
  /**
   * The connection object
   * @see Connection
   */
  this.connection = connection;

  /**
   * The name of the keyspace
   */
  this.name = definition.name;

  /**
   * The thrift definition of the keyspace
   */
  this.definition = definition;

  /**
   * A cache of column familes to help with performance
   */
  this.columnFamilies = {};
};

/**
 * Gets a column family from the cache or loads it up
 * @param {String} columnFamily The name of the columnFamily to get the definition for
 * @param {Function} callback The callback to invoke once the column family has been retreived
 */
Keyspace.prototype.get = function(columnFamily, callback){
  var self = this;

  if(this.columnFamilies[columnFamily]){
    callback(null, this.columnFamilies[columnFamily]);
  } else {
    this.describe(function(err, columnFamilies){
      if(err){
        callback(err);
      } else {
        if(columnFamilies[columnFamily]){
          self.columnFamilies = columnFamilies;
          callback(null, columnFamilies[columnFamily]);
        } else {
          var e = new Error('ColumnFamily ' + columnFamily + ' Not Found');
          e.name = 'HelenusNotFoundError';
          callback(e);
        }
      }
    });
  }
};

/**
 * Loads gets all the column families and calls back with them
 * @param {Function} callback The callback to invoke once the column families have been retreived
 */
Keyspace.prototype.describe = function(callback){
  var self = this;

  function onComplete(err, definition){
    if (err) {
      callback(err);
      return;
    }

    var i = 0, cf, columnFamilies = {},
        len = definition.cf_defs.length;

    for(; i < len; i += 1) {
      cf = definition.cf_defs[i];
      columnFamilies[cf.name] = new ColumnFamily(self, cf);
    }

    callback(null, columnFamilies);
  }

  this.connection.execute('describe_keyspace', this.name, onComplete);
};

/**
 * Creates a column family with options
 * @param {String} name The name of the column family to create
 * @param {Object} options The options for the columns family
 * @param {Function} callback The callback to invoke once the column family has been created
 */
Keyspace.prototype.createColumnFamily = function(name, options, callback){
  if(typeof options === 'function'){
    callback = options;
    options = {};
  }

  callback = callback || NOOP;
  options = options || {};
  
  var cfdef = new ttypes.CfDef({
    keyspace: this.name,
    name: name,
    column_type: options.column_type || 'Standard',
    comparator_type: options.comparator_type || 'BytesType',
    subcomparator_type: options.subcomparator_type,
    comment: options.comment,
    read_repair_chance: options.read_repair_chance || 1,
    column_metadata: options.column_metadata,
    gc_grace_seconds: options.gc_grace_seconds,
    default_validation_class: options.default_validation_class,
    min_compaction_threshold: options.min_compaction_threshold,
    max_compaction_threshold: options.max_compaction_threshold,
    replicate_on_write: options.replicate_on_write,
    merge_shards_chance: options.merge_shards_chance,
    key_validation_class: options.key_validation_class,
    key_alias: options.key_alias,
    compaction_strategy: options.compaction_strategy,
    compaction_strategy_options: options.compaction_strategy_options,
    compression_options: options.compression_options,
    bloom_filter_fp_chance: options.bloom_filter_fp_chance
  });

  this.connection.execute('system_add_column_family', cfdef, callback);
};

/**
 * Drops a column family
 * @param {String} name The name of the column family to drop
 * @param {Function} callback The callback to invoke once the column family has been created
 */
Keyspace.prototype.dropColumnFamily = function(name, callback){
  callback = callback || NOOP;
  this.connection.execute('system_drop_column_family', name, callback);
};

module.exports = Keyspace;