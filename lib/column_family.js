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
 * Gets an array of columns from an object
 * @param {Object} columns
 * @private
 * @memberOf ColumnFamily
 * @returns {Array} and array of columns
 */
function getColumns(columns){
  var keys = Object.keys(columns), len = keys.length, i = 0, key, value, arr = [],
      ts = new Date();
  for(; i < len; i += 1){
    key = keys[i];
    value = columns[key];
    
    if(value === null || value === undefined){
      value = '';
    }
    
    arr.push(new Column(key, value, ts));
  }
  return arr;
}

/**
 * Gets a slcie predicate based on some options
 * @private
 * @memberOf ColumnFamily
 * @returns {SlicePredicate}
 */
function getSlicePredicate(options, serializer){
  var predicate = new ttype.SlicePredicate();
  
  if(Array.isArray(options.columns)){
    var cols = [], i = 0, len = options.columns.length;
    for(; i < len; i += 1){
      cols.push( serializer.serialize(options.columns[i]) );
    }
    predicate.column_names = cols;
  } else {
    predicate.slice_range = new ttype.SliceRange({
      start:options.start,
      finish:options.end,
      reversed:options.reversed,
      count:options.max
    });
  }

  return predicate;
}

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

  this.parent = new ttype.ColumnParent({
    column_family: this.name
  });

  this.columns = {};

  if(definition.column_metadata && Array.isArray(definition.column_metadata)){
    var i = 0, len = definition.column_metadata.length, col;
    for(; i < len; i += 1){
      col = definition.column_metadata[i];
      this.columns[col.name] = new Marshal(col.validation_class);
    }
  }
};

/**
 * Performs a set command to the cluster
 *
 * @param {String} key The key for the row
 * @param {Object} columns The value for the columns as represented by JSON or an array of Column objects
 * @param {Object} options The options for the insert
 * @param {Function} callback The callback to call once complete
 */
ColumnFamily.prototype.insert = function(key, columns, options, callback){
  if (typeof options === 'function'){
    callback = options;
    options = {};
  }

  if(!Array.isArray(columns)){
    columns = getColumns(columns);
  }
  
  var len = columns.length, i = 0, valueMarshaller, col,
      mutations = [], batch = {},
      consistency = options.consistency || DEFAULT_WRITE_CONSISTENCY;
    
  for(; i < len; i += 1){
    col = columns[i];
    valueMarshaller = this.columns[col.name] || this.valueMarshaller;
    mutations.push(new ttype.Mutation({
      column_or_supercolumn: new ttype.ColumnOrSuperColumn({
        column: col.toThrift(this.columnMarshaller, valueMarshaller)
      })
    }));
  }
  
  var marshalledKey = this.keyMarshaller.serialize(key).toString('binary');

  batch[marshalledKey] = {};
  batch[marshalledKey][this.definition.name] = mutations;

  this.connection.execute('batch_mutate', batch, consistency, callback);
};

/**
 * Get a row by its key
 * @param {String} key The key to get
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>start: the from part of the column name</li>
 *     <li>end: the to part of the column name</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>columns: an {Array} of column names to get</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 * @param {Function} callback The callback to invoke once the response has been received
 */
ColumnFamily.prototype.get = function(key, options, callback){
  if (typeof options === 'function'){
    callback = options;
    options = {};
  }

  callback = callback || NOOP;

  var self = this,
      consistency = options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
      marshalledKey = this.keyMarshaller.serialize(key).toString('binary'),
      predicate = getSlicePredicate(options, this.columnMarshaller);
  
  function onComplete(err, val){
    if(err){
      callback(err);
      return;
    }

    callback(null, Row.fromThrift(key, val, self));
  }

  this.connection.execute('get_slice', marshalledKey, this.parent, predicate, consistency, onComplete);
};

/**
 * Gets rows by their indexed fields
 * @param {Object} query Options for the rows part of the get
 *   <ul>
 *     <li>fields: an array of objects thjat contain { column:column_name, operator: 'EQ', value:value }
 *       <ul>
 *         <li>column: {String} The name of the column with the index</li>
 *         <li>operator: {String} The operator to use, can be EQ, GTE, GT, LTE, ot LT</li>
 *         <li>value: {String} The value to query by</li>
 *       </ul>
 *     </li>
 *     <li>start: the start key to get</li>
 *     <li>max: the total amount of rows to return</li>
 *   </ul>
 * @param {Object} options Options for the get, can have start, end, max, consistencyLevel
 *   <ul>
 *     <li>start: the from part of the column name</li>
 *     <li>end: the to part of the column name</li>
 *     <li>max: the max amount of columns to return</li>
 *     <li>columns: an {Array} of column names to get</li>
 *     <li>consistencyLevel: the read consistency level</li>
 *   </ul>
 * @param {Function} callback The callback to invoke once the response has been received
 */
ColumnFamily.prototype.getIndexed = function(query, options, callback){
  if (typeof options === 'function'){
    callback = options;
    options = {};
  }

  callback = callback || NOOP;

  var self = this, indexClause, indexExpressions = [],
      i = 0, len = query.fields.length, field, valueMarshaller,
      consistency = options.consistencyLevel || DEFAULT_READ_CONSISTENCY,
      predicate = getSlicePredicate(options, this.columnMarshaller);
  
  for(; i < len; i += 1){
    field = query.fields[i];
    valueMarshaller = this.columns[field.column] || this.valueMarshaller;

    indexExpressions.push(new ttype.IndexExpression({
      column_name:this.columnMarshaller.serialize(field.column),
      op:ttype.IndexOperator[field.operator],
      value: valueMarshaller.serialize(field.value)
    }));
  }

  indexClause = new ttype.IndexClause({
    expressions: indexExpressions,
    start_key:query.start || '',
    count:query.max || 100
  });

  function onComplete(err, val){
    if(err){
      callback(err);
      return;
    }
    
    var results = [], i = 0, len = val.length, row;
    for(; i < len; i += 1){
      row = val[i];
      results.push(Row.fromThrift(row.key, row.columns, self));
    }
    callback(null, results);
  }

  this.connection.execute('get_indexed_slices', this.parent, indexClause, predicate, consistency, onComplete);
};

module.exports = ColumnFamily;