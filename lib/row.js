var util = require('util'),
    Column = require('./column'),
    Marshal = require('./marshal');

/**
 * Represents the columns in a row
 * @constructor
 * @param {Object} data The data returned from the cql request
 * @param {Object} schema The schema returned from the cql request
 */
var Row = function(data, schema){
  var i = 0, len = data.columns.length, item, deserializeValue, name,
      deserializeValueDefault = new Marshal(schema.default_value_type || 'BytesType').deserialize;

  this.key = data.key;
  this._nameMarshaller = new Marshal(schema.default_name_type || 'BytesType');
  this._map = {};
  this._schema = schema;

  for(; i < len; i += 1){
    item = data.columns[i];
    name = this._nameMarshaller.deserialize(item.name);

    //default value decoder
    deserializeValue = deserializeValueDefault;

    //individual columns can be set with a different value deserializer
    if(schema.value_types && schema.value_types[item.name]){
      deserializeValue = new Marshal(schema.value_types[item.name]).deserialize;
    }

    //when doing select * you get a column called KEY, it's not good eats.
    if(item.name !== 'KEY'){
      this.push(new Column(name,deserializeValue(item.value), new Date(item.timestamp), item.ttl));
      this._map[item.name] = i;
    }
  }

  /**
   * Return the columns count, synonymous to length
   */
  this.__defineGetter__('count', function(){
    return this.length;
  });
};
util.inherits(Row, Array);

/**
 * Create a row object using data returned from a thrift request
 * @param {String} key The key of the row
 * @param {Array} columns The response from the thrift request
 * @param {ColumnFamily} cf The column family creating the row
 */
Row.fromThrift = function(key, columns, cf){
  var data = { columns: [], key:key }, 
      schema = {}, i = 0, len = columns.length;
  
  //TODO: Implement counter and super columns
  for(; i < len; i += 1){
    data.columns.push( columns[i].column );
  }
    
  schema.value_types = {}; //TODO:implement this
  schema.default_value_type = cf.definition.default_validation_class;
  schema.default_name_type = cf.definition.comparator_type;
  
  return new Row(data, schema);
};

/**
 * Overrides the Array.forEach to callback with (name, value, timestamp, ttl)
 * @param {Function} callback The callback to invoke once for each column in the Row
 */
Row.prototype.forEach = function(callback){
  var i = 0, len = this.length, item;
  for(; i < len; i += 1){
    item = this[i];
    callback(item.name, item.value, item.timestamp, item.ttl);
  }
};

/**
 * Adds the ability to get a column by its name rather than by its array index
 * @param {String} name The name of the column to get
 * @returns {Object} a tuple of the column name, timestamp, ttl and value
 */
Row.prototype.get = function(name){
  name = this._nameMarshaller.serialize(name).toString('binary');
  return this[this._map[name]];
};

/**
 * Inspect method for columns
 */
Row.prototype.inspect = function(){
  var i = 0, names = Object.keys(this._map), len = names.length, cols = [];
  for(; i < len; i += 1){
    cols.push(this._nameMarshaller.deserialize(names[i]));
  }

  return util.format("<Row: Key: '%s', ColumnCount: %s, Columns: [ '%s' ]>", this.key, this.length, cols);
};

/**
 * Slices out columns based on their name
 * @param {String} start The starting string
 * @param {String} end The ending string
 * @returns {Row} Row with the sliced out columns
 */
Row.prototype.nameSlice = function(start, end){
  start = start || ' ';
  end = end || '~';

  var names = Object.keys(this._map),
      i = 0, len = names.length, matches = [], key;

  for(; i < len; i += 1){
    key = names[i];
    if(key >= start && key < end){
      matches.push(this.get(key));
    }
  }

  return new Row({ key:this.key, columns:matches }, this._schema);
};

/**
 * Slices out columns based ont their index
 * @param {Number} start The starting index for the columns slice
 * @param {Number} end The ending index for the columns slice
 * @returns {Row} Row with the sliced out columns
 */
Row.prototype.slice = function(start, end){
  start = start || 0;
  end = end || this.length - 1;

  var matches = Array.prototype.slice.call(this, start, end);
  return new Row({ key:this.key, columns:matches }, this._schema);
};

/**
 * ToString method for columns
 * @see Row#inspect
 */
Row.prototype.toString = function(){
  return this.inspect();
};

module.exports = Row;