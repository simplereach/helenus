var util = require('util'),
    Marshal = require('./marshal');

/**
 * Represents the columns in a row
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
      this.push({
        name:name,
        value:deserializeValue(item.value),
        timestamp:new Date(item.timestamp),
        ttl:item.ttl
      });
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
 * Overrides the Array.forEach to callback with (name, value, timestamp, ttl)
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
 */
Row.prototype.get = function(name){
  name = this._nameMarshaller.serialize(name).toString('binary');
  return this[this._map[name]];
};

/**
 * Inspect method for columns
 */
Row.prototype.inspect = function(){
  var cols = Object.keys(this._map).join('\', \'');
  return util.format("<Row: Key: '%s', ColumnCount: %s, Columns: [ '%s' ]>", this.key, this.length, cols);
};

/**
 * Slices out columns based on their name
 * @param {String} start The starting string
 * @param {String} end The ending string
 * @return
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
 */
Row.prototype.slice = function(start, end){
  start = start || 0;
  end = end || this.length - 1;

  var matches = Array.prototype.slice.call(this, start, end);
  return new Row({ key:this.key, columns:matches }, this._schema);
};

/**
 * ToString method for columns
 */
Row.prototype.toString = function(){
  return this.inspect();
};

module.exports = Row;