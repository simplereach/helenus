var util = require('util'),
    ttypes = require('./cassandra/1.0/cassandra_types'),
    Column = require('./column');

var SuperColumn = function(name, columns) {
  this.name = name;
  this.columns = columns;

  if(Array.isArray(columns)) {
    this.columns = columns;
  }
  else {
    this.columns = new Array(columns.length);
    var keys = Object.keys(columns);
    for(var i=0; i < keys.length; i++) {
      this.columns[i] = new Column(keys[i], columns[keys[i]]);
    }
  }
};

/*
 *
 */
SuperColumn.prototype.toThrift = function(columnMarshaller, subcolumnMarshaller, valueMarshaller) {
  var serializedColumns = new Array(this.columns.length);
  for(var i=0; i < this.columns.length; i++) {
    serializedColumns[i] = this.columns[i].toThrift(subcolumnMarshaller, valueMarshaller);
  }
  return new ttypes.SuperColumn({
    name: columnMarshaller.serialize(this.name),
    columns: serializedColumns
  });
};

module.exports = SuperColumn;
