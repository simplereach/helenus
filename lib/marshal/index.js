var Serializers = require('./serializers'),
    Deserializers = require('./deserializers');


/**
 * No-Operation
 */
var NOOP = function(){};


var TYPES = {
  'BytesType': { ser:Serializers.encodeBinary, de:Deserializers.decodeBinary },
  'LongType': { ser:Serializers.encodeLong, de:Deserializers.decodeLong },
  'IntegerType': { ser:Serializers.encodeInt32, de:Deserializers.decodeInt32 },
  'UTF8Type': { ser:Serializers.encodeUTF8, de:Deserializers.decodeUTF8 },
  'AsciiType': { ser:Serializers.encodeAscii, de:Deserializers.decodeAscii },
  'LexicalUUIDType': { ser:Serializers.encodeUUID, de:Deserializers.decodeUUID }, //TODO: This may not be right
  'TimeUUIDType': { ser:Serializers.encodeUUID, de:Deserializers.decodeUUID },    //TODO: This may not be right
  'CounterColumnType': { ser:NOOP, de:NOOP },
  'FloatType': { ser:Serializers.encodeFloat, de:Deserializers.decodeFloat },
  'DoubleType':{ ser:Serializers.encodeDouble, de:Deserializers.decodeDouble },
  'DateType':{ ser:Serializers.encodeDate, de:Deserializers.decodeDate },
  'BooleanType':{ ser:Serializers.encodeBoolean, de:Deserializers.decodeBoolean },
  'UUIDType': { ser:Serializers.encodeUUID, de:Deserializers.decodeUUID }        //TODO: This may not be right
};
/**
 * Given a string like org.apache.cassandra.db.marshal.UTF8Type
 * return a string of UTF8Type
 * @return {string} TypeName
 * @private
 */
function getType(str){
  var classes = str.split('.');
  return classes[classes.length - 1];
}

/**
 * Returns the type string inside of the parentheses
 * @private
 */
function getInnerType(str){
  return str.substring(str.indexOf('(') + 1, str.length - 1);
}

/**
 * Returns an array of types for composite columns
 * @private
 */
function getCompositeTypes(str){
  var types = getInnerType(str).split(','),
      i = 0, ret = [], typeLength = types.length;

  for(; i < typeLength; i += 1){
    ret.push( getType(types[i]) );
  }

  return ret;
}

/**
 * Parses the type string and decides what types to return
 * @private
 */
function parseTypeString(type){
  if (type.indexOf('CompositeType') > -1){
    return getCompositeTypes(type);
  } else if(type.indexOf('ReversedType') > -1){
    return getInnerType(type);
  } else if(type === null || type === undefined) {
    return 'BytesType';
  } else {
    return getType(type);
  }
}

/**
 * Creates a serializer for composite types
 * @private
 */
function compositeSerializer(serializers){
  return function(vals){
    var i = 0, buffers = [], totalLength = 0,
        valLength = vals.length, val;

    if(!Array.isArray(vals)){
      vals = [vals];
      valLength = vals.length;
    }

    for(; i < valLength; i += 1){
      val = serializers[i](vals[i]);
      buffers.push(val);
      totalLength += val.length + 3;
    }

    var buf = new Buffer(totalLength),
        buffersLength = buffers.length,
        writtenLength = 0;

    i = 0;
    for(; i < buffersLength; i += 1){
      val = buffers[i];
      buf.writeUInt16BE(val.length, writtenLength);
      writtenLength += 2;
      val.copy(buf, writtenLength, 0);
      writtenLength += val.length;
      buf.write('\x00', writtenLength);
      writtenLength += 1;
    }

    return buf;
  };
}

/**
 * Creates a deserializer for composite types
 * @private
 */
function compositeDeserializer(serializers){
  return function(vals){
    throw('Not Implemented');
  };
}

/**
 * Gets the serializer(s) for a specific type
 * @private
 */
function getSerializer(type){
  if (Array.isArray(type)){
    var i = 0, typeLength = type.length, serializers = [];
    for(; i < typeLength; i += 1){
      serializers.push( getSerializer(type[i]));
    }

    return compositeSerializer(serializers);
  } else {
    return TYPES[type].ser;
  }
}

/**
 * Gets the deserializer(s) for a specific type
 * @private
 */
function getDeserializer(type){
  if (Array.isArray(type)){
    var i = 0, typeLength = type.length, deserializers = [];
    for(; i < typeLength; i += 1){
      deserializers.push( getDeserializer(type[i]));
    }

    return compositeDeserializer(deserializers);
  } else {
    return TYPES[type].de;
  }
}

/**
 * Creates a SerDe object for a column family
 */
var Marshal = function(type){
  var parsedType = parseTypeString(type);

  if(Array.isArray(parsedType)){
    this.isComposite = true;
  } else {
    this.isComposite = false;
  }

  this.serialize = getSerializer(parsedType);
  this.deserialize = getDeserializer(parsedType);
};

module.exports = Marshal;