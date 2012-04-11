var UUID = require('../uuid').UUID,
    TimeUUID = require('../uuid').TimeUUID;

/**
 * Deserializers for various types
 * @class
 */
var Deserializers = {};

Deserializers.decodeInteger = function(bits, val){
  if(val === null || val === undefined){
    return null;
  }

  var hex = new Buffer(val, 'binary').toString('hex');
  return parseInt(hex, 16);
};

/**
 * Does binary decoding
 * @static
 * @param {String} val The binary string to decode
 * @returns {Buffer} a buffer containing the bytes decoded from the string
 */
Deserializers.decodeBinary = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return new Buffer(val, 'binary');
};

/**
 * Decodes a Long (UInt64)
 * @static
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeLong = function(val){
  return Deserializers.decodeInteger(64, val);
};

/**
 * Decodes a 32bit Unsinged Integer
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeInt32 = function(val){
  return Deserializers.decodeInteger(32, val);
};

/**
 * Decodes for UTF8
 * @param {String} val The binary string to decode
 * @returns {String} The utf8 value decoded from the binary string
 */
Deserializers.decodeUTF8 = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return new Buffer(val, 'binary').toString('utf8');
};

/**
 * Decodes for Ascii
 * @param {String} val The binary string to decode
 * @returns {String} The ascii value decoded from the binary string
 */
Deserializers.decodeAscii = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return new Buffer(val, 'binary').toString('ascii');
};

/**
 * Decode a Double Precision Floating Point Type
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeDouble = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = new Buffer(val, 'binary');
  return buf.readDoubleBE(0);
};

/**
 * Decode a Single Precision Floating Point Type
 * @param {String} val The binary string to decode
 * @returns {Number} The number value decoded from the binary string
 */
Deserializers.decodeFloat = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = new Buffer(val, 'binary');
  return buf.readFloatBE(0);
};

/**
 * Decodes a boolean type
 * @param {String} val The binary string to decode
 * @returns {Boolean} The boolean value decoded from the binary string
 */
Deserializers.decodeBoolean = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var buf = new Buffer(val, 'binary');

  if(buf[0] === 0){
    return false;
  } else if(buf[0] === 1) {
    return true;
  }
};

/**
 * Decodes a Date object
 * @param {String} val The binary string to decode
 * @returns {Date} The date value decoded from the binary string
 */
Deserializers.decodeDate = function(val){
  if(val === null || val === undefined){
    return null;
  }

  var date = Deserializers.decodeLong(val);
  return new Date(date);
};

/**
 * Decodes a UUID Object
 * @param {String} val The binary string to decode
 * @returns {UUID} The uuid value decoded from the binary string
 */
Deserializers.decodeUUID = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return UUID.fromBinary(val);
};

/**
 * Decodes a TimeUUID Object
 * @param {String} val The binary string to decode
 * @returns {TimeUUID} The uuid value decoded from the binary string
 */
Deserializers.decodeTimeUUID = function(val){
  if(val === null || val === undefined){
    return null;
  }

  return TimeUUID.fromBinary(val);
};
module.exports = Deserializers;
