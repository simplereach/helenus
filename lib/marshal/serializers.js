var UUID = require('../uuid'),
    bignum = require('bignum');

/**
 * Serializers for various types
 * @class
 */
var Serializers = {};

/**
 * Encodes an N length Integer
 * @param {Number} bits The number of bits to encode to
 * @param {Number} num The number to encode
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeInteger = function(bits, num){
  switch(bits){
    case 32:
        var buf = new Buffer(bits/8), func;
        if(typeof num !== 'number'){
            num = parseInt(num, 10);
        }
        buf.writeUInt32BE(num, 0);
        return buf;
    case 64:
      if(typeof num !== 'object'){
        num = bignum(num, 10);
      }
      return num.toBuffer({size:8});
  }
};

/**
 * Does binary encoding
 * @param {String} val The binary string to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeBinary = function(val){
  if(Buffer.isBuffer(val)){
    return val;
  } else {
    return new Buffer(val.toString(), 'binary');
  }
};

/**
 * Encodes a Long (UInt64)
 * @param {Number} val The number to serialize into a long
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeLong = function(val){
  return Serializers.encodeInteger(64, val);
};

/**
 * Encodes a 32bit Unsinged Integer
 * @param {Number} val The number to serialize into a 32-bit integer
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeInt32 = function(val){
  return Serializers.encodeInteger(32, val);
};

/**
 * Encodes for UTF8
 * @param {String} val The utf8 string value to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeUTF8 = function(val){
  if(Buffer.isBuffer(val)){
    return val;
  }

  if(val === undefined || val === null){
    val = '';
  }

  return new Buffer(val.toString(), 'utf8');
};

/**
 * Encodes for Ascii
 * @param {String} val The ascii string value to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeAscii = function(val){
  if(Buffer.isBuffer(val)){
    return val;
  }

  if(val === undefined || val === null){
    val = '';
  }

  return new Buffer(val.toString(), 'ascii');
};

/**
 * Encode a Double Precision Floating Point Type
 * @param {Number} val The number to serialize into a Double Precision Floating Point
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeDouble = function(val){
  var buf = new Buffer(8);
  buf.writeDoubleBE(val, 0);
  return buf;
};

/**
 * Encode a Double Precision Floating Point Type
 * @param {Number} val The number to serialize into a Single Precision Floating Point
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeFloat = function(val){
  var buf = new Buffer(4);
  buf.writeFloatBE(val, 0);
  return buf;
};

/**
 * Encodes a boolean type
 * @param {Boolean} val true or false, will be serialized into the proper boolean value
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeBoolean = function(val){
  if(val){
    return new Buffer([0x01]);
  } else {
    return new Buffer([0x00]);
  }
};

/**
 * Encodes a Date object
 * @param {Date} val The date to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeDate = function(val){
  var t;

  if(val instanceof Date){
    t = val.getTime();
  } else {
    t = new Date(val).getTime();
  }

  return Serializers.encodeLong(t);
};

/**
 * Encodes a UUID Object
 * @param {UUID} val The uuid object to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeUUID = function(val){
  if (val instanceof UUID.UUID) {
    return val.toBuffer();
  } else {
    var uuid = new UUID.UUID(val);
    return uuid.toBuffer();
  }
};

/**
 * Encodes a TimeUUID Object
 * @param {TimeUUID} val The uuid object to serialize
 * @returns {Buffer} A buffer containing the value bytes
 * @static
 */
Serializers.encodeTimeUUID = function(val){
  if (val instanceof UUID.TimeUUID) {
    return val.toBuffer();
  } else {
    var timeUUID = new UUID.TimeUUID(val);
    return timeUUID.toBuffer();
  }
};

module.exports = Serializers;
