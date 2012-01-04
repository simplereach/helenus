var UUIDjs = require('uuid-js');
/**
 * No-Operation
 */
var NOOP = function(){};

var decodeInteger = exports.decodeInteger = function(bits, val){
  var hex = new Buffer(val, 'binary').toString('hex');
  return parseInt(hex, 16);
};

/**
 * Does binary decoding
 */
var decodeBinary = exports.decodeBinary = function(val){
  return new Buffer(val, 'binary');
};

/**
 * Decodes a Long (UInt64)
 */
var decodeLong = exports.decodeLong = function(val){
  return decodeInteger(64, val);
};

/**
 * Decodes a 16bit Unsinged Integer
 */
var decodeInt16 = exports.decodeInt16 = function(val){
  return decodeInteger(16, val);
};

/**
 * Decodes a 32bit Unsinged Integer
 */
var decodeInt32 = exports.decodeInt32 = function(val){
  return decodeInteger(32, val);
};

/**
 * Decodes for UTF8
 */
var decodeUTF8 = exports.decodeUTF8 = function(val){
  return new Buffer(val, 'binary').toString('utf8');
};

/**
 * Decodes for Ascii
 */
var decodeAscii = exports.decodeAscii = function(val){
  return new Buffer(val, 'binary').toString('ascii');
};

/**
 * Decode a Double Precision Floating Point Type
 */
var decodeDouble = exports.decodeDouble = function(val){
  var buf = new Buffer(val, 'binary');
  return buf.readDoubleBE(0);
};

/**
 * Decode a Double Precision Floating Point Type
 */
var decodeFloat = exports.decodeFloat = function(val){
  var buf = new Buffer(val, 'binary');
  return buf.readFloatBE(0);
};

/**
 * Decodes a boolean type
 */
var decodeBoolean = exports.decodeBoolean = function(val){
  var buf = new Buffer(val, 'binary');

  if(buf[0] === 0){
    return false;
  } else if(buf[0] === 1) {
    return true;
  } else {
    throw(new Error('Invalid Boolean Type Read'));
  }
};

/**
 * Decodes a Date object
 */
var decodeDate = exports.decodeDate = function(val){
  var date = decodeLong(val);
  return new Date(date);
};

/**
 * Decodes a UUID Object
 */
var decodeUUID = exports.decodeUUID = function(val){
  return UUIDjs.fromBinary(val).hex;
};