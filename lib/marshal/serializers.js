var UUIDjs = require('uuid-js');
/**
 * No-Operation
 */
var NOOP = function(){};

/**
 * Encodes an N length Integer
 */
var encodeInteger = exports.encodeInteger = function(bits, num){
  var buf = new Buffer(bits/8), func;
  
  if(typeof num !== 'number'){
    num = parseInt(num, 10);
  }
  
  switch(bits){
    case 8:  buf.writeUInt8(num, 0);    break;
    case 16: buf.writeUInt16BE(num, 0); break;
    case 32: buf.writeUInt32BE(num, 0); break;
    case 64: 
      var hex = num.toString(16);    
      hex = new Array(17 - hex.length).join('0') + hex;
      buf.writeUInt32BE(parseInt(hex.slice( 0, 8), 16), 0);
      buf.writeUInt32BE(parseInt(hex.slice( 8, 16), 16), 4);
  }
  
  return buf;
};

/**
 * Does binary encoding
 */
var encodeBinary = exports.encodeBinary = function(val){
  return new Buffer(val, 'binary');
};

/**
 * Encodes a Long (UInt64)
 */
var encodeLong = exports.encodeLong = function(val){
  return encodeInteger(64, val);
};

/**
 * Encodes a 16bit Unsinged Integer
 */
var encodeInt16 = exports.encodeInt16 = function(val){
  return encodeInteger(16, val);
};

/**
 * Encodes a 32bit Unsinged Integer
 */
var encodeInt32 = exports.encodeInt32 = function(val){
  return encodeInteger(32, val);
};

/**
 * Encodes for UTF8
 */
var encodeUTF8 = exports.encodeUTF8 = function(val){
  return new Buffer(val, 'utf8');
};

/**
 * Encodes for Ascii
 */
var encodeAscii = exports.encodeAscii = function(val){
  return new Buffer(val, 'ascii');
};

/**
 * Encode a Double Precision Floating Point Type
 */
var encodeDouble = exports.encodeDouble = function(val){
  if(typeof val !== 'number'){
    val = parseInt(val, 10);
  }
  
  var buf = new Buffer(8);
  buf.writeDoubleBE(val, 0);
  return buf;  
};

/**
 * Encode a Double Precision Floating Point Type
 */
var encodeFloat = exports.encodeFloat = function(val){
  if(typeof val !== 'number'){
    val = parseInt(val, 10);
  }
  
  var buf = new Buffer(4);
  buf.writeFloatBE(val, 0);
  return buf;  
};

/**
 * Encodes a boolean type
 */
var encodeBoolean = exports.encodeBoolean = function(val){
  var buf = new Buffer(1);
  if(val){
    buf.write('\x01', 0);
  } else {
    buf.write('\x00', 0);
  }
  return buf;
};

/**
 * Encodes a Date object
 */
var encodeDate = exports.encodeDate = function(val){
  var t = 1000;
  
  if(val instanceof Date){
    t *= val.getTime();
  } else {
    t *= new Date(val).getTime();
  }
  
  return encodeLong(t);
};

/**
 * Encodes a UUID Object
 */
var encodeUUID = exports.encodeUUID = function(val){
  return new Buffer(val.toBytes());
};