
/*!
 * Helenus
 * Copyright(c) 2011 SimpleReach <rbradberry@simplereach.com>
 * MIT Licensed
 */

/**
 * Helenus, a NodeJS Cassandra Driver
 * @class
 */
var Helenus = {};

/**
 * The current dirver version
 * @static
 * @constant
 */
Helenus.version = '0.3.2';

/**
 * The connection pool
 * @static
 * @see Pool
 */
Helenus.ConnectionPool = require('./pool');

/**
 * The connection
 * @static
 * @see Connection
 */
Helenus.Connection = require('./connection');

/**
 * The Keyspace
 * @static
 * @see Keyspace
 */
Helenus.Keyspace = require('./keyspace');

/**
 * The ColumnFamily
 * @static
 * @see ColumnFamily
 */
Helenus.ColumnFamily = require('./column_family');

/**
 * The Column
 * @static
 * @see Column
 */
Helenus.Column = require('./column');

/**
 * The Row
 * @static
 * @see Row
 */
Helenus.Row = require('./row');

/**
 * An object for de/serializing data for consumption by Cassandra
 * @static
 * @see Marshal
 */
Helenus.Marshal = require('./marshal');

/**
 * UUID
 * @see UUID
 */
Helenus.UUID = require('./uuid').UUID;

/**
 * TimeUUID
 * @see TimeUUID
 */
Helenus.TimeUUID = require('./uuid').TimeUUID;


module.exports = Helenus;
