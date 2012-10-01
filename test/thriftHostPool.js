/**
 * Runs tests defined in thrift.js, but with ConnectionPool
 * option hostPoolSize set to 5.
 */
var config = require('./helpers/thrift'),
    system = require('./helpers/connection'),
    thriftTest = require('./thrift'),
    Helenus, conn, ks, cf_standard, row_standard, cf_composite, cf_counter;

system.hostPoolSize = 5;

module.exports = thriftTest;

