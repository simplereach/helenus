var Connection = require('./connection'),
    ColumnFamily = require('./column_family'),
    util = require('util');


/**
 * Creates a connection to a keyspace for each of the servers in the pool;
 * @param {Object} options The options for the connection

 * Example:
 *
 *   var pool = new ConnectionPool({
 *     hosts      : ['host1:9160', 'host2:9170', 'host3', 'host4'],
 *     keyspace   : 'database',
 *     user       : 'mary',
 *     pass       : 'qwerty',
 *     timeout    : 30000
 *   });
 *
 * @constructor
 */
var Pool = function(options){
  this.clients = [];  
  this.keyspace = options.keyspace;
  this.user = options.user;
  this.password = options.password;
  this.timeout = options.timeout;

  if(!options.hosts && options.host){
    this.hosts = [options.hosts];
  } else {
    this.hosts = options.hosts;  
  }
  
  if(!Array.isArray(this.hosts)){
    throw(new Error('HelenusError: Invalid hosts supplied for connection pool'));
  }
};
util.inherits(Pool, process.EventEmitter);

/**
 * Connects to each of the servers in the connection pool
 *
 * TODO: Implement Retries
 * @param {Function} callback The callback to invoke when all connections have been made
 */
Pool.prototype.connect = function(callback){
  var i = 0, finished = 0, client, self = this, len = this.hosts.length,
      connected = 0;

  function onConnect(err, connection){
    finished += 1;

    if (err){
      callback(err);
    } else {
      connected += 1;
      self.clients.push(connection);
      if(connected === 1){
        self.getColumnFamilies(callback);
      }
    }

    if(finished === len){
      if(self.clients.length === 0){
        callback(new Error('No Available Connections'));
      }
    }
  }

  function connect(host){
    var connection = new Connection({
      host: host,
      keyspace: self.keyspace,
      user: self.user,
      password: self.password,
      timeout: self.timeout
    });

    connection.on('error', function(err){
      self.emit('error', err);
    });

    connection.connect(function(err){
      onConnect(err, connection);
    });
  }

  for(; i < len; i += 1){
    connect(this.hosts[i]);
  }
};

/**
 * Executes a command on a single client from the pool
 * @param {String} command The command to execute
 * additional params are supplied to the command to be executed
 */
Pool.prototype.execute = function(command){
  var args = Array.prototype.slice.apply(arguments),
      conn = this.getConnection();

  conn.execute.apply(conn, args);
};

/**
 * Executes a CQL Query Against the DB.
 * @param {String} cmd A string representation of the query: 'select %s, %s from MyCf where key=%s'
 * @param {arguments} args0...argsN An Array of arguments for the string ['arg0', 'arg1', 'arg2']
 * @param {Function} callback The callback function for the results
 */
Pool.prototype.cql = function(){
  var args = Array.prototype.slice.apply(arguments),
      conn = this.getConnection();

  conn.cql.apply(conn, args);
};

/**
 * Gets a random connection from the connection pool
 */
Pool.prototype.getConnection = function(){
  var len = this.clients.length,
      rnd = Math.floor(Math.random() * len),
      host = this.clients[rnd];

  if (!host){
    throw(new Error('No Available Connections'));
  }

  if (host.ready){
    return host;
  } else {
    /**
     * if the host comes back as not ready then we loop through all the hosts
     * and find the ready ones then call getConnection again
     **/
    var i = 0, valid = [];
    for(; i < len; i += 1){
      if(this.clients[i].ready){
        valid.push(this.clients[i]);
      }
      this.clients = valid;
    }

    return this.getConnection();
  }
};

/**
 * Gets all the column familes and loads them as sum obects on the column families object
 */
Pool.prototype.getColumnFamilies = function(callback){
  var self = this;

  function onKeyspace(err, definition){
    if (err) {
      callback(err);
      return;
    }

    var i = 0, cf, columnFamilies = {},
        len = definition.cf_defs.length;

    for(; i < len; i += 1) {
      cf = definition.cf_defs[i];
      columnFamilies[cf.name] = new ColumnFamily(self, cf);
    }

    callback(err, columnFamilies);
  }

  this.execute('describe_keyspace', this.keyspace, onKeyspace);
};

/**
 * Creates a keyspace see Connection.prototype.createKeyspace
 */
Pool.prototype.createKeyspace = function(name, options, callback){
  var conn = this.getConnection();
  conn.createKeyspace.call(conn, name, options, callback);  
};

/**
 * Creates a keyspace see Connection.prototype.createKeyspace
 */
Pool.prototype.dropKeyspace = function(name, callback){
  var conn = this.getConnection();
  conn.dropKeyspace.call(conn, name, callback);  
};

/**
 * Closes all open connections
 */
Pool.prototype.close = function(){
  var i = 0, len = this.clients.length;
  for(; i < len; i += 1){
    this.clients[i].close();
  }
};
module.exports = Pool;