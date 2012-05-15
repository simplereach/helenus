var Connection = require('./connection'),
    util = require('util');

/**
 * A No-Operation default for empty callbacks
 * @private
 * @memberOf Pool
 */
var NOOP = function(){};

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
 *     timeout    : 30000,
 *     cqlVersion : '3.0.0'
 *   });
 *
 * @constructor
 */
var Pool = function(options){
  this.clients = [];
  this.dead = [];

  this.keyspace = options.keyspace;
  this.user = options.user;
  this.password = options.password;
  this.timeout = options.timeout;
  this.cqlVersion = options.cqlVersion;
  this.retryInterval = null;
  this.closing = false;

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
      connected = 0, errored;

  function onConnect(err, connection, keyspace, host){
    finished += 1;

    if (err){
      self.dead.push(host);
    } else {
      connected += 1;
      self.clients.push(connection);

      //we only want to callback once, after we get a valid connection
      if(connected === 1){
        //set the keyspaces connection to be the pool
        if(keyspace){
          keyspace.connection = self;
        }
        callback(null, keyspace);

        //now that we have a connection, lets start monitoring
        self.monitorConnections();
      }
    }

    if(finished === len){
      if(self.clients.length === 0){
        callback(new Error('Could Not Connect To Any Nodes'));
      }
    }
  }

  function connect(host){
    var connection = new Connection({
      host: host,
      keyspace: self.keyspace,
      user: self.user,
      password: self.password,
      timeout: self.timeout,
      cqlVersion: self.cqlVersion
    });

    connection.on('error', function(err){
      self.emit('error', err);
    });

    connection.connect(function(err, keyspace){
      onConnect(err, connection, keyspace, host);
    });
  }

  for(; i < len; i += 1){
    connect(this.hosts[i]);
  }
};

/**
 * Changes the current keyspace for the connection
 */
Pool.prototype.use = function(keyspace, callback){
  callback = callback || NOOP;

  var self = this, i = 0, len = this.clients.length,
      finished = 0, error, ks;

  function onUse(err, keyspace){
    finished += 1;
    error = err;
    ks = keyspace;
    if(keyspace){
      keyspace.connection = self;
    }

    if(finished === len){
      callback(error, ks);
    }
  }

  for(; i < len; i += 1){
    this.clients[i].use(keyspace, onUse);
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
    this.emit('error', new Error('No Available Connections'));
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
      } else {
        this.dead.push(this.clients[i].host + ':' + this.clients[i].port);
      }
    }

    this.clients = valid;
    return this.getConnection();
  }
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
 * Monitors the dead pool to ensure that if a connection has dropped, it will get retried
 * until it connects or until all connections have been lost
 */
Pool.prototype.monitorConnections = function(){
  var self = this;

  // If the pool is already closing, don't bother about monitoring connections
  if(self.closing){
    return;
  }

  /**
   * Try to connect, if success then add to the client pool, if fail the add to the dead pool.
   */
  function connect(host){
    var connection = new Connection({
      host: host,
      keyspace: self.keyspace,
      user: self.user,
      password: self.password,
      timeout: self.timeout,
      cqlVersion : self.cqlVersion
    });

    connection.on('error', function(err){
      self.emit('error', err);
    });

    connection.connect(function(err, keyspace){
      if(err){
        self.dead.push(host);
      } else {
        self.clients.push(connection);
      }
    });
  }

  /**
   * Recursively pop through the dead hosts and try to reconnect
   */
  function checkDead(){
    if(self.dead.length > 0){
      connect(self.dead.pop());
      checkDead();
    }
  }

  this.retryInterval = setInterval(checkDead, 5000);
};

/**
 * Closes all open connections
 */
Pool.prototype.close = function(){
  var self = this, i = 0, j = 0, len = this.clients.length;

  // Make sure no intervals get set
  self.closing = true;

  clearInterval(this.retryInterval);

  function closed(){
    j += 1;
    if(j === len){
      self.emit('close');
    }
  }
  for(; i < len; i += 1){
    this.clients[i].on('close', closed);
    this.clients[i].close();
  }
};
module.exports = Pool;
