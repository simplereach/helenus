var Client = require('./client'),
    ColumnFamily = require('./column_family'),
    util = require('util');

/**
 * Creates a connection to a keyspace for each of the servers in the pool;
 * @param {Array} servers The servers to make connections to
 * @constructor
 */
var Pool = function(servers){
  this.clients = {};
  this.connections = 0;
  this.keyspace = null;
  this.columnFamilies = {};
  
  if(typeof servers === 'string'){
    this.clients[servers] = new Client(servers);
  } else if(Array.isArray(servers)){
    var i = 0, len = servers.length;
    for(; i < len; i += 1){
      this.clients[servers[i]] = new Client(servers[i]);
    }    
  } else if (servers === undefined || servers === null){
    var cli = new Client(), host = cli.host + ':' + cli.port;
    this.clients[host] = cli;
  } else {
    throw(new Error('Invalid Parameter Supplied for servers'));
  }
};
util.inherits(Pool, process.EventEmitter);

/**
 * Connects to each of the servers in the connection pool
 *
 * TODO: Implement Retries
 * @param {String} keyspace The keyspace to connect to
 * @param {Function} callback The callback to invoke when all connections have been made
 */
Pool.prototype.connect = function(keyspace, callback){
  var i = 0, finished = 0, client, self = this,
      hosts = Object.keys(this.clients), 
      len = hosts.length;
  
  this.keyspace = keyspace;
      
  function onConnect(err){
    finished += 1;
    if (err){
      self.emit('error', err);
    } else {
      self.connections += 1;
      
      self.getColumnFamilies(function(err, columnFamilies){
        self.columnFamilies = columnFamilies;
        callback(null, columnFamilies);
      });
    }
    
    if(finished === len){
      if(self.connections === 0){
        callback(new Error('No Available Connections'));
      }
    }
  }
  
  for(; i < len; i += 1){
    client = this.clients[hosts[i]];
    client.on('error', this.emit);
    client.connect(this.keyspace, onConnect);
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
  var hosts = Object.keys(this.clients), len = hosts.length,
      rnd = Math.floor(Math.random() * len),
      host = hosts[rnd];
      
  if (!host){
    throw(new Error('No Available Connections'));
  }
  
  if (this.clients[host].ready){
    return this.clients[host];
  } else {
    delete this.clients[host];
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

module.exports = Pool;