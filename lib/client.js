
var util = require('util'),
    thrift = require('thrift'),
    Cassandra = require('./cassandra/1.0/Cassandra'),    
    ttype = require('./cassandra/1.0/cassandra_types'),
    ColumnFamily = require('./column_family');

/**
 * A No-Op function for default callbacks
 */
var NOOP = function(){};
/**
 * Default port for cassandra
 * @private
 */
var DEFAULT_PORT = 9160;

/**
 * Default host
 * @private
 */
var DEFAULT_HOST = 'localhost';

/**
 * The Cassandra Client
 *
 * @param {Number} port The port to connect on, defaults to 9160
 * @param {String} host The host to connect to, defaults to localhost
 * @contructor
 */
var Client = function(port, host){
  if (typeof port === 'string' && port.indexOf(':') > -1){
    var parts = port.split(':');
    port = parts[1];
    host = parts[0];
  }
  
  /**
   * The port to connect to
   */
  this.port = port || DEFAULT_PORT;

  /**
   * The host to connect to
   */
  this.host = host;

  /**
   * Ready state of the client
   */
  this.ready = false;
};
util.inherits(Client, process.EventEmitter);

/**
 * Connects to the cassandra cluster
 */
Client.prototype.connect = function(keyspace, options, callback){
  var self = this;
  
  if (typeof keyspace === 'function'){
    callback = keyspace;
    keyspace = undefined;
  }
  
  if (typeof options === 'function'){
    callback = options;
    options = {};
  }
  
  //set callback to a noop to prevent explosion
  callback = callback || NOOP;
  
  /**
   * Thrift Connection
   */
  this._connection = thrift.createConnection(this.host, this.port);

  //bubble up all errors
  this._connection.on('error', this.emit);
  this._connection.on('close', function(){
    self.ready = false;
    self.emit('error', new Error(self.host + ':' + self.port + ' Client Disconnect'));    
  });

  /**
   * Thrift Client
   */
  this._client = thrift.createClient(Cassandra, this._connection);
  
  /**
   * Handles what happens when we connect to the cassandra cluster
   *
   * @private
   * @param {Error} err A connection error with cassandra
   */
  function onConnect(err) {
    if (err){
      callback(err);
      return;
    }

    //set the state to ready
    self.ready = true;

    // if keyspace is specified, use that ks
    if (keyspace !== undefined){
      self.use(keyspace, callback);
    } else {
      callback();
    }
  }
  this._connection.on('connect', onConnect);
};

/**
 * Sets the current keyspace
 *
 * @param {String} keyspace The keyspace to use
 */
Client.prototype.use = function(keyspace, callback){
  var self = this;
  callback = callback || NOOP;
  
  function onKeyspace(err, definition){
    if (err) {
      callback(err);
      return;
    }

    /**
     * Callback for the Keyspace Set function
     *
     * @param {Error} err An error object
     * @private
     */
    function onKeyspaceSet(err){
      callback(err);
    }
    self._client.set_keyspace(keyspace, onKeyspaceSet);
  }
  
  this._client.describe_keyspace(keyspace, onKeyspace);
};

/**
 * Executes a command via the thrift connection
 * @param {String} command The command to execute
 * additional params are supplied to the command to be executed
 */
Client.prototype.execute = function(){
  var args = Array.prototype.slice.apply(arguments),
      command = args.shift();
  
  this._client[command].apply(this._client, args);
};

/**
 * Executes a CQL Query Against the DB.
 * @param {String} cmd A string representation of the query: 'select %s, %s from MyCf where key=%s'
 * @param {arguments} args0...argsN An Array of arguments for the string ['arg0', 'arg1', 'arg2']
 * @param {Function} callback The callback function for the results
 */
Client.prototype.cql = function(cmd){
  var args = Array.prototype.slice.call(arguments, 1),
      callback = args.pop();
  
  if(typeof callback !== 'function'){
    args.push(callback);
    callback = function(){};
  }
  
  args.unshift(cmd);
  var cql = new Buffer(util.format.apply(this, args));
  console.log(cql.toString())
  this.execute('execute_cql_query', cql, ttype.Compression.NONE, callback);
};

//export our client
module.exports = Client;