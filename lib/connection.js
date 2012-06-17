
var util = require('util'),
    thrift = require('helenus-thrift'),
    Cassandra = require('./cassandra/Cassandra'),
    ttype = require('./cassandra/cassandra_types'),
    Row = require('./row'),
    zlib = require('zlib'),
    Keyspace = require('./keyspace');

/**
 * A No-Operation function for default callbacks
 * @private
 * @constant
 * @memberOf Connection
 */
var NOOP = function(){};

/**
 * Default port for cassandra
 * @private
 * @constant
 * @memberOf Connection
 */
var DEFAULT_PORT = 9160;

/**
 * Default host
 * @private
 * @constant
 * @memberOf Connection
 */
var DEFAULT_HOST = 'localhost';

/**
 * Default Timeout
 * @private
 * @constant
 * @memberOf Connection
 */
var DEFAULT_TIMEOUT = 4000;

/**
 * Creates a JS Error from a Cassndra Error
 * @param {Object} err The cassandra error eg: { name:'Exception', why:'Some Reason' }
 * @private
 * @memberOf Connection
 */
function createError(err){
  //sometimes the message comes back as the field why and sometimes as the field message
  var error = new Error(err.why || err.message);
  error.name = 'Helenus' + err.name;
  Error.captureStackTrace(error, createError);
  return error;
}

/**
 * Formats CQL properly, paradigm borrowed from node-mysql:
 * https://github.com/felixge/node-mysql/blob/master/lib/client.js#L145-199
 *
 * `params` is an Array of positional parameters that replace '?', '#', '%s',
 * '%d' and '%j' with it's values. '#' parameters are double-quoted and all
 * others are single-quoted. `boolean` and `number` values are not quoted at
 * all.
 *
 * Named parameters is an object that replaces named place holders that begin
 * with ':'. If the place holder ends with ':', then the value will be wrapped
 * in single quotes, otherwise it will be wrapped in double quotes. In cql3
 * values should be single quoted and all other identifiers should be double
 * quoted. In cql2, everything should be single-quoted. `boolean` and `number`
 * values are not quoted at all.
 *
 * If you only have named_params, you can use it as the second argument.
 *
 * Example using named parameters in cql3:
 *
 * formatCQL
 *   ( "SELECT :col1, :col2 FROM :cf WHERE :col1 = :val1: and :col3 = :val3:"
 *   , { "col1": "name"
 *     , "col2": "value"
 *     , "col3": "type"
 *     , "cf"  : "MyColumnFamily"
 *     , "val1": "age"
 *     , "val3": "number" } )
 *
 *   > SELECT "name", "value" FROM "MyColumnFamily" WHERE "name" = 'age' AND
 *                                                        "type" = 'number'
 *
 * The same example using positional parameters in cql3::
 *
 * formatCQL
 *   ( "SELECT #, # FROM # WHERE # = ? and # = ?"
 *   , [ "name"
 *     , "value"
 *     , "MyColumnFamily"
 *     , "name"
 *     , "age"
 *     , "type"
 *     , "number" ] )
 *
 * @param {String} cql
 * @param {Array} params
 * @param {Object} named_params
 * @private
 * @memberOf Connection
 * @returns {String} The formatted CQL statement
 */
function formatCQL(cql, params, named_params){
  if (!named_params) {
    if (!Array.isArray(params)) {
      named_params = params;
      params = [];
    } else {
      named_params = {};
    }
  }

  //replace a %% with a % to maintain backward compatibility with util.format
  cql = cql.replace(/%%/, '%');

  //remove existing quotes around parameters in case the user has already wrapped them
  cql = cql.replace(/'(#|\?|%[sjd]|:[^\s,]+)'/g, '$1');

  //escape the params and format the CQL string
  cql = cql.replace(/#|\?|%[sjd]|:[^\s,]+/g, function(place) {
    if (place.match(/^:/)) {
      var quote = place.match(/:$/) ? "'" : '"';

      place = place.replace(/:/g, '');
      if (!named_params.hasOwnProperty(place)) {
        throw createError(new Error('Named Parameter Missing :'+place));
      }

      return escapeCQL(named_params[place], quote);
    } else {
      if (params.length === 0) {
        throw createError(new Error('Too Few Parameters Given'));
      }

      return escapeCQL(params.shift(), place == '#' ? '"' : "'");
    }
  });

  if (params.length) {
    throw createError(new Error('Too Many Parameters Given'));
  }

  return cql;
}

/**
 * Escapes CQL, adapted from node-mysql
 * @param {String} val   The value to be escaped
 * @param {String} quote The quote character. If unspecified, single-quote
 *                       will be used.
 * @private
 * @memberOf Connection
 * @returns {String} The sanitized string
 */
function escapeCQL(val, quote) {
  if (quote === undefined || quote === null) {
    quote = '"';
  }
  if (val === undefined || val === null) {
    return 'NULL';
  }

  if(val instanceof Buffer){
    return val.toString('hex');
  }

  if(typeof val === 'boolean' || typeof val === 'number'){
    return val.toString();
  }

  if (Array.isArray(val)) {
    var sanitized = val.map( function( v ) { return escapeCQL( v ); } );
    return quote + sanitized.join(quote + "," + quote) + quote;
  }

  if (typeof val === 'object') {
    val = (typeof val.toISOString === 'function') ? val.toISOString() : val.toString();
  }

  if (quote == "'") {
    val = val.replace(/\'/img, "''");
  } else {
    // Protect against injection attacks
    if (val.indexOf(quote) >= 0) {
      throw new Error("Illegal quote character in value " + val)
    }
  }

  return quote + val + quote;
}

/**
 * The Cassandra Connection
 *
 * @param {Object} options The options for the connection defaults to:
     {
       port: 9160,
       host: 'localhost'
       user: null,
       password: null,
       keyspace: null,
       timeout: 1000
     }
 * @constructor
 * @exec
 */
var Connection = function(options){
  if(!options.port && options.host && options.host.indexOf(':') > -1){
    var split = options.host.split(':');
    options.host = split[0];
    options.port = split[1];
  }

  /**
   * The port to connect to
   * @default 9160
   */
  this.port = options.port || DEFAULT_PORT;

  /**
   * The host to connect to
   * @default localhost
   */
  this.host = options.host || DEFAULT_HOST;

  /**
   * The timeout for the connection
   * @default 3000
   */
  this.timeout = options.timeout || DEFAULT_TIMEOUT;

  /**
  * The username to authenticate with
  */
  this.user = options.user;

  /**
   * The password to connect with
   */
  this.password = options.password;

  /**
   * The keyspace to authenticate to
   */
  this.keyspace = options.keyspace;

  /**
   * The CQL version.
  *
   * - Cassandra 1.0 supports CQL 2.0.0
   * - Cassandra 1.1 supports CQL 2.0.0 and 3.0.0 (with 2.0.0 the default)
   * - Cassandra 1.2 will have CQL 3.0.0 as the default
   *
   * Cassandra will support choosing the CQL version for a while,
   * @see https://issues.apache.org/jira/browse/CASSANDRA-3990
   */
  this.cqlVersion = options.cqlVersion;

  /**
   * Ready state of the client
   */
  this.ready = false;
};
util.inherits(Connection, process.EventEmitter);

/**
 * Connects to the cassandra cluster
 */
Connection.prototype.connect = function(callback){
  var self = this, timer;

  //set callback to a noop to prevent explosion
  callback = callback || NOOP;

  /**
   * Thrift Connection
   */
  this._connection = thrift.createConnection(this.host, this.port);

  self._connection.on('error', function(err){
    clearTimeout(timer);
    callback(err);
  });

  //if we close we don't want ot be ready anymore, and emit it as an error
  this._connection.on('close', function(){
    clearTimeout(timer);
    self.ready = false;
    self.emit('close');
  });

  /**
   * Thrift Client
   */
  this._client = thrift.createClient(Cassandra, this._connection);

  /**
   * Handles what happens when we set the CQL version for the current
   * connection
   *
   * @private
   * @param {Error} err A cassandra error if cql version couldn't be set.
   */
  function onCqlVersionSelected() {
    //set the state to ready
    //@TODO shouldn't this better be done after self.use()? @ctavan
    self.ready = true;

    // if keyspace is specified, use that ks
    if (self.keyspace !== undefined){
      self.use(self.keyspace, callback);
    } else {
      callback();
    }
  }

  /**
   * Handles what happens when we connect to the cassandra cluster
   *
   * @private
   * @param {Error} err A connection error with cassandra
   */
  function onAuthentication(err) {
    clearTimeout(timer);

    if (err){
      self._connection.connection.destroy();
      callback(createError(err));
      return;
    }

    if (self.cqlVersion) {
      self._client.set_cql_version(self.cqlVersion, function(err) {
        if (err) {
          self._connection.connection.destroy();
          callback(createError(err));
          return;
        }
        onCqlVersionSelected();
      });
      return;
    }
    onCqlVersionSelected();
  }

  //after we connect, we authenticate
  this._connection.on('connect', function(err){
    if(err){
      callback(err);
    } else {
      //bubble up all errors
      self._connection.removeAllListeners('error');
      self._connection.on('error', function(err){
        self.emit('error', err);
      });

      self.authenticate(onAuthentication);
    }
  });

  timer = setTimeout(function(){
    callback(createError({ name: 'TimeoutException', why: 'Connection Timed Out'}));
    self._connection.connection.destroy();
  }, this.timeout);
};

/**
 * Sets the current keyspace
 *
 * @param {String} keyspace The keyspace to use
 */
Connection.prototype.use = function(keyspace, callback){
  var self = this;
  callback = callback || NOOP;

  function onDescribe(err, definition){
    if (err) {
      callback(createError(err));
      return;
    }

    self._client.set_keyspace(keyspace, function(err){
      if(err){
        callback(createError(err));
      } else {
        callback(null, new Keyspace(self, definition));
      }
    });
  }

  this._client.describe_keyspace(keyspace, onDescribe);
};

/**
 * Authenticates the user
 */
Connection.prototype.authenticate = function(callback){
  callback = callback || NOOP;
  var self = this;

  if(this.user || this.password){
    var credentials = {username: this.user, password: this.password},
        authRequest = new ttype.AuthenticationRequest({ credentials: credentials });

    self._client.login(authRequest, function(err){
      if (err){
        callback(createError(err));
      } else {
        callback(null);
      }
    });
  } else {
    callback();
  }
};

/**
 * Executes a command via the thrift connection
 * @param {String} command The command to execute
 * additional params are supplied to the command to be executed
 */
Connection.prototype.execute = function(){
  var args = Array.prototype.slice.apply(arguments),
      command = args.shift(),
      callback = args.pop();

  if(typeof callback !== 'function'){
    args.push(callback);
    callback = NOOP;
  }

  /**
   * Processes the return results of the query
   * @private
   */
  function onReturn(err, results){
    if(err){
      callback(createError(err));
    } else {
      callback(null, results);
    }
  }

  args.push(onReturn);

  this._client[command].apply(this._client, args);
};

/**
 * Executes a CQL Query Against the DB.
 *
 * If there is one Object argument, it will be used for options. To use
 * named_params, you *must* pass a subsequent object for options, even
 * if it's empty({}).
 *
 * See formatCQL for parameter format details.
 *
 * @param {String} cmd A string representation of the query: 'select %s, %s from MyCf where key=%s'
 * @param {Array} params0...paramsN An Array of arguments for the string ['arg0', 'arg1', 'arg2']
 * @param {Object} named_params An object of named arguments for the string {val0: 'arg0', val1: 'arg1'}
 * @param {Object} options An object with options for the query, { gzip:true }
 * @param {Function} callback The callback function for the results
 */
Connection.prototype.cql = function() {
  var args = Array.prototype.slice.apply(arguments);

  var callback
    , options
    , named_params
    , params
    , cmd;


  /**
   * Parses cql() arguments
   */
  function next_arg() {
    var next = args.pop();

    if (typeof next === 'function') {
      // callback is the only function argument
      if (callback) {
        throw new Error('Mutliple function arguments passed');
      }
      callback = next;
    } else if (Array.isArray(next)) {
      // params is the only Array argument
      if (params) {
        throw new Error('Multiple Array arguments passed');
      }
      params = next;
    } else if (typeof next === 'object') {
      // named_params and options. If only one object is passed, it will be
      // options for backward compatibility. If two are passed then the first
      // is named_params and the second is options.
      if (!options) {
        options = next;
      } else {
        if (named_params) {
          throw new Error('More than two object arguments passed');
        }
        named_params = next;
      }
    } else if (typeof next === 'string') {
      // cmd is the only string argument
      if (cmd) {
        throw new Error('Multiple string arguments passed');
      }
      cmd = next;
    } else {
      // whoops! What the heck di you pass us?
      throw new Error('Invalid argument type passed: '+ typeof next);
    }

    if (args.length > 0) {
      next_arg();
    }
  }
  next_arg();

  if(options === undefined || options === null){
    options = {};
  }

  //in case a callback is not supplied
  if(typeof callback !== 'function'){
    callback = NOOP;
  }

  var cql, escaped = [], self = this;

  if(params || named_params){
    cql = new Buffer(formatCQL(cmd, params, named_params));
  } else {
    cql = new Buffer(cmd);
  }

  function onReturn(err, res){
    if (err){
      callback(err);
      return;
    }

    if(res.type === ttype.CqlResultType.ROWS){
      var rows = [], i = 0, rowlength = res.rows.length;
      for(; i < rowlength; i += 1){
        rows.push(new Row(res.rows[i], res.schema));
      }
      callback(null, rows);
    } else if(res.type === ttype.CqlResultType.INT){
      callback(null, res.num);
    } if (res.type === ttype.CqlResultType.VOID) {
      callback(null);
    }
  }

  if(options.gzip === true){
    zlib.deflate(cql, function(err, cqlz){
      self.execute('execute_cql_query', cqlz, ttype.Compression.GZIP, onReturn);
    });
  } else {
    self.execute('execute_cql_query', cql, ttype.Compression.NONE, onReturn);
  }
};

/**
 * Creates a keyspace
 *
 * @param {String} keyspace The name of the keyspace to create
 * @param {Object} options Keyspace options
 * @param {Function} callback The callback to invoke once complete
 */
Connection.prototype.createKeyspace = function(keyspace, options, callback){
  if(typeof options === 'function'){
    callback = options;
    options = {};
  }

  callback = callback || NOOP;
  options = options || {};

  if(!keyspace){
    callback(createError({name:'InvalidNameError', why:'Keyspace name not specified'}));
    return;
  }

  var args = {
    name:keyspace,
    strategy_class: options.strategyClass || 'SimpleStrategy',
    strategy_options: options.strategyOptions || {},
    replication_factor: options.replication || 1,
    durable_writes: options.durable || true,
    cf_defs: []
  };
  if (args.strategy_class === 'SimpleStrategy' &&
      !args.strategy_options.replication_factor) {
    args.strategy_options.replication_factor = '' + args.replication_factor;
  }
  var ksdef = new ttype.KsDef(args);

  /**
   * Once finished, fix the error if needed
   */
  function onComplete(err, response){
    if(err){
      callback(createError(err));
    } else {
      callback(null, response);
    }
  }

  this._client.system_add_keyspace(ksdef, onComplete);
};

/**
 * Drops a keyspace
 ** @param {String} name The keyspace name
 */
Connection.prototype.dropKeyspace = function(keyspace, callback){
  callback = callback || NOOP;

  if (typeof keyspace !== 'string'){
    callback({name:'InvalidNameError', why:'Keyspace name not specified'});
  }

  function onComplete(err, response){
    if(err){
      callback(createError(err));
    } else {
      callback(null, response);
    }
  }
  this._client.system_drop_keyspace(keyspace, onComplete);
};


/**
 * Closes the connection to the server
 */
Connection.prototype.close = function(){
  this._connection.end();
};
//export our client
module.exports = Connection;
