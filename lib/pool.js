var Connection = require('./connection'),
    util = require('util'),
    errors = require('./errors');

/**
 * A No-Operation default for empty callbacks
 * @private
 * @memberOf Pool
 */
var NOOP = function(){};


var replyNotAvailable = function (callback) {

    if(typeof callback === 'function'){
        callback(errors.create({ why  : 'Could Not Connect To Any Nodes',
                                 name : 'NoAvailableNodesException'  }));
    }
};

/**
 * Normalizes (flattens) the rules
 */
function normalizeRules(denormrules){
  //flatten rules to simplify matching
  var rules = [];
  denormrules.forEach(function(rule){
    if(rule.datacenter){
      if(rule.racks){
        rule.racks.forEach(function(rack){
          if(rule.hosts){
            rule.hosts.forEach(function(host){
              rules.push({ datacenter: rule.datacenter, rack: rack, host: host});
            });
          } else {
            rules.push({ datacenter: rule.datacenter, rack: rack});
          }
        });
      } else if(rule.hosts){
        rule.hosts.forEach(function(host){
          rules.push({ host: host });
        });
      } else {
        rules.push({ datacenter: rule.datacenter });
      }
    } else if (rule.hosts) {
      rule.hosts.forEach(function(host){
        rules.push({ host: host });
      });
    }
  });
  return rules;
}


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
 *     cqlVersion : '3.0.0',
 *     hostPoolSize : 1,
 *     autoDiscover: true, *
 *     whitelist: [
 *         { datacenter: 'cassandra-east', 'racks': ['rack1'], hosts:['10.10.10.10'] }
 *     ],
 *     blacklist: [
 *       { datacenter: 'cassandra-east', 'racks': ['rack1'], hosts:['10.10.10.10'] }
 *     ]
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
  this.hostPoolSize = options.hostPoolSize ? options.hostPoolSize : 1;
  this.retryInterval = null;
  this.closing = false;
  if (options.autoDiscover){
    this.autoDiscover = true;
    if(options.whitelist && options.blacklist){
      throw errors.create({ why  : 'Please either either whitelist OR blacklist, not both',
                            name : 'InvalidOptionsException'  });
    }

    if (Array.isArray(options.whitelist)){
      this.rules = options.whitelist;
      this.ruleType = 'whitelist';
    } else if (Array.isArray(options.blacklist)){
      this.rules = options.blacklist;
      this.ruleType = 'blacklist';
    } else {
      this.rules = [];
      this.ruleType = 'all';
    }
  }


  if(!options.hosts && options.host){
    this.hosts = [options.host];
  } else {
    this.hosts = options.hosts;
  }

  if (!this.hosts){
    this.hosts = ['localhost'];
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
  var i = 0, finished = 0, self = this,
      len = this.hosts.length * this.hostPoolSize,
      connected = 0;

  callback = callback || NOOP;

  function onConnect(err, connection, keyspace, host){
    finished += 1;

    if (err){
      self.dead.push(host);
    } else {
      connected += 1;
      if(keyspace){
        keyspace.connection = self;
      }
      self.clients.push(connection);
      if (connected === 1) {
        callback(null, keyspace);
      }
      if (self.closing) {
        connection.close();
      }
    }

    if(finished === len){
      // if there are no clients, we haven't called back and are not available
      if(connected === 0){
        replyNotAvailable(callback);
      }

      //now that we have a connection, lets start monitoring
      self.monitorConnections();
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

  for(; i < this.hosts.length; i += 1){
    for(var j=0; j < this.hostPoolSize; j +=1){
        connect(this.hosts[i]);
    }
  }
};

/**
 * After connecting to a seed host, auto-detect the cluster and connect to the remaining nodes
 */
Pool.prototype.discover = function(callback){
  var self = this,
      rules = normalizeRules(this.rules);

  this.execute('describe_ring', this.keyspace, function(err, details){
    if(err){
      return callback(err);
    }

    //use an object to ensure unique values
    var hosts = {};

    details.forEach(function(tokenRange){
      tokenRange.endpoint_details.forEach(function(host){
        if (self.ruleType !== 'all'){
          var match = false, i = 0, rule;

          for (; i < rules.length; i += 1){
            rule = rules[i];
            if(rule.host){
              if(rule.host === host.host){
                match = true;
                break;
              }
            } else {
              if(rule.datacenter && host.datacenter === rule.datacenter){
                if(rule.rack){
                  if(rule.rack === host.rack){
                    match = true;
                    break;
                  }
                } else {
                  match = true;
                  break;
                }
              }
            }
          }

          if(match && self.ruleType === 'whitelist'){
            hosts[host.host] = '';
          }

          if (!match && self.ruleType === 'blacklist'){
            hosts[host.host] = '';
          }
        } else {
          hosts[host.host] = '';
        }
      });
    });

    callback(null, Object.keys(hosts));
  });
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
      conn = this.getConnection(),
      callback;

  if (!conn) {
    callback = args.pop();
    replyNotAvailable(callback);
  } else {
    conn.execute.apply(conn, args);
  }

};

/**
 * Executes a CQL Query Against the DB.
 * @param {String} cmd A string representation of the query: 'select %s, %s from MyCf where key=%s'
 * @param {arguments} args0...argsN An Array of arguments for the string ['arg0', 'arg1', 'arg2']
 * @param {Function} callback The callback function for the results
 */
Pool.prototype.cql = function(){
  var args = Array.prototype.slice.apply(arguments),
      conn = this.getConnection(),
      callback;

  if(!conn) {
    callback = args.pop();
    replyNotAvailable(callback);
  } else {
    conn.cql.apply(conn, args);
  }
};

/**
 * Gets a random connection from the connection pool
 */
Pool.prototype.getConnection = function(){
  var len = this.clients.length,
      rnd = Math.floor(Math.random() * len),
      host = this.clients[rnd];

  if (!host){
    this.emit('error', errors.create({ why  : 'No Available Connections',
                                       name : 'NoAvailableNodesException' }));
    return;
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

  if (!conn) {
    if (typeof options === 'function') {
      callback = options;
    }
    replyNotAvailable(callback);
  } else {
    conn.createKeyspace.call(conn, name, options, callback);
  }
};

/**
 * Creates a keyspace see Connection.prototype.createKeyspace
 */
Pool.prototype.dropKeyspace = function(name, callback){
  var conn = this.getConnection();

  if(!conn){
    replyNotAvailable(callback);
  } else {
    conn.dropKeyspace.call(conn, name, callback);
  }
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

    if(self.closing) {
      return;
    }

    if (self.autoDiscover){
      self.discover(function(err, hosts){
        if(err){
          self.emit('error', err);
        }

        //add new hosts
        hosts.forEach(function(host){
          if(!(host in self.hosts) && !(host in self.dead)){
            connect(host);
          }
        });

        //remove hosts that are no longer in the ring
        //they should be dead, so no need to go over the live hosts
        var validDead = [];
        self.dead.forEach(function(host){
          if(host in hosts){
            validDead.push(host);
          }
        });
        self.dead = validDead;

        if(self.dead.length > 0){
          connect(self.dead.pop());
          checkDead();
        }
      });
    } else {
      if(self.dead.length > 0){
        connect(self.dead.pop());
        checkDead();
      }
    }
  }

  this.retryInterval = setInterval(checkDead, 15000);
  //do it once at the beginning
  checkDead();
};

/**
 * Closes all open connections
 */
Pool.prototype.close = function(){
  var self = this, i = 0, j = 0, len = this.clients.length;

  // Make sure no intervals get set
  self.closing = true;

  clearInterval(this.retryInterval);

  if (len === 0){
    this.emit('close');
  }

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
