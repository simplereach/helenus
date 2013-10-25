var time = {};

try {
  var microtime = require('microtime');
  time.microtime = microtime.now;
} catch (e) {
  time.microtime = function() {
    return Date.now() * 1000;
  };
}

module.exports = time;
