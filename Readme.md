
# Helenus

  NodeJS Bindings for Cassandra

  Currently the driver has full CQL support and we are currently adding
  support for additional "non-cql" commands.

  This is very much work in progress, if you would like to contribute, please contact Russ Bradberry &lt;rbradberry@simplereach.com&gt;

## Installation

    npm install helenus

## Usage

```javascript
  var helenus = require('helenus'),
      pool = new helenus.ConnectionPool({
        hosts      : ['localhost:9160'],
        keyspace   : 'helenus_test',
        user       : 'test',
        password   : 'test1233',
        timeout    : 3000
      });

  //if you don't listen for error, it will bubble up to `process.uncaughtException`
  pool.on('error', function(err){
    console.error(err.name, err.message);
  });

  //makes a connection to the pool, this will return once there is at least one
  //valid connection, other connections may still be pending
  pool.connect(function(err, keyspace){
    if(err){
      throw(err);
    } else {
      //keyspace is the object for interacting the the specific column families
      //in the keyspace, it is accessed like this: `keyspace.cf_one.insert(...)`
      //or `keyspace.cf_two.get(...)`, **this is currently being built out**

      //to use cql, access the pool object once connected
      //the first argument is the CQL string, the second is an `Array` of items
      //to interpolate into the format string, the last is the callback
      //for formatting specific see `http://nodejs.org/docs/latest/api/util.html#util.format`
      //results is an array of row objects
      
      pool.cql("SELECT '%s' FROM '%s' WHERE key='%s'", ['col','cf_one','key123'], function(err, results){
        console.log(err, results);
      }); 
    }
  });
```

## Row

The Helenus Row object acts like an array but contains some helper methods to 
make your life a bit easier when dealing with dynamic columns in Cassandra

### row.count

Returns the number of columns in the row

### row[N]

This will return the column at index N

    results.forEach(function(row){
      //gets the 5th column of each row
      console.log(row[5]);
    });
    
### row.get(name)

This will return the column with a specific name

    results.forEach(function(row){
      //gets the column with the name 'foo' of each row
      console.log(row.get('foo'));
    });

### row.slice(start, finish)

Slices columns in the row based on their numeric index, this allows you to get 
columns x through y, it returns a Helenus row object of columns that match the slice.

    results.forEach(function(row){
      //gets the first 5 columns of each row
      console.log(row.slice(0,5));
    });

### row.nameSlice(start, finish)

Slices the columns based on part of their column name. returns a helenus row of columns
that match the slice

    results.forEach(function(row){
      //gets all columns that start with a, b, c, or d
      console.log(row.slice('a','e'));
    });

## License

(The MIT License)

Copyright (c) 2011 SimpleReach &lt;rbradberry@simplereach.com&gt;

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
