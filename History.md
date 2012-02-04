
0.0.1 / 2011-12-22
==================

  * Initial release

0.1.0 / 2012-01-04
==================

  * Added CQL Support
  * More Robust Connection Pooling
  * Respond with JS errors, not TException objects
  * Added proper serialization/deserialization support for all types
  
0.1.1 / 2012-01-06
==================

  * Fixed issue with the deserialization of column names in inspect
  
0.1.2 / 2012-01-09
==================
  
  * Better in-line documentation
  * Added JSDoc HTML documentation
  * Added keyspace object for referencing a keyspace
  * Added ability to create keyspaces
  * Added ability to drop keyspaces
  * Made ConnectionPool act more like connection
  * Added ability to create column families
  * Added ability to drop column families
  * Added ability to get a row, or part of a row
  
0.1.3 / 2012-01-11
==================

  * Better code coverage
  * Better documentation
  * Bug Fixes in serialization/deserialization
  * Tests for serialization/deserialization
  * Support for non default column values in column families
  
0.2.0 / 2012-01-12
==================

  * Added more test coverage
  * Added support for composite columns
  * Added support for composite keys
  * Fixed some deserialization issues
  
0.2.1 / 2012-01-13
==================

  * Fixed a buffer overflow issue when encoding numbers as UTF8
  
0.2.2 / 2012-01-16
==================

  * Added ability to specify column names in "get"

0.2.3 / 2012-02-04
==================

  * Fix issue in binary serialization, issue #1