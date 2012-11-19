module.exports = {
  "keyspace"     : "helenus_test_keyspace",
  "cf_standard"  : "cf_standard_test",
  "cf_standard_composite"  : "cf_standard_composite_test",
  "cf_supercolumn"  : "cf_supercolumn_test",
  "cf_counter"   : "cf_counter_test",
  "cf_reversed"   : "cf_reversed_test",
  "cf_composite_nested_reversed"   : "cf_composite_nested_reversed_test",
  "cf_invalid"   : "cf_invalid_test",
  "cf_error"     : "<!`~;/?}]|\\-",
  "cf_standard_options"   : {
    "key_validation_class"     : "UTF8Type",
    "comparator_type"          : "UTF8Type",
    "default_validation_class" : "UTF8Type",
    "columns" : [
      { "name" : "bytes-test", "validation_class" : "BytesType" },
      { "name" : "long-test", "validation_class" : "LongType" },
      { "name" : "integer-test", "validation_class" : "IntegerType" },
      { "name" : "utf8-test", "validation_class" : "UTF8Type" },
      { "name" : "ascii-test", "validation_class" : "AsciiType" },
      { "name" : "lexicaluuid-test", "validation_class" : "LexicalUUIDType" },
      { "name" : "timeuuid-test", "validation_class" : "TimeUUIDType" },
      { "name" : "float-test", "validation_class" : "FloatType" },
      { "name" : "double-test", "validation_class" : "DoubleType" },
      { "name" : "date-test", "validation_class" : "DateType" },
      { "name" : "boolean-test", "validation_class" : "BooleanType" },
      { "name" : "uuid-test", "validation_class" : "UUIDType" },
      { "name" : "index-test", "validation_class" : "UTF8Type", "index_type":0 }
    ]
  },
  "cf_standard_composite_options"   : {
    "key_validation_class"     : "CompositeType(UTF8Type, UUIDType)",
    "comparator_type"          : "CompositeType(LongType, DateType)",
    "default_validation_class" : "UTF8Type"
  },
  "cf_supercolumn_options" : {
    "column_type" : "Super",
    "key_validation_class"  : "UTF8Type",
    "default_validation_class" : "UTF8Type",
    "comparator_type" : "UTF8Type",
    "subcomparator_type" : "UTF8Type"
  },
  "cf_counter_options" : {
    "default_validation_class" : "CounterColumnType",
    "key_validation_class" : "UTF8Type",
    "comparator_type" : "UTF8Type"
  },
  "cf_reversed_options": {
    "key_validation_class" : "UTF8Type",
    "default_validation_class" : "UTF8Type",
    "comparator_type" : "TimeUUIDType(reversed=true)"
  },
  "cf_composite_nested_reversed_options": {
    "key_validation_class" : "UTF8Type",
    "default_validation_class" : "UTF8Type",
    "comparator_type" : "CompositeType(TimeUUIDType(reversed=true),UTF8Type)"
  },
  "standard_row_key" : "standard_row_1",
  "standard_insert_values" : {
    "one"   : "a",
    "two"   : "b",
    "three" : "c",
    "four"  : null
  },
  "standard_get_options" : {
    "start"    : "s",
    "end"      : "a",
    "max"      : 1,
    "reversed" : true
  },
  "standard_get_names_options" : {
    "columns"  : ["one","three"]
  },
  "standard_get_options_error" : {
    "start"    : "a",
    "end"      : "z",
    "max"      : 1,
    "reversed" : true
  }
}
