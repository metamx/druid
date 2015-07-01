# Lookups

Lookups are a concept in Druid where dimension values are (optionally) replaced with a new value.

Lookups can be specified as part of the runtime properties file.
 ```json
 druid.query.extraction.namespaceList=\
   [{ "type":"uri", "namespace":"some_uri_lookup","uri": "file:/tmp/prefix/",\
   "namespaceParseSpec":\
     {"type":"csv","columns":["key","value"]},\
   "pollPeriod":"PT5M"},\
   { "type":"jdbc", "namespace":"some_jdbc_lookup",\
   "connectorConfig":{"createTables":true,"connectURI":"jdbc:mysql://localhost:3306/druid","user":"druid","password":"diurd"},\
   "table": "lookupTable", "keyColumn": "mykeyColumn", "valueColumn": "MyValueColumn", "tsColumn": "timeColumn"}]
 ```

## URI lookup update
The remapping values for each lookup can be specified by a lookup update json object as per
```json
{
  "type":"uri",
  "lookup":"some_lookup",
  "uri": "s3://bucket/some/key/prefix/",
  "lookupParseSpec":{
    "type":"csv",
    "columns":["key","value"]
  },
  "pollPeriod":"PT5M",
  "versionRegex": "renames-[0-9]*\\.gz"
}
```

The `pollPeriod` value specifies the period in ISO 8601 format between checks for updates. If the source of the lookup is capable of providing a timestamp, the lookup will only be updated if it has changed since the prior tick of `pollPeriod`. A value of 0, an absent parameter, or `null` all mean populate once and do not attempt to update. Whenever an update occurs, the updating system will look for a file with the most recent timestamp and assume that one with the most recent data.

The `versionRegex` value specifies a regex to use to determine if a filename in the parent path of the uri should be considered when trying to find the latest version. Omitting this setting or setting it equal to `null` will match to all files it can find (equivalent to using `".*"`). The search occurs in the most significant "directory" of the uri.

The lookupParseSpec can be one of a number of values. Each of the examples below would rename foo to bar, baz to bat, and buck to truck. All parseSpec types assumes each input is delimited by a new line.

Data on lookups can be acquired through the `/druid/coordinator/v1/lookups` endpoint on the coordinator. `GET` returns all known lookups. `POST` will submit a new lookup with update information. And `DELETE` against `/druid/coordinator/v1/lookups/{lookup}` will remove a lookup.

### csv lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|

*example input*
```
bar,something,foo
bat,something2,baz
truck,something3,buck
```

*example lookupParseSpec*
```json
"lookupParseSpec": {
  "type": "csv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value"
}
```

### tsv lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|
|`delimiter`|The delimiter in the file|no|tab (`\t`)|


*example input*
```
bar|something,1|foo
bat|something,2|baz
truck|something,3|buck
```

*example lookupParseSpec*
```json
"lookupParseSpec": {
  "type": "tsv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value",
  "delimiter": "|"
}
```

### customJson lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`keyFieldName`|The field name of the key|yes|null|
|`valueFieldName`|The field name of the value|yes|null|

*example input*
```json
{"key": "foo", "value": "bar", "somethingElse" : "something"}
{"key": "baz", "value": "bat", "somethingElse" : "something"}
{"key": "buck", "somethingElse": "something", "value": "truck"}
```

*example lookupParseSpec*
```json
"parseSpec": {
  "type": "customJson",
  "keyFieldName": "key",
  "valueFieldName": "value"
}
```


### simpleJson lookupParseSpec
The `simpleJson` lookupParseSpec does not take any parameters. It is simply a line delimited json file where the field is the key, and the field's value is the value.

*example input*
 
```json
{"foo": "bar"}
{"baz": "bat"}
{"buck": "truck"}
```

*example lookupParseSpec*
```json
"lookupParseSpec":{
  "type": "simpleJson"
}
```


## JDBC lookup update

The JDBC lookups will pull from a database cache. If the `tsColumn` is set it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. For example, the following must be valid sql for the table `SELECT * FROM some_lookup_table WHERE timestamp_column >  '2015-01-01 00:00:00'`. If `tsColumn` is set, the caching service will attempt to only pull values that were written *after* the last sync. If `tsColumn` is not set, the entire table is pulled every time.

```json
{
  "type":"jdbc",
  "lookup":"some_lookup",
  "connectorConfig":{
    "createTables":true,
    "connectURI":"jdbc:mysql://localhost:3306/druid",
    "user":"druid",
    "password":"diurd"
  },
  "table":"some_lookup_table",
  "keyColumn":"the_old_dim_value",
  "valueColumn":"the_new_dim_value",
  "tsColumn":"timestamp_column",
  "updateMs":600000
}
```

# Kafka lookup update (Experimental)
It is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8). This requires the following extension: "io.druid.extensions:druid-dim-rename-kafka8"
```json
{
  "type":"kafka",
  "lookup":"testTopic",
  "kafkaTopic":"testTopic"
}
```
## Kafka renames
The extension `druid-dim-rename-kafka8` enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

Currently the historical node caches the key/value pairs from the kafka feed in an ephemeral memory mapped DB via MapDB.

## Configuration
The following options are used to define the behavior:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.rename.kafka.properties`|A json map of kafka consumer properties. See below for special properties.|See below|

The following are the handling for kafka consumer properties in `druid.query.rename.kafka.properties`

|Property|Description|Default|
|--------|-----------|-------|
|`zookeeper.connect`|Zookeeper connection string|`localhost:2181/kafka`|
|`group.id`|Group ID, auto-assigned for publish-subscribe model and cannot be overridden|`UUID.randomUUID().toString()`|
|`auto.offset.reset`|Setting to get the entire kafka rename stream. Cannot be overridden|`smallest`|

## Testing the kafka rename functionality
To test this setup, you can send key/value pairs to a kafka stream via the following producer console
`./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic`
Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter)
