# otel-json-to-proto-udf
Apache Flink® UDF to convert the serialization format from OTEL JSON to OTEL Protocol Buffer.
One of the challenges when using Flink is how to handle data with complex schemas. For example, data such as metrics and traces defined in OpenTelemetry often have deeply nested and cyclic schemas, making their structure highly complex. Defining this as a Flink Table Schema is difficult, and expressing a cyclic schema using SQL is not possible. As a result, it leads to errors like "Could not map Schema Registry schema to Flink types." On the other hand, there are cases where OpenTelemetry data is curated as raw JSON and sent to sink destinations like Splunk O11y in OTLP format, serialized as Protobuf. To address such use cases, this provides a UDF that facilitates this conversion.

This code consists of fewer than 50 lines and is fairly simple. It is designed as an introduction to Flink UDFs, with the expectation that it can be customized and used in various environments.

## How to get it running
Build Flink image with the jar file and start Flink sql client:
```bash
./sql-client.sh --jar /tmp/otel-json-to-proto-udf-1.0-SNAPSHOT.jar
```

## Define user-defined functions
Register your UDF with any name of your choise:
```bash
CREATE FUNCTION TO_OTEL_PROTO AS 'com.example.OtelJsonToOtelProto';
```

Create a table to handle otel event data:
```bash
CREATE TABLE `otel` (
  val STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '$TOPIC',
  'properties.bootstrap.servers' = '$BOOTSTRAP_SERVERS',
  'properties.group.id' = 'test',
  'format' = 'raw',
  'scan.startup.mode' = 'earliest-offset'
);
```

Test UDF for checking the serialized data:
```bash
### Metrics
SELECT TO_OTEL_PROTO(val, 'metrics') from `otel`;


### Trace
SELECT TO_OTEL_PROTO(val, 'trace') from `otel`;


### Logs
SELECT TO_OTEL_PROTO(val, 'logs') from `otel`;
```
