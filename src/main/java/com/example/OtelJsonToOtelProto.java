package com.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.logs.v1.LogsData;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.flink.table.functions.ScalarFunction;
import java.util.function.Supplier;
import com.google.protobuf.Message;

public class OtelJsonToOtelProto extends ScalarFunction {
  private static final String TRACE = "trace";
  private static final String LOGS = "logs";
  private static final String METRICS = "metrics";

  public byte[] eval(String json, String dataType) throws InvalidProtocolBufferException {
    if (json == null || json.isEmpty()) {
      return null;
    }

    if (dataType == null || dataType.isEmpty()) {
      throw new IllegalArgumentException("Data type cannot be null or empty.");
    }

    switch (dataType.toLowerCase()) {
    case TRACE:
      return convert(json, TracesData::newBuilder);
    case LOGS:
      return convert(json, LogsData::newBuilder);
    case METRICS:
      return convert(json, MetricsData::newBuilder);
    default:
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private <T extends Message.Builder> byte[] convert(String json, Supplier<T> builderSupplier) {
    try {
      T builder = builderSupplier.get();
      JsonFormat.parser().merge(json, builder);
      Message message = builder.build();
      return message.toByteArray();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Failed to convert JSON to Proto. DataType: " + builderSupplier.get().getClass().getSimpleName()  + ", Error: " + e.getMessage(), e);
    }
  }
}
