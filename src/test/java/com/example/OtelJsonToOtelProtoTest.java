package com.example;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.logs.v1.LogsData;
import io.opentelemetry.proto.trace.v1.TracesData;

public class OtelJsonToOtelProtoTest {

    private final OtelJsonToOtelProto udf = new OtelJsonToOtelProto();

    @Test
    public void testMetricsValidJson() throws Exception {
	String json = "{\n" +
                "  \"resourceMetrics\": [\n" +
                "    {\n" +
                "      \"resource\": {\n" +
                "        \"attributes\": [\n" +
                "          {\n" +
                "            \"key\": \"service.name\",\n" +
                "            \"value\": {\n" +
                "              \"stringValue\": \"my.service\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"scopeMetrics\": [\n" +
                "        {\n" +
                "          \"scope\": {\n" +
                "            \"name\": \"my.library\",\n" +
                "            \"version\": \"1.0.0\",\n" +
                "            \"attributes\": [\n" +
                "              {\n" +
                "                \"key\": \"my.scope.attribute\",\n" +
                "                \"value\": {\n" +
                "                  \"stringValue\": \"some scope attribute\"\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"metrics\": [\n" +
                "            {\n" +
                "              \"name\": \"my.counter\",\n" +
                "              \"unit\": \"1\",\n" +
                "              \"description\": \"I am a Counter\",\n" +
                "              \"sum\": {\n" +
                "                \"aggregationTemporality\": 1,\n" +
                "                \"isMonotonic\": true,\n" +
                "                \"dataPoints\": [\n" +
                "                  {\n" +
                "                    \"asDouble\": 5,\n" +
                "                    \"startTimeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"timeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"attributes\": [\n" +
                "                      {\n" +
                "                        \"key\": \"my.counter.attr\",\n" +
                "                        \"value\": {\n" +
                "                          \"stringValue\": \"some value\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                ]\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        byte[] ret = udf.eval(json, "metrics");
        assertNotNull(ret);

	MetricsData.Builder builder = MetricsData.newBuilder();
        JsonFormat.parser().merge(json, builder);
        assertArrayEquals(builder.build().toByteArray(), ret);

        MetricsData md = MetricsData.parseFrom(ret);
	assertEquals("service.name", md.getResourceMetrics(0).getResource().getAttributes(0).getKey());
	assertEquals("my.service", md.getResourceMetrics(0).getResource().getAttributes(0).getValue().getStringValue());
    }

    @Test
    public void testLogsValidJson() throws Exception {
        String json = "{\n" +
                "  \"resourceLogs\": [\n" +
                "    {\n" +
                "      \"resource\": {\n" +
                "        \"attributes\": [\n" +
                "          {\n" +
                "            \"key\": \"service.name\",\n" +
                "            \"value\": {\n" +
                "              \"stringValue\": \"my.service\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"scopeLogs\": [\n" +
                "        {\n" +
                "          \"scope\": {\n" +
                "            \"name\": \"my.library\",\n" +
                "            \"version\": \"1.0.0\",\n" +
                "            \"attributes\": [\n" +
                "              {\n" +
                "                \"key\": \"my.scope.attribute\",\n" +
                "                \"value\": {\n" +
                "                  \"stringValue\": \"some scope attribute\"\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"logRecords\": [\n" +
                "            {\n" +
                "              \"timeUnixNano\": \"1544712660300000000\",\n" +
                "              \"observedTimeUnixNano\": \"1544712660300000000\",\n" +
                "              \"severityNumber\": 10,\n" +
                "              \"severityText\": \"Information\",\n" +
                "              \"traceId\": \"5B8EFFF798038103D269B633813FC60C\",\n" +
                "              \"spanId\": \"EEE19B7EC3C1B174\",\n" +
                "              \"body\": {\n" +
                "                \"stringValue\": \"Example log record\"\n" +
                "              },\n" +
                "              \"attributes\": [\n" +
                "                {\n" +
                "                  \"key\": \"string.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"stringValue\": \"some string\"\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"key\": \"boolean.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"boolValue\": true\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"key\": \"int.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"intValue\": \"10\"\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"key\": \"double.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"doubleValue\": 637.704\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"key\": \"array.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"arrayValue\": {\n" +
                "                      \"values\": [\n" +
                "                        {\n" +
                "                          \"stringValue\": \"many\"\n" +
                "                        },\n" +
                "                        {\n" +
                "                          \"stringValue\": \"values\"\n" +
                "                        }\n" +
                "                      ]\n" +
                "                    }\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"key\": \"map.attribute\",\n" +
                "                  \"value\": {\n" +
                "                    \"kvlistValue\": {\n" +
                "                      \"values\": [\n" +
                "                        {\n" +
                "                          \"key\": \"some.map.key\",\n" +
                "                          \"value\": {\n" +
                "                            \"stringValue\": \"some value\"\n" +
                "                          }\n" +
                "                        }\n" +
                "                      ]\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        byte[] ret = udf.eval(json, "logs");
        assertNotNull(ret);

	LogsData.Builder builder = LogsData.newBuilder();
        JsonFormat.parser().merge(json, builder);
        assertArrayEquals(builder.build().toByteArray(), ret);

        LogsData md = LogsData.parseFrom(ret);
	assertEquals("service.name", md.getResourceLogs(0).getResource().getAttributes(0).getKey());
	assertEquals("my.service", md.getResourceLogs(0).getResource().getAttributes(0).getValue().getStringValue());
    }


    @Test
    public void testTraceValidJson() throws Exception {
	String json = "{\n" +
                "  \"resourceSpans\": [\n" +
                "    {\n" +
                "      \"resource\": {\n" +
                "        \"attributes\": [\n" +
                "          {\n" +
                "            \"key\": \"service.name\",\n" +
                "            \"value\": {\n" +
                "              \"stringValue\": \"my.service\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"scopeSpans\": [\n" +
                "        {\n" +
                "          \"scope\": {\n" +
                "            \"name\": \"my.library\",\n" +
                "            \"version\": \"1.0.0\",\n" +
                "            \"attributes\": [\n" +
                "              {\n" +
                "                \"key\": \"my.scope.attribute\",\n" +
                "                \"value\": {\n" +
                "                  \"stringValue\": \"some scope attribute\"\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"spans\": [\n" +
                "            {\n" +
                "              \"traceId\": \"5B8EFFF798038103D269B633813FC60C\",\n" +
                "              \"spanId\": \"EEE19B7EC3C1B174\",\n" +
                "              \"parentSpanId\": \"EEE19B7EC3C1B173\",\n" +
                "              \"name\": \"I'm a server span\",\n" +
                "              \"startTimeUnixNano\": \"1544712660000000000\",\n" +
                "              \"endTimeUnixNano\": \"1544712661000000000\",\n" +
                "              \"kind\": 2,\n" +
                "              \"attributes\": [\n" +
                "                {\n" +
                "                  \"key\": \"my.span.attr\",\n" +
                "                  \"value\": {\n" +
                "                    \"stringValue\": \"some value\"\n" +
                "                  }\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        byte[] ret = udf.eval(json, "trace");
        assertNotNull(ret);

	TracesData.Builder builder = TracesData.newBuilder();
        JsonFormat.parser().merge(json, builder);
        assertArrayEquals(builder.build().toByteArray(), ret);

        TracesData md = TracesData.parseFrom(ret);
	assertEquals("service.name", md.getResourceSpans(0).getResource().getAttributes(0).getKey());
	assertEquals("my.service", md.getResourceSpans(0).getResource().getAttributes(0).getValue().getStringValue());
    }


    @Test
    public void testInvalidJson() {
        String invalidJson = "{\"invalid\"}";
        assertThrows(RuntimeException.class, () -> udf.eval(invalidJson, "metrics"));
    }

    @Test
    public void testUnsupportedDataType() {
	String json = "{\n" +
                "  \"resourceMetrics\": [\n" +
                "    {\n" +
                "      \"resource\": {\n" +
                "        \"attributes\": [\n" +
                "          {\n" +
                "            \"key\": \"service.name\",\n" +
                "            \"value\": {\n" +
                "              \"stringValue\": \"my.service\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"scopeMetrics\": [\n" +
                "        {\n" +
                "          \"scope\": {\n" +
                "            \"name\": \"my.library\",\n" +
                "            \"version\": \"1.0.0\",\n" +
                "            \"attributes\": [\n" +
                "              {\n" +
                "                \"key\": \"my.scope.attribute\",\n" +
                "                \"value\": {\n" +
                "                  \"stringValue\": \"some scope attribute\"\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"metrics\": [\n" +
                "            {\n" +
                "              \"name\": \"my.counter\",\n" +
                "              \"unit\": \"1\",\n" +
                "              \"description\": \"I am a Counter\",\n" +
                "              \"sum\": {\n" +
                "                \"aggregationTemporality\": 1,\n" +
                "                \"isMonotonic\": true,\n" +
                "                \"dataPoints\": [\n" +
                "                  {\n" +
                "                    \"asDouble\": 5,\n" +
                "                    \"startTimeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"timeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"attributes\": [\n" +
                "                      {\n" +
                "                        \"key\": \"my.counter.attr\",\n" +
                "                        \"value\": {\n" +
                "                          \"stringValue\": \"some value\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                ]\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        assertThrows(IllegalArgumentException.class, () -> udf.eval(json, "metric"));
    }

    @Test
    public void testNullAndEmptyJson() {
	assertDoesNotThrow(() -> assertNull(udf.eval(null, "metrics")));
        assertDoesNotThrow(() -> assertNull(udf.eval("", "metrics")));
    }

    @Test
    public void testNullAndEmptyDataType() {
	String json = "{\n" +
                "  \"resourceMetrics\": [\n" +
                "    {\n" +
                "      \"resource\": {\n" +
                "        \"attributes\": [\n" +
                "          {\n" +
                "            \"key\": \"service.name\",\n" +
                "            \"value\": {\n" +
                "              \"stringValue\": \"my.service\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"scopeMetrics\": [\n" +
                "        {\n" +
                "          \"scope\": {\n" +
                "            \"name\": \"my.library\",\n" +
                "            \"version\": \"1.0.0\",\n" +
                "            \"attributes\": [\n" +
                "              {\n" +
                "                \"key\": \"my.scope.attribute\",\n" +
                "                \"value\": {\n" +
                "                  \"stringValue\": \"some scope attribute\"\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"metrics\": [\n" +
                "            {\n" +
                "              \"name\": \"my.counter\",\n" +
                "              \"unit\": \"1\",\n" +
                "              \"description\": \"I am a Counter\",\n" +
                "              \"sum\": {\n" +
                "                \"aggregationTemporality\": 1,\n" +
                "                \"isMonotonic\": true,\n" +
                "                \"dataPoints\": [\n" +
                "                  {\n" +
                "                    \"asDouble\": 5,\n" +
                "                    \"startTimeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"timeUnixNano\": \"1544712660300000000\",\n" +
                "                    \"attributes\": [\n" +
                "                      {\n" +
                "                        \"key\": \"my.counter.attr\",\n" +
                "                        \"value\": {\n" +
                "                          \"stringValue\": \"some value\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                ]\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

	assertThrows(IllegalArgumentException.class, () -> assertNull(udf.eval(json, null)));
        assertThrows(IllegalArgumentException.class, () -> assertNull(udf.eval(json, "")));
    }
} 
