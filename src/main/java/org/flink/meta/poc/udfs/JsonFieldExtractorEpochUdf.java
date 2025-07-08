package org.flink.meta.poc.udfs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.time.Instant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

@FunctionHint(
    input = {
        @DataTypeHint("STRING"),  // JSON input
        @DataTypeHint("STRING")   // path input
    },
    output = @DataTypeHint("STRING")
)
public class JsonFieldExtractorEpochUdf extends ScalarFunction {
  private final ObjectMapper mapper = new ObjectMapper();

  // Return Long (epoch millis) for date fields
  public Long eval(String json, String path) {
    if (json == null || path == null) {
      return null;
    }
    try {
      // Extract the field from JSON
      String value = JsonPath.read(json, "$." + path);

      // Parse ISO8601 string to Instant and get epoch millis
      Instant instant = Instant.parse(value);
      return instant.toEpochMilli();
    } catch (Exception e) {
      // If parse fails, return null
      return null;
    }
  }
}
