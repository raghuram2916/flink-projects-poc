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
public class JsonFieldExtractorUdf extends ScalarFunction {
  private final ObjectMapper mapper = new ObjectMapper();

  public String eval(String json, String path) {
    if (json == null || path == null) {
      return null;
    }
    try {
      Object result = JsonPath.read(json, "$." + path);
      if (result instanceof net.minidev.json.JSONArray || result instanceof java.util.Map) {
        return mapper.writeValueAsString(result);
      }
      return result != null ? result.toString() : null;
    } catch (PathNotFoundException e) {
      return null;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize extracted JSON object", e);
    }
  }

}
