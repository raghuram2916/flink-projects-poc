package org.flink.meta.poc.util;

/*import org.flink.meta.poc.config.IngestionConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

public class ConfigLoader {
  public static IngestionConfig load(String fileName) {
    InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName);
    if (inputStream == null) {
      throw new IllegalArgumentException("File not found in classpath: " + fileName);
    }
    return new Yaml().loadAs(inputStream, IngestionConfig.class);
  }
}*/

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import org.flink.meta.poc.config.IngestionConfig;

public class ConfigLoader {
  public static IngestionConfig load(String fileName) {
    try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
      if (in == null) {
        throw new IllegalArgumentException("File not found: " + fileName);
      }
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(in, IngestionConfig.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config", e);
    }
  }
}
