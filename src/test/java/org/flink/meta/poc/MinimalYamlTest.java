package org.flink.meta.poc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import org.flink.meta.poc.config.IngestionConfig;
import org.flink.meta.poc.util.ConfigLoader;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;

public class MinimalYamlTest {


  @Test
  void parseYamlManually() {
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-ingestion-config.yaml");
    if (inputStream == null) {
      throw new RuntimeException("YAML file not found in classpath!");
    }

    Yaml yaml = new Yaml();
    //Object raw = yaml.load(inputStream);
    //System.out.println("Parsed object:\n" + raw);
    Map<String, Object> data = yaml.load(inputStream);
    System.out.println("Top-level keys: " + data.keySet());
  }

  @Test
  void testYamlLoading() {
    IngestionConfig config = ConfigLoader.load("test-ingestion-config.yaml");
    assertNotNull(config.typeRules, "typeRules should not be null");
    assertNotNull(config.sources, "sources should not be null");
    System.out.println("Sources: " + config.sources.size());
  }


}
