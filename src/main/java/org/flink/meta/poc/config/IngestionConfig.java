package org.flink.meta.poc.config;

import java.util.List;
import java.util.Map;

public class IngestionConfig {

  public IngestionConfig(){}

  public List<SourceConfig> sources;
  public List<JoinConfig> joins;
  public TargetConfig target;
  public List<SchemaField> schema;
  public Map<String, List<String>> typeRules;
  public Map<String, String> ruleTemplates;
}

