package org.flink.meta.poc.config;

import java.util.List;

public class FieldConfig {

  public FieldConfig(){}

  public String name;
  public String sourceField;
  public String type;
  public String defaultValue;
  public List<String> rules;
  public String sourceAlias;
}
