package org.flink.meta.poc.config;

import java.util.List;

public class SchemaField {

  public SchemaField(){}

  public String name;
  public String sourceField;
  public String sourceAlias;
  public String type;
  public List<String> rules;
  public String defaultValue;

}
