package org.flink.meta.poc.config;

import java.util.List;

public class TargetConfig {

  public TargetConfig(){}

  public String table;
  public List<String> partitionedBy;
}
