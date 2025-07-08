package org.flink.meta.poc.udfs;

import org.apache.flink.table.functions.ScalarFunction;

public class CustomConcatUdf extends ScalarFunction {
  public String eval(String str, String suffix) {
    if (str == null) return suffix;
    return str + suffix;
  }
}
