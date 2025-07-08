package org.flink.meta.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.io.File;
import org.flink.meta.poc.config.IngestionConfig;
import org.flink.meta.poc.udfs.CustomConcatUdf;
import org.flink.meta.poc.udfs.JsonFieldExtractorEpochUdf;
import org.flink.meta.poc.udfs.JsonFieldExtractorUdf;
import org.flink.meta.poc.util.ConfigLoader;

public class JobLauncher {

  public static void main(String[] args) throws Exception {
    IngestionConfig config = ConfigLoader.load("user_data.yml");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

    // --- ENHANCEMENT: Register UDFs ---
    // Register the UDF so it can be used in SQL queries.
    tEnv.createTemporarySystemFunction("JsonFieldExtractorUdf", JsonFieldExtractorUdf.class);
    tEnv.createTemporarySystemFunction("JsonFieldExtractorEpochUdf", new JsonFieldExtractorEpochUdf());


    // Load rule templates into the DDLGenerator
    DDLGenerator.loadRuleTemplates(config);

    // Generate and execute DDLs
    for (String ddl : DDLGenerator.generateAllSourceDDLs(config)) {
      System.out.println("Executing DDL:\n" + ddl);
      tEnv.executeSql(ddl);
    }

    String targetDDL = DDLGenerator.generateTargetDDL(config);
    System.out.println("Executing DDL:\n" + targetDDL);
    tEnv.executeSql(targetDDL);

    // Generate and execute the main ETL query
    String insertSql = DDLGenerator.generateETLStatement(config);
    System.out.println("Executing ETL Statement:\n" + insertSql);
    tEnv.executeSql(insertSql);
  }
}